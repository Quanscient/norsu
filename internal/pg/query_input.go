package pg

import (
	"bufio"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/koskimas/norsu/internal/maps"
	"github.com/koskimas/norsu/internal/ptr"
)

var paramRegex = buildParamRegex()

type QueryInput struct {
	// Model holds the name of the input model with the package name.
	// For example `api.Person`.
	Model  string
	Inputs []QueryInputInfo
}

type QueryInputInfo struct {
	// Ref holds the reference as it was written in the SQL query.
	// For example `someInput` or `someInput.someProp`.
	Ref string

	// PlaceholderIndex holds the 1-based index of the postgres parameter placeholder.
	PlaceholderIndex int

	// Type holds the parsed data type if the input. This can be nil if the input
	// couldn't be determined.
	Type *DataType
}

// parametrizeInputs finds all inputs with format `:someInput` using a regex
// and replaces them with postgres parameter placeholders $1, $2, etc.
//
// This function also detects data types of inputs when postgres casts are used
// (for example :someInput::INT). We could detect these casts in the AST parsing
// phase but in order to find all inputs, we'd need to traverse the whole AST tree
// which is difficult since the `pg_query` library doesn't come with a visitor
// implementation.
func parametrizeInputs(sql string, input *QueryInput) (string, error) {
	s := bufio.NewScanner(strings.NewReader(sql))

	linesOut := make([]string, 0)
	for s.Scan() {
		line := s.Text()

		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			linesOut = append(linesOut, line)
			continue
		}

		matches := paramRegex.FindAllStringSubmatchIndex(line, -1)
		// Iterate over matches in reverse order so that we can replace them
		// with parameter placeholders without having to update the indexes
		// of other matches.
		slices.Reverse(matches)

		for _, m := range matches {
			// First group is the reference.
			ref := line[m[2]:m[3]]

			var cast *string
			var isArray bool
			if m[6] != -1 {
				// Third group is the data type of an optional cast.
				cast = ptr.V(line[m[6]:m[7]])
				// Fourth group is the array brackets if they exist.
				isArray = m[8] != -1
			}

			var in *QueryInputInfo
			for i := range input.Inputs {
				if input.Inputs[i].Ref == ref {
					in = &input.Inputs[i]
					break
				}
			}

			if in == nil {
				input.Inputs = append(input.Inputs, QueryInputInfo{
					Ref:              ref,
					PlaceholderIndex: len(input.Inputs) + 1,
				})

				in = &input.Inputs[len(input.Inputs)-1]
			}

			if cast != nil && in.Type == nil {
				in.Type = &DataType{
					Name:  strings.ToLower(*cast),
					Array: isArray,
				}
			}

			newLine := line[:m[0]+1]
			newLine += fmt.Sprintf("$%d", in.PlaceholderIndex)

			if cast != nil {
				newLine += fmt.Sprintf("::%s", *cast)
				if isArray {
					newLine += "[]"
				}
			}

			newLine += line[m[1]:]
			line = newLine
		}

		linesOut = append(linesOut, line)
	}

	return strings.Join(linesOut, "\n"), nil
}

// buildParamRegex builds the regex used to parse query inputs from the SQL.
//
// :foo               --> ok
// :foo.bar           --> ok
// :foo.bar::INT      --> ok
// :foo.bar.baz::int  --> ok
// :foo::INT[]        --> ok
// ::INT              --> fail
// :foo::UNKNOWN_TYPE --> fail
func buildParamRegex() *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf(`[^:]:([\w\.]+)(::((?i)%s)(\[\])?)?`, strings.Join(maps.Keys(DataTypes), "|")))
}
