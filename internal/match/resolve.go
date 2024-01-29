package match

import (
	"fmt"
	"strings"

	"github.com/koskimas/norsu/internal/model"
)

func ResolveRef(schema *model.Schema, ref string) (*SchemaPath, error) {
	path := &SchemaPath{
		Path: make([]string, 0),
	}

	if err := resolve(schema, ref, path); err != nil {
		return nil, fmt.Errorf(`failed to resolve reference "%s": %w`, ref, err)
	}

	return path, nil
}

func resolve(schema *model.Schema, ref string, path *SchemaPath) error {
	dot := strings.IndexByte(ref, '.')

	var refPart string
	if dot != -1 {
		refPart = ref[0:dot]
	} else {
		refPart = ref
	}

	normRefPart := Normalize(matchTypeColumn, refPart)

	for n, p := range schema.Properties {
		if Normalize(matchTypeColumn, n) == normRefPart {
			path.Path = append(path.Path, n)

			if dot != -1 {
				return resolve(p, ref[dot+1:], path)
			} else {
				path.Schema = p
			}

			return nil
		}
	}

	if len(path.Path) > 0 {
		return fmt.Errorf(`could not resolve property "%s" of object "%s"`, refPart, path.Path[len(path.Path)-1])
	}

	return fmt.Errorf(`could not resolve property "%s"`, refPart)
}
