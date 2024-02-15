package pg

import "strings"

// stringBuilder is used to build the outputs of `String` methods in this package.
type stringBuilder struct {
	strings.Builder
	newLine bool
	indent  int
}

func (s *stringBuilder) Indent() {
	s.indent += 1
}

func (s *stringBuilder) DeIndent() {
	s.indent -= 1
}

func (s *stringBuilder) WriteNewLine() {
	_ = s.Builder.WriteByte('\n')
	s.newLine = true
}

func (s *stringBuilder) WriteString(str string) {
	s.checkNewline()
	_, _ = s.Builder.WriteString(str)
}

func (s *stringBuilder) WriteRune(r rune) {
	s.checkNewline()
	_, _ = s.Builder.WriteRune(r)
}

func (s *stringBuilder) checkNewline() {
	if s.newLine {
		s.newLine = false
		for i := 0; i < s.indent; i += 1 {
			_, _ = s.Builder.WriteString("  ")
		}
	}
}
