package match

import "fmt"

type MatchError struct {
	Message      string
	MismatchPath *SchemaPath
}

func (e *MatchError) Error() string {
	return e.Message
}

func matchErrorf(mismatchPath *SchemaPath, format string, args ...any) *MatchError {
	return &MatchError{
		Message:      fmt.Sprintf(format, args...),
		MismatchPath: mismatchPath,
	}
}
