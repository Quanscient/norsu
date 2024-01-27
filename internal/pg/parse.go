package pg

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func parseSql(m string) (*pg_query.ParseResult, error) {
	ast, err := pg_query.Parse(m)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse AST: %w`, err)
	}

	return ast, nil
}
