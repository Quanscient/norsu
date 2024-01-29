package pg

import pg_query "github.com/pganalyze/pg_query_go/v5"

func getString(node *pg_query.Node) string {
	return node.GetString_().GetSval()
}

func resolveLine(sql string, pos int) int {
	line := 1

	for i := 0; i < len(sql); i += 1 {
		if i == pos {
			break
		}

		if sql[i] == '\n' {
			line += 1
		}
	}

	return line
}
