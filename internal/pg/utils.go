package pg

import pg_query "github.com/pganalyze/pg_query_go/v5"

func getString(node *pg_query.Node) string {
	return node.GetString_().GetSval()
}
