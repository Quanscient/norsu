package pg

import (
	"bufio"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/koskimas/norsu/internal/ptr"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

const (
	dataTypeJson  = "json"
	dataTypeJsonb = "jsonb"

	funcJsonAgg  = "json_agg"
	funcJsonbAgg = "jsonb_agg"
)

type Query struct {
	Name string
	SQL  string
	In   *QueryInput
	Out  *QueryOutput
}

type QueryInput struct {
	Model  string
	Inputs []QueryInputValue
}

type QueryInputValue struct {
	Ref              string
	PlaceholderIndex int
	Type             *DataType
}

type QueryOutput struct {
	Model string
	Table *Table
}

type QueryParseContext struct {
	DB           *DB
	JoinedTables []JoinedTable
}

type JoinedTable struct {
	Table         TableName
	Alias         TableName
	SubQueryDepth int
}

func ParseQuery(db *DB, sql string) (*Query, error) {
	var q Query

	if err := parseHeader(sql, &q); err != nil {
		return nil, err
	}

	if s, err := parseInputs(sql, &q); err != nil {
		return nil, err
	} else {
		sql = s
	}

	q.SQL = sql

	ctx := &QueryParseContext{
		DB:           db,
		JoinedTables: make([]JoinedTable, 0),
	}

	ast, err := parseSql(sql)
	if err != nil {
		return nil, err
	}

	if len(ast.GetStmts()) > 1 {
		return nil, errors.New("only one SQL query per file is supported")
	}

	o, err := parseStmt(ctx, ast.GetStmts()[0].GetStmt())
	if err != nil {
		return nil, err
	}

	if q.Out != nil {
		q.Out.Table = o
	}

	return &q, nil
}

func parseHeader(sql string, q *Query) error {
	s := bufio.NewScanner(strings.NewReader(sql))

	for s.Scan() {
		line := strings.TrimSpace(s.Text())

		if !strings.HasPrefix(line, "-- :name") {
			continue
		}

		fields := strings.Fields(line)
		for i, f := range fields[:len(fields)-1] {
			if f == ":name" {
				q.Name = fields[i+1]
			} else if f == ":in" {
				q.In = &QueryInput{
					Model: fields[i+1],
				}
			} else if f == ":out" {
				q.Out = &QueryOutput{
					Model: fields[i+1],
				}
			}
		}

		break
	}

	return nil
}

func parseInputs(sql string, q *Query) (string, error) {
	s := bufio.NewScanner(strings.NewReader(sql))
	re := regexp.MustCompile(`[^:](:[\w\.]+)`)

	linesOut := make([]string, 0)
	for s.Scan() {
		line := (s.Text())

		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			linesOut = append(linesOut, line)
			continue
		}

		line = re.ReplaceAllStringFunc(line, func(s string) string {
			prefix := s[0:1]
			ref := s[2:]

			for _, in := range q.In.Inputs {
				if in.Ref == ref {
					return fmt.Sprintf("%s$%d", prefix, in.PlaceholderIndex)
				}
			}

			in := QueryInputValue{
				Ref:              ref,
				PlaceholderIndex: len(q.In.Inputs) + 1,
			}

			q.In.Inputs = append(q.In.Inputs, in)
			return fmt.Sprintf("%s$%d", prefix, in.PlaceholderIndex)
		})

		linesOut = append(linesOut, line)
	}

	return strings.Join(linesOut, "\n"), nil
}

func parseStmt(ctx *QueryParseContext, stmt *pg_query.Node) (*Table, error) {
	switch n := stmt.GetNode().(type) {
	case *pg_query.Node_SelectStmt:
		return parseSelectStmt(ctx, n.SelectStmt)
	case *pg_query.Node_InsertStmt:
		return parseInsertStmt(ctx, n.InsertStmt)
	case *pg_query.Node_UpdateStmt:
		return parseUpdateStmt(ctx, n.UpdateStmt)
	case *pg_query.Node_DeleteStmt:
		return parseDeleteStmt(ctx, n.DeleteStmt)
	}

	return nil, fmt.Errorf(`unhandled statement type "%+T"`, stmt.GetNode())
}

func parseSelectStmt(ctx *QueryParseContext, stmt *pg_query.SelectStmt) (*Table, error) {
	ctx = ctx.CloneForSubquery()

	// Add CTEs as tables to ctx.DB.
	for _, cte := range stmt.GetWithClause().GetCtes() {
		if err := addTableFromCTE(ctx, cte.GetCommonTableExpr()); err != nil {
			return nil, err
		}
	}

	// Add from statements and joins as tables to ctx.DB and to ctx.JoinedTables.
	for _, f := range stmt.GetFromClause() {
		if err := addTablesFromNode(ctx, f); err != nil {
			return nil, err
		}
	}

	return parseTargetList(ctx, stmt.GetTargetList())
}

func parseTargetList(ctx *QueryParseContext, targets []*pg_query.Node) (*Table, error) {
	table := NewTable()

	for _, t := range targets {
		switch n := t.GetNode().(type) {
		case *pg_query.Node_ResTarget:
			if sel, err := parseSelection(ctx, n.ResTarget); err != nil {
				return nil, err
			} else {
				sel.ForEachColumn(table.AddColumn)
			}
		default:
			return nil, fmt.Errorf(`unhandled target "%+T"`, t.GetNode())
		}
	}

	return table, nil
}

func addTableFromCTE(ctx *QueryParseContext, cte *pg_query.CommonTableExpr) error {
	t, err := parseStmt(ctx, cte.GetCtequery())
	if err != nil {
		return err
	}

	t.Name = NewTableNamePtr(cte.GetCtename())
	ctx.DB.AddTable(t)

	return nil
}

func addTablesFromNode(ctx *QueryParseContext, node *pg_query.Node) error {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_RangeVar:
		if err := addTablesFromRangeVar(ctx, n.RangeVar); err != nil {
			return err
		}
	case *pg_query.Node_JoinExpr:
		if err := addTablesFromJoinExpr(ctx, n.JoinExpr); err != nil {
			return err
		}
	case *pg_query.Node_RangeSubselect:
		if err := addTablesFromSubSelect(ctx, n.RangeSubselect); err != nil {
			return err
		}
	case *pg_query.Node_RangeFunction:
		if err := addTablesFromFunction(ctx, n.RangeFunction); err != nil {
			return err
		}
	default:
		return fmt.Errorf(`failed to add tables from expression of type "%+T"`, node.GetNode())
	}

	return nil
}

func addTablesFromRangeVar(ctx *QueryParseContext, r *pg_query.RangeVar) error {
	name := NewTableName(r.GetRelname(), r.GetSchemaname())

	t := ctx.DB.TablesByName[name]
	if t == nil {
		return fmt.Errorf(`could not find table "%s"`, name.String())
	}

	jt := JoinedTable{Table: name, Alias: name}

	if r.GetAlias() != nil {
		jt.Alias = NewTableName(r.GetAlias().GetAliasname())
	}

	ctx.JoinedTables = append(ctx.JoinedTables, jt)
	return nil
}

func addTablesFromJoinExpr(ctx *QueryParseContext, j *pg_query.JoinExpr) error {
	if err := addTablesFromNode(ctx, j.GetLarg()); err != nil {
		return err
	}

	return addTablesFromNode(ctx, j.GetRarg())
}

func addTablesFromSubSelect(ctx *QueryParseContext, subSelect *pg_query.RangeSubselect) error {
	stmt := subSelect.GetSubquery().GetSelectStmt()

	if len(stmt.GetTargetList()) > 0 {
		return addTablesFromSubSelectWithSelectClause(ctx, subSelect)
	}

	return errors.New("failed to add table for sub select")
}

func addTablesFromSubSelectWithSelectClause(ctx *QueryParseContext, subSelect *pg_query.RangeSubselect) error {
	t, err := parseSelectStmt(ctx, subSelect.GetSubquery().GetSelectStmt())
	if err != nil {
		return err
	}

	if subSelect.GetAlias() == nil {
		return errors.New("subquery must have an alias")
	}

	t.Name = NewTableNamePtr(subSelect.GetAlias().GetAliasname())
	ctx.DB.AddTableToFront(t)
	ctx.JoinedTables = append(ctx.JoinedTables, JoinedTable{Table: *t.Name, Alias: *t.Name})

	return nil
}

func addTablesFromFunction(ctx *QueryParseContext, f *pg_query.RangeFunction) error {
	name, err := getRangeFunctionName(f)
	if err != nil {
		return err
	}

	name = strings.ToLower(name)
	if name != "json_to_record" && name != "json_to_recordset" && name != "jsonb_to_record" && name != "jsonb_to_recordset" {
		return errors.New("unsupported range function")
	}

	if f.GetAlias() == nil {
		return errors.New("range function didn't have an alias ")
	}

	if len(f.GetColdeflist()) == 0 {
		return errors.New("range function didn't have column defintions")
	}

	t, err := parseColumnDefList(f.GetColdeflist())
	if err != nil {
		return err
	}

	t.Name = NewTableNamePtr(f.GetAlias().GetAliasname())
	ctx.DB.AddTableToFront(t)
	ctx.JoinedTables = append(ctx.JoinedTables, JoinedTable{Table: *t.Name, Alias: *t.Name})

	return nil
}

func getRangeFunctionName(rf *pg_query.RangeFunction) (string, error) {
	if len(rf.GetFunctions()) != 1 {
		return "", errors.New("failed to get range function name: wrong number of functions")
	}

	f := rf.GetFunctions()[0]
	if f.GetList() == nil || len(f.GetList().GetItems()) == 0 {
		return "", errors.New("failed to get range function name: wrong list size")
	}

	i := f.GetList().GetItems()[0]
	if i.GetFuncCall() == nil {
		return "", errors.New("failed to get range function name: no function call")
	}

	fc := i.GetFuncCall()
	if len(fc.GetFuncname()) != 1 {
		return "", errors.New("failed to get range function name: more or less than one name part")
	}

	return getString(fc.GetFuncname()[0]), nil
}

func parseColumnDefList(list []*pg_query.Node) (*Table, error) {
	t := NewTable()

	for _, cd := range list {
		switch n := cd.GetNode().(type) {
		case *pg_query.Node_ColumnDef:
			if c, err := parseColumnDef(n.ColumnDef); err != nil {
				return nil, err
			} else {
				t.AddColumn(c)
			}
		}
	}

	return t, nil
}

type selection struct {
	Column *Column
	Table  *Table
}

func (s *selection) ForEachColumn(f func(*Column)) {
	if s.Column != nil {
		f(s.Column)
	} else {
		for _, c := range s.Table.Columns {
			f(c)
		}
	}
}

func parseSelection(ctx *QueryParseContext, res *pg_query.ResTarget) (*selection, error) {
	sel, err := parseSelectionNode(ctx, res.GetVal())
	if err != nil {
		return nil, fmt.Errorf("failed to parse selection: %w", err)
	}

	// Handle alias.
	if len(res.GetName()) != 0 && sel.Column != nil {
		sel.Column.Name = res.GetName()
	}

	if sel.Column != nil && !sel.Column.HasName() {
		return nil, errors.New("failed to determine name for selection")
	}

	return sel, nil
}

func parseSelectionNode(ctx *QueryParseContext, node *pg_query.Node) (*selection, error) {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_ColumnRef:
		return parseColumnRefSelection(ctx, n.ColumnRef)
	case *pg_query.Node_SubLink:
		return parseSubQuerySelection(ctx, n.SubLink)
	case *pg_query.Node_TypeCast:
		return parseTypeCastSelection(ctx, n.TypeCast)
	case *pg_query.Node_FuncCall:
		return parseFuncCallSelection(ctx, n.FuncCall)
	case *pg_query.Node_CoalesceExpr:
		return parseCoalesceSelection(ctx, n.CoalesceExpr)
	case *pg_query.Node_AConst:
		return parseConstantSelection(ctx, n.AConst)
	case *pg_query.Node_AExpr:
		return nil, fmt.Errorf("expression selections need an explicit type cast")
	}

	return nil, fmt.Errorf(`unhandled selection "%+T"`, node.GetNode())
}

func parseColumnRefSelection(ctx *QueryParseContext, ref *pg_query.ColumnRef) (*selection, error) {
	var colName string
	var tableName *TableName

	f := ref.GetFields()
	if len(f) == 1 {
		colName = getColumnNameFromField(f[0])
	} else if len(f) == 2 {
		colName = getColumnNameFromField(f[1])
		tableName = ptr.V(NewTableName(getString(f[0])))
	} else if len(f) == 3 {
		colName = getColumnNameFromField(f[2])
		tableName = ptr.V(NewTableName(getString(f[1]), getString(f[0])))
	} else {
		return nil, fmt.Errorf("unexpected number of parts (%d) in a column reference", len(f))
	}

	selTable := NewTable()
	for _, jt := range ctx.JoinedTables {
		table := ctx.DB.TablesByName[jt.Table]

		if tableName != nil {
			if tableName.HasSchema() {
				// If the table reference has a schema, it must match.
				if *tableName != jt.Alias {
					continue
				}
			} else {
				// If the table reference doesn't have a schema, only
				// the name needs to match.
				if tableName.Name != jt.Alias.Name {
					continue
				}
			}
		}

		if colName == "*" && jt.SubQueryDepth == 0 {
			for _, tc := range table.Columns {
				if _, ok := selTable.ColumnsByName[tc.Name]; !ok {
					selTable.AddColumn(tc.Clone())
				}
			}
		} else if tc := table.ColumnsByName[colName]; tc != nil {
			selTable.AddColumn(tc.Clone())
			break
		}
	}

	if len(selTable.Columns) == 0 {
		if tableName != nil {
			return nil, fmt.Errorf(`unknown column "%s.%s"`, tableName.String(), colName)
		} else {
			return nil, fmt.Errorf(`unknown column "%s"`, colName)
		}
	}

	if colName == "*" {
		return &selection{
			Table: selTable,
		}, nil
	}

	return &selection{
		Column: selTable.Columns[0],
	}, nil
}

func getColumnNameFromField(f *pg_query.Node) string {
	if f.GetAStar() != nil {
		return "*"
	}

	return getString(f)
}

func parseSubQuerySelection(ctx *QueryParseContext, subLink *pg_query.SubLink) (*selection, error) {
	subTable, err := parseSelectStmt(ctx, subLink.GetSubselect().GetSelectStmt())
	if err != nil {
		return nil, err
	}

	if len(subTable.Columns) != 1 {
		return nil, errors.New("subqueries must only select one column")
	}

	return &selection{
		Column: subTable.Columns[0],
	}, nil
}

func parseTypeCastSelection(ctx *QueryParseContext, cast *pg_query.TypeCast) (*selection, error) {
	sel, err := parseSelectionNode(ctx, cast.GetArg())
	if err != nil {
		return nil, err
	}

	dataType, err := parseTypeName(cast.GetTypeName())
	if err != nil {
		return nil, err
	}

	if sel.Column != nil {
		sel.Column.Type.Name = dataType.Name
	} else {
		return nil, errors.New("can't cast a star selection")
	}

	return sel, nil
}

func parseFuncCallSelection(ctx *QueryParseContext, call *pg_query.FuncCall) (*selection, error) {
	funcName := getString(call.GetFuncname()[0])

	if funcName == funcJsonAgg || funcName == funcJsonbAgg {
		if sel, err := parseJsonAggSelection(ctx, call); err != nil {
			return nil, fmt.Errorf("failed to parse a %s selection: %w", funcName, err)
		} else {
			return sel, nil
		}
	}

	return nil, fmt.Errorf(`failed to parse function "%s" call selection`, funcName)
}

func parseJsonAggSelection(ctx *QueryParseContext, call *pg_query.FuncCall) (*selection, error) {
	funcName := getString(call.GetFuncname()[0])

	dataType := dataTypeJson
	if funcName == funcJsonbAgg {
		dataType = dataTypeJsonb
	}

	args := call.GetArgs()
	if len(args) != 1 {
		return nil, fmt.Errorf("expected one argument, got %d", len(args))
	}

	var t *Table
	switch n := call.GetArgs()[0].GetNode().(type) {
	case *pg_query.Node_ColumnRef:
		f := n.ColumnRef.GetFields()
		if len(f) != 1 {
			return nil, errors.New("argument name should have a single part")
		}
		tn := getString(f[0])
		t = ctx.DB.TablesByName[NewTableName(tn)]
		if t == nil {
			return nil, fmt.Errorf(`failed to resolve table "%s"`, tn)
		}
	default:
		return nil, fmt.Errorf(`unhandled %s selection type "%+T"`, funcName, call.GetArgs()[0].GetNode())
	}

	return &selection{
		Column: &Column{
			Name: funcName,
			Type: DataType{
				Name:     dataType,
				IsArray:  true,
				JsonType: t.Clone(),
			},
		},
	}, nil
}

func parseCoalesceSelection(ctx *QueryParseContext, expr *pg_query.CoalesceExpr) (*selection, error) {
	if len(expr.GetArgs()) != 2 || expr.GetArgs()[1].GetAConst() == nil || expr.GetArgs()[1].GetAConst().GetIsnull() {
		return nil, errors.New("only coalesce expressions with two args (expression and a non-null constant) are supported in selections")
	}

	sel, err := parseSelectionNode(ctx, expr.GetArgs()[0])
	if err != nil {
		return nil, err
	}

	if sel.Column != nil {
		// This is not always true. Coalesce might end up returning null if
		// all expressions are null, but let's simplify things for now.
		sel.Column.Type.NotNull = true
	}

	return sel, nil
}

func parseConstantSelection(ctx *QueryParseContext, expr *pg_query.A_Const) (*selection, error) {
	sel := &selection{
		Column: &Column{
			Type: DataType{
				NotNull: !expr.GetIsnull(),
			},
		},
	}

	switch expr.GetVal().(type) {
	case *pg_query.A_Const_Sval:
		sel.Column.Type.Name = "text"
	case *pg_query.A_Const_Boolval:
		sel.Column.Type.Name = "bool"
	case *pg_query.A_Const_Ival:
		sel.Column.Type.Name = "int8"
	case *pg_query.A_Const_Fval:
		sel.Column.Type.Name = "float8"
	default:
		sel.Column.Type.Name = "text"
	}

	return sel, nil
}

func parseInsertStmt(ctx *QueryParseContext, stmt *pg_query.InsertStmt) (*Table, error) {
	ctx = ctx.CloneForSubquery()

	// Add CTEs as tables to ctx.DB.
	for _, cte := range stmt.GetWithClause().GetCtes() {
		if err := addTableFromCTE(ctx, cte.GetCommonTableExpr()); err != nil {
			return nil, err
		}
	}

	// Add the insertion target as a table to ctx.DB and ctx.JoinedTables.
	if err := addTablesFromRangeVar(ctx, stmt.GetRelation()); err != nil {
		return nil, err
	}

	if stmt.GetSelectStmt() != nil && stmt.GetSelectStmt().GetSelectStmt() != nil {
		for _, f := range stmt.GetSelectStmt().GetSelectStmt().GetFromClause() {
			if err := addTablesFromNode(ctx, f); err != nil {
				return nil, err
			}
		}
	}

	return parseTargetList(ctx, stmt.GetReturningList())
}

func parseUpdateStmt(ctx *QueryParseContext, stmt *pg_query.UpdateStmt) (*Table, error) {
	ctx = ctx.CloneForSubquery()

	// Add CTEs as tables to ctx.DB.
	for _, cte := range stmt.GetWithClause().GetCtes() {
		if err := addTableFromCTE(ctx, cte.GetCommonTableExpr()); err != nil {
			return nil, err
		}
	}

	// Add the update target as a table to ctx.DB and ctx.JoinedTables.
	if err := addTablesFromRangeVar(ctx, stmt.GetRelation()); err != nil {
		return nil, err
	}

	// Add from statements and joins as tables to ctx.DB and to ctx.JoinedTables.
	for _, f := range stmt.GetFromClause() {
		if err := addTablesFromNode(ctx, f); err != nil {
			return nil, err
		}
	}

	return parseTargetList(ctx, stmt.GetReturningList())
}

func parseDeleteStmt(ctx *QueryParseContext, stmt *pg_query.DeleteStmt) (*Table, error) {
	ctx = ctx.CloneForSubquery()

	// Add CTEs as tables to ctx.DB.
	for _, cte := range stmt.GetWithClause().GetCtes() {
		if err := addTableFromCTE(ctx, cte.GetCommonTableExpr()); err != nil {
			return nil, err
		}
	}

	// Add the deletion target as a table to ctx.DB and ctx.JoinedTables.
	if err := addTablesFromRangeVar(ctx, stmt.GetRelation()); err != nil {
		return nil, err
	}

	// Add using statements and joins as tables to ctx.DB and to ctx.JoinedTables.
	for _, f := range stmt.GetUsingClause() {
		if err := addTablesFromNode(ctx, f); err != nil {
			return nil, err
		}
	}

	return parseTargetList(ctx, stmt.GetReturningList())
}

func (ctx *QueryParseContext) CloneForSubquery() *QueryParseContext {
	clone := &QueryParseContext{
		DB:           ctx.DB.Clone(),
		JoinedTables: make([]JoinedTable, 0, len(ctx.JoinedTables)),
	}

	for _, jt := range ctx.JoinedTables {
		clone.JoinedTables = append(clone.JoinedTables, JoinedTable{
			Table:         jt.Table,
			Alias:         jt.Alias,
			SubQueryDepth: jt.SubQueryDepth + 1,
		})
	}

	return clone
}
