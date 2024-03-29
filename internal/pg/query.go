package pg

import (
	"bufio"
	"errors"
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	pg_parser "github.com/pganalyze/pg_query_go/v5/parser"
)

const (
	funcJsonAgg          = "json_agg"
	funcJsonbAgg         = "jsonb_agg"
	funcToJson           = "to_json"
	funcToJsonb          = "to_jsonb"
	funcJsonToRecord     = "json_to_record"
	funcJsonbToRecord    = "jsonb_to_record"
	funcJsonToRecordSet  = "json_to_recordset"
	funcJsonbToRecordSet = "jsonb_to_recordset"
	funcJsonBuildObject  = "json_build_object"
	funcJsonbBuildObject = "jsonb_build_object"

	selectionStar = "*"
)

type Query struct {
	Name string
	SQL  string

	In  *QueryInput
	Out *QueryOutput
}

type QueryOutput struct {
	Model string
	Table *Table
}

type QueryParseContext struct {
	DB           *DB
	JoinedTables []JoinedTable
	SQL          string
	In           *QueryInput
	locations    []int32
}

type JoinedTable struct {
	Table         TableName
	Alias         TableName
	SubQueryDepth int
}

type parseError struct {
	err  error
	line int
}

func (err *parseError) Error() string {
	return fmt.Sprintf("near line %d: %s", err.line, err.err.Error())
}

func (err *parseError) Unwrap() error {
	return err.err
}

func (ctx *QueryParseContext) pushLocation(loc int32) {
	ctx.locations = append(ctx.locations, loc)
}

func (ctx *QueryParseContext) popLocation() {
	ctx.locations = ctx.locations[:len(ctx.locations)-1]
}

func (ctx *QueryParseContext) Errorf(message string, args ...any) error {
	err := fmt.Errorf(message, args...)

	var perr *parseError
	if !errors.As(err, &perr) && len(ctx.locations) > 0 {
		// If the error doesn't already wrap a parseError and some source
		// location is known, wrap the error into a parseError.
		err = &parseError{
			err:  err,
			line: resolveLine(ctx.SQL, int(ctx.locations[len(ctx.locations)-1])),
		}
	}

	return err
}

// ParseQuery parses the query SQL using postgres source code (pg_query package)
// and determines
func ParseQuery(db *DB, sql string) (*Query, error) {
	var q Query
	if err := parseHeader(sql, &q); err != nil {
		return nil, err
	}

	// Parametrize inputs so that postgres is able to parse the query.
	if s, err := parametrizeInputs(sql, q.In); err != nil {
		return nil, err
	} else {
		sql = s
		q.SQL = s
	}

	ctx := &QueryParseContext{
		DB:           db,
		JoinedTables: make([]JoinedTable, 0),
		SQL:          q.SQL,
		In:           q.In,
		locations:    make([]int32, 0),
	}

	ast, err := parseSql(sql)
	if err != nil {
		return nil, handleParseError(sql, err)
	}

	if len(ast.GetStmts()) > 1 {
		return nil, ctx.Errorf("only one SQL query per file is supported")
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
				q.In = &QueryInput{Model: fields[i+1]}
			} else if f == ":out" {
				q.Out = &QueryOutput{Model: fields[i+1]}
			}
		}

		break
	}

	if len(q.Name) == 0 {
		return errors.New("no valid header line was found")
	}

	return nil
}

func handleParseError(sql string, err error) error {
	var parseError *pg_parser.Error
	if errors.As(err, &parseError) {
		// Decorate parse errors with a line number.
		parseError.Message = fmt.Sprintf("line %d: %s", resolveLine(sql, parseError.Cursorpos), parseError.Message)
		return parseError
	}

	return err
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

	return nil, ctx.Errorf(`unhandled statement type "%+T"`, stmt.GetNode())
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
		if err := addTablesFromFromNode(ctx, f); err != nil {
			return nil, err
		}
	}

	return parseSelections(ctx, stmt.GetTargetList())
}

func parseSelections(ctx *QueryParseContext, targets []*pg_query.Node) (*Table, error) {
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
			return nil, ctx.Errorf(`unhandled target "%+T"`, t.GetNode())
		}
	}

	return table, nil
}

func addTableFromCTE(ctx *QueryParseContext, cte *pg_query.CommonTableExpr) error {
	ctx.pushLocation(cte.GetLocation())
	defer ctx.popLocation()

	t, err := parseStmt(ctx, cte.GetCtequery())
	if err != nil {
		return err
	}

	t.Name = NewTableNamePtr(cte.GetCtename())
	ctx.DB.AddTable(t)

	return nil
}

func addTablesFromFromNode(ctx *QueryParseContext, node *pg_query.Node) error {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_RangeVar:
		return addTablesFromRangeVar(ctx, n.RangeVar)
	case *pg_query.Node_JoinExpr:
		return addTablesFromJoinExpr(ctx, n.JoinExpr)
	case *pg_query.Node_RangeSubselect:
		return addTablesFromSubSelect(ctx, n.RangeSubselect)
	case *pg_query.Node_RangeFunction:
		return addTablesFromFunction(ctx, n.RangeFunction)
	}

	return ctx.Errorf(`failed to add tables from expression of type "%+T"`, node.GetNode())
}

func addTablesFromRangeVar(ctx *QueryParseContext, r *pg_query.RangeVar) error {
	ctx.pushLocation(r.GetLocation())
	defer ctx.popLocation()

	name := NewTableName(r.GetRelname(), r.GetSchemaname())

	t := ctx.DB.TablesByName[name]
	if t == nil {
		return ctx.Errorf(`could not find table "%s"`, name.String())
	}

	jt := JoinedTable{Table: name, Alias: name}

	if r.GetAlias() != nil {
		jt.Alias = NewTableName(r.GetAlias().GetAliasname())
	}

	ctx.JoinedTables = prepend(ctx.JoinedTables, jt)
	return nil
}

func addTablesFromJoinExpr(ctx *QueryParseContext, j *pg_query.JoinExpr) error {
	if err := addTablesFromFromNode(ctx, j.GetLarg()); err != nil {
		return err
	}

	return addTablesFromFromNode(ctx, j.GetRarg())
}

func addTablesFromSubSelect(ctx *QueryParseContext, subSelect *pg_query.RangeSubselect) error {
	stmt := subSelect.GetSubquery().GetSelectStmt()

	if len(stmt.GetTargetList()) > 0 {
		return addTablesFromSubSelectWithSelectClause(ctx, subSelect)
	}

	return ctx.Errorf("subqueries must have at least one selection")
}

func addTablesFromSubSelectWithSelectClause(ctx *QueryParseContext, subSelect *pg_query.RangeSubselect) error {
	t, err := parseSelectStmt(ctx, subSelect.GetSubquery().GetSelectStmt())
	if err != nil {
		return err
	}

	if subSelect.GetAlias() == nil {
		return ctx.Errorf("subquery must have an alias")
	}

	t.Name = NewTableNamePtr(subSelect.GetAlias().GetAliasname())
	ctx.DB.AddTableToFront(t)
	ctx.JoinedTables = prepend(ctx.JoinedTables, JoinedTable{Table: *t.Name, Alias: *t.Name})

	return nil
}

func addTablesFromFunction(ctx *QueryParseContext, f *pg_query.RangeFunction) error {
	fc, err := getRangeFunction(ctx, f)
	if err != nil {
		return err
	}

	ctx.pushLocation(fc.GetLocation())
	defer ctx.popLocation()

	name, err := getFunctionName(ctx, fc)
	if err != nil {
		return err
	}

	if name != funcJsonToRecord && name != funcJsonToRecordSet && name != funcJsonbToRecord && name != funcJsonbToRecordSet {
		return ctx.Errorf(`unsupported range function "%s"`, name)
	}

	if f.GetAlias() == nil {
		return ctx.Errorf(`range function "%s" didn't have an alias`, name)
	}

	if len(f.GetColdeflist()) == 0 {
		return ctx.Errorf(`range function "%s" didn't have column defintions`, name)
	}

	t, err := parseColumnDefList(f.GetColdeflist())
	if err != nil {
		return err
	}

	if ctx.In != nil {
		if err := tryParseInputTypeFromJsonToRecordFunction(ctx, fc, t); err != nil {
			return err
		}
	}

	t.Name = NewTableNamePtr(f.GetAlias().GetAliasname())
	ctx.DB.AddTableToFront(t)
	ctx.JoinedTables = prepend(ctx.JoinedTables, JoinedTable{Table: *t.Name, Alias: *t.Name})

	return nil
}

func tryParseInputTypeFromJsonToRecordFunction(ctx *QueryParseContext, fc *pg_query.FuncCall, t *Table) error {
	name, err := getFunctionName(ctx, fc)
	if err != nil {
		return err
	}

	if len(fc.Args) != 1 {
		return ctx.Errorf("%s function should only have one argument", name)
	}

	p := fc.GetArgs()[0].GetParamRef()
	if p == nil {
		// Function argument is not a parameter reference. Nothing to do.
		return nil
	}

	var in *QueryInputInfo
	for i := range ctx.In.Inputs {
		if ctx.In.Inputs[i].PlaceholderIndex == int(p.GetNumber()) {
			in = &ctx.In.Inputs[i]
			break
		}
	}

	if in == nil {
		return ctx.Errorf("failed to find input for parameter %d", p.GetNumber())
	}

	if in.Type == nil {
		in.Type = &DataType{Record: t, NotNull: true}

		if name == funcJsonToRecordSet || name == funcJsonbToRecordSet {
			in.Type.RecordArray = true
		}

		if name == funcJsonbToRecord || name == funcJsonbToRecordSet {
			in.Type.Name = DataTypeJsonb
		} else {
			in.Type.Name = DataTypeJson
		}
	}

	return nil
}

func getRangeFunction(ctx *QueryParseContext, rf *pg_query.RangeFunction) (*pg_query.FuncCall, error) {
	if len(rf.GetFunctions()) != 1 {
		return nil, ctx.Errorf("failed to get range function: wrong number of functions")
	}

	f := rf.GetFunctions()[0]
	if f.GetList() == nil || len(f.GetList().GetItems()) == 0 {
		return nil, ctx.Errorf("failed to get range function: wrong list size")
	}

	i := f.GetList().GetItems()[0]
	if i.GetFuncCall() == nil {
		return nil, ctx.Errorf("failed to get range function: no function call")
	}

	return i.GetFuncCall(), nil
}

func getFunctionName(ctx *QueryParseContext, fc *pg_query.FuncCall) (string, error) {
	if len(fc.GetFuncname()) != 1 {
		return "", ctx.Errorf("failed to get function name: more or less than one name part")
	}

	return strings.ToLower(getString(fc.GetFuncname()[0])), nil
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

func (s *selection) String() string {
	if s.Column != nil {
		return s.Column.String()
	}

	return s.Table.String()
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
	ctx.pushLocation(res.GetLocation())
	defer ctx.popLocation()

	sel, err := parseSelectionNode(ctx, res.GetVal())
	if err != nil {
		return nil, ctx.Errorf("failed to parse selection: %w", err)
	}

	// Handle alias.
	if len(res.GetName()) != 0 && sel.Column != nil {
		sel.Column.Name = res.GetName()
	}

	if sel.Column != nil && len(sel.Column.Name) == 0 {
		return nil, ctx.Errorf("failed to determine name for selection")
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
	case *pg_query.Node_CaseExpr:
		return parseCaseSelection(ctx, n.CaseExpr)
	case *pg_query.Node_AExpr:
		return nil, ctx.Errorf("expression selections need an explicit type cast %s", n.AExpr.String())
	}

	return nil, ctx.Errorf(`unhandled selection "%+T"`, node.GetNode())
}

func parseColumnRefSelection(ctx *QueryParseContext, ref *pg_query.ColumnRef) (*selection, error) {
	parts := make([]string, len(ref.GetFields()))
	for i, f := range ref.GetFields() {
		parts[i] = columnRefPartToString(f)
	}

	switch len(parts) {
	case 1:
		return parseColumnRefSelectionOnePart(ctx, parts[0])
	case 2:
		return parseColumnRefSelectionTwoParts(ctx, parts[0], parts[1])
	case 3:
		return parseColumnRefSelectionThreeParts(ctx, parts[0], parts[1], parts[2])
	}

	return nil, ctx.Errorf(`unexpected number of parts (%d) in a column reference "%s"`, len(parts), strings.Join(parts, "."))
}

func parseColumnRefSelectionOnePart(ctx *QueryParseContext, ref string) (*selection, error) {
	// Star selection.
	if ref == selectionStar {
		allColumns := NewTable()

		for _, jt := range ctx.JoinedTables {
			if jt.SubQueryDepth == 0 {
				table := ctx.DB.TablesByName[jt.Table]

				for _, c := range table.Columns {
					if _, ok := allColumns.ColumnsByName[c.Name]; !ok {
						allColumns.AddColumn(c.Clone())
					}
				}
			}
		}

		return &selection{Table: allColumns}, nil
	}

	// Check for single column selection.
	for _, jt := range ctx.JoinedTables {
		table := ctx.DB.TablesByName[jt.Table]

		if c, ok := table.ColumnsByName[ref]; ok {
			return &selection{Column: c.Clone()}, nil
		}
	}

	// If we got here, check for a table selection.
	for _, jt := range ctx.JoinedTables {
		if jt.Alias.Name == ref {
			table := ctx.DB.TablesByName[jt.Table]

			// If a table is selected using a table name, it results in a
			// record selection. The record's underlying type is the table's
			// type.
			return &selection{
				Column: &Column{
					Name: ref,
					Type: DataType{
						Name:        DataTypeRecord,
						NotNull:     true,
						RecordArray: true,
						Record:      table.Clone(),
					},
				},
			}, nil
		}
	}

	return nil, ctx.Errorf(`failed to resolve column reference "%s"`, ref)
}

func parseColumnRefSelectionTwoParts(ctx *QueryParseContext, ref1 string, ref2 string) (*selection, error) {
	if ref2 == selectionStar {
		for _, jt := range ctx.JoinedTables {
			if jt.Alias.Name == ref1 && jt.SubQueryDepth == 0 {
				table := ctx.DB.TablesByName[jt.Table]
				return &selection{Table: table.Clone()}, nil
			}
		}
	} else {
		for _, jt := range ctx.JoinedTables {
			if jt.Alias.Name == ref1 {
				table := ctx.DB.TablesByName[jt.Table]

				if c, ok := table.ColumnsByName[ref2]; ok {
					return &selection{Column: c.Clone()}, nil
				}
			}
		}
	}

	return nil, ctx.Errorf(`failed to resolve column reference "%s.%s"`, ref1, ref2)
}

func parseColumnRefSelectionThreeParts(ctx *QueryParseContext, ref1 string, ref2 string, ref3 string) (*selection, error) {
	tableRef := NewTableName(ref2, ref1)

	if ref3 == selectionStar {
		for _, jt := range ctx.JoinedTables {
			if jt.Alias == tableRef && jt.SubQueryDepth == 0 {
				table := ctx.DB.TablesByName[jt.Table]
				return &selection{Table: table.Clone()}, nil
			}
		}
	} else {
		for _, jt := range ctx.JoinedTables {
			if jt.Alias == tableRef {
				table := ctx.DB.TablesByName[jt.Table]

				if c, ok := table.ColumnsByName[ref3]; ok {
					return &selection{Column: c.Clone()}, nil
				}
			}
		}
	}

	return nil, ctx.Errorf(`failed to resolve column reference "%s.%s.%s"`, ref1, ref2, ref3)
}

func columnRefPartToString(f *pg_query.Node) string {
	if f.GetAStar() != nil {
		return selectionStar
	}

	return getString(f)
}

func parseSubQuerySelection(ctx *QueryParseContext, subLink *pg_query.SubLink) (*selection, error) {
	subTable, err := parseSelectStmt(ctx, subLink.GetSubselect().GetSelectStmt())
	if err != nil {
		return nil, err
	}

	if len(subTable.Columns) != 1 {
		return nil, ctx.Errorf("subqueries must only select one column")
	}

	return &selection{
		Column: subTable.Columns[0],
	}, nil
}

func parseTypeCastSelection(ctx *QueryParseContext, cast *pg_query.TypeCast) (*selection, error) {
	var sel *selection
	if s, err := parseSelectionNode(ctx, cast.GetArg()); err != nil {
		// Could not parse the nested selection. Just create an empty selection
		sel = &selection{Column: &Column{Name: "unknown"}}
	} else {
		sel = s
	}

	dataType, err := parseTypeName(cast.GetTypeName())
	if err != nil {
		return nil, err
	}

	if sel.Column != nil {
		sel.Column.Type.Name = dataType.Name
	} else {
		return nil, ctx.Errorf("can't cast a star selection")
	}

	return sel, nil
}

func parseFuncCallSelection(ctx *QueryParseContext, call *pg_query.FuncCall) (*selection, error) {
	funcName := getString(call.GetFuncname()[0])

	if funcName == funcJsonAgg || funcName == funcJsonbAgg || funcName == funcToJson || funcName == funcToJsonb {
		if sel, err := parseJsonSelection(ctx, call); err != nil {
			return nil, ctx.Errorf("failed to parse a %s: %w", funcName, err)
		} else {
			return sel, nil
		}
	} else if funcName == funcJsonBuildObject || funcName == funcJsonbBuildObject {
		if sel, err := parseJsonBuildObjectSelection(ctx, call); err != nil {
			return nil, ctx.Errorf("failed to parse a %s: %w", funcName, err)
		} else {
			return sel, nil
		}
	}

	return nil, ctx.Errorf(`failed to parse function "%s" (hint: add an explicit type cast for the selected expression)`, funcName)
}

func parseJsonSelection(ctx *QueryParseContext, call *pg_query.FuncCall) (*selection, error) {
	funcName := getString(call.GetFuncname()[0])

	dataType := DataTypeJson
	if funcName == funcJsonbAgg || funcName == funcToJsonb {
		dataType = DataTypeJsonb
	}

	args := call.GetArgs()
	if len(args) != 1 {
		return nil, ctx.Errorf("expected one argument, got %d", len(args))
	}

	sel, err := parseSelectionNode(ctx, call.GetArgs()[0])
	if err != nil {
		return nil, err
	}

	var t *Table
	if sel.Table != nil {
		t = sel.Table
	} else if sel.Column != nil && sel.Column.Type.Record != nil {
		t = sel.Column.Type.Record
	}

	if t != nil {
		var isArray bool
		if funcName == funcJsonAgg || funcName == funcJsonbAgg {
			isArray = true
		}

		return &selection{
			Column: &Column{
				Name: funcName,
				Type: DataType{
					Name:        dataType,
					RecordArray: isArray,
					Record:      t.Clone(),
				},
			},
		}, nil
	}

	// Return an untyped json column by default.
	return &selection{
		Column: &Column{
			Name: funcName,
			Type: DataType{
				Name: dataType,
			},
		},
	}, nil
}

func parseJsonBuildObjectSelection(ctx *QueryParseContext, call *pg_query.FuncCall) (*selection, error) {
	funcName := getString(call.GetFuncname()[0])

	dataType := DataTypeJson
	if funcName == funcJsonbBuildObject {
		dataType = DataTypeJsonb
	}

	t := NewTable()
	c := &Column{
		Name: funcName,
		Type: DataType{
			Name:    dataType,
			NotNull: true,
			Record:  t,
		},
	}

	args := call.GetArgs()
	if len(args)%2 != 0 {
		return nil, ctx.Errorf("expected an even number of arguments, got %d", len(args))
	}

	for i := 0; i < len(call.GetArgs()); i += 2 {
		keyArg := args[i]
		valueArg := args[i+1]

		if key := keyArg.GetAConst(); key != nil {
			value, err := parseSelectionNode(ctx, valueArg)
			if err != nil {
				return nil, err
			}

			if value.Column != nil || len(value.Table.Columns) == 1 {
				value.ForEachColumn(func(c *Column) {
					c.Name = key.GetSval().GetSval()
					t.AddColumn(c)
				})
			} else {
				return nil, ctx.Errorf("records are not supported as property values")
			}
		} else {
			return nil, ctx.Errorf(`only constant keys are supported, got "%+T"`, keyArg.GetNode())
		}
	}

	return &selection{
		Column: c,
	}, nil
}

func parseCoalesceSelection(ctx *QueryParseContext, expr *pg_query.CoalesceExpr) (*selection, error) {
	if len(expr.GetArgs()) != 2 || expr.GetArgs()[1].GetAConst() == nil || expr.GetArgs()[1].GetAConst().GetIsnull() {
		return nil, ctx.Errorf("only coalesce expressions with two args (expression and a non-null constant) are supported in selections")
	}

	sel, err := parseSelectionNode(ctx, expr.GetArgs()[0])
	if err != nil {
		return nil, err
	}

	if sel.Column != nil {
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

func parseCaseSelection(ctx *QueryParseContext, expr *pg_query.CaseExpr) (*selection, error) {
	cases := make([]*selection, 0)

	for _, a := range expr.GetArgs() {
		if sel, err := parseSelectionNode(ctx, a.GetCaseWhen().GetResult()); err != nil {
			return nil, err
		} else {
			cases = append(cases, sel)
		}
	}

	if sel, err := parseSelectionNode(ctx, expr.GetDefresult()); err != nil {
		return nil, err
	} else {
		cases = append(cases, sel)
	}

	var dataType *DataType
	for _, sel := range cases {
		if sel.Column == nil {
			return nil, ctx.Errorf("only single column selections are supported in a case expression")
		}
		if dataType == nil {
			dataType = &sel.Column.Type
		} else if dataType.Name != sel.Column.Type.Name {
			return nil, ctx.Errorf("all cases in a case expression must have the same type")
		}
	}

	sel := cases[0]
	if sel.Column == nil {
		return nil, ctx.Errorf("only single column selections are supported in a case expression")
	}

	sel.Column.Name = "case"
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
			if err := addTablesFromFromNode(ctx, f); err != nil {
				return nil, err
			}
		}
	}

	return parseSelections(ctx, stmt.GetReturningList())
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
		if err := addTablesFromFromNode(ctx, f); err != nil {
			return nil, err
		}
	}

	return parseSelections(ctx, stmt.GetReturningList())
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
		if err := addTablesFromFromNode(ctx, f); err != nil {
			return nil, err
		}
	}

	return parseSelections(ctx, stmt.GetReturningList())
}

// CloneForSubquery creates a deep clone of the parse context to be used
// in a subquery. The database and joined tables list are copied so that
// the subquery can add its own tables from `FROM` and joins without them
// being visible to the parent queries.
//
// `SubQueryDepth` of each joined table is also increased by one to keep
// track how far up the table was joined.
func (ctx *QueryParseContext) CloneForSubquery() *QueryParseContext {
	clone := &QueryParseContext{
		DB:           ctx.DB.Clone(),
		SQL:          ctx.SQL,
		In:           ctx.In,
		JoinedTables: make([]JoinedTable, 0, len(ctx.JoinedTables)),
		locations:    make([]int32, len(ctx.locations)),
	}

	copy(clone.locations, ctx.locations)

	for _, jt := range ctx.JoinedTables {
		clone.JoinedTables = append(clone.JoinedTables, JoinedTable{
			Table:         jt.Table,
			Alias:         jt.Alias,
			SubQueryDepth: jt.SubQueryDepth + 1,
		})
	}

	return clone
}

func prepend[T any](s []T, i T) []T {
	return append([]T{i}, s...)
}
