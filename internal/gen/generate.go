package gen

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/koskimas/norsu/internal/config"
	"github.com/koskimas/norsu/internal/match"
	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/pg"
)

const (
	idParamInput   = "in"
	idParamDb      = "db"
	idParamCtx     = "ctx"
	idParamQueries = "q"
	idParamQuery   = "query"
	idParamArgs    = "args"

	idVarRows         = "rows"
	idVarRow          = "row"
	idVarErr          = "err"
	idVarOutput       = "out"
	idVarInputSuffix  = "In"
	idVarOutputSuffix = "Out"

	idPropDb           = "db"
	idInterfaceDb      = "DB"
	idInterfaceQueries = "Queries"
	idStructQueries    = "QueriesImpl"
	idFuncNewQueries   = "NewQueries"
)

var (
	queryLocalVars = []string{
		idVarErr,
		idVarRows,
		idVarRow,
		idParamCtx,
		idParamInput,
		idVarOutput,
		idParamQueries,
	}
)

func GenerateCode(
	cfg config.Config,
	workingDir string,
	models map[string]model.Model,
	queries []pg.Query,
) error {
	f := jen.NewFile(path.Base(cfg.Package.Path))

	genQueryInterface(f, models, queries)
	genDbInterface(f)
	genQueriesStruct(f)
	genNewQueriesFunc(f)

	for _, q := range queries {
		im := models[q.In.Model]
		om := models[q.Out.Model]

		genQuery(f, q, im, om)
	}

	return writeQueriesToFile(f, cfg, workingDir)
}

func genQueryInterface(f *jen.File, models map[string]model.Model, queries []pg.Query) {
	f.Type().Id(idInterfaceQueries).InterfaceFunc(func(g *jen.Group) {
		for _, q := range queries {
			im := models[q.In.Model]
			om := models[q.Out.Model]

			g.Id(q.Name).Params(
				jen.Id(idParamCtx).Qual("context", "Context"),
				jen.Id(idParamInput).Qual(im.Package, im.Name),
			).Params(
				jen.Index().Qual(om.Package, om.Name),
				jen.Error(),
			)
		}
	})
	f.Empty()
}

func genDbInterface(f *jen.File) {
	f.Type().Id(idInterfaceDb).Interface(
		jen.Id("QueryContext").Params(
			jen.Id(idParamCtx).Qual("context", "Context"),
			jen.Id(idParamQuery).String(),
			jen.Id(idParamArgs).Op("...").Id("any"),
		).Params(
			jen.Op("*").Qual("database/sql", "Rows"),
			jen.Error(),
		),
		jen.Id("ExecContext").Params(
			jen.Id(idParamCtx).Qual("context", "Context"),
			jen.Id(idParamQuery).String(),
			jen.Id(idParamArgs).Op("...").Id("any"),
		).Params(
			jen.Qual("database/sql", "Result"),
			jen.Error(),
		),
	)
	f.Empty()
}

func genQueriesStruct(f *jen.File) {
	f.Type().Id(idStructQueries).Struct(
		jen.Id(idPropDb).Id(idInterfaceDb),
	)
	f.Empty()
}

func genNewQueriesFunc(f *jen.File) {
	f.Func().Id(idFuncNewQueries).Params(
		jen.Id(idParamDb).Id(idInterfaceDb),
	).Id(idInterfaceQueries).Block(
		jen.Return(
			jen.Op("&").Id(idStructQueries).Values(jen.Dict{
				jen.Id(idPropDb): jen.Id(idParamDb),
			}),
		),
	).Empty()
}

func genQuery(f *jen.File, q pg.Query, im model.Model, om model.Model) {
	genQuerySqlConstant(f, q)

	f.Func().Params(
		jen.Id(idParamQueries).Op("*").Id(idStructQueries),
	).Id(q.Name).Params(
		jen.Id(idParamCtx).Qual("context", "Context"),
		jen.Id(idParamInput).Qual(im.Package, im.Name),
	).Params(
		jen.Index().Qual(om.Package, om.Name),
		jen.Error(),
	).BlockFunc(func(g *jen.Group) {
		genQueryBody(g, q, im, om)
	})
}

func genQuerySqlConstant(f *jen.File, q pg.Query) {
	f.Const().Id(getSqlConstName(q)).Op("=").Id("`\n" + q.SQL + "`").Empty()
}

func genQueryBody(g *jen.Group, q pg.Query, im model.Model, om model.Model) {
	genQueryInputVars(g, q, im)
	genQueryExecute(g, q, im)
	genReadRows(g, q, om)
	g.Return(jen.Id(idVarOutput), jen.Nil())
}

func genQueryInputVars(g *jen.Group, q pg.Query, im model.Model) {
	for _, in := range q.In.Inputs {
		r, _ := match.ResolveRef(im.Schema, in.Ref)

		if isObjectOrArray(r.Schema) {
			// Marshal all object and array inputs into JSON. Create a local
			// variable for each that we can later pass to the query.
			g.List(jen.Id(getVarNameForInputRef(r)), jen.Err()).Op(":=").Qual("encoding/json", "Marshal").Call(
				jen.Id(idParamInput).Dot(r.GoString()),
			)
			genHandleError(g)
			g.Empty()
		}
	}
}

func genQueryExecute(g *jen.Group, q pg.Query, im model.Model) {
	g.List(
		jen.Id(idVarRows),
		jen.Err(),
	).Op(":=").Id(idParamQueries).Dot(idPropDb).Dot("QueryContext").CallFunc(func(g *jen.Group) {
		genQueryInputsParams(g, q, im)
	})
	genHandleError(g)
	g.Defer().Id(idVarRows).Dot("Close").Call()
	g.Empty()

}

func genQueryInputsParams(g *jen.Group, q pg.Query, im model.Model) {
	g.Id(idParamCtx)
	g.Id(getSqlConstName(q))

	for _, in := range q.In.Inputs {
		r, _ := match.ResolveRef(im.Schema, in.Ref)

		if isObjectOrArray(r.Schema) {
			// We've created local variables for all object and array inputs.
			g.Id(getVarNameForInputRef(r))
		} else {
			g.Id(idParamInput).Dot(r.GoString())
		}
	}
}

func genReadRows(g *jen.Group, q pg.Query, om model.Model) {
	g.Var().Id(idVarOutput).Index().Id(q.Out.Model)
	g.For(jen.Id(idVarRows).Dot("Next").Call()).BlockFunc(func(g *jen.Group) {
		genReadRowsLoopBody(g, q, om)
	})

	g.Empty()
	g.If(jen.Err().Op(":=").Id(idVarRows).Dot("Err").Call(), jen.Err().Op("!=").Nil()).Block(
		jen.Return(jen.Nil(), jen.Err()),
	)

	g.Empty()
}

func genReadRowsLoopBody(g *jen.Group, q pg.Query, om model.Model) {
	genOutputRowVars(g, q, om)

	g.If(
		jen.Err().Op(":=").Id(idVarRows).Dot("Scan").CallFunc(func(g *jen.Group) {
			getScanParams(g, q, om)
		}),
		jen.Err().Op("!=").Nil(),
	).Block(
		jen.Return(jen.Nil(), jen.Err()),
	)

	genAssignOutput(g, q, om)

	g.Empty()
	g.Id(idVarOutput).Op("=").Id("append").Call(
		jen.Id(idVarOutput),
		jen.Id(idVarRow),
	)
}

func genOutputRowVars(g *jen.Group, q pg.Query, om model.Model) {
	g.Var().Id(idVarRow).Id(q.Out.Model)
	g.Empty()

	didGen := false
	for _, c := range q.Out.Table.Columns {
		r, err := match.ResolveRef(om.Schema, c.Name)
		if err != nil {
			// Just ignore extra columns here.
			continue
		}

		if isJson(*c) {
			// Generate a RawBytes variable for each json output column.
			g.Var().Id(getVarNameForOutputRef(r)).Qual("database/sql", "RawBytes")
			didGen = true
		} else if r.Nullable() && r.Schema.Type.IsPrimitive() {
			g.Var().Id(getVarNameForOutputRef(r)).Qual("database/sql", getSqlNullType(*r.Schema))
			didGen = true
		}
	}

	if didGen {
		g.Empty()
	}
}

func getScanParams(g *jen.Group, q pg.Query, om model.Model) {
	for _, c := range q.Out.Table.Columns {
		r, err := match.ResolveRef(om.Schema, c.Name)
		if err != nil {
			// Create an inline `sql.RawBytes` target for selections that don't
			// exist in the target model.
			g.Op("&").Qual("database/sql", "RawBytes").Values()
			continue
		}

		if isJson(*c) {
			g.Op("&").Id(getVarNameForOutputRef(r))
		} else if r.Nullable() && r.Schema.Type.IsPrimitive() {
			g.Op("&").Id(getVarNameForOutputRef(r))
		} else if c.Type.Array {
			var target *jen.Statement
			if r.Nullable() {
				target = jen.Id(idVarRow).Dot(r.GoString())
			} else {
				target = jen.Op("&").Id(idVarRow).Dot(r.GoString())
			}
			g.Qual("github.com/lib/pq", "Array").Call(target)
		} else {
			g.Op("&").Id(idVarRow).Dot(r.GoString())
		}
	}
}

func genAssignOutput(g *jen.Group, q pg.Query, om model.Model) {
	for _, c := range q.Out.Table.Columns {
		r, err := match.ResolveRef(om.Schema, c.Name)
		if err != nil {
			continue
		}

		outputVar := getVarNameForOutputRef(r)
		if isJson(*c) {
			g.Empty()
			// Unmarshal objects and arrays into the row object.
			g.If(jen.Id(outputVar).Op("!=").Nil()).BlockFunc(func(g *jen.Group) {
				g.If(
					jen.Err().Op(":=").Qual("encoding/json", "Unmarshal").Call(
						jen.Id(outputVar),
						jen.Op("&").Id(idVarRow).Dot(r.GoString()),
					),
					jen.Err().Op("!=").Nil(),
				).Block(
					jen.Return(jen.Nil(), jen.Err()),
				)
			})
		} else if r.Nullable() && r.Schema.Type.IsPrimitive() {
			g.Empty()
			g.If(jen.Id(outputVar).Dot("Valid")).BlockFunc(func(g *jen.Group) {
				g.Id(idVarRow).Dot(r.GoString()).Op("=").Op("&").Id(outputVar).Dot(getSqlNullTypeProp(*r.Schema))
			})
		}
	}
}

func getSqlNullType(schema model.Schema) string {
	return fmt.Sprintf("Null%s", getSqlNullTypeProp(schema))
}

func getSqlNullTypeProp(schema model.Schema) string {
	switch schema.Type {
	case model.TypeString:
		return "String"
	case model.TypeInt, model.TypeInt64:
		return "Int64"
	case model.TypeInt32:
		return "Int32"
	case model.TypeFloat32, model.TypeFloat64:
		return "Float64"
	case model.TypeTime:
		return "Time"
	case model.TypeBool:
		return "Bool"
	}

	return "String"
}

func isJson(col pg.Column) bool {
	return col.Type.Name == pg.DataTypeJson || col.Type.Name == pg.DataTypeJsonb
}

func isObjectOrArray(schema *model.Schema) bool {
	return schema.Type == model.TypeObject || schema.Type == model.TypeArray
}

func getVarNameForInputRef(r *match.SchemaPath) string {
	return getVarNameForRef(r, idVarInputSuffix)
}

func getVarNameForOutputRef(r *match.SchemaPath) string {
	return getVarNameForRef(r, idVarOutputSuffix)
}

func getVarNameForRef(r *match.SchemaPath, suffix string) string {
	name := r.Path[0]

	for _, p := range r.Path[1:] {
		name += firstUpper(p)
	}

	name += suffix

	if slices.Contains(queryLocalVars, name) {
		name += "_"
	}

	return name
}

func genHandleError(g *jen.Group) {
	g.If(jen.Err().Op("!=").Nil()).Block(
		jen.Return(jen.Nil(), jen.Err()),
	)
}

func writeQueriesToFile(f *jen.File, cfg config.Config, workingDir string) error {
	filePath := filepath.Join(workingDir, cfg.Package.Path, filepath.Base(cfg.Package.Path)+".go")

	if err := os.MkdirAll(filepath.Dir(filePath), 0700); err != nil {
		return err
	}

	return os.WriteFile(filePath, []byte(f.GoString()), 0600)
}

func getSqlConstName(q pg.Query) string {
	return fmt.Sprintf("%sSql", firstLower(q.Name))
}

func firstLower(s string) string {
	return strings.ToLower(s[0:1]) + s[1:]
}

func firstUpper(s string) string {
	return strings.ToUpper(s[0:1]) + s[1:]
}
