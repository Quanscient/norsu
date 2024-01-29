package match

import (
	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/pg"
)

type matchType string

const (
	matchTypeColumn matchType = "column"
	matchTypeJson   matchType = "json"
)

// DoesTablePopulateModel checks recursively if `table` fully populates `schema`.
// Returns a `MatchError` in case it doesn't.
func DoesTablePopulateModel(table pg.Table, schema model.Schema) error {
	if err := doesTablePopulateModel(matchTypeColumn, table, schema, &SchemaPath{}); err != nil {
		return err
	}

	return nil
}

func doesTablePopulateModel(
	aType matchType,
	table pg.Table,
	schema model.Schema,
	schemaPath *SchemaPath,
) error {
	for pn, p := range schema.Properties {
		normPropName := Normalize(aType, pn)
		var column *pg.Column

		for _, c := range table.Columns {
			normColName := Normalize(aType, c.Name)

			if normPropName == normColName {
				column = c
				break
			}
		}

		schemaPath.Path = append(schemaPath.Path, pn)
		schemaPath.Schema = p

		if column == nil {
			return matchErrorf(schemaPath, `selection missing for output property %s`, schemaPath.GoString())
		}

		if p.Type == model.TypeObject {
			if column.Type.Name != pg.DataTypeJson && column.Type.Name != pg.DataTypeJsonb {
				return matchErrorf(schemaPath, `invalid selection type "%s" for an object output property %s`, column.Type.String(), schemaPath.GoString())
			}

			if column.Type.IsArray {
				return matchErrorf(schemaPath, `array selected for object output property %s`, schemaPath.GoString())
			}

			if column.Type.Record != nil {
				if err := doesTablePopulateModel(matchTypeJson, *column.Type.Record, *p, schemaPath); err != nil {
					return err
				}
			}
		}

		if p.Type == model.TypeArray {
			if column.Type.Name != pg.DataTypeJson && column.Type.Name != pg.DataTypeJsonb {
				return matchErrorf(schemaPath, `invalid selection type "%s" for an array output property %s`, column.Type.String(), schemaPath.GoString())
			}

			if !column.Type.IsArray {
				return matchErrorf(schemaPath, `object selected for array output property %s`, schemaPath.GoString())
			}

			if column.Type.Record != nil {
				if err := doesTablePopulateModel(matchTypeJson, *column.Type.Record, *p.Items, schemaPath); err != nil {
					return err
				}
			}
		}

		// TODO: Check column types. The conversion rules are based on database/sql package
		//       and are really complex.

		schemaPath.Path = schemaPath.Path[:len(schemaPath.Path)-1]
		schemaPath.Schema = nil
	}

	return nil
}
