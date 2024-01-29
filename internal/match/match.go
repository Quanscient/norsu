package match

import (
	"fmt"
	"strings"

	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/pg"
)

type MatchType string

const (
	MatchTypeColumn MatchType = "column"
	MatchTypeJson   MatchType = "json"
)

type SchemaPath struct {
	Path   []string
	Schema *model.Schema
}

func (p *SchemaPath) GoString() string {
	goProps := make([]string, len(p.Path))

	for i := range p.Path {
		goProps[i] = ModelPropToGo(p.Path[i])
	}

	return strings.Join(goProps, ".")
}

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

// DoesTablePopulateModel checks recursively if `table` fully populates `schema`.
// Returns a `MatchError` in case it doesn't.
func DoesTablePopulateModel(table pg.Table, schema model.Schema) error {
	if err := doesTablePopulateModel(MatchTypeColumn, table, schema, &SchemaPath{}); err != nil {
		return err
	}

	return nil
}

func doesTablePopulateModel(
	aType MatchType,
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
				if err := doesTablePopulateModel(MatchTypeJson, *column.Type.Record, *p, schemaPath); err != nil {
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
				if err := doesTablePopulateModel(MatchTypeJson, *column.Type.Record, *p.Items, schemaPath); err != nil {
					return err
				}
			}
		}

		schemaPath.Path = schemaPath.Path[:len(schemaPath.Path)-1]
		schemaPath.Schema = nil
	}

	return nil
}

func Normalize(aType MatchType, prop string) string {
	if aType == MatchTypeColumn {
		return strings.ToLower(strings.ReplaceAll(prop, "_", ""))
	}

	return strings.ToLower(prop)
}

func ModelPropToGo(modelProp string) string {
	return strings.ToUpper(modelProp[0:1]) + modelProp[1:]
}

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

	normRefPart := Normalize(MatchTypeColumn, refPart)

	for n, p := range schema.Properties {
		if Normalize(MatchTypeColumn, n) == normRefPart {
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
