package match

import (
	"fmt"

	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/pg"
)

func Input(input pg.QueryInput, schema model.Schema) error {
	for _, i := range input.Inputs {
		r, err := ResolveRef(&schema, i.Ref)
		if err != nil {
			return fmt.Errorf("query inputs: %w", err)
		}

		if i.Type != nil && i.Type.Json() {
			if r.Schema.Type == model.TypeArray && i.Type.RecordArray {
				if err := doesTablePopulateModel(matchTypeJson, *i.Type.Record, *r.Schema.Items, &SchemaPath{}); err != nil {
					// TODO: Better error
					return fmt.Errorf("query inputs: %w", err)
				}
			} else if r.Schema.Type == model.TypeObject && !i.Type.RecordArray {
				if err := doesTablePopulateModel(matchTypeJson, *i.Type.Record, *r.Schema, &SchemaPath{}); err != nil {
					// TODO: Better error
					return fmt.Errorf("query inputs: %w", err)
				}
			} else {
				// TODO: Better error
				return fmt.Errorf("%s FUCKED", r.GoString())
			}
		}

		// TODO: Check input types once the pg package can output them.
	}

	return nil
}
