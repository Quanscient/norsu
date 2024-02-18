package match

import (
	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/pg"
)

func Output(output pg.QueryOutput, schema model.Schema) error {
	if err := doesTablePopulateModel(matchTypeColumn, *output.Table, schema, &SchemaPath{}); err != nil {
		return err
	}

	return nil
}
