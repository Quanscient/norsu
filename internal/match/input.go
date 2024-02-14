package match

import (
	"fmt"

	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/pg"
)

func Input(input pg.QueryInput, schema model.Schema) error {
	for _, i := range input.Inputs {
		_, err := ResolveRef(&schema, i.Ref)
		if err != nil {
			return fmt.Errorf("query inputs: %w", err)
		}

		// TODO: Check input types once the pg package can output them.
	}

	return nil
}
