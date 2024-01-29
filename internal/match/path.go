package match

import (
	"strings"

	"github.com/koskimas/norsu/internal/model"
)

type SchemaPath struct {
	Path   []string
	Schema *model.Schema
}

func (p *SchemaPath) GoString() string {
	goProps := make([]string, len(p.Path))

	for i := range p.Path {
		goProps[i] = modelPropToGo(p.Path[i])
	}

	return strings.Join(goProps, ".")
}

func modelPropToGo(modelProp string) string {
	return strings.ToUpper(modelProp[0:1]) + modelProp[1:]
}
