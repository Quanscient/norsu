package match

import (
	"strings"

	"github.com/koskimas/norsu/internal/model"
)

type SchemaPath struct {
	Path         []string
	Schema       *model.Schema
	ParentSchema *model.Schema
}

func (p *SchemaPath) Nullable() bool {
	return !p.ParentSchema.Required[p.LastPathPart()]
}

func (p *SchemaPath) LastPathPart() string {
	return p.Path[len(p.Path)-1]
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
