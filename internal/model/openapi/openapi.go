package openapi

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/ptr"
	"gopkg.in/yaml.v3"
)

type File struct {
	Components Components `yaml:"components"`
}

type Components struct {
	Schemas map[string]Schema `yaml:"schemas"`
}

type Schema struct {
	Type       string            `yaml:"type"`
	Ref        *string           `yaml:"$ref"`
	Properties map[string]Schema `yaml:"properties"`
	Items      *Schema           `yaml:"items"`
	Required   []string          `yaml:"required"`
}

type AbsoluteFilePath = string
type ModelName = string

type context struct {
	Files map[AbsoluteFilePath]*fileContext
}

type fileContext struct {
	File   *File
	Models map[ModelName]*model.Schema
}

func ReadModels(filePaths []AbsoluteFilePath) (map[AbsoluteFilePath]map[ModelName]*model.Schema, error) {
	ctx := &context{
		Files: make(map[AbsoluteFilePath]*fileContext),
	}

	for _, p := range filePaths {
		if err := resolveFile(ctx, p); err != nil {
			return nil, err
		}
	}

	models := make(map[AbsoluteFilePath]map[ModelName]*model.Schema)
	for path, f := range ctx.Files {
		models[path] = f.Models
	}

	return models, nil
}

func resolveFile(ctx *context, filePath AbsoluteFilePath) error {
	if _, ok := ctx.Files[filePath]; ok {
		return nil
	}

	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf(`failed to read OpenAPI file "%s": %w`, filePath, err)
	}

	var file File
	if err := yaml.Unmarshal(fileData, &file); err != nil {
		return fmt.Errorf(`failed to unmarshal OpenAPI file "%s": %w`, filePath, err)
	}

	fileCtx := &fileContext{
		File:   &file,
		Models: make(map[ModelName]*model.Schema),
	}

	ctx.Files[filePath] = fileCtx

	for name := range file.Components.Schemas {
		if _, err := resolveRootModel(ctx, filePath, name); err != nil {
			return err
		}
	}

	return nil
}

func resolveRootModel(
	ctx *context,
	filePath AbsoluteFilePath,
	name ModelName,
) (*model.Schema, error) {
	fileCtx := ctx.Files[filePath]

	if m, ok := fileCtx.Models[name]; ok {
		return m, nil
	}

	schema := ctx.Files[filePath].File.Components.Schemas[name]
	return resolveModel(ctx, schema, filePath, &name)
}

func resolveModel(
	ctx *context,
	schema Schema,
	filePath AbsoluteFilePath,
	name *ModelName,
) (*model.Schema, error) {
	var mod *model.Schema

	if schema.Ref != nil {
		if m, err := resolveReference(ctx, schema, filePath); err != nil {
			return nil, err
		} else {
			mod = m
		}
	} else {
		modelType, err := parseType(schema)
		if err != nil {
			return nil, err
		}

		if *modelType == model.TypeObject {
			if m, err := resolveObject(ctx, schema, filePath); err != nil {
				return nil, err
			} else {
				mod = m
			}
		} else if *modelType == model.TypeArray {
			if m, err := resolveArray(ctx, schema, filePath); err != nil {
				return nil, err
			} else {
				mod = m
			}
		} else {
			mod = &model.Schema{
				Type: *modelType,
			}
		}
	}

	if name != nil {
		ctx.Files[filePath].Models[*name] = mod
	}

	return mod, nil
}

func resolveReference(ctx *context, schema Schema, filePath AbsoluteFilePath) (*model.Schema, error) {
	const refPath = "#/components/schemas/"
	r := *schema.Ref

	parts := strings.Split(r, refPath)
	if len(parts) != 2 {
		return nil, fmt.Errorf(`couldn't parse reference "%s"`, r)
	}

	modelName := parts[1]

	if len(parts[0]) == 0 {
		return resolveRootModel(ctx, filePath, modelName)
	}

	refFilePath := filepath.Join(filepath.Dir(filePath), parts[0])

	if err := resolveFile(ctx, refFilePath); err != nil {
		return nil, err
	}

	return resolveRootModel(ctx, refFilePath, modelName)
}

func resolveObject(ctx *context, schema Schema, filePath AbsoluteFilePath) (*model.Schema, error) {
	m := &model.Schema{
		Type:       model.TypeObject,
		Properties: make(map[string]*model.Schema, 0),
	}

	for pn, ps := range schema.Properties {
		if pm, err := resolveModel(ctx, ps, filePath, nil); err != nil {
			return nil, err
		} else {
			pm.Nullable = !slices.Contains(schema.Required, pn)
			m.Properties[pn] = pm
		}
	}

	return m, nil
}

func resolveArray(ctx *context, schema Schema, filePath AbsoluteFilePath) (*model.Schema, error) {
	im, err := resolveModel(ctx, *schema.Items, filePath, nil)
	if err != nil {
		return nil, err
	}

	m := &model.Schema{
		Type:  model.TypeArray,
		Items: im,
	}

	return m, nil
}

func parseType(schema Schema) (*model.Type, error) {
	switch schema.Type {
	case "object":
		return ptr.V(model.TypeObject), nil
	case "array":
		return ptr.V(model.TypeArray), nil
	case "string":
		return ptr.V(model.TypeString), nil
	case "integer":
		return ptr.V(model.TypeInt), nil
	case "boolean":
		return ptr.V(model.TypeBool), nil
	case "number":
		return ptr.V(model.TypeFloat64), nil
	}

	return nil, fmt.Errorf(`unsupported OpenAPI schema type "%s"`, schema.Type)
}
