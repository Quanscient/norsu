package cmd

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	// Import here to keep this in the go.mod file. Only
	// the generated code actually uses this packages.
	_ "github.com/jackc/pgx/v5"

	"github.com/koskimas/norsu/internal/config"
	"github.com/koskimas/norsu/internal/gen"
	"github.com/koskimas/norsu/internal/maps"
	"github.com/koskimas/norsu/internal/match"
	"github.com/koskimas/norsu/internal/model"
	"github.com/koskimas/norsu/internal/model/openapi"
	"github.com/koskimas/norsu/internal/pg"
)

const configFile = "norsu.yaml"

type Settings struct {
	WorkingDir string
}

func Run(s Settings) error {
	config, err := config.Read(filepath.Join(s.WorkingDir, configFile))
	if err != nil {
		return err
	}

	db, err := parseMigrations(s, *config)
	if err != nil {
		return err
	}

	models, err := readModels(s, *config)
	if err != nil {
		return err
	}

	queries, err := parseQueries(s, *config, db)
	if err != nil {
		return err
	}

	if err := matchModelsAndQueries(models, queries); err != nil {
		return err
	}

	return gen.GenerateCode(*config, s.WorkingDir, models, queries)
}

func parseMigrations(s Settings, config config.Config) (*pg.DB, error) {
	db := pg.NewDB()

	for _, m := range config.Migrations {
		path := filepath.Join(s.WorkingDir, m.Path)

		files, err := filepath.Glob(path)
		if err != nil {
			return nil, fmt.Errorf(`failed to resolve migration files using glob "%s": %w`, m.Path, err)
		}

		for _, f := range files {
			sql, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf(`failed to read migration file "%s": %w`, f, err)
			}

			if err := pg.ParseMigration(db, string(sql)); err != nil {
				return nil, err
			}
		}
	}

	return db, nil
}

// readModels reads the models from files specified by `cfg.Models`. The keys of the returned
// map are package prefixed model names like `person.Person` and values are `packagedModel`
// objects.
func readModels(s Settings, cfg config.Config) (map[string]model.Model, error) {
	openApiFiles, err := getOpenApiFiles(s, cfg)
	if err != nil {
		return nil, err
	}

	models, err := openapi.ReadModels(maps.Keys(openApiFiles))
	if err != nil {
		return nil, fmt.Errorf("failed to read OpenAPI models: %w", err)
	}

	modelsOut := make(map[string]model.Model)
	for filePath, fileModels := range models {
		pkgCfg, ok := openApiFiles[filePath]
		if !ok {
			continue
		}

		pkgPath := pkgCfg.Package.Path
		pkgName := path.Base(pkgCfg.Package.Path)

		for name, m := range fileModels {
			modelsOut[fmt.Sprintf("%s.%s", pkgName, name)] = model.Model{
				Name:    name,
				Package: pkgPath,
				Schema:  m,
			}
		}
	}

	return modelsOut, nil
}

// getOpenApiFiles resolves and returns all Open API file paths in the keys of the
// returned map. The values are the corresponding `config.Model` entries from
// the config.
func getOpenApiFiles(s Settings, cfg config.Config) (map[string]config.Model, error) {
	paths := make(map[string]config.Model)

	for _, c := range cfg.Models {
		path := filepath.Join(s.WorkingDir, c.OpenApi.Path)

		files, err := filepath.Glob(path)
		if err != nil {
			return nil, fmt.Errorf(`failed to resolve opeapi files using glob "%s": %w`, c.OpenApi.Path, err)
		}

		for _, f := range files {
			paths[f] = c
		}
	}

	return paths, nil
}

func parseQueries(s Settings, cfg config.Config, db *pg.DB) ([]pg.Query, error) {
	queries := make([]pg.Query, 0)

	for _, qc := range cfg.Queries {
		path := filepath.Join(s.WorkingDir, qc.Path)

		files, err := filepath.Glob(path)
		if err != nil {
			return nil, fmt.Errorf(`failed to resolve sql query files using glob "%s": %w`, qc.Path, err)
		}

		for _, f := range files {
			sql, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf(`failed to read query file "%s": %w`, f, err)
			}

			q, err := pg.ParseQuery(db, string(sql))
			if err != nil {
				return nil, fmt.Errorf(`failed to parse query "%s": %w`, f, err)
			}

			queries = append(queries, *q)
		}
	}

	return queries, nil
}

func matchModelsAndQueries(models map[string]model.Model, queries []pg.Query) error {
	for _, q := range queries {
		if q.In != nil {
			im := models[q.In.Model]

			if err := match.Input(*q.In, *im.Schema); err != nil {
				return fmt.Errorf("query %s: %w", q.Name, err)
			}
		}

		if q.Out != nil {
			om := models[q.Out.Model]

			if err := match.Output(*q.Out, *om.Schema); err != nil {
				return fmt.Errorf("query %s: %w", q.Name, err)
			}
		}
	}

	return nil
}
