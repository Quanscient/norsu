package pg

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/koskimas/norsu/internal/ptr"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func Migrate(db *DB, sql string) error {
	upMigration, err := omitDownMigration(sql)
	if err != nil {
		return fmt.Errorf(`failed to omit down migration: %w`, err)
	}

	ast, err := parseSql(upMigration)
	if err != nil {
		return fmt.Errorf(`failed to parse: %w`, err)
	}

	for _, s := range ast.GetStmts() {
		switch node := s.GetStmt().GetNode().(type) {
		case *pg_query.Node_CreateStmt:
			if err := createTable(db, node.CreateStmt); err != nil {
				return fmt.Errorf(`failed to parse a create table statement: %w`, err)
			}
		case *pg_query.Node_DropStmt:
			if err := dropTable(db, node.DropStmt); err != nil {
				return fmt.Errorf(`failed to parse a drop table statement: %w`, err)
			}
		case *pg_query.Node_AlterTableStmt:
			if err := alterTable(db, node.AlterTableStmt); err != nil {
				return fmt.Errorf(`failed to parse an alter table statement: %w`, err)
			}
		case *pg_query.Node_RenameStmt:
			if err := rename(db, node.RenameStmt); err != nil {
				return fmt.Errorf(`failed to parse a rename statement: %w`, err)
			}
		}
	}

	return nil
}

func MigrateFile(db *DB, filePath string) error {
	sql, err := readMigration(filePath)
	if err != nil {
		return fmt.Errorf(`failed to read migration file "%s": %w`, filePath, err)
	}

	if err := Migrate(db, sql); err != nil {
		return fmt.Errorf(`failed to apply migration file "%s": %w`, filePath, err)
	}

	return nil
}

func createTable(db *DB, stmt *pg_query.CreateStmt) error {
	rel := stmt.GetRelation()
	if rel == nil {
		return errors.New("no relation")
	}

	name := rel.GetRelname()
	if len(name) == 0 {
		return errors.New("empty table name")
	}

	table := NewTable(NewTableName(name, rel.GetSchemaname()))
	for _, c := range stmt.GetTableElts() {
		if def := c.GetColumnDef(); def != nil {
			if err := addColumn(table, def); err != nil {
				return err
			}
		} else if like := c.GetTableLikeClause(); like != nil {
			likeTable := db.TablesByName[NewTableName(like.GetRelation().GetRelname())]
			if likeTable == nil {
				return fmt.Errorf(`tried to create a table using like clause with unknown table "%s"`, like.GetRelation().GetRelname())
			}

			for _, col := range likeTable.Columns {
				table.AddColumn(col)
			}
		}
	}

	db.AddTable(table)
	return nil
}

func addColumn(table *Table, def *pg_query.ColumnDef) error {
	if col, err := parseColumnDef(def); err != nil {
		return err
	} else {
		table.AddColumn(col)
	}

	return nil
}

func parseColumnDef(def *pg_query.ColumnDef) (*Column, error) {
	col := Column{
		Name: def.GetColname(),
	}

	if t, err := parseColumnType(def); err != nil {
		return nil, fmt.Errorf(`failed to parse type for column "%s": %w`, col.Name, err)
	} else {
		col.Type = *t
	}

	return &col, nil
}

func parseColumnType(def *pg_query.ColumnDef) (*DataType, error) {
	typeName := def.GetTypeName()
	if typeName == nil {
		return nil, errors.New("no type name")
	}

	t, err := parseTypeName(typeName)
	if err != nil {
		return nil, err
	}

	t.NotNull = isNotNull(def)
	return t, nil
}

func parseTypeName(typeName *pg_query.TypeName) (*DataType, error) {
	t := &DataType{}

	names := typeName.GetNames()
	if len(names) == 2 {
		t.Schema = ptr.V(getString(names[0]))
		t.Name = getString(names[1])
	} else if len(names) == 1 {
		t.Name = getString(names[0])
	} else {
		return nil, fmt.Errorf("a surprising amount of names (%d) in a type name", len(names))
	}

	t.Name = strings.ToLower(t.Name)
	if t.Schema != nil {
		t.Schema = ptr.V(strings.ToLower(*t.Schema))
	}

	return t, nil
}

func isNotNull(def *pg_query.ColumnDef) bool {
	for _, c := range def.GetConstraints() {
		switch c.GetConstraint().GetContype() {
		case pg_query.ConstrType_CONSTR_NOTNULL, pg_query.ConstrType_CONSTR_PRIMARY:
			return true
		}

	}

	return false
}

func dropTable(db *DB, stmt *pg_query.DropStmt) error {
	for _, o := range stmt.GetObjects() {
		for _, i := range o.GetList().GetItems() {
			tableName := getString(i)

			table := db.TablesByName[NewTableName(tableName)]
			if table == nil {
				return fmt.Errorf(`unknown table "%s"`, tableName)
			}

			db.RemoveTable(*table.Name)
		}
	}

	return nil
}

func alterTable(db *DB, stmt *pg_query.AlterTableStmt) error {
	rel := stmt.GetRelation()
	if rel == nil {
		return errors.New("no relation")
	}

	name := rel.GetRelname()
	if len(name) == 0 {
		return errors.New("empty table name")
	}

	table := db.TablesByName[NewTableName(name)]
	if table == nil {
		return fmt.Errorf(`table "%s" hasn't been created`, name)
	}

	for _, cmd := range stmt.GetCmds() {
		alter := cmd.GetAlterTableCmd()

		switch alter.Subtype {
		case pg_query.AlterTableType_AT_AddColumn:
			if err := addColumn(table, alter.Def.GetColumnDef()); err != nil {
				return fmt.Errorf("failed to add column: %w", err)
			}
		case pg_query.AlterTableType_AT_DropColumn:
			if err := removeColumn(table, alter.GetName()); err != nil {
				return fmt.Errorf("failed to drop column: %w", err)
			}
		case pg_query.AlterTableType_AT_SetNotNull:
			if err := setNotNull(table, alter.GetName()); err != nil {
				return fmt.Errorf("failed to set column not null: %w", err)
			}
		case pg_query.AlterTableType_AT_DropNotNull:
			if err := dropNotNull(table, alter.GetName()); err != nil {
				return fmt.Errorf("failed to drop not null: %w", err)
			}
		case pg_query.AlterTableType_AT_AlterColumnType:
			if err := alterColumnType(table, alter.GetName(), alter.Def.GetColumnDef()); err != nil {
				return fmt.Errorf("failed to alter column type: %w", err)
			}
		}
	}

	return nil
}

func removeColumn(table *Table, colName string) error {
	_, ok := table.ColumnsByName[colName]
	if !ok {
		return fmt.Errorf(`could not find column "%s" in table "%s"`, colName, table.Name)
	}

	table.RemoveColumn(colName)
	return nil
}

func setNotNull(table *Table, colName string) error {
	col, ok := table.ColumnsByName[colName]
	if !ok {
		return fmt.Errorf(`could not find column "%s" in table "%s"`, colName, table.Name)
	}

	col.Type.NotNull = true
	return nil
}

func dropNotNull(table *Table, colName string) error {
	col, ok := table.ColumnsByName[colName]
	if !ok {
		return fmt.Errorf(`could not find column "%s" in table "%s"`, colName, table.Name)
	}

	col.Type.NotNull = false
	return nil
}

func alterColumnType(table *Table, columnName string, def *pg_query.ColumnDef) error {
	t, err := parseColumnType(def)
	if err != nil {
		return err
	}

	col, ok := table.ColumnsByName[columnName]
	if !ok {
		return fmt.Errorf(`could not find column "%s" in table "%s"`, columnName, table.Name)
	}

	col.Type.Name = strings.ToLower(t.Name)
	return nil
}

func rename(db *DB, stmt *pg_query.RenameStmt) error {
	rel := stmt.GetRelation()
	if rel == nil {
		return errors.New("no relation")
	}

	tableName := rel.GetRelname()
	if len(tableName) == 0 {
		return errors.New("empty table name")
	}

	table := db.TablesByName[NewTableName(tableName)]
	if table == nil {
		return fmt.Errorf(`unknown table "%s"`, tableName)
	}

	switch stmt.GetRenameType() {
	case pg_query.ObjectType_OBJECT_COLUMN:
		if col := table.ColumnsByName[stmt.GetSubname()]; col == nil {
			return fmt.Errorf(`unknown column "%s" in table "%s"`, stmt.GetSubname(), tableName)
		} else {
			table.RenameColumn(stmt.GetSubname(), stmt.GetNewname())
		}
	case pg_query.ObjectType_OBJECT_TABLE:
		db.RenameTable(*table.Name, NewTableName(stmt.GetNewname()))
	default:
		return fmt.Errorf("unknown rename type %s", stmt.GetRenameType().String())
	}

	return nil
}

func readMigration(filePath string) (string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf(`failed to read file: %w`, err)
	}

	return string(data), nil
}

func omitDownMigration(m string) (string, error) {
	lines := make([]string, 0)

	for _, l := range strings.Split(m, "\n") {
		if isDownMigrationStartLine(l) {
			break
		}

		lines = append(lines, l)
	}

	return strings.Join(lines, "\n"), nil
}

func isDownMigrationStartLine(l string) bool {
	return strings.HasPrefix(l, "-- +goose Down")
}
