package pg

import (
	"slices"
)

type DB struct {
	Tables       []*Table
	TablesByName map[TableName]*Table
}

func NewDB() *DB {
	return &DB{
		Tables:       make([]*Table, 0),
		TablesByName: make(map[TableName]*Table),
	}
}

func (db *DB) Clone() *DB {
	clone := &DB{
		Tables:       make([]*Table, 0, len(db.Tables)),
		TablesByName: make(map[TableName]*Table, len(db.Tables)),
	}

	for _, t := range db.Tables {
		clone.AddTable(t.Clone())
	}

	return clone
}

func (db *DB) AddTable(table *Table) {
	db.TablesByName[*table.Name] = table
	db.Tables = append(db.Tables, table)
}

func (db *DB) AddTableToFront(table *Table) {
	db.TablesByName[*table.Name] = table
	db.Tables = append([]*Table{table}, db.Tables...)
}

func (db *DB) RemoveTable(name TableName) {
	delete(db.TablesByName, name)
	db.Tables = slices.DeleteFunc(db.Tables, func(t *Table) bool { return *t.Name == name })
}

func (db *DB) RenameTable(name TableName, newName TableName) {
	t := db.TablesByName[name]
	delete(db.TablesByName, name)

	t.Name = &newName
	db.TablesByName[newName] = t
}
