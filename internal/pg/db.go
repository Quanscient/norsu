package pg

import (
	"fmt"
	"slices"
	"strings"

	"github.com/koskimas/norsu/internal/ptr"
)

type DB struct {
	Tables       []*Table
	TablesByName map[TableName]*Table
}

type Table struct {
	Name          *TableName
	Columns       []*Column
	ColumnsByName map[string]*Column
}

type TableName struct {
	Name   string
	Schema string
}

type Column struct {
	Name string
	Type DataType
}

type DataType struct {
	Name    string
	NotNull bool
	IsArray bool
	Schema  *string
	// Record holds the nested record type in case of a record,
	// json or jsonb type.
	Record *Table
}

func NewDB() *DB {
	return &DB{
		Tables:       make([]*Table, 0),
		TablesByName: make(map[TableName]*Table),
	}
}

func NewTable(name ...TableName) *Table {
	t := &Table{
		Columns:       make([]*Column, 0),
		ColumnsByName: make(map[string]*Column),
	}

	if len(name) > 0 {
		t.Name = &name[0]
	}

	return t
}

func NewTableName(name string, schema ...string) TableName {
	var t TableName

	t.Name = name
	if len(schema) > 0 {
		t.Schema = schema[0]
	}

	return t
}

func NewTableNamePtr(name string, schema ...string) *TableName {
	return ptr.V(NewTableName(name, schema...))
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

func (t *Table) HasName() bool {
	return t.Name != nil
}

func (t *Table) AddColumn(col *Column) {
	t.ColumnsByName[col.Name] = col
	t.Columns = append(t.Columns, col)
}

func (t *Table) RemoveColumn(name string) {
	delete(t.ColumnsByName, name)
	t.Columns = slices.DeleteFunc(t.Columns, func(c *Column) bool { return c.Name == name })
}

func (t *Table) RenameColumn(name string, newName string) {
	c := t.ColumnsByName[name]
	delete(t.ColumnsByName, name)

	c.Name = newName
	t.ColumnsByName[newName] = c
}

func (t *Table) writeString(s *stringBuilder, omitName bool) {
	if t.Name != nil && !omitName {
		t.Name.string(s)
		s.WriteString(" ")
	}

	s.WriteString("(")
	s.WriteNewLine()
	s.Indent()

	for i, c := range t.Columns {
		c.writeString(s)

		if i != len(t.Columns)-1 {
			s.WriteString(",")
		}

		s.WriteNewLine()
	}

	s.DeIndent()
	s.WriteString(")")
}

func (t *Table) String() string {
	var s stringBuilder
	t.writeString(&s, false)
	return s.String()
}

func (t *Table) Clone() *Table {
	clone := NewTable()
	clone.Name = t.Name.Clone()

	for _, c := range t.Columns {
		clone.AddColumn(c.Clone())
	}

	return clone
}

func (c *Column) HasName() bool {
	return len(c.Name) > 0
}

func (c *Column) Clone() *Column {
	return &Column{
		Name: c.Name,
		Type: c.Type.Clone(),
	}
}

func (c *Column) writeString(s *stringBuilder) {
	if c.HasName() {
		s.WriteString(c.Name)
		s.WriteString(" ")
	}

	c.Type.writeString(s)
}

func (c *Column) String() string {
	var s stringBuilder
	c.writeString(&s)
	return s.String()
}

func (d *DataType) writeString(s *stringBuilder) {
	if d.Schema != nil {
		s.WriteString(*d.Schema)
		s.WriteByte('.')
	}

	s.WriteString(d.Name)

	if d.NotNull {
		s.WriteString(" not null")
	}

	if d.Record != nil {
		s.WriteString(" ")
		d.Record.writeString(s, true)
	}
}

func (d *DataType) String() string {
	var s stringBuilder
	d.writeString(&s)
	return s.String()
}

func (d *DataType) Clone() DataType {
	clone := DataType{
		Name:    d.Name,
		NotNull: d.NotNull,
		IsArray: d.IsArray,
		Schema:  d.Schema,
	}

	if d.Record != nil {
		clone.Record = d.Record.Clone()
	}

	return clone
}

func (n *TableName) HasSchema() bool {
	return len(n.Schema) != 0
}

func (n *TableName) Clone() *TableName {
	return &TableName{
		Name:   n.Name,
		Schema: n.Schema,
	}
}

func (n *TableName) string(s *stringBuilder) {
	if n.HasSchema() {
		s.WriteString(n.Schema)
		s.WriteByte('.')
	}

	s.WriteString(n.Name)
}

func (n *TableName) String() string {
	var s stringBuilder
	n.string(&s)
	return s.String()
}

func PRINT(x any) {
	fmt.Printf("%+v\n\n", x)
}

type stringBuilder struct {
	strings.Builder
	newLine bool
	indent  int
}

func (s *stringBuilder) Indent() {
	s.indent += 1
}

func (s *stringBuilder) DeIndent() {
	s.indent -= 1
}

func (s *stringBuilder) WriteNewLine() {
	_ = s.Builder.WriteByte('\n')
	s.newLine = true
}

func (s *stringBuilder) WriteString(str string) {
	s.checkNewline()
	_, _ = s.Builder.WriteString(str)
}

func (s *stringBuilder) WriteByte(b byte) {
	s.checkNewline()
	_ = s.Builder.WriteByte(b)
}

func (s *stringBuilder) WriteRune(r rune) {
	s.checkNewline()
	_, _ = s.Builder.WriteRune(r)
}

func (s *stringBuilder) checkNewline() {
	if s.newLine {
		s.newLine = false
		for i := 0; i < s.indent; i += 1 {
			_, _ = s.Builder.WriteString("  ")
		}
	}
}
