package pg

import (
	"slices"

	"github.com/koskimas/norsu/internal/ptr"
)

type Table struct {
	Name          *TableName
	Columns       []*Column
	ColumnsByName map[string]*Column
}

type TableName struct {
	Name   string
	Schema string
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

func (t *Table) Clone() *Table {
	clone := NewTable()

	if t.Name != nil {
		clone.Name = t.Name.Clone()
	}

	for _, c := range t.Columns {
		clone.AddColumn(c.Clone())
	}

	return clone
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
