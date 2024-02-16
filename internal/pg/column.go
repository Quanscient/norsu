package pg

// Column represents a table column or any named property
// that has a type, such as a selection.
type Column struct {
	Name string
	Type DataType
}

func (c *Column) Clone() *Column {
	return &Column{
		Name: c.Name,
		Type: c.Type.Clone(),
	}
}

func (c *Column) writeString(s *stringBuilder) {
	s.WriteString(c.Name)
	s.WriteString(" ")
	c.Type.writeString(s)
}

func (c *Column) String() string {
	var s stringBuilder
	c.writeString(&s)
	return s.String()
}
