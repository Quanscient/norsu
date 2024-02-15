package pg

type Column struct {
	Name string
	Type DataType
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
