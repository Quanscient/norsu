package model

type Type string

func (t Type) IsPrimitive() bool {
	return t != TypeObject && t != TypeArray
}

const (
	TypeBool    Type = "bool"
	TypeString  Type = "string"
	TypeFloat32 Type = "float32"
	TypeFloat64 Type = "float64"
	TypeInt     Type = "int"
	TypeInt32   Type = "int32"
	TypeInt64   Type = "int64"
	TypeArray   Type = "array"
	TypeObject  Type = "object"
	TypeTime    Type = "time"
)

type Schema struct {
	Type       Type
	Properties map[string]*Schema
	Required   map[string]bool
	Items      *Schema
}

type Model struct {
	Name    string
	Package string
	Schema  *Schema
}
