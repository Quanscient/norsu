package model

type Type string

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
	Nullable   bool
	Properties map[string]*Schema
	Items      *Schema
}

type Model struct {
	Name    string
	Package string
	Schema  *Schema
}
