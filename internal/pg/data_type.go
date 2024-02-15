package pg

const (
	DataTypeJson   = "json"
	DataTypeJsonb  = "jsonb"
	DataTypeRecord = "record"
)

var DataTypes = map[string]bool{
	"text":                        true,
	"varchar":                     true,
	"char":                        true,
	"character":                   true,
	"character varying":           true,
	"smallint":                    true,
	"int2":                        true,
	"int":                         true,
	"integer":                     true,
	"int4":                        true,
	"bigint":                      true,
	"int8":                        true,
	"smallserial":                 true,
	"serial2":                     true,
	"serial":                      true,
	"serial4":                     true,
	"bigserial":                   true,
	"serial8":                     true,
	"double precision":            true,
	"float8":                      true,
	"real":                        true,
	"float4":                      true,
	"numeric":                     true,
	"decimal":                     true,
	"money":                       true,
	"bool":                        true,
	"boolean":                     true,
	"time without time zone":      true,
	"time":                        true,
	"time with time zone":         true,
	"timetz":                      true,
	"timestamp without time zone": true,
	"timestamp":                   true,
	"timestamp with time zone":    true,
	"timestamptz":                 true,
	"date":                        true,
	"interval":                    true,
	"uuid":                        true,
	"bit":                         true,
	"bit varying":                 true,
	"varbit":                      true,
	"bytea":                       true,
	DataTypeJson:                  true,
	DataTypeJsonb:                 true,
	DataTypeRecord:                true,
}

// DataType represents a postgres data type. Nested record and json types
// are stored in the `Record` property.
type DataType struct {
	Name    string
	Schema  *string
	NotNull bool

	// Array is true if the type is a postgres array. For example `INT[]`
	// would produce a DataType `{ Name: "int", Array: true }`.
	Array bool

	// Record holds the nested record type in case of a record,
	// json or jsonb type.
	Record *Table

	// RecordArray is true if there's an array of records instead
	// of a single record.
	RecordArray bool
}

func (d *DataType) Json() bool {
	return d.Name == DataTypeJson || d.Name == DataTypeJsonb
}

func (d *DataType) Clone() DataType {
	clone := DataType{
		Name:        d.Name,
		NotNull:     d.NotNull,
		Array:       d.Array,
		Schema:      d.Schema,
		RecordArray: d.RecordArray,
	}

	if d.Record != nil {
		clone.Record = d.Record.Clone()
	}

	return clone
}

func (d *DataType) writeString(s *stringBuilder) {
	if d.Schema != nil {
		s.WriteString(*d.Schema)
		s.WriteByte('.')
	}

	s.WriteString(d.Name)

	if d.Array {
		s.WriteString("[]")
	}

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
