package pets

type Species string

const (
	SpeciesDog Species = "dog"
	SpeciesCat Species = "cat"
)

type Pet struct {
	Id      string  `json:"id"`
	Name    string  `json:"name"`
	Species Species `json:"species"`
}
