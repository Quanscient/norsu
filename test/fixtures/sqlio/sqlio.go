package sqlio

import "github.com/koskimas/norsu/test/fixtures/persons"

type Id struct {
	Id string `json:"id"`
}

type NewPerson struct {
	Id     string            `json:"id"`
	Person persons.NewPerson `json:"person"`
}
