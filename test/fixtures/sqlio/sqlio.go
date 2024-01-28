package sqlio

import "github.com/koskimas/norsu/test/fixtures/persons"

type Id struct {
	Id string `json:"id"`
}

type PersonUpdate struct {
	Id     string               `json:"id"`
	Person persons.PersonUpdate `json:"person"`
}
