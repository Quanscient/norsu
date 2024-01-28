package persons

import "github.com/koskimas/norsu/test/fixtures/pets"

type PersonId string

type Person struct {
	Id        PersonId   `json:"id"`
	FirstName string     `json:"firstName"`
	LastName  *string    `json:"lastName"`
	Age       int        `json:"age"`
	Address   Address    `json:"address"`
	Pets      []pets.Pet `json:"pets"`
}

type Address struct {
	PostalCode string `json:"postalCode"`
	Street     string `json:"street"`
}

type PersonUpdate struct {
	FirstName string  `json:"firstName"`
	LastName  *string `json:"lastName"`
	Age       int     `json:"age"`
	Address   Address `json:"ddress"`
}
