-- :name InsertPerson :in sqlio.NewPerson :out persons.Person
INSERT INTO persons (
  id,
  first_name,
  last_name,
  age,
  address
) VALUES (
  :id,
  :person.firstName,
  :person.lastName,
  :person.age,
  :person.address
) RETURNING *
;
