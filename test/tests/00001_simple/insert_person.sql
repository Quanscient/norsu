-- :name InsertPerson :in sqlio.PersonUpdate :out persons.Person
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
