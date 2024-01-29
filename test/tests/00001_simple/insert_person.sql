-- :name InsertPerson :in sqlio.PersonUpdate :out sqlio.Id
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
)
RETURNING
  id
;
