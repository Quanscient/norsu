-- :name DeletePerson :in sqlio.PersonUpdate :out persons.Person
DELETE FROM
  persons
WHERE
  id = :id
RETURNING
  persons.*
;
