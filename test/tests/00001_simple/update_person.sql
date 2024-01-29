-- :name UpdatePerson :in sqlio.PersonUpdate :out persons.Person
UPDATE
  persons
SET
  first_name = :person.firstName,
  last_name = :person.lastName,
  age = :person.age
WHERE
  id = :id
RETURNING
  persons.*,
  (
    SELECT
      JSON_AGG(pets ORDER BY name)
    FROM
      pets
    WHERE
      pets.owner_id = persons.id
  ) pets
;
