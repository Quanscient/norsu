-- :name DeletePerson :in sqlio.PersonUpdate :out sqlio.Id
DELETE FROM
  persons
WHERE
  id = :id
RETURNING
  id
;
