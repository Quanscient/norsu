-- :name FindPersons :in sqlio.Id :out persons.Person
SELECT
  p.*
FROM
  persons p
WHERE
  id = :id
;