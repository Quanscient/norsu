-- :name FindPersons :in sqlio.Id :out persons.Person
SELECT
  p.*,
  (
    SELECT
      COALESCE(JSON_AGG(pets ORDER BY id), '[]')
    FROM
    (
      SELECT
        pets.*
      FROM
        pets
      ORDER BY
        pets.name
    ) pets
  ) pets
FROM
  persons p
WHERE
  id = :id
;