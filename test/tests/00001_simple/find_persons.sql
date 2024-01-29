-- :name FindPersons :in sqlio.Id :out persons.Person
SELECT
  p.*,
  (
    SELECT
      COALESCE(JSON_AGG(pets), '[]')
    FROM
    (
      SELECT
        pets.id,
        pets.name,
        pets.species
      FROM
        pets
      WHERE
        pets.owner_id = p.id
      ORDER BY
        pets.name
    ) pets
  ) pets
FROM
  persons p
WHERE
  id = :id
;