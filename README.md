# Norsu

Norsu is a postgres query code generator for golang. It solves a quite specific problem of writing type-safe SQL queries easily given you've defined your entities in OpenAPI files.

You can write a query like this:

```sql
-- :name FindPersons :in sqlio.Id :out persons.Person
SELECT
  p.*,
  (
    SELECT
      COALESCE(JSON_AGG(pets), '[]')
    FROM
    (
      SELECT
        pets.*
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
```

Norsu will read all your OpenAPI files and determine the schema of `sqlio.Id` and `person.Person` models. It then analyses the query (by parsing it using the actual postgres source code) and makes sure you've selected all columns in the output and used the input correctly. The nested `pets` JSON subquery selection will get unmarshalled into the `.Pets` array of `person.Person` and its selections are checked recursively.

Then Norsu generates a simple function for you to call:

```go
func (q *Queries) InsertPerson(
  ctx context.Context,
  in person.NewPerson,
) ([]person.Person, error) {
  // ...
}
```

Norsu determines the database schema without a connection to the DB by reading and analyzing your migration files.

# 🚧 UNDER CONSTRUCTION 🚧

Norsu is currently just a POC. All critical parts already somewhat work, but a lot more work is needed before it's in any way useful.