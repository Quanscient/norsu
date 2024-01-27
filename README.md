# Norsu

Norsu is a postgres query code generator for golang. It solves a quite narrow problem of writing type-safe SQL queries easily given you've defined your entities in Open API files.

You can write a query like this:

```sql
-- :name InsertPerson :in person.NewPerson :out person.Person
INSERT INTO persons (
  id,
  first_name,
  last_name
)
VALUES (
  :id,
  :first_name,
  :last_name
)
RETURNING
  *
```

Norsu will read all your Open API files and determine the schema of `person.NewPerson` and `person.Person` models. It then analyses the query (by parsing it using the actual postgres source code) and makes sure you've selected all columns in the output and used the input correctly.

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