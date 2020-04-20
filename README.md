# pg-migrations
Migrations for go-pg

Currently, the following arguments are supported:
- up - runs all available migrations;
- down - reverts last migration;
- reset - reverts all migrations;
- version - prints current db version;

example:
./migrations -postgres_address=localhost:5432 -postgres_database=saas -postgres_username=saas -postgres_password=saas -migration_directory_path=/Users/akcps/go/src/pg-migrations/sql -command="up"
