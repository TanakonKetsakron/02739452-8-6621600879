#!/bin/bash
# Runs once on first Postgres start: creates the separate data database "cars".
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-SQL
  CREATE DATABASE ${POSTGRES_CARS_DB:-cars};
SQL
