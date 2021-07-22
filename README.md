# PostgreSQL Query Plan Exuecutor

Get queries plan for registerd queries in minutes

## How to use

### Compile and publish application for Mac

``$ dotnet publish -c Release -r osx-x64 --self-contained true``

### Create table for query data source

```sql
create table source
(
    id serial not null
        constraint source_pkey
        primary key,
    name varchar(500),
    object varchar(500),
    query_string text,
    modified_query_string text
);
```

### Create table for query result

```sql
create table result
(
    id serial not null
        constraint result
        primary key,
    name varchar(500),
    object varchar(500),
    query_string text,
    modified_query_string text,
    run_id varchar(500),
    query_plan text,
    cost_min double precision,
    cost_max double precision,
    modified_query_plan text,
    modified_cost_min double precision,
    modified_cost_max double precision
);
```

### Configure appsettings.json

1. Copy `appsettings.example.json` to `appsettings.json`
2. Change `ConnectionStrings`
3. Change `WorkingTable.QuerySource` and `WorkingTable.QueryResult` with table name

### Run application

``$ ./psql-qplan-executer [[optinal] run-id] [[optional] object-filter]``

### Run without publishing

``$ dotnet run -- [[optinal] run-id] [[optional] object-filter]``

## Requriments

- dotnet core