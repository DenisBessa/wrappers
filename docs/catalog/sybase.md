---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Sybase SQL Anywhere

[SAP SQL Anywhere](https://www.sap.com/products/technology-platform/sql-anywhere.html) (formerly Sybase SQL Anywhere) is a relational database management system designed for embedded and mobile applications.

The Sybase Wrapper allows you to read data from SAP SQL Anywhere within your Postgres database using ODBC connectivity.

## Supported Sybase Versions

This wrapper supports SAP SQL Anywhere through ODBC/FreeTDS connectivity. It has been tested with:

- SAP SQL Anywhere 17+
- Sybase Adaptive Server Anywhere

## Prerequisites

### FreeTDS and unixODBC

The Sybase FDW requires FreeTDS and unixODBC to be installed on your system:

**macOS (Homebrew):**
```bash
brew install unixodbc freetds
```

**Ubuntu/Debian:**
```bash
sudo apt-get install unixodbc unixodbc-dev freetds-dev freetds-bin
```

**RHEL/CentOS:**
```bash
sudo yum install unixODBC unixODBC-devel freetds freetds-devel
```

### ODBC Configuration

Configure the FreeTDS driver and your Sybase data source:

**1. Configure the ODBC driver (`/etc/odbcinst.ini` or `~/.odbcinst.ini`):**
```ini
[FreeTDS]
Description = FreeTDS Driver
Driver = /path/to/libtdsodbc.so
Setup = /path/to/libtdsodbc.so
FileUsage = 1
```

**2. Configure your data source (`/etc/odbc.ini` or `~/.odbc.ini`):**
```ini
[SybaseDB]
Driver = FreeTDS
Description = Sybase SQL Anywhere
Server = your-server-host
Port = 2638
Database = your-database
TDS_Version = 5.0
```

## Preparation

Before you can query Sybase SQL Anywhere, you need to enable the Wrappers extension.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Sybase Wrapper

Enable the `sybase_wrapper` FDW:

```sql
create foreign data wrapper sybase_wrapper
  handler sybase_fdw_handler
  validator sybase_fdw_validator;
```

### Connecting to Sybase SQL Anywhere

You can connect using one of these methods:

=== "Using DSN"

    ```sql
    create server sybase_server
      foreign data wrapper sybase_wrapper
      options (
        dsn 'SybaseDB',
        user 'your_username',
        password 'your_password'
      );
    ```

=== "Using Connection String"

    ```sql
    create server sybase_server
      foreign data wrapper sybase_wrapper
      options (
        conn_string 'DRIVER={FreeTDS};SERVER=your-host;PORT=2638;DATABASE=your-db;UID=user;PWD=password;TDS_Version=5.0'
      );
    ```

=== "Using Individual Options"

    ```sql
    create server sybase_server
      foreign data wrapper sybase_wrapper
      options (
        host 'your-server-host',
        port '2638',
        database 'your-database',
        user 'your_username',
        password 'your_password'
      );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists sybase;
```

## Options

### Server Options

| Option | Description |
|--------|-------------|
| `dsn` | ODBC Data Source Name (from odbc.ini) |
| `conn_string` | Full ODBC connection string |
| `conn_string_id` | Vault secret ID containing the connection string |
| `host` | Server hostname |
| `port` | Server port (default: 2638) |
| `database` | Database name |
| `user` | Username |
| `password` | Password |

### Foreign Table Options

| Option | Description |
|--------|-------------|
| `table` | Source table, view, or subquery in Sybase SQL Anywhere (required) |

The `table` option can be:
- A table name: `'users'`
- A qualified table name: `'dbo.users'`
- A subquery: `'(SELECT * FROM users WHERE active = 1)'`

## Entities

### Sybase SQL Anywhere Tables

This is an object representing Sybase SQL Anywhere tables and views.

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| table/view |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table sybase.users (
  id bigint,
  name text,
  email text
)
  server sybase_server
  options (
    table 'users'
  );
```

## Query Pushdown Support

This FDW supports `where`, `order by` and `limit` clause pushdown using Sybase SQL syntax:

- WHERE clauses are pushed down to filter data at the source
- ORDER BY clauses are pushed down for server-side sorting
- LIMIT uses Sybase's `TOP` syntax for efficient limiting

## Supported Data Types

| Postgres Type    | Sybase SQL Anywhere Type       |
| ---------------- | ------------------------------ |
| boolean          | bit                            |
| char             | tinyint                        |
| smallint         | smallint                       |
| real             | float(24)                      |
| integer          | int                            |
| double precision | float(53)                      |
| bigint           | bigint                         |
| numeric          | numeric/decimal                |
| text             | varchar/char/text              |
| date             | date                           |
| timestamp        | datetime/timestamp             |
| timestamptz      | datetime/timestamp             |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to full data transfer requirement
- Only supports specific data type mappings between Postgres and Sybase SQL Anywhere
- Only support read operations (no INSERT, UPDATE, DELETE, or TRUNCATE)
- Requires FreeTDS ODBC driver to be installed and configured
- Materialized views using these foreign tables may fail during logical backups

## Examples

### Basic Example

Create and query a foreign table:

```sql
create foreign table sybase.customers (
  id bigint,
  name text,
  created_at timestamp
)
  server sybase_server
  options (
    table 'customers'
  );

select * from sybase.customers where id < 100;
```

### Query System Tables

Access Sybase system catalog:

```sql
create foreign table sybase.tables (
  table_name text,
  table_type text
)
  server sybase_server
  options (
    table 'sys.systable'
  );

select table_name from sybase.tables where table_type = 'BASE' limit 20;
```

### Remote Subquery Example

Create a foreign table using a subquery:

```sql
create foreign table sybase.active_users (
  id bigint,
  name text,
  email text
)
  server sybase_server
  options (
    table '(SELECT id, name, email FROM users WHERE status = 1)'
  );

select * from sybase.active_users;
```

### Aggregation Example

Perform aggregations:

```sql
create foreign table sybase.order_stats (
  total_orders bigint,
  total_amount numeric
)
  server sybase_server
  options (
    table '(SELECT COUNT(*) as total_orders, SUM(amount) as total_amount FROM orders)'
  );

select * from sybase.order_stats;
```
