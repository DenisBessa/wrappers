# Sybase Foreign Data Wrapper

This FDW enables PostgreSQL to connect to SAP SQL Anywhere (formerly Sybase SQL Anywhere) databases using ODBC via the FreeTDS driver.

## Architecture

Unlike the MSSQL FDW which uses the `tiberius` library with TDS 7.x protocol, this FDW uses ODBC with FreeTDS because:

1. SAP SQL Anywhere uses TDS 5.0 protocol (different from SQL Server's TDS 7.x)
2. The `tiberius` library only supports TDS 7.x/8.x (SQL Server protocol)
3. FreeTDS supports both TDS 5.0 (Sybase) and TDS 7.x (SQL Server)

## Dependencies

- `odbc-api`: Rust ODBC bindings
- `lazy_static`: Thread-safe static initialization for ODBC environment
- `thiserror`: Error handling

## System Requirements

1. **unixODBC**: ODBC driver manager
2. **FreeTDS**: TDS protocol implementation with ODBC driver

### Installation

**macOS:**
```bash
brew install unixodbc freetds
```

**Ubuntu/Debian:**
```bash
sudo apt-get install unixodbc unixodbc-dev freetds-dev
```

## ODBC Configuration

### Driver Configuration (`/etc/odbcinst.ini`)

```ini
[FreeTDS]
Description = FreeTDS Driver
Driver = /path/to/libtdsodbc.so
Setup = /path/to/libtdsodbc.so
FileUsage = 1
```

### Data Source Configuration (`/etc/odbc.ini`)

```ini
[SybaseDB]
Driver = FreeTDS
Server = hostname
Port = 2638
Database = dbname
TDS_Version = 5.0
```

## Connection Options

The FDW supports multiple connection methods:

1. **DSN-based**: Uses pre-configured ODBC data source
   ```sql
   OPTIONS (dsn 'SybaseDB', user 'user', password 'pass')
   ```

2. **Connection string**: Full ODBC connection string
   ```sql
   OPTIONS (conn_string 'DRIVER={FreeTDS};SERVER=host;PORT=2638;...')
   ```

3. **Individual options**: Host, port, database, user, password
   ```sql
   OPTIONS (host 'hostname', port '2638', database 'db', user 'u', password 'p')
   ```

## Supported Data Types

| PostgreSQL       | Sybase SQL Anywhere |
|------------------|---------------------|
| boolean          | bit                 |
| char             | tinyint             |
| smallint         | smallint            |
| real             | float(24)           |
| integer          | int                 |
| double precision | float(53)           |
| bigint           | bigint              |
| numeric          | numeric/decimal     |
| text             | varchar/char        |
| date             | date                |
| timestamp        | datetime/timestamp  |
| timestamptz      | datetime/timestamp  |

## Query Pushdown

Supports pushdown of:
- WHERE clauses
- ORDER BY clauses  
- LIMIT (using Sybase's `TOP` syntax)

## Build Requirements

When building, ensure the ODBC library path is in `LIBRARY_PATH`:

```bash
export LIBRARY_PATH="/opt/homebrew/opt/unixodbc/lib:$LIBRARY_PATH"
cargo build --features sybase_fdw
```

## Testing

Run tests with:

```bash
export LIBRARY_PATH="/opt/homebrew/opt/unixodbc/lib:$LIBRARY_PATH"
cargo pgrx test pg15 --package wrappers --features sybase_fdw
```

## Limitations

- Read-only (no INSERT, UPDATE, DELETE support)
- Requires FreeTDS ODBC driver installed on the system
- All data is fetched as text and converted to appropriate PostgreSQL types
