# Glossary

| Term | Meaning |
|---|---|
| **Data lake** | A store of raw data files from many sources, kept as received |
| **Data warehouse** | Structured, cleaned tables organized for asking questions with SQL |
| **Pipeline** | Automated steps that move data from a source through cleaning into storage |
| **Raw / clean / curated** | The three layers: untouched, tidied, query-ready (the "medallion" layout) |
| **Schema** | A named section inside a warehouse that groups related tables (e.g. `ocean`, `astro`) |
| **Provenance** | A record of where data came from, when, and that it is unchanged |
| **Checksum (sha256)** | A fingerprint of a file's bytes; any change to the file changes it |
| **Sidecar file** | A small companion file stored next to a data file, holding its metadata |
| **Parquet** | A compact, typed, column-based file format for tables (used in the clean layer) |
| **DuckDB** | A free database that runs as a library and queries files with SQL, no server needed |
| **Connector** | Code that knows how to fetch one source |
| **Feature** | A column a machine-learning model learns from |
| **Type (dtype)** | What kind of value a column holds: text, whole number (int), decimal (float), timestamp |
| **Null** | The marker for "value missing or unusable" (not the same as empty text or zero) |
| **PyArrow** | The Python library that reads and writes Parquet files |
| **Parser** | A small function that turns text like `"12.5"` into a real number |
| **Re-creatable** | Clean files can be deleted and rebuilt from raw at any time, so they may be overwritten |
| **CSV** | "Comma-separated values": a plain-text table, one row per line, columns split by commas |
| **Row / column** | A row is one record (one fish sighting); a column is one field across all records (depth) |
| **Table** | Rows and columns with a fixed set of typed columns |
| **Timestamp** | A value holding an exact date and time |
| **UTC** | The world-standard time zone, used so times from different places compare correctly |
| **Test (pytest)** | Code that checks our code works; `pytest` is the tool that runs them |
| **Fake data** | Small made-up data used in tests, so tests never touch real data |
| **Dependency** | An outside library our code needs, listed in `pyproject.toml` (e.g. pyarrow) |
| **Editable install** | `pip install -e .` makes our `src/` code importable while we keep editing it |
| **Commit / push** | A commit saves a snapshot of changes in git; a push sends commits to GitHub |
| **SQL** | The language for asking questions of tables ("show me all fish deeper than 20m") |
| **Query** | One question written in SQL |
| **Warehouse table** | A curated table inside the warehouse, loaded from a clean Parquet file |
| **Schema (warehouse)** | A named section grouping a domain's tables, e.g. `ocean.sightings` |
| **Read-only** | Opened so questions can be asked but the data cannot be changed |
| **SQL injection** | A bug where untrusted text becomes SQL commands; avoided with parameters and name checks |
| **Parameter (SQL)** | A safe placeholder (`?`) for a value, so text is never treated as commands |
| **API** | A web address a program calls to request data from a service (here, Global Fishing Watch) |
| **API token** | A secret password-like string that proves who you are to an API; never goes in code or git |
| **Environment variable** | A named setting outside your code, like `GFW_API_ACCESS_TOKEN`, where secrets can live |
| **`.env` file** | A git-ignored text file holding secrets for local use |
| **Client** | A library object that makes the API calls for you |
| **Fake client** | A stand-in client in tests that returns made-up data, so tests need no network or token |
| **Dependency injection** | Passing the client into a function instead of creating it inside, so tests can swap in a fake |
| **async / await** | Python's way to wait for slow network replies without freezing; the GFW client uses it |
| **JSON** | A text format for nested data (a record can hold smaller records); used when a table is too flat |
| **AIS gap event** | A period when a ship's tracking transmitter went silent ("went dark") |
| **Optional dependencies** | Extra libraries only some features need, installed with `pip install -e ".[gfw]"` |
