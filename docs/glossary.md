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
