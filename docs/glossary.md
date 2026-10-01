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
| **Nested data** | A record that contains smaller records, like a gap event holding a `vessel` with its own id and flag |
| **Path (dotted)** | A way to point inside nested data: `vessel.id` means "the id inside vessel" |
| **Flatten** | Turning nested records into ordinary table columns, one value per column |
| **Missing vs. bad** | Missing: the value is absent or null (normal). Bad: it exists but does not fit its type (counted and reported) |
| **Boolean (bool)** | A true/false value |
| **Refactor** | Reorganizing code without changing what it does; the old tests prove nothing broke |
| **End-to-end test** | A test that runs the whole pipeline (fetch, raw, clean, warehouse, SQL) on fake data |
| **Collision** | Two different things trying to use the same name or path, so one overwrites the other |
| **Stem** | A filename without its extension: the stem of `north.csv` is `north` |
| **Reproduce first (failing test)** | Write a test that shows the bug before fixing it, so you know the fix is what made it pass |
| **Orchestration** | One function or command that runs several pipeline steps in order (fetch, raw, clean, warehouse) |
| **Smoke test** | A quick run of the real entry point to check that nothing obviously breaks |
| **CLI (command-line interface)** | Running a program by typing a command, like `python -m datalake.connectors.gfw 2022-01-01 2022-05-01` |
| **Idempotent** | Safe to run twice with the same result; clean and warehouse steps are, raw refuses instead |
| **git pull** | Download the newest commits from GitHub into your local copy of the repo |
| **Stale copy** | A local copy that is behind GitHub, so it is missing newer code |
| **Idempotent (in practice)** | `--raw` can be run again and again and gives the same result, because clean and warehouse are rebuilt from the unchanged raw file |
| **SSVID / MMSI** | A ship's broadcast ID number, sent by its tracker; its first three digits usually hint at the registration country |
| **Verified against live data** | Checked with real API output, not just fake test data; fake tests prove the code works, live runs prove our assumptions about the source are right |
| **Upsert** | "Update or insert": if a row with the same key exists, replace it; otherwise add it |
| **Key (natural key)** | The column(s) that uniquely identify a row; for a gap event, the vessel id plus the start time |
| **Deduplicate** | Removing repeated copies of the same record so each appears once |
| **Lineage** | A record of where each row came from; here the `_source_file` column |
| **Transaction** | A group of database steps that all succeed together or are all undone, so a failure never leaves half-finished changes |
| **Rollback** | Undoing a transaction's steps after a failure |
| **Schema drift** | The shape of incoming data changing (new or missing columns); we stop loudly instead of mixing shapes |
| **Mutation check** | Deliberately breaking code to prove the tests notice; a test that cannot fail proves nothing |
| **Hidden (transitive) dependency** | A library your code needs only because another library uses it; if that library drops it, things break (pytz did) |
| **Local time vs UTC** | Local time shifts by place; UTC does not. We store and show UTC so data looks the same on every machine |
| **Window function** | SQL that looks at a row together with its group, e.g. "how many rows share this vessel and start time?" |
| **Glob (wildcard)** | A pattern like `*.parquet` that matches many files at once |
| **Duplicate vs different record** | Two rows sharing a key might be true copies or genuinely different events; check before assuming |
| **True duplicate** | Two records identical in every field; safe to collapse into one |
| **Overlap (date window)** | An event counts as inside a date range if any part of it overlaps the range, so an event can start before and end after the window |
| **Outlier** | A value far outside the normal range (a 3.7-year gap among gaps measured in hours or days); it can distort averages and models |
| **Pagination (paging)** | An API sending a long list in pages (here 100 events each); you ask for page after page until it runs out |
| **Offset** | "Skip this many results first"; page 3 of 100 means offset 200 |
| **Truncation** | Getting only part of the data without being told; a result of exactly the page limit is the warning sign |
| **Rate limit** | How many requests a source lets you make per day or month; GFW allows 50,000 a day |
| **HTTP 429** | The "too many requests" reply; GFW blocks you for a day or a month after it |
| **Complete vs incomplete fetch** | Whether we know we got everything; an incomplete fetch is flagged loudly, never passed off as full |
| **Gaia** | A European Space Agency telescope that measures the position, distance and motion of over a billion stars; DR3 is its third public data release |
| **Parallax** | The tiny yearly wobble in a star's position as Earth orbits the Sun; the bigger the wobble, the closer the star. Measured in milliarcseconds (mas) |
| **Parsec (pc)** | A distance unit, about 3.26 light-years; a star with a parallax of 50 mas is 20 pc away |
| **Proper motion** | How far a star drifts across the sky each year (mas per year) |
| **Magnitude (G mag)** | Brightness on a backwards scale: a smaller number is brighter; G is Gaia's own colour band |
| **bp_rp** | A star's colour: its blue brightness minus its red brightness; bigger means redder (the Sun is about 0.8) |
| **Radial velocity** | Speed toward or away from us in km/s; many stars have none measured, so it is null |
| **TAP** | Table Access Protocol: astronomy's standard way to ask an archive a question over the web |
| **ADQL** | Astronomical Data Query Language: SQL with extras for sky positions, used to question TAP archives |
| **Synchronous vs asynchronous query** | Sync waits for the answer in one request (quick, small); async runs in the background (slower, bigger) |
| **Rule of three** | Wait until you have copied something three times before sharing it as common code; two near-copies are cheaper than a wrong shared design |

