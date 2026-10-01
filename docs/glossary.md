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
| **Data-quality check** | A rule that describes a suspicious row (as a SQL condition) so we can count and look at such rows after every load |
| **Flag vs delete** | Flagging marks and reports a strange row; deleting destroys it. We only flag, because a strange value may be a real discovery |
| **Error vs warning** | Error = impossible (end before start); warning = merely suspicious (a gap over a year). Errors fail a run, warnings do not |
| **Exit code** | The number a command gives back when it finishes: 0 = fine, anything else = failed. Automation tools like GitHub Actions use it to stop a job |
| **Read-only connection** | Opening the database so it can be read but never changed; even a buggy rule cannot damage the data |
| **Tripwire (assumption) check** | A check that should never fire, like a star with a weak parallax after we asked only for strong ones; if it does, something we assumed has changed |
| **Plausible but wrong** | A value inside every allowed range that is still false (Sirius showing a brightness of 8.5); simple rules miss these, comparing columns can catch them |
| **Mean (average) vs median** | Mean = add everything up and divide; median = the middle value when sorted. A few huge values drag the mean up but barely move the median |
| **Robust statistic** | A summary that stays honest when outliers are present; the median is robust, the mean is not |
| **Band (shell)** | A slice of space between two distances (here, between two parallax values); fetching band by band keeps every answer under the row limit |
| **Overlap on purpose** | Letting neighbouring slices share an edge so nothing falls in the crack; the upsert makes the repeats harmless |
| **Replay** | Feeding saved raw files through the pipeline again, in their original order, to rebuild what came after them |
| **Idempotent** | Safe to run twice: the second run gives the same result as the first and breaks nothing |
| **Swap (build beside, then replace)** | Build the new file under a temporary name and rename it into place only once it is complete, so a failure never leaves a half-built file |
| **Sidecar (.meta.json)** | The small file next to each raw file recording where it came from, when it was fetched and its checksum; the rebuild reads the fetch time from it |
| **EXCEPT (SQL)** | "Rows in this table that are not in that one"; run both ways, it proves two tables hold the same rows |
| **Incremental fetch** | Fetching only what is new since last time instead of everything again |
| **Watermark** | A marker for how far we have already collected; here, the latest end date of a complete fetch for a region |
| **Overlap window** | Deliberately re-asking for the last few days, because sources add and revise recent records late; the upsert makes the repeats harmless |
| **Catch-up (throttle)** | When far behind, covering only a limited window per run and continuing next run, so no single run is huge |
| **Exit code 2 (here)** | Our signal for "finished, but the fetch may be incomplete", so a scheduled job can fail visibly instead of looking fine |
| **Backfill** | Filling in older data after the fact, as opposed to fetching only what is new |
| **Slice** | One small piece of a big job (a distance shell, a date window), sized so it can be fetched completely |
| **Queue (here)** | The ordered list of slices plus a saved record of which are done, so each run continues where the last stopped |
| **Shell (Gaia)** | The stars between two distances, like the layer of an onion; its edges are shared with the next shell so nothing is missed or counted twice |
| **State file** | A small file that remembers where a job got to between runs (`lake/state/backfill.json`) |
| **Throttle** | Deliberately limiting how much one run does (default one slice), to stay under rate limits and keep each run small |
| **Tangential speed** | The real sideways speed of a star in km/s: 4.74047 x proper motion (mas/yr) / parallax (mas). The conversion constant is how many km/s one arcsecond per year is at one parsec |
| **Relative to the Sun** | Speeds measured from where we are, so they include the Sun's own motion through the galaxy; fine for ranking, not the star's "true" speed |
| **Flyby** | A star passing close to the Sun. A close one could tug on the comet cloud and send comets inward |
| **Closest approach / miss distance** | The moment a star is nearest the Sun, and how near it gets. For straight-line motion the miss distance depends only on the sideways speed |
| **Myr** | Million years |
| **AU (astronomical unit)** | The Earth-Sun distance. One parsec is about 206,265 AU. The Sun's comet cloud is thought to reach out to roughly 50,000 to 100,000 AU |
| **Oort cloud** | A huge, far-out shell of icy bodies around the Sun, where many comets come from |
| **Straight-line approximation** | Pretending a star moves at constant speed in a straight line. Good for the next million years or so; the galaxy's gravity bends real paths over longer times |
| **Error bar (margin of error)** | How uncertain a measurement is, in the same units: a radial velocity of -10 km/s with an error of 0.2 is solid, -374 with an error of 80 is not |
| **rv_nb_transits** | How many times Gaia's telescope passed over a star while measuring its radial velocity. More passes usually means a steadier number |
| **Dataset (here)** | One kind of table we pull from a source, with its own raw folder, columns and warehouse table. A new dataset leaves the old ones untouched |
| **Join** | Matching rows of two tables by a shared key (here `source_id`) so each star's measurements and its error bars sit side by side |

| **Absolute magnitude** | How bright a star would look from a standard 10 parsecs away, so it shows true brightness. Formula: G + 5 x log10(parallax in mas) - 10. Smaller numbers are brighter |
| **Colour-magnitude diagram** | A plot of colour (bp_rp) against absolute magnitude. Stars are not scattered evenly: they gather in a few groups, so a star far from every group stands out |
| **Main sequence** | The big band of ordinary stars that fuse hydrogen in their cores, including the Sun. Redder ones are fainter and smaller |
| **White dwarf** | The leftover core of a dead Sun-like star: hot and bluish-white but tiny, so very faint. On the colour-magnitude diagram it sits well below the main sequence |
| **Detector** | A rule or model that flags rows that stand out. Ours only flags: it never changes or deletes data. A flag is a lead to check, not a verdict |
| **Neighbour count (density)** | How many other stars sit in a small box around one star on the colour-magnitude diagram. Few neighbours means the star is in a lonely spot. Used by `datalake.misfits` |
| **Exoplanet** | A planet orbiting a star other than our Sun. The NASA Exoplanet Archive lists the confirmed ones |
| **Host star** | The star a planet orbits. Our link between planets and Gaia is the host star's Gaia number |
| **Snapshot (of a table)** | A full copy of a whole table at one moment. Good for small tables that get revised, like the planet list: fetch it all, and let the upsert replace what changed |
| **Count check (completeness)** | Asking the archive "how many rows do you have?" and comparing with what we received. Better than guessing a row limit, because it works whatever the limit is |
| **pscomppars** | The archive's "Planetary Systems Composite Parameters" table: one row per known planet, with the best-known values combined from many papers |
| **Minimum mass (m sin i)** | The planet mass from the radial-velocity method is only a lower limit, because we do not know how tilted the orbit is. Most `pl_bmasse` values for Radial Velocity planets are this |
| **Classifier** | A machine-learning model that sorts rows into groups, here "has a known planet" or "not". It gives each star a probability, not just a yes/no |
| **Features** | The inputs a model learns from, here distance, brightness, colour, absolute magnitude and whether Gaia has a radial velocity |
| **Labels** | The answers the model learns to predict, here "is this star a known planet host?". Wrong labels teach the model wrong lessons |
| **Cross-validation** | Splitting the data into parts (folds), training on all but one and scoring the part left out, in turn. Every star is scored by a model that never saw it, so a high score is earned, not memorised |
| **AUC** | "Area under the curve": the chance the model ranks a random host above a random non-host. 0.5 = coin flip, 1.0 = perfect |
| **Logistic regression** | A simple classifier: a weighted sum of the features, squeezed into a probability between 0 and 1. Easy to inspect, a good first model |
| **Gradient boosting** | A stronger but harder-to-inspect classifier that builds many small decision trees, each fixing the last one's mistakes |
| **scikit-learn** | The standard Python library for classic machine learning (models, cross-validation, scores). Install with `pip install scikit-learn` |
| **Epoch (of a position)** | The date a sky position is for. Stars move, so a position needs a date: Gaia DR3 positions are for 2016, many catalogues use 2000 |
| **Cross-match** | Finding the same star in two catalogues. Best by a shared ID; otherwise by sky position, within a small radius |
| **Match radius** | How close (in arcseconds) two catalogue positions must be to count as the same star. Too big catches neighbours, too small misses real matches |
| **Out-of-fold score** | A star's score from the cross-validation model that was trained WITHOUT that star. The only honest score for "is this star surprising?" |
| **Weight (coefficient)** | How strongly logistic regression leans on a feature. Features are first put on a common scale (standardised), so weights can be compared: + pushes towards "host", - away |
| **Standardise** | Rescale a feature to average 0 and spread 1, so distance in parsecs and brightness in magnitudes can be compared fairly |
| **Average precision** | A score for how well the true hosts are packed at the top of the ranking. Random guessing scores the share of hosts (here about 0.03) |
| **Companion (star)** | Another star bound to ours, as in a binary (two stars) or triple. We count one when another star in our table sits at the same distance and closer than 1000 AU on the sky |
| **Projected separation** | How far apart two stars look, turned into AU: angle on the sky (arcsec) x distance (pc). It is a minimum, because one star may also sit in front of the other |
| **Mutation check** | Breaking the code on purpose (one small change) and checking that a test fails. If no test fails, the tests were not really guarding that line |
| **Spy (in a test)** | A wrapper around a real function that records how it was called. We use one to prove the scores really come from cross-validation |
