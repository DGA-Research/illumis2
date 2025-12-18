# LegiScan Vote Explorer & JSON Exporter

This repository contains two complementary ways to analyze LegiScan data:

1. **Streamlit Vote Explorer UI** - interactive exploration of CSV archives with filtering and exports.
2. **JSON Vote Exporter CLI** - a lightweight script that converts LegiScan JSON archives into an Excel workbook for one legislator.

Use whichever workflow fits your data ingestion process, or combine both for broader coverage.

---

## Streamlt Hosted App Link:
https://illumis2-gev8nx3exq5m3c9hj8xxld.streamlit.app/


## JSON Vote Exporter CLI

The CLI workflow consumes LegiScan JSON archives (directories named `bill/`, `people/`, and `vote/`) and produces an Excel workbook for a single legislator.

### Remote JSON Archives (GCS Beta)
Set these environment variables (or add them to .streamlit/secrets.toml) to source archives from Google Cloud Storage:
 - ILLUMIS_GCS_BUCKET - bucket that stores the JSON ZIPs and manifest.
 - ILLUMIS_GCS_MANIFEST - optional blob name (default manifest.json) listing archives per state.
 - ILLUMIS_ARCHIVE_CACHE_DIR - optional local cache directory for downloaded ZIPs (defaults to the system temp dir).
 - When present, the app lists archives per selected state by reading the manifest, downloads only the needed ZIPs into the cache, and reuses them until the manifest reports a newer updated value.

### Highlights
- Accepts one or more LegiScan JSON ZIP archives from the same state and extracts them into temporary staging areas.
- Automatically discovers every session directory that includes `bill`, `people`, and `vote` subfolders, ensuring only complete datasets are processed.
- Validates that all archives belong to a single state before any work begins (`determine_json_state`).
- Locates the requested legislator by exact name match inside the JSON `people` payloads and gathers every vote they cast across the supplied sessions.
- Builds per-party vote tallies (`For`, `Against`, `Absent`, `Not`) for each roll call so summary counts are captured alongside the legislator's own vote.
- Writes an Excel workbook (`openpyxl`) using the standardized `WORKBOOK_HEADERS` list shared with the Streamlit exports.

### Requirements
- Python 3.9+.
- Install dependencies (same `requirements.txt` as the Streamlit app):
  ```bash
  pip install -r requirements.txt
  ```
  `pandas` and `openpyxl` are the only packages strictly required for the CLI, but installing the full list keeps environments consistent.

### Preparing JSON Archives
1. Download LegiScan bulk JSON archives for a single state (LegiScan labels them per session, e.g., `MN_2023_RollCall_JSON.zip`).
2. Keep the ZIPs intact. Each archive should contain directories laid out like `<STATE>/<SESSION>/bill`, `people`, and `vote` with `*.json` files in each folder.
3. Store the ZIPs in a directory of your choice (defaults to `JSON DATA/` alongside the scripts).

> **Important:** Every archive passed into the exporter must belong to the same state. The loader infers the state code from the parent directory name above each session folder and raises an error if multiple states are detected.

### Running the Exporter
Basic usage (defaults to every `*.zip` found in `JSON DATA/`):
```bash
python generate_json_vote_export.py "Kristin Robbins"
```

Common flags:
- `--json-dir PATH` - Directory containing the ZIP archives (used when `--archives` is omitted).
- `--archives ZIP1 ZIP2 ...` - Explicit list of archive files to process.
- `--output PATH` - Target Excel file (default `json_vote_export.xlsx`).

The script prints the number of rows written. The workbook includes every roll call where the named legislator appears in the JSON `votes` array.

### Processing Pipeline
`generate_vote_export_from_json()` strings the steps together:
1. **Extract archives** - `extract_archives()` unzips each file into a temporary directory and tracks the cleanup handles.
2. **Locate session directories** - `gather_json_session_dirs()` scans the extracted trees for folders that contain `bill`, `people`, and `vote` subdirectories. Each matching folder represents one session's JSON payload.
3. **Validate state** - `determine_json_state()` inspects the parent directory names above each session to ensure the dataset covers exactly one state.
4. **Load reference maps** - For every session, `load_people_map()` and `load_bill_map()` build `people_id -> person` and `bill_id -> bill` dictionaries.
5. **Filter by legislator** - `collect_vote_rows_from_json()` matches the requested name exactly (case-sensitive) against the `person["name"]` field and skips sessions where the legislator does not appear.
6. **Iterate roll calls** - `iter_roll_calls()` streams each `vote/*.json` file, tallies party counts via `_tally_votes()`, and pulls contextual metadata (bill titles, committee, status, last action, roll-call result, etc.).
7. **Write the workbook** - `write_workbook()` (from `generate_kristin_robbins_votes.py`) appends the standardized headers, writes all collected rows, and saves the Excel file.

### Output Schema
Each row in the Excel export follows `WORKBOOK_HEADERS`, providing:
- Core identifiers: chamber, session, bill number/ID, roll call ID, committee.
- Context: bill title/description, motion, last action + date, status code/description/date, source URL.
- Legislator data: person name, party (normalized to Democrat/Republican/Other), raw vote text, derived `Vote Bucket`, vote date, pass/fail result.
- Aggregated counts: party-level and total vote counts for each bucket.

### Programmatic Use
- Call `collect_vote_rows_from_json(session_dirs, "Name")` to get the raw rows list.
- Use `build_summary_dataframe_from_json(session_dirs, "Name")` to receive a `pandas.DataFrame` with extra `Date_dt` and `Year` columns for further analysis.
- Wrap your own archive discovery around `extract_archives()` + `gather_json_session_dirs()` if your data already lives on disk.

### CLI Troubleshooting
- **"No vote records found"** - The legislator name must match exactly (including punctuation and middle initials). Confirm it appears inside the `people/*.json` files.
- **"No JSON session directories were found"** - Ensure each archive preserves the original `bill/`, `people`, and `vote` subfolders; some OS unzip tools flatten the structure.
- **"Multiple states detected"** - Mixes of states in the same run are blocked. Run the exporter separately per state or remove out-of-state archives from the list.
- **Missing votes in the output** - Only roll calls that include the named legislator `people_id` in the `votes` array are exported. Session folders that do not contain the legislator are silently skipped.

---

Both workflows share the constants and helpers in `generate_kristin_robbins_votes.py`. Use the Streamlit UI for exploratory analysis and narrative exports, and use the JSON CLI when you need a quick spreadsheet straight from LegiScan JSON archives.
