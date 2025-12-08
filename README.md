# LegiScan JSON Vote Exporter

This repo contains a lightweight toolchain for transforming LegiScan JSON archives into a structured Excel workbook for a single legislator. It focuses on the JSON bulk format (directories named `bill/`, `people/`, and `vote/`) and skips the CSV- and Streamlit-based flows. Point the scripts at one or more state archives, specify the legislator's exact name, and the exporter will aggregate every matching roll call into a shareable spreadsheet.

## Highlights
- Accepts one or more LegiScan JSON ZIP archives from the same state and extracts them into temporary staging areas.
- Automatically discovers every session directory that includes `bill`, `people`, and `vote` subfolders, ensuring only complete data sets are processed.
- Validates that all archives belong to a single state before any work begins (`determine_json_state`).
- Locates the requested legislator by exact name match inside the JSON `people` payloads and gathers every vote they cast across the supplied sessions.
- Builds per-party vote tallies (`For`, `Against`, `Absent`, `Not`) for each roll call so summary counts are captured alongside the legislator's own vote.
- Writes an Excel workbook (`openpyxl`) using the standardized `WORKBOOK_HEADERS` list - identical to the layout used elsewhere in the project.

## Streamlit Hosted App:
https://illumisathome-ocj7zc4myap2gpcmcrebpb.streamlit.app/

## Key Modules
| File | Purpose |
| --- | --- |
| `generate_json_vote_export.py` | CLI wrapper that parses arguments, resolves the archive list, and calls the exporter. |
| `json_vote_builder.py` | Core JSON workflow: loads people/bills/votes, filters by legislator, tallies results, and writes rows. |
| `json_legiscan_loader.py` | Helpers for extracting ZIPs, discovering session directories, streaming roll-call JSON, and determining the state code. |
| `generate_kristin_robbins_votes.py` | Provides shared constants (`WORKBOOK_HEADERS`, `VOTE_BUCKETS`) and the `write_workbook` helper reused by the JSON exporter. |

## Requirements
- Python 3.9+.
- Install dependencies (same set used by the broader repo):
  ```bash
  pip install -r requirements.txt
  ```
  Only `pandas` and `openpyxl` are required for the JSON CLI, but installing the full list keeps environments consistent.

## Preparing JSON Archives
1. Download LegiScan bulk JSON archives for a single state (LegiScan labels them per session, e.g., `MN_2023_RollCall_JSON.zip`).
2. Keep the ZIPs intact. Each archive should contain directories laid out like `<STATE>/<SESSION>/bill`, `people`, and `vote` with `*.json` files in each folder.
3. Store the ZIPs in a directory of your choice (defaults to `JSON DATA/` alongside the scripts).

> **Important:** Every archive passed into the exporter must belong to the same state. The loader infers the state code from the parent directory name above each session folder and raises an error if multiple states are detected.

## Running the Exporter
Basic usage (defaults to every `*.zip` found in `JSON DATA/`):
```bash
python generate_json_vote_export.py "Kristin Robbins"
```

Common flags:
- `--json-dir PATH` - Directory containing the ZIP archives (used when `--archives` is omitted).
- `--archives ZIP1 ZIP2 ...` - Explicit list of archive files to process.
- `--output PATH` - Target Excel file (default `json_vote_export.xlsx`).

The script prints the number of rows written. The workbook includes every roll call where the named legislator appears in the JSON `votes` array.

## Processing Pipeline
`generate_vote_export_from_json()` strings the steps together:
1. **Extract archives** - `extract_archives()` unzips each file into a temporary directory and tracks the cleanup handles.
2. **Locate session directories** - `gather_json_session_dirs()` scans the extracted trees for folders that contain `bill`, `people`, and `vote` subdirectories. Each matching folder represents one session's JSON payload.
3. **Validate state** - `determine_json_state()` inspects the parent directory names above each session to ensure the dataset covers exactly one state.
4. **Load reference maps** - For every session, `load_people_map()` and `load_bill_map()` build `people_id -> person` and `bill_id -> bill` dictionaries.
5. **Filter by legislator** - `collect_vote_rows_from_json()` matches the requested name exactly (case-sensitive) against the `person["name"]` field and skips sessions where the legislator does not appear.
6. **Iterate roll calls** - `iter_roll_calls()` streams each `vote/*.json` file, tallies party counts via `_tally_votes()`, and pulls contextual metadata (bill titles, committee, status, last action, roll-call result, etc.).
7. **Write the workbook** - `write_workbook()` (from `generate_kristin_robbins_votes.py`) appends the standardized headers, writes all collected rows, and saves the Excel file.

## Output Schema
Every row in the Excel export follows `WORKBOOK_HEADERS`, including:
- Core identifiers: chamber, session, bill number/ID, roll call ID, committee.
- Context: bill title/description, motion, last action + date, status code/description/date, source URL.
- Legislator data: person name, party (normalized to Democrat/Republican/Other), raw vote text, derived `Vote Bucket`, vote date, pass/fail result.
- Aggregated counts: party-level and total vote counts for each bucket so you can quickly see how the chamber split.

## Programmatic Use
If you want to embed this workflow elsewhere:
- Call `collect_vote_rows_from_json(session_dirs, "Name")` to get the raw rows list.
- Use `build_summary_dataframe_from_json(session_dirs, "Name")` to receive a `pandas.DataFrame` with extra `Date_dt` and `Year` columns for further analysis.
- Wrap your own archive discovery around `extract_archives()` + `gather_json_session_dirs()` if your data already lives on disk.

## Troubleshooting JSON Inputs
- **"No vote records found"** - The legislator's name must match exactly (including punctuation and middle initials). Confirm it appears inside the `people/*.json` files.
- **"No JSON session directories were found"** - Ensure each archive preserves the original `bill/`, `people/`, and `vote/` subfolders; some OS unzip tools flatten the structure.
- **"Multiple states detected"** - Mixes of states in the same run are blocked. Run the exporter separately per state or remove out-of-state archives from the list.
- **Missing votes in the output** - Only roll calls that include the named legislator's `people_id` in the `votes` array are exported. Session folders that do not contain the legislator are silently skipped.

This README intentionally focuses on the JSON ingestion/export path. The Streamlit UI and CSV builders remain in the repo but are outside the scope of this document.
