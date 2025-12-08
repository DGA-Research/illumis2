# LegiScan Vote Explorer & JSON Exporter

This repository contains two complementary ways to analyze LegiScan data:

1. **Streamlit Vote Explorer UI** - interactive exploration of CSV archives with filtering and exports.
2. **JSON Vote Exporter CLI** - a lightweight script that converts LegiScan JSON archives into an Excel workbook for one legislator.

Use whichever workflow fits your data ingestion process, or combine both for broader coverage.

---

## Streamlit Vote Explorer (CSV-Based UI)

### Highlights
- Combine LegiScan session archives from the same state, whether uploaded during a session or selected from the repository `bulkLegiData` cache.
- Automatically detect the reporting state, available legislators, the latest bill action date, and calendar years present in the dataset.
- Switch between perspectives: All Votes, Votes Against Party, Votes With Person, Votes Against Person, Minority Votes, Deciding Votes, Skipped Votes, Sponsored/Cosponsored Bills, and keyword search.
- Sponsored/Cosponsored view deduplicates by LegiScan `bill_id` and surfaces status metadata (`Status`, `Status Description`, `Status Date`, `Last Action`).
- Persist processing results in the Streamlit session so vote tables and bullet selections remain intact until uploads or filters change.
- Export the active view as an Excel sheet, a multi-view workbook, or a Word bullet summary that honors the per-row `BULLET?` checkboxes.
- Optionally store newly uploaded archives in `bulkLegiData` and mirror them to GitHub when API credentials are configured.

### Access Options
- Hosted app: https://illumisathome-ocj7zc4myap2gpcmcrebpb.streamlit.app/
- Local run: clone the repo and follow the setup steps below.

### Local Prerequisites
- Python 3.9 or newer.
- Pip for installing Python packages.
- LegiScan bulk download ZIP archives that include `bills.csv`, `people.csv`, `rollcalls.csv`, and `votes.csv` inside their `.../csv/` folders.

Install dependencies (Streamlit, pandas, openpyxl, google-cloud-storage, etc.) with:
```bash
pip install -r requirements.txt
```

### Local Setup and Launch
1. Clone or download this repository.
2. (Optional) Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # macOS/Linux
   source .venv/bin/activate
   ```
3. Install dependencies (see above).
4. Run the app:
   ```bash
   streamlit run streamlit_app.py
   ```
5. Open the reported local URL (default `http://localhost:8501`) in your browser.

### Workflow
1. **Load LegiScan data** - Upload one or more ZIP archives for a single state, or pick from the bundled archives stored in `bulkLegiData`. The sidebar state filter helps narrow the bundled list, and the app flags skipped uploads, duplicates, and archives that are already up to date.
2. **Pick a legislator and configure filters** - After processing the archives, the app detects the state, the available legislators, and calendar years. Choose a legislator, select a vote perspective, optionally enter a search term, and adjust any sliders/dropdowns that appear. Press **Generate current view summary** to cache the results for the current inputs.
3. **Review the vote breakdown** - The main table mirrors the exported Excel columns and includes party/chamber counts, comparison columns, and the `BULLET?` checkbox column. Toggle rows you want called out in the Word summary, then click **Update bullet selections** to persist the toggles. When no rows are selected, the bullet summary falls back to every filtered record.
4. **Export results** - Use **Download filtered Excel sheet** for the current view, **Download bullet summary** for a DOCX narrative, or **Generate all views workbook** to build a multi-tab Excel file covering the key perspectives. The app reuses cached data so subsequent downloads are immediate unless you change inputs or reload the page.

### Sponsor and Status Details
The `Sponsored/Cosponsored Bills` view deduplicates by LegiScan `bill_id` so each sponsored bill appears exactly once, even if the bill number repeats across sessions. The exported data includes the latest status code, status description, status date, and last action captured from LegiScan alongside the sponsorship role.

### Managing Bundled Archives
Archives saved under `bulkLegiData` appear in the sidebar selector. Use **Select all bundled** or **Clear bundled** to manage the current choice. When `st.secrets["github"]` credentials are provided, newly saved archives are automatically uploaded to the configured GitHub repository; otherwise they remain local for reuse in future sessions.

### Remote JSON Archives (GCS Beta)
Set these environment variables (or add them to `.streamlit/secrets.toml`) to source archives from Google Cloud Storage:
- `ILLUMIS_GCS_BUCKET` - bucket that stores the JSON ZIPs and manifest.
- `ILLUMIS_GCS_MANIFEST` - optional blob name (default `manifest.json`) listing archives per state.
- `ILLUMIS_ARCHIVE_CACHE_DIR` - optional local cache directory for downloaded ZIPs (defaults to the system temp dir).

When present, the app lists archives per selected state by reading the manifest, downloads only the needed ZIPs into the cache, and reuses them until the manifest reports a newer `updated` value.

### Streamlit Troubleshooting
- **Invalid ZIP** - Streamlit highlights the archive; re-download it and try again.
- **Mixed states detected** - Upload archives from only one state at a time; remove out-of-state files before rerunning.
- **No votes found** - Confirm the legislator exists in the uploaded sessions and loosen filters (search terms, comparison legislator, minority thresholds).
- **Bullet selections not sticking** - Click **Update bullet selections** after toggling the `BULLET?` column, and regenerate the view if you change filters.

---

## JSON Vote Exporter CLI

The CLI workflow consumes LegiScan JSON archives (directories named `bill/`, `people/`, and `vote/`) and produces an Excel workbook for a single legislator.

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
