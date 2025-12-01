# LegiScan Vote Explorer

LegiScan Vote Explorer is a Streamlit application for exploring how a single legislator voted or sponsored legislation across LegiScan bulk downloads. Combine multiple session archives from one state, filter them by context, and export shareable summaries in Excel or Word.

## Highlights
- Combine LegiScan session archives from the same state, whether uploaded during a session or selected from the repository's `bulkLegiData` cache.
- Automatically detect the reporting state, available legislators, the latest bill action date, and calendar years present in the data set.
- Switch between multiple perspectives: All Votes, Votes Against Party, Votes With Person, Votes Against Person, Minority Votes, Deciding Votes, Skipped Votes, Sponsored/Cosponsored Bills, and keyword search.
- Sponsored/Cosponsored view deduplicates by LegiScan `bill_id` and surfaces status metadata (`Status`, `Status Description`, `Status Date`, and `Last Action`) so every sponsored bill appears once.
- Persist processing results in the Streamlit session so vote tables and bullet selections remain intact until uploads or filters change.
- Export the active view as an Excel sheet, a multi-view workbook, or a Word bullet summary that honors the per-row `BULLET?` checkboxes.
- Optionally store newly uploaded archives in `bulkLegiData` and mirror them to GitHub when API credentials are configured.

## Access Options
- Hosted app: https://illumisathome-ocj7zc4myap2gpcmcrebpb.streamlit.app/
- Local run: clone the repo and follow the setup steps below.

## Local Prerequisites (optional)
- Python 3.9 or newer.
- Pip for installing Python packages.
- LegiScan bulk download ZIP archives that include `bills.csv`, `people.csv`, `rollcalls.csv`, and `votes.csv` within their `.../csv/` folders.

## Local Setup (optional)
1. Clone or download this repository.
2. (Optional) Create and activate a virtual environment:  
   `python -m venv .venv` followed by `.venv\Scripts\activate` (Windows) or `source .venv/bin/activate` (macOS/Linux).
3. Install dependencies:  
   `pip install -r requirements.txt`  
   *(Alternatively, install `streamlit`, `pandas`, and `openpyxl` manually.)*

## Run the App Locally (optional)
1. From the project directory, launch Streamlit:
   ```bash
   streamlit run streamlit_app.py
   ```
2. Open the reported local URL (default `http://localhost:8501`) in your browser.

## App Workflow
1. **Load LegiScan data**  
   Upload one or more ZIP archives for a single state, or pick from the bundled archives stored in `bulkLegiData`. The sidebar state filter helps narrow the bundled list, and the app flags skipped uploads, duplicates, and archives that are already up to date.
2. **Pick a legislator & configure filters**  
   After processing the archives, the app detects the state, the available legislators, and calendar years. Choose a legislator, select a vote perspective, optionally enter a search term, and adjust any sliders/dropdowns that appear. Press **Generate current view summary** to cache the results for the current inputs.
3. **Review the vote breakdown**  
   The main table mirrors the exported Excel columns and includes party/chamber counts, comparison columns, and the `BULLET?` checkbox column. Toggle rows you want called out in the Word summary, then click **Update bullet selections** to persist the toggles. When no rows are selected, the bullet summary falls back to every filtered record.
4. **Export results**  
   Use **Download filtered Excel sheet** for the current view, **Download bullet summary** for a DOCX narrative, or **Generate all views workbook** to build a multi-tab Excel file covering the key perspectives. The app reuses cached data so subsequent downloads are immediate unless you change inputs or reload the page.

## Sponsor and Status Details
The `Sponsored/Cosponsored Bills` view deduplicates by LegiScan `bill_id` so each sponsored bill appears exactly once, even if the bill number repeats across sessions. The exported data now includes the latest status code, status description, status date, and last action captured from LegiScan alongside the sponsorship role.

## Managing Bundled Archives
Archives saved under `bulkLegiData` appear in the sidebar selector. Use **Select all bundled** or **Clear bundled** to manage the current choice. When `st.secrets["github"]` credentials are provided, newly saved archives are automatically uploaded to the configured GitHub repository; otherwise they remain local for reuse in future sessions.

## Troubleshooting
- **Invalid ZIP** - Streamlit highlights the archive; re-download it and try again.
- **Mixed states detected** - Upload archives from only one state at a time; remove out-of-state files before rerunning.
- **No votes found** - Confirm the legislator exists in the uploaded sessions and loosen filters (search terms, comparison legislator, minority thresholds).
- **Bullet selections not sticking** - Make sure to click **Update bullet selections** after toggling the `BULLET?` column, and regenerate the view if you change filters.
