# Wisconsin NG911 Data Pipeline

This repository contains the preparation pipeline used to standardize Wisconsin NG911 Site/Structure Address Points into county-level GeoParquet files for Territory Toolbox.

## What belongs in this repository

- `prepare_wi_ng911_counties.py`
- dependency and documentation files
- future tests and source adapters

## What must not be committed

- the statewide NG911 ZIP
- extracted `.gdb` directories
- full-fidelity GeoParquet files
- runtime county GeoParquet files
- generated reports containing large extracts

The source and generated data remain local during preparation. Approved runtime county files will later be uploaded to Cloudflare R2.

## 1. Install Python

Use Python 3.11 or 3.12. Python 3.12 is recommended.

Confirm installation:

```bash
python --version
```

On Windows, use `py` instead of `python` if needed.

## 2. Clone the repository

```bash
git clone https://github.com/YOUR-USERNAME/wi-ng911-data-pipeline.git
cd wi-ng911-data-pipeline
```

## 3. Create a virtual environment

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Windows Command Prompt:

```bat
py -3.12 -m venv .venv
.venv\Scripts\activate.bat
```

macOS/Linux:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

## 4. Install dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Verify File Geodatabase support:

```bash
python -c "import pyogrio; print(pyogrio.list_drivers().get('OpenFileGDB'))"
```

A readable driver value should be displayed.

## 5. Add the source locally

Place the downloaded statewide ZIP inside:

```text
data/source/
```

Example:

```text
data/source/WI_NG911_Site_Structure_Address_Points.zip
```

The ZIP is ignored by Git and will not be uploaded to GitHub.

## 6. Run the initial validation group

Windows PowerShell:

```powershell
python prepare_wi_ng911_counties.py `
  --input "data/source/WI_NG911_Site_Structure_Address_Points.zip" `
  --output "data/output/wi_ng911_pipeline" `
  --counties Waukesha Adams Dane Milwaukee Crawford
```

Windows Command Prompt:

```bat
python prepare_wi_ng911_counties.py ^
  --input "data/source/WI_NG911_Site_Structure_Address_Points.zip" ^
  --output "data/output/wi_ng911_pipeline" ^
  --counties Waukesha Adams Dane Milwaukee Crawford
```

One-line alternative:

```bash
python prepare_wi_ng911_counties.py --input "data/source/WI_NG911_Site_Structure_Address_Points.zip" --output "data/output/wi_ng911_pipeline" --counties Waukesha Adams Dane Milwaukee Crawford
```

## 7. Review outputs

The run creates folders including:

```text
data/output/wi_ng911_pipeline/
├── full_fidelity/
├── runtime/
├── quarantine/
├── reports/
├── manifest/
├── source_metadata/
└── logs/
```

Review these first:

- `reports/county_summary.csv`
- `reports/run_summary.json`
- `manifest/coverage_manifest.csv`
- `manifest/coverage_manifest.json`
- the processing log

Do not upload runtime files to Cloudflare R2 until their validation status and county coverage have been reviewed.

## 8. Run the full represented-state dataset

After the test counties pass:

```bash
python prepare_wi_ng911_counties.py --input "data/source/WI_NG911_Site_Structure_Address_Points.zip" --output "data/output/wi_ng911_pipeline"
```

Add `--overwrite` only when intentionally replacing an existing generated release.

## Repository workflow

For normal changes:

```bash
git status
git add prepare_wi_ng911_counties.py README.md requirements.txt .gitignore
git commit -m "Set up Wisconsin NG911 preparation pipeline"
git push
```

Generated data should not appear in `git status`. If it does, stop before committing and check `.gitignore`.

## Current coverage policy

- Waukesha: validated statewide source
- Milwaukee: county-specific override required
- Crawford: incomplete statewide coverage
- Iowa, Kewaunee, Lafayette, Langlade, Oneida, Taylor and Vilas: unavailable in the current statewide source
- Other represented counties: require validation before public release
