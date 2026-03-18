# AI Economic Early Warning System (EWS) v1

A practical, research-oriented **AI Economic Early Warning System** that monitors whether rapid AI progress is beginning to create economic stress through four channels:

1. labor displacement and wage pressure
2. falling cost of useful intelligence
3. concentration of economic power
4. broader macro-financial fragility

This repository ships as a **working end-to-end project** with a local data pipeline, reproducible storage layers, composite indices, a browser dashboard, and documented methodology.

## What works now

- Source-specific ingestion adapters with raw snapshot metadata.
- Reproducible storage layers in `data/raw/`, `data/processed/`, `data/marts/`, and `data/reference/`.
- Four core indices:
  - AI Affordability Index
  - Labor Substitution Pressure Index
  - AI Concentration Index
  - Economic Fragility Index
- A local dashboard served from `app/` with overview, module sections, methodology, and data source registry.
- Basic automated tests for index math and end-to-end pipeline execution.

## Why this implementation is dependency-light

The execution environment available for this build does not allow installing external Python packages. To ensure the project is still **fully runnable**, v1 is implemented with the Python standard library only.

That means:

- storage uses **CSV + JSON** rather than Parquet in this environment
- the dashboard is a **local static HTML app served by Python's built-in HTTP server** rather than Streamlit
- the methodology and pipeline architecture still mirror the requested EWS design closely

If you later run this project in a normal development environment, it can be upgraded to pandas/Parquet/Streamlit with the same data model and directory structure.

## Architecture

```text
app/
  dashboard.py
  index.html
  dashboard_data.json
src/ews/
  config/
  ingest/
  process/
  indices/
  utils/
data/
  raw/
  processed/
  marts/
  reference/
tests/
```

## Data strategy

The dashboard **never calls live external APIs at render time**.

The system uses a hybrid pattern:

- **attempted live access** to official AI pricing pages for metadata capture
- **deterministic local fallback snapshots** for reproducibility and offline resilience
- **processed marts** saved to local files for dashboard use
- **editable reference tables** for v1 concentration and occupational exposure inputs

## Index methodology

### 1) AI Affordability Index

Measures how fast useful AI is becoming cheaper.

- Capability score = mean of curated benchmark measures (`mmlu`, `gpqa`, `swe_bench`)
- Cost proxy = mean of input and output API prices per 1M tokens
- Raw affordability = capability / cost
- Final index = min-max normalization to a 0–100 scale

### 2) Labor Substitution Pressure Index

Measures pressure on AI-exposed occupations.

For occupation *i* at time *t*:

- `SubstitutionPressure_it = AIExposure_i × AIAffordability_t × WageIndexed_it`
- `RiskScore_it = SubstitutionPressure_it × (100 / postings_index_it) × (100 / employment_index_it)`
- `LSPI_t` = employment-weighted average of occupation risk scores, then normalized to 0–100

### 3) AI Concentration Index

Measures concentration of power in AI access markets.

- Uses a Herfindahl-Hirschman Index (HHI): `HHI = Σ s_f^2`
- Shares come from `data/reference/concentration_reference.csv`
- v1 labels these values explicitly as **manual proxy** inputs

### 4) Economic Fragility Index

Measures whether AI-related adjustment strain is spilling into the broader economy.

Composite raw score includes:

- wage-productivity divergence
- unemployment rate
- job-openings weakness
- sector stress proxy
- AI affordability spillover

Weighted raw composite is then normalized to 0–100.

## Source registry

See `data/reference/source_registry.csv` for the current source inventory. In v1:

- **local-curated**: capability snapshot, occupational exposure table, concentration reference table
- **fallback-backed**: macro/labor time series, occupational labor panel, AI pricing history

## Assumptions and limitations

- This is **v1**, focused on monitoring rather than scenario analysis.
- Some data sources remain **proxy or fallback datasets** to keep the system functional and reproducible without credentials.
- AI pricing adapters currently attempt official vendor pages but do not yet parse full historical price histories automatically.
- Labor and macro integrations should later be upgraded to official production connectors.
- Concentration shares are transparent manual proxies and should be revised when stronger evidence is available.

## Setup

No third-party dependencies are required for this environment. Run commands directly with `PYTHONPATH=src`.

## Run the pipeline

```bash
PYTHONPATH=src python -m ews.run_pipeline
```

This populates:

- `data/raw/` with raw snapshots and metadata
- `data/processed/` with processed intermediate files
- `data/marts/` with dashboard-ready JSON and CSV files
- `app/dashboard_data.json` with the dashboard payload

## Run the dashboard

```bash
python app/dashboard.py
```

Then open:

```text
http://127.0.0.1:8501/index.html
```

## Run tests

```bash
PYTHONPATH=src pytest
```

## Exact commands for evaluation

```bash
python -m pip install -r requirements.txt
PYTHONPATH=src python -m ews.run_pipeline
PYTHONPATH=src pytest
python app/dashboard.py
```

## What still needs real-world source hookup

1. Historical AI pricing extraction from official provider pages or archived price logs.
2. Ongoing benchmark ingestion from a maintained benchmark dataset/API.
3. Official macro/labor connectors such as FRED/BLS.
4. Production job-postings ingestion from a real labor demand source.
5. Stronger concentration evidence than the current manual proxy share table.

## Portfolio framing

This repo is designed to be cloneable, runnable, inspectable, and extensible. It emphasizes:

- transparent methodology
- reproducible local data artifacts
- modular ingest/process/index code
- dashboard usability
- explicit labeling of real vs proxy/fallback inputs
