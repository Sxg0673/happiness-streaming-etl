# Happiness Streaming ETL

Streaming ETL pipeline with Apache Kafka and a pre-trained regression model that predicts the World Happiness score in real time (2015-2019 data).

Course: ETL (G01) - Workshop-3, Universidad Autonoma de Occidente.

## Project description

The project has two parts:

- **Part A (offline):** profile the five yearly World Happiness files, clean and harmonize them into one dataset, and train a regression model saved as `model.pkl`.
- **Part B (streaming):** a Kafka producer publishes each record as an event, and a consumer validates it, predicts the happiness score and stores raw events and predictions in a database used by the dashboard.

## Architecture

_Pending (full diagram at the end of the project)._

Part A is a Python package (`src/happiness_etl/`) run by `main.py`:

```
data/raw/*.csv -> extract -> clean -> transform -> validate (quality gate) -> load -> data/processed/happiness_unified.csv
```

| Module | Responsibility |
|---|---|
| `config.py` | Paths, years, canonical names and the column map of each year (single source of truth) |
| `extract.py` | Read the original CSV files as they are |
| `clean.py` | Per year: canonical column names, `year` column, country text, numeric types, exact duplicates |
| `transform.py` | Across years: merge, unify country names, interpolate missing values, sort |
| `validate.py` | Quality gate before loading and reconciliation after loading |
| `load.py` | Atomic CSV write and typed read of the unified dataset |

Intermediate steps stay in memory; only the final validated dataset is written to disk, because the training notebook and the Kafka producer read it from separate processes. The file is not versioned: it is regenerated with `main.py`.

## Data cleaning decisions

The data quality profiling is in [`notebooks/profiling.ipynb`](notebooks/profiling.ipynb) (Step 1 of the workshop). Main decisions:

| Finding | Decision |
|---|---|
| The same concept has up to three column names depending on the year | Map every year to canonical names (`config.COLUMN_MAP_BY_YEAR`) |
| Files have no year column | Add `year` from the source file |
| Columns not used: ranks, `Region`, standard error, confidence intervals, whiskers, `Dystopia Residual` | Dropped. Ranks and the residual are derived from the score, so using them would be target leakage |
| One missing value: `corruption` of United Arab Emirates in 2018 | Linear interpolation within the country (2017-2019 average, 0.253). Only between observed years, never extrapolated; logged as a warning |
| Six countries are written differently across years (e.g. `Trinidad & Tobago`) | Use the name that appears in most years (`config.COUNTRY_NAME_MAP`): 170 names become 164 countries |
| One zero per variable and year, and 102 IQR outliers | Kept: zeros are the minimum of each scale and the outliers are plausible values, not errors |
| `family` and `health` change scale between years | Not changed in cleaning; studied in the model EDA |

The quality gate (`validate.py`) stops the pipeline before writing if any rule fails, and reports all failures at once: exact schema, no nulls, non-empty country, year in 2015-2019, non-negative features, score between 0 and 10 and unique (country, year). Result: 782 rows, 164 countries, no missing values.

## Feature engineering decisions

_Pending._

## Kafka pipeline and event contract

_Pending._

## Database schema

_Pending._

## Dashboard

_Pending._

## Execution instructions

_Partial: Part A only for now._

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m happiness_etl.main   # builds data/processed/happiness_unified.csv
python -m pytest -q             # runs the tests
```

`main.py` exits with code 1 and lists the failed rules if the quality gate stops the pipeline.
