# DLT-META People Demo

A Databricks Asset Bundle that demonstrates [DLT-META](https://github.com/databrickslabs/dlt-meta) — a metadata-driven framework for Delta Live Tables pipelines. Define your ingestion pipelines via JSON configuration instead of writing pipeline code.

## What this demo shows

- **Auto Loader ingestion** (CSV via `cloudFiles`) into a bronze Delta table
- **SCD Type 1 merge** from bronze to silver
- **Silver fanout** — one bronze table feeding multiple silver tables filtered by salary
- **Data quality expectations** at both bronze and silver layers (`expect`, `expect_or_drop`)
- **Custom transforms** injected via Python functions
- **Per-target configuration** — variables change between dev and prod with no code changes

## Project structure

```
.
├── databricks.yml                          # Bundle definition + per-target variables
├── setup.py                                # One-time setup: creates volume, uploads data + configs
├── resources/
│   ├── pipelines.yml                       # Bronze + Silver DLT pipeline definitions
│   └── jobs.yml                            # Onboarding + execution job definitions
├── conf/
│   ├── onboarding_bronze_silver_people.template   # Bronze/silver metadata (template)
│   ├── onboarding_silver_fanout_people.template   # Silver fanout metadata (template)
│   ├── silver_queries_people.json                 # Silver transformation SQL expressions
│   └── dqe/
│       ├── dqe_bronze_people.json                 # Bronze data quality rules
│       └── dqe_silver_people.json                 # Silver data quality rules
├── notebooks/
│   └── init_dlt_meta_pipeline.py           # DLT pipeline entry point notebook
└── data/
    └── people/
        └── people.csv                      # Sample source data (includes dirty rows for DQ demo)
```

## Prerequisites

- Databricks CLI configured with a profile (e.g., `test`)
- An existing Unity Catalog and schema (e.g., `users.eri_adepoju`)
- Python 3.10+

## Setup

### 1. Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install PyYAML databricks-sdk
```

### 2. Configure your target

Edit `databricks.yml` to set the catalog and schema for your target:

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog_name: users          # your catalog
      schema_name: eri_adepoju     # your schema
```

### 3. Run setup

```bash
python setup.py --profile <your-profile> --target dev
```

This creates a UC Volume, uploads source data and config files, and renders the onboarding templates with concrete paths.

### 4. Deploy the bundle

```bash
databricks bundle deploy --target dev --profile <your-profile>
```

### 5. Run the onboarding job

```bash
databricks bundle run onboard_people -t dev --profile <your-profile>
```

Creates the `bronze_dataflowspec_table` and `silver_dataflowspec_table` metadata tables that DLT-META reads at runtime.

### 6. Execute the pipelines

```bash
databricks bundle run execute_pipelines -t dev --profile <your-profile>
```

Runs the bronze pipeline (Auto Loader ingestion + DQ checks) then the silver pipeline (SCD Type 1 + transformations + DQ + fanout).

## Pipeline flow

```
people.csv (UC Volume)
    │
    ▼
┌─────────────────────────┐
│  Bronze Pipeline        │
│  - cloudFiles ingestion │
│  - DQE: drop null IDs   │
│    and null salaries    │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────────────┐
│  Silver Pipeline                │
│  - SCD Type 1 merge on `id`    │
│  - DQE: drop null IDs,         │
│    negative salaries            │
│  - DQE: warn on invalid SSN,   │
│    empty country, null names    │
│  - Fanout:                      │
│    → people_silver (all)        │
│    → people_silver_sal_above_50K│
│    → people_silver_sal_below_50K│
└─────────────────────────────────┘
```

## Data quality rules

**Bronze** (`expect_or_drop` — rows are dropped):
| Rule | Expression |
|------|-----------|
| `row_has_id` | `id IS NOT NULL` |
| `salary_is_number` | `salary IS NOT NULL` |

**Silver** (`expect_or_drop` — rows are dropped):
| Rule | Expression |
|------|-----------|
| `valid_id` | `id IS NOT NULL` |
| `positive_salary` | `salary >= 0` |

**Silver** (`expect` — rows pass through, violations logged):
| Rule | Expression |
|------|-----------|
| `valid_first_name` | `first_name IS NOT NULL AND first_name != ''` |
| `valid_ssn_format` | `ssn RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'` |
| `valid_country` | `country IS NOT NULL AND country != ''` |

## How variables work

Variables are defined in `databricks.yml` and overridden per target:

- `${var.catalog_name}` and `${var.schema_name}` are used in pipeline and job definitions (resolved by DAB at deploy time)
- Template placeholders (`{uc_catalog_name}`, `{uc_volume_path}`, etc.) are used in onboarding config files (resolved by `setup.py` before uploading to the volume)

This separation exists because DAB resolves `${var.*}` in YAML resources, but DLT-META reads onboarding/DQE/transformation configs at runtime via Spark, which requires concrete UC Volume paths.

## Cleanup

```bash
databricks bundle destroy --target dev --profile <your-profile>
```

To also remove the UC Volume:

```sql
DROP VOLUME users.eri_adepoju.dlt_meta_files;
```
