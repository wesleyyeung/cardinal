# CARDINAL

**CARDINAL** is an automated clinical data processing pipeline designed to streamline the ingestion, cleaning, and transformation of healthcare data into standardized research-ready formats. It is built for hospital-scale datasets with support for multiple EHR and cardiology information systems.

## Overview

CARDINAL supports a multi-stage pipeline:
1. **Preprocessing** raw CSVs from various systems
2. **Cleaning** and PHI redaction
3. **Schema inference and generation**
4. **Merging** longitudinal data across schema versions
5. **Transformation** into harmonized formats for analytics or export

It is configured via YAML and designed to run modularly from a single CLI entry point.

## Key Components

| File | Purpose |
|------|---------|
| `main.py` | Orchestrates the full pipeline from raw ingestion to clean output |
| `preprocess.py` | Handles file scanning, table inference, and raw data validation |
| `clean.py` | Applies column filtering, PHI redaction, and type coercion |
| `merge.py` | Resolves schema mismatches and merges longitudinal records |
| `transforms.py` | Applies user-defined transformation logic for downstream harmonization |
| `generate_schema.py` / `create_schema.py` | Builds or loads SQL schema definitions for the processed data |
| `tableinferer.py` | Uses regex and heuristics to assign table names to incoming files |
| `utils.py` | Shared utilities such as logging, redaction, and type checking |

## Configuration

Located at `config/config.yml`:

```yaml
datasets:
  - ccd
  - cis
  - ecg
  - ehr
  - scdb

raw_path: /path/to/raw
preprocessed_path: /path/to/preprocessed
clean_path: /path/to/clean
chunksize: 10000
```

Additional redaction and PHI-handling rules are stored in `phi_columns.yml`.

## Usage

Run the full pipeline:

```bash
python main.py
```

Run components independently:

```bash
python preprocess.py         # Preprocess raw files
python clean.py              # Apply cleaning rules
python merge.py --dataset X  # Merge longitudinal records for dataset X
```

## Dependencies

- Python ≥ 3.8
- pandas, numpy, pyyaml, sqlalchemy

Install with:

```bash
pip install -r requirements.txt
```

## Directory Layout

```
upload/
├── main.py
├── preprocess.py
├── clean.py
├── merge.py
├── transforms.py
├── config/
│   ├── config.yml
│   └── phi_columns.yml
├── cleaning/               # Custom cleaning scripts (if any)
├── transform/              # Output transformation logic
├── logs/
├── docs/                   # Project documentation
```

## License

Internal use only. For research collaboration inquiries, please contact the project lead.