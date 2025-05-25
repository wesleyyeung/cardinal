
# 📊 Postgres ETL Pipeline

This project is a modular, extensible ETL pipeline for ingesting and cleaning CSV data before loading it into a PostgreSQL database.

---

## 🗂 Folder Structure

```
csv/
├── bin
├── datasets /
|   ├── ccd/ 
│   |   ├── raw/
│   |   └── clean/
|   ├── cis/
│   |   ├── raw/
│   |   └── clean/
|   ├── ecg/
│   |   ├── raw/
│   |   └── clean/
|   └── ehr/
│       ├── raw/
│       └── clean/
config/
├── db_credentials.yml
├── ccd_staging_config.yml
├── cis_private_health_information.yml
lib/
└── cleaning/
    ├── base_cleaner.py
    ├── clean_ccd.py
    ├── clean_cis.py
    ├── clean_ecg.py
    └── clean_ehr.py
bin/
├── clean.py
├── create_schema.py
├── load.py
├── merge.py
└── orchestrate_etl.py
```

---

## 🚀 Quick Start

```bash
# Run entire ETL pipeline for a dataset (ccd or cis)
./run_all_etl.sh
```

---

## 🧽 Individual Steps

```bash
python bin/clean.py --dataset ccd         # Clean raw CSVs
python bin/create_schema.py --dataset ccd # Create Postgres schemas
python bin/load.py --dataset ccd          # Load cleaned data into staging
python bin/merge.py --dataset ccd         # Merge staging into final schema
```

---

## 🧱 Adding a New Table Cleaner

1. Open or create a file in `lib/cleaning/`, e.g., `clean_ccd.py`
2. Add a class and decorate it:

```python
from lib.cleaning_dispatcher import register_cleaner
from lib.base_cleaner import BaseCleaner

@register_cleaner("my_new_table")
class MyTableCleaner(BaseCleaner):
    def clean(self, df):
        # your custom logic
        return df
```

3. Save your cleaned CSV as `csv/ccd/raw/my_new_table.csv`
4. Re-run `python bin/clean.py --dataset ccd`

---

## 🔒 PHI Exclusion (for CIS)

- Controlled by `config/cis_private_health_information.yml`
- Cleaner will exclude listed columns per file
- `Study Num` is hashed using SHA-1

---

## 🛠 Requirements

- Python 3.8+
- PostgreSQL
- `pandas`, `sqlalchemy`, `pyyaml`, `psycopg2-binary`

---

## 📌 Notes

- Raw files are auto-deleted after successful load
- Cleaned files follow naming convention: `<table>.csv`
- Final schema is named after the dataset (e.g., `ccd`, `cis`)
- Staging schema is prefixed: `staging_ccd`, `staging_cis`

---

## ✅ Status

- Modular cleaner registry ✅
- One-click orchestrator ✅
- Raw file cleanup ✅
- Table-wise staging and merge ✅
