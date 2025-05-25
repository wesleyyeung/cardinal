
import os
import argparse
import pandas as pd
import yaml
from pathlib import Path
from sqlalchemy import create_engine
import re
import sys

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

def get_engine(config_path="csv/config/db_credentials.yml"):
    config_path = Path(__file__).parent.parent / "config" / "db_credentials.yml"
    with open(config_path, "r") as file:
        cfg = yaml.safe_load(file)
    db_url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(db_url)

def sanitize_table_name(fname):
    return re.sub(r"\.csv$", "", fname.lower()).replace(" ", "_").replace("-","_")

def load_cleaned_data(dataset):
    base_path = Path(__file__).parent.parent
    CLEAN_DIR = base_path / "datasets" / dataset / "clean"
    RAW_DIR = base_path / "datasets" / dataset / "raw"
    SCHEMA = f"staging_{dataset}"

    engine = get_engine()

    clean_files = os.listdir(CLEAN_DIR)
    print(f"Found the following cleaned files: {clean_files}")

    for fname in clean_files:
        if not fname.endswith('.csv'):
            continue

        table_name = sanitize_table_name(fname)
        clean_path = CLEAN_DIR / fname
        raw_path = RAW_DIR / fname

        print(f"Loading {fname} → {SCHEMA}.{table_name}")
        df = pd.read_csv(clean_path)

        column_names = df.columns.tolist()
        #Sanitise column names one more time
        new_column_names = {}
        for col in column_names:
            # Lowercase, replace spaces/periods with underscore, remove special characters
            new_column_name = col.strip().lower()
            new_column_name = re.sub(r'#', 'no', new_column_name) 
            new_column_name = re.sub(r"[ .]+", "_", new_column_name)
            new_column_name = re.sub(r"[^a-z0-9_]", "", new_column_name)
            if new_column_name[-1] == '_':
                new_column_name = new_column_name[:-1]
            new_column_names[col] = new_column_name
        if 'right' in column_names:
            new_column_names['right'] = 'right_'
        df.rename(columns=new_column_names,inplace=True)
        
        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists='replace',
            index=False
        )

        if raw_path.exists():
            raw_path.unlink()
            print(f"Removed raw file: {raw_path}")

    print("All tables loaded and raw files cleaned up.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., ccd or cis)")
    args = parser.parse_args()
    load_cleaned_data(args.dataset)
