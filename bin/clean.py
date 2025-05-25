
import os
import argparse
import pandas as pd
from pathlib import Path
import sys

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from lib.cleaning_dispatcher import clean_table

def infer_table_name(filename: str) -> str:
    return filename.replace(".csv", "").lower()

def get_max_dt(df: pd.DataFrame) -> str:
    try:
        dt_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        max_dt = df[dt_cols].max()
        return max_dt.strftime(format='%d-%m-%Y') 
    except:
        return 'unknowndate'
    
def clean_raw_data(dataset):
    base_path = Path(__file__).parent.parent
    raw_dir = base_path / "rawdata" 
    clean_dir = base_path / "datasets" / dataset / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)

    raw_files = raw_dir.glob("*.csv")
    clean_files = [file.name for file in clean_dir.glob("*.csv")]
    for csv_path in raw_files:
        print(f"Cleaning file: {csv_path.name} (table: {dataset})")
        if csv_path.name in clean_files:
            print("Cleaned file found, skipping.")
            continue
        else:
            df = pd.read_csv(csv_path)
            max_dt = get_max_dt(df)
            df_cleaned = clean_table(df, table_name=dataset, filename=csv_path.name)
            
            name = csv_path.name.replace('.csv','')

            output_path = clean_dir / name + max_dt + '.csv'
            df_cleaned.to_csv(output_path, index=False)
            print(f"Saved cleaned file to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., ccd or cis)")
    args = parser.parse_args()

    clean_raw_data(args.dataset)
