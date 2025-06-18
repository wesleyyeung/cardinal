import csv
import argparse
import pandas as pd
import yaml
from pathlib import Path
from utils import get_engine, infer_dataset_tablename

class LoadStagingTable:
    
    def __init__(self, clean_path: str):
        self.clean_path = clean_path
        self.preprocessed_files = []
        try:
            with open('logs/load.csv','r') as csvfile:
                reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
                for row in reader:
                    self.preprocessed_files += [row[0]]
        except:
            pass

    def load_cleaned_data(self):
        CLEAN_DIR = Path(self.clean_path)

        engine = get_engine()

        clean_files = CLEAN_DIR.glob("**/*.csv")
        clean_files = [str(file) for file in clean_files]
        print(f"Found the following cleaned files: {clean_files}")

        for fname in clean_files:
            if fname in self.preprocessed_files:
                print(f'{fname} exists in logs, skipping..')
                continue
            if not fname.endswith('.csv'):
                continue
            _, self.dataset, self.tablename = infer_dataset_tablename(fname)
            SCHEMA = f"staging_{self.dataset}"
            table_name = fname.split('/')[-1]
            clean_path = CLEAN_DIR / fname

            print(f"Loading {fname} → {SCHEMA}.{table_name}")
            df = pd.read_csv(clean_path)
    
            df.to_sql(
                name=table_name,
                con=engine,
                schema=SCHEMA,
                if_exists='replace',
                index=False
            )
            with open('logs/load.csv','a') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    csvwriter.writerow([fname , self.tablename, str(pd.Timestamp.now())])

        print("All tables loaded.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    with open('config/config.yml','r') as file:
        conf = yaml.safe_load(file)
    parser.add_argument("--clean_path", default=conf['clean_path'], required=False, help="Path to directory containing clean files")
    args = parser.parse_args()

    lst = LoadStagingTable(args.clean_path)
    lst.load_cleaned_data()