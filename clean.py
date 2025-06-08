import argparse
import glob
from pathlib import Path
import re
import warnings
warnings.filterwarnings('ignore', message="^Columns.*")
warnings.filterwarnings('ignore', message="A value is trying to be set on a copy of a slice from a DataFrame")
import yaml
import pandas as pd
from cleaning import CLEANER_REGISTRY
from utils import infer_dataset_tablename

class Clean:

    def __init__(self, preprocessed_path: str, clean_path: str):
        self.preprocessed_path = preprocessed_path
        self.clean_path = clean_path

        with open('config/schema.yml','r') as file:
            self.known_schemas = yaml.safe_load(file)
            #Check if config/schemas has the same keys as CLEANER_REGISTRY
            CLEANER_REGISTRY_list = []
            for key in CLEANER_REGISTRY.keys():
                for name in CLEANER_REGISTRY[key]:
                    CLEANER_REGISTRY_list += [key + '.' + name]
            mismatch = [item for item in set(CLEANER_REGISTRY_list) if item not in set(list(self.known_schemas.keys()))]
            if mismatch:
                raise ValueError(f"Known schema config file does not match cleaner registry: {mismatch}")
        
    def get_cleaner(self, dataset: str, tablename: str):
        cleaner = CLEANER_REGISTRY.get(dataset,{}).get(tablename,None)
        if cleaner is None:
            warnings.warn(f"No cleaner found for table: {tablename}")
        self.cleaner = cleaner.clean # type: ignore
        
    def clean(self):
        clean_dir = Path(self.clean_path)
        try:
            clean_dir.mkdir(parents=True, exist_ok=False)
        except:
            pass
        #Load file manifest
        preprocessed_files = glob.glob(self.preprocessed_path+"/*.csv")
        clean_files = clean_dir.rglob("*csv")
        clean_files = [file.name for file in clean_files]
        for csv_path in preprocessed_files:
            fname, self.dataset, self.tablename = infer_dataset_tablename(csv_path)
            print(f"Cleaning file: {fname} (dataset: {self.dataset}, table: {self.tablename})")
            if fname in clean_files:
                print(f"Cleaned file {csv_path} found, skipping.")
                continue
            else:
                #Read data
                df = pd.read_csv(csv_path)
                #Get appropriate cleaner based on table type
                self.get_cleaner(dataset=self.dataset,tablename=self.tablename)
                if self.cleaner is None:
                    print(f"Skipping {csv_path}")
                    continue 
                #Clean data
                df_cleaned = self.cleaner(df)
                #Save to clean_dir with exact sample filename
                output_path = clean_dir / self.dataset / fname
                #Create output diretory if not exists
                try:
                    output_dir = clean_dir / self.dataset
                    directory_path = Path(output_dir)
                    directory_path.mkdir(parents=True, exist_ok=False)
                except:
                    pass
                #Save output
                df_cleaned.to_csv(output_path, index=False)
                print(f"Saved cleaned file to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    with open('config/config.yml','r') as file:
        conf = yaml.safe_load(file)
    parser.add_argument("--preprocessed_path", default=conf['preprocessed_path'], required=False, help="Path to directory for preprocessed files")
    parser.add_argument("--clean_path", default=conf['clean_path'], required=False, help="Path to directory containing clean files")
    args = parser.parse_args()

    c = Clean(args.preprocessed_path,args.clean_path)
    c.clean()
