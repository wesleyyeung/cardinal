import argparse
from pathlib import Path
import yaml
import pandas as pd
from transform import TRANSFORM_REGISTRY
from utils import infer_dataset_tablename

class Transforms:

    def __init__(self, clean_path: str):
        self.clean_path = clean_path

    def get_transform(self,dataset: str, tablename: str):
        transformation = TRANSFORM_REGISTRY.get(dataset,{}).get(tablename,None)
        self.transformation = transformation.transform # type: ignore

    def transform(self):
        CLEAN_DIR = Path(self.clean_path)
        clean_files = CLEAN_DIR.glob("**/*.csv")
        clean_files = [str(file) for file in clean_files]
        for file in clean_files:
            fname, dataset, tablename = infer_dataset_tablename(file)
            try:
                self.get_transform(dataset=dataset,tablename=tablename)
                print(f"Transforming {fname} (dataset: {dataset}, table: {tablename})")
            except:
                continue
            df = pd.read_csv(file)
            df, suffix = self.transformation(df)
            _, datepart = fname.split('.csv')[0].split('_')
            save_path = self.clean_path + '/' + dataset + '/' + dataset + '.' + tablename + '_' + suffix + '_' + datepart + '.csv'
            df.to_csv(save_path,index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    with open('config/config.yml','r') as file:
        conf = yaml.safe_load(file)
    parser.add_argument("--clean_path", default=conf['clean_path'], required=False, help="Path to directory containing clean files")
    args = parser.parse_args()

    t = Transforms(args.clean_path)
    t.transform()
