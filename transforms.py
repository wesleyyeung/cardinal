import argparse
from pathlib import Path
import json
import pandas as pd
from transform import TRANSFORM_REGISTRY
from utils import infer_dataset_tablename
import sqlite3

class Transforms:

    def __init__(self):
        with open('config/config.json','r') as file:
            self.conf = json.load(file)
        self.con = sqlite3.connect(f"{self.conf['data_path']}/clean.db")

    def get_transform(self,dataset: str, tablename: str):
        transformation = TRANSFORM_REGISTRY.get(dataset,{}).get(tablename,None)
        self.transformation = transformation.transform # type: ignore

    def transform(self):
        clean_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name != 'clean_log'",self.con)["name"].tolist()
        for table in clean_tables:
            dataset, tablename = infer_dataset_tablename(table)
            try:
                self.get_transform(dataset=dataset,tablename=tablename)
                print(f"Transforming dataset: {dataset}, table: {tablename}")
            except:
                continue
            df = pd.read_sql_query(f"SELECT * FROM '{table}'",self.con)
            schema, df, suffix = self.transformation(df)
            df.to_sql(f"{schema}_{tablename}_{suffix}",if_exists="append",index=False)

    def exit(self):
        self.con.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    with open('config/config.json','r') as file:
        conf = json.load(file)
    t = Transforms()
    t.transform()
    t.exit()
