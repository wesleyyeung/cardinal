import warnings
warnings.filterwarnings('ignore', message="^Columns.*")
warnings.filterwarnings('ignore', message="A value is trying to be set on a copy of a slice from a DataFrame")
import json
import pandas as pd
from cleaning import CLEANER_REGISTRY
from utils import infer_dataset_tablename
import sqlite3

class Clean:

    def __init__(self):
        with open('config/config.json','r') as file:
            self.conf = json.load(file)
        print('Connecting to clean SQLite database')
        self.con = sqlite3.connect(f"{self.conf['data_path']}/clean.db")
        print('Reading clean_logs')
        cur = self.con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS clean_log(dataset, tablename, datetime)")
        self.con.commit()
        print('Connecting to preprocess SQLite database')
        self.preprocess_con = sqlite3.connect(f"{self.conf['data_path']}/preprocess.db")
        print('Loading config file')
        with open('config/config.json','r') as file:
            config = json.load(file)
        self.chunksize = config['chunksize']
        print('Loading known schema')
        with open('config/schema.json','r') as file:
            self.known_schemas = json.load(file)
            #Check if config/schemas has the same keys as CLEANER_REGISTRY
            CLEANER_REGISTRY_list = []
            for key in CLEANER_REGISTRY.keys():
                for name in CLEANER_REGISTRY[key]:
                    CLEANER_REGISTRY_list += [key + '.' + name]
            mismatch = [item for item in set(CLEANER_REGISTRY_list) if item not in set(list(self.known_schemas.keys()))]
            if mismatch:
                raise ValueError(f"Known schema config file does not match cleaner registry: {mismatch}")

        self.cleaned_tables = []
        try:
            print('Loading cleaned tables from the log...')
            self.cleaned_tables = pd.read_sql_query("SELECT tablename FROM clean_log",self.con)['tablename'].tolist()
            print(f"Found the following cleaned tables: {self.cleaned_tables}")
        except Exception as e:
            warnings.warn(f"Unable to load cleaned tables due to {e}")

    def get_cleaner(self, dataset: str, tablename: str):
        cleaner = CLEANER_REGISTRY.get(dataset,{}).get(tablename,None)
        if cleaner is None:
            warnings.warn(f"No cleaner found for table: {tablename}")
        self.cleaner = cleaner.clean # type: ignore
        
    def clean(self):
        #Load file manifest from preprocess SQLite database
        preprocessed_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name != 'preprocess_log'",self.preprocess_con)['name'].tolist()
        for table in preprocessed_tables:
            self.dataset, self.tablename = infer_dataset_tablename(table)
            original_dataset = self.dataset
            original_tablename = self.tablename
            print(f"Cleaning dataset: {self.dataset}, table: {self.tablename}")
            if self.tablename in self.cleaned_tables:
                print(f"Cleaned file for {table} found, skipping.")
                continue
            else:
                try:
                    #Count rows
                    query = f"SELECT COUNT(DISTINCT *) AS rows FROM '{table}'"
                    nrows = pd.read_sql_query(query, self.preprocess_con)['rows'].iloc[0]
                    #Read data
                    query = f"SELECT DISTINCT * FROM '{table}'"
                    # Read in chunks
                    for n, chunk in enumerate(pd.read_sql_query(query, self.preprocess_con, chunksize=self.chunksize)):
                        print(f"Processing {n*self.chunksize}/{nrows} rows")
                        # Get appropriate cleaner based on table type (only once, outside the loop)
                        if not hasattr(self, 'cleaner_initialized') or not self.cleaner_initialized:
                            self.get_cleaner(dataset=self.dataset, tablename=self.tablename)
                            self.cleaner_initialized = True
                        # Skip if no cleaner
                        if self.cleaner is None:
                            print(f"Skipping {table}")
                            break
                        # Clean chunk
                        df_cleaned, destination_schema, destination_tablename = self.cleaner(chunk)
                        # Allow cleaner module to override schema/tablename
                        if destination_schema is not None:
                            self.dataset = destination_schema
                        if destination_tablename is not None:
                            self.tablename = destination_tablename
                        # Append to output table
                        df_cleaned.to_sql(self.dataset + '.' + self.tablename,con=self.con,if_exists='append',index=False)
                    #Log successful processing of file
                    values = f"('{original_dataset}', '{original_tablename}', '{str(pd.Timestamp.now())}')"
                    cur = self.con.cursor()
                    cur.execute(f"INSERT INTO clean_log VALUES {values}")
                    self.con.commit()
                    self.cleaner_initialized = False #reset for next table
                    print('Complete!')
                    cur = self.preprocess_con.cursor()
                    cur.execute(f"DROP TABLE '{table}'")
                    self.preprocess_con.commit()
                    print('Table cleaned from preprocess database')
                except Exception as e:
                    warnings.warn(f"Failed to preprocess {self.tablename} due to {e}, skipping...")
                    raise
        print('Complete! Cleaning up...')
        cur = self.preprocess_con.cursor()
        cur.execute(f"VACUUM;")
        self.preprocess_con.commit()
        print('preprocess database cleaned')

    def exit(self):
        self.con.close()
        self.preprocess_con.close()

if __name__ == "__main__":
    print('Starting cleaning process...')
    c = Clean()
    c.clean()
    c.exit()