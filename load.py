import argparse
import json
import pandas as pd
from utils import get_engine, infer_dataset_tablename, query
import sqlite3

class Load:
    
    def __init__(self,specify_schema,specify_tablename):
        with open('config/config.json','r') as file:
            self.conf = json.load(file)
        self.con = sqlite3.connect(f"{self.conf['data_path']}/load.db")
        self.clean_con = sqlite3.connect(f"{self.conf['data_path']}/clean.db")
        self.engine = get_engine()
        self.specify_schema = specify_schema
        self.specify_tablename = specify_tablename
        cur = self.con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS load_log(dataset, tablename, datetime)")
        self.con.commit()
        self.loaded_tables = pd.read_sql_query("SELECT tablename FROM load_log",self.con)['tablename'].tolist()    
        with open('config/config.json','r') as file:
            self.chunksize = json.load(file)['chunksize']

    def load(self):
        clean_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name != 'clean_log'",self.clean_con)['name'].tolist()    
        print(f"Found the following cleaned files: {clean_tables}")

        datasets = []
        for table in clean_tables:
            dataset, _ = infer_dataset_tablename(table)
            datasets += [dataset]
        datasets = list(set(datasets))
        for dataset in datasets:
            SCHEMA = f"staging_{dataset}"
            print(F"Cleaning up existing staging schema: {SCHEMA}")
            query(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;",return_df=False)
            print("Done")

        for table in clean_tables:
            dataset, tablename = infer_dataset_tablename(table)
            if dataset == self.specify_schema:
                pass
            elif tablename == self.specify_tablename:
                pass
            else:
                if tablename in self.loaded_tables:
                    print(f'{tablename} exists in logs, skipping..')
                    continue
            SCHEMA = f"staging_{dataset}"
            query(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};",return_df=False)
            try:
                print(f"Loading {table} → {SCHEMA}.{tablename}")
                print("Counting number of rows")
                nrows = pd.read_sql_query(f"SELECT COUNT(*) AS nrow FROM '{table}'",self.clean_con)['nrow'].iloc[0]
                print(f"Found {nrows } rows")
                print("Reading table from clean.db")
                reader = pd.read_sql_query(f"SELECT * FROM '{table}'",self.clean_con,chunksize=self.chunksize)
                #Load by chunk
                for i, chunk in enumerate(reader):
                    print(f"Loading {i*self.chunksize}/{nrows}")
                    chunk.to_sql(
                        name=tablename,
                        con=self.engine,
                        schema=SCHEMA,
                        if_exists='append',
                        index=False
                    )
                final_rows = pd.read_sql_query(f"SELECT COUNT(*) FROM {SCHEMA}.{tablename}",con=self.engine)
                assert nrows == final_rows, "Final row count different from initial row count!"
                #Log to database                
                values = f"('{dataset}', '{tablename}', '{str(pd.Timestamp.now())}')"
                cur = self.con.cursor()
                cur.execute(f"INSERT INTO load_log VALUES {values}")
                self.con.commit()
            except Exception as e:
                print(f"Failed to load due to {e}, skipping...")
                #Rollback changes
                try:
                    query_string = f"DROP TABLE '{dataset}.{tablename}'"
                    query(query_string,return_df=False)
                except Exception as e:
                    print(f'Failed to drop existing table due to {e}')
        print("All tables loaded. Cleaning up...")
        #Remove from clean database
        cur = self.clean_con.cursor()
        cur.execute(f"VACUUM;")
        self.con.commit()

    def exit(self):
        self.con.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema",default=None,required=False,help='Specify schema to load')
    parser.add_argument("--tablename",default=None,required=False, help="Tablename to load in the format schema.tablename")
    args = parser.parse_args()
    l = Load(args.schema,args.tablename)
    l.load()
    l.exit()