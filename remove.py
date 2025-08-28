import argparse
import os
import glob
from collections import defaultdict
import pandas as pd
from sqlalchemy import inspect, text
from utils import get_engine
import sqlite3
import json

if __name__ == "__main__":
    with open('config/config.json','r') as file:
        conf = json.load(file)
    parser = argparse.ArgumentParser()
    parser.add_argument("--where", required=True, help="preprocess, clean, load, staging_schema, schema")
    parser.add_argument("--namepart", required=True, help="namepart to remove or drop")
    args = parser.parse_args()

    if args.where in ['preprocess','load','clean']:
        db_location = f'{conf['data_path']}/{args.where}.db'
        con = sqlite3.connect(db_location)
        
        try:
            tablename = pd.read_sql_query(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%{args.namepart}%'",con=con)
            tablename = tablename.iloc[0,0]
        except:
            tablename = args.namepart
            print(f'Failed to find table containing {args.namepart} in {db_location}')
        
        try:
            cur = con.cursor()
            print(f"Dropped {tablename} from {db_location}")
            cur.execute(f"DROP TABLE '{tablename}'")
            con.commit()
        except Exception as e:
            print(f'Failed to drop table {tablename} from {db_location} due to {e}')
        try:
            cur = con.cursor()
            print(f'Removing {args.namepart} from {args.where}_log')
            cur.execute(f"DELETE FROM {args.where}_log WHERE tablename LIKE '%{args.namepart}%'")
            con.commit()
        except Exception as e:
            print(f'Failed to remove record from {args.where}_log due to {e}')
        
    else:
        engine = get_engine()
        inspector = inspect(engine)
        table_names = inspector.get_table_names(schema=args.where)
        with engine.begin() as conn:
            for table in table_names():
                if args.namepart in table:
                    target = f'{args.where}.{table}'
                    print(f'Dropping {target}')
                    conn.execute(text(f'DROP TABLE IF EXISTS {target};'))