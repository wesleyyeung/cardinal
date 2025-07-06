import argparse
import os
import glob
from collections import defaultdict
import pandas as pd
from sqlalchemy import inspect, text
from utils import get_engine
import yaml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--where", required=True, help="preprocess, clean, load, staging_schema, schema")
    parser.add_argument("--namepart", required=True, help="namepart to remove or drop")
    args = parser.parse_args()

    if args.where in ['preprocessing','load']:
        path = f'logs/{args.where}.csv'
        df = pd.read_csv(path,names=['filename','table','datetime'])
        df = df[df['filename'].apply(lambda row: args.namepart not in row)]
        df.to_csv(path,index=False)
    elif args.where == 'clean':
        with open('config/config.yml','r') as file:
            conf = yaml.safe_load(file)
        clean_files = glob.glob(conf['clean_path']+'/**/*')
        remove_files = [file for file in clean_files if args.namepart in file]
        print(f'Removing {len(remove_files)} files in {conf["clean_path"]} matching "{args.namepart}"')
        for file in remove_files:
            try:
                os.remove(file)
            except Exception as e:
                print(f'Failed to remove {file} due to {e}')
    else:
        engine = get_engine()
        inspector = inspect(engine)
        table_names = inspector.get_table_names(schema=args.type)

        with engine.begin() as conn:
            for table in table_names():
                if args.namepart in table:
                    target = f'{args.where}.{table}'
                    print(f'Dropping {target}')
                    conn.execute(text(f'DROP TABLE IF EXISTS {target};'))