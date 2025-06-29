import argparse
from collections import defaultdict
import pandas as pd
from sqlalchemy import inspect, text
from utils import get_engine

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--where", required=True, help="preprocess, load, staging_schema, schema")
    parser.add_argument("--namepart", required=True, help="namepart to remove or drop")
    args = parser.parse_args()

    if args.where in ['preprocessing','load']:
        path = f'logs/{args.where}.csv'
        df = pd.read_csv(path,names=['filename','table','datetime'])
        df = df[df['filename'].apply(lambda row: args.namepart not in row)]
        df.to_csv(path,index=False)
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