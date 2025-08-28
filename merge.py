import argparse
from sqlalchemy import inspect, text
import json
from pathlib import Path
import sys
import re
from collections import defaultdict
from utils import get_engine, sanitize_table_name

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

class Merge:

    def __init__(self, dataset: str):
        self.dataset = dataset

    @staticmethod
    def get_column_types(engine, schema, table_list):
        inspector = inspect(engine)
        table_cols = {}

        for table in table_list:
            columns = inspector.get_columns(table, schema=schema)
            table_cols[table] = {col["name"]: str(col["type"]).upper() for col in columns}

        # Use the first table as the canonical source for column order
        first_table = table_list[0]
        canonical_order = list(table_cols[first_table].keys())

        # Collect all column names across all tables (union)
        all_columns = set()
        for cols in table_cols.values():
            all_columns.update(cols.keys())

        # Preserve order from first table, then append any new columns in order of appearance
        column_order = [col for col in canonical_order if col in all_columns]
        column_order += [col for col in all_columns if col not in column_order]

        # Resolve common types, even for columns that appear in only some tables
        def resolve_type(colname):
            types = {
                table_cols[table][colname]
                for table in table_list
                if colname in table_cols[table]
            }

            if not types:
                return "TEXT"  # Fallback for entirely missing (should not happen if column_order is correct)

            # Priority: TEXT > DOUBLE PRECISION > INTEGER
            if any(t.startswith("TEXT") or "CHAR" in t for t in types):
                return "TEXT"
            elif any("DOUBLE" in t or "FLOAT" in t or "NUMERIC" in t for t in types):
                return "DOUBLE PRECISION"
            elif any("INT" in t for t in types):
                return "BIGINT"
            else:
                return list(types)[0]  # fallback

        # Build type_map for all columns in column_order
        type_map = {col: resolve_type(col) for col in column_order}
        return column_order, type_map, table_cols

    def setup(self):
        staging_schema = f"staging_{self.dataset}"
        final_schema = self.dataset

        engine = get_engine()
        inspector = inspect(engine)
        table_names = inspector.get_table_names(schema=staging_schema)

        # Group tables by base name
        grouped = defaultdict(list)
        for t in table_names:
            base = t.split('.')[-1]
            grouped[base].append(t)

        return grouped, staging_schema, final_schema, engine

    def merge_staging_tables(self,tablename=None):
        grouped, staging_schema, final_schema, engine = self.setup()
        with engine.begin() as conn:
            for base_name, tables in grouped.items():
                if (tablename is not None) and (tablename != base_name):
                    continue                    
                print(f"Merging {len(tables)} tables for base name '{base_name}' from '{staging_schema}' into '{self.dataset}'")
                column_order, type_map, table_cols = self.get_column_types(engine, staging_schema, tables)
                if not column_order:
                    print(f"Skipping {base_name}: no common columns")
                    continue

                # Generate SELECTs with CASTs
                selects = []
                for table in tables:
                    select_clause = select_clause = ', '.join(
                        f'CAST("{col}" AS {type_map.get(col, "TEXT")}) AS "{col}"'
                        if col in table_cols[table]
                        else f'NULL::{"TEXT" if col not in type_map else type_map[col]} AS "{col}"'
                        for col in column_order
                    )
                    selects.append(f'SELECT {select_clause} FROM "{staging_schema}"."{table}"')

                union_query = " UNION ALL ".join(selects)
                dedup_query = f"SELECT DISTINCT * FROM ({union_query}) AS merged"
                
                final_table = f'"{final_schema}"."{base_name}"'

                print(f"Creating {final_table} with deduplicated rows and harmonized types.")
                conn.execute(text(f'DROP TABLE IF EXISTS {final_table};'))
                try:
                    conn.execute(text(f'CREATE TABLE {final_table} AS {dedup_query};'))
                except Exception as e:
                    with open('logs/errors.txt','a') as file:
                        file.writelines(str(e))
                    raise e

        print("Merge completed.")

    def batch_merge_staging_tables(self, tablename=None, batch_size: int = 10000):
        grouped, staging_schema, final_schema, engine = self.setup()
        with engine.begin() as conn:
            for base_name, tables in grouped.items():
                if (tablename is not None) and (tablename != base_name):
                    continue
                print(f"Batch merging {len(tables)} tables for base name '{base_name}' from '{staging_schema}' into '{self.dataset}'")

                column_order, type_map, table_cols = self.get_column_types(engine, staging_schema, tables)
                if not column_order:
                    print(f"Skipping {base_name}: no common columns")
                    continue

                final_table = f'"{final_schema}"."{base_name}"'
                columns_ddl = ', '.join(f'"{col}" {type_map[col]}' for col in column_order)

                print(f"Creating {final_table}.")
                conn.execute(text(f'DROP TABLE IF EXISTS {final_table};'))
                conn.execute(text(f'CREATE TABLE {final_table} ({columns_ddl});'))

                for table in tables:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{staging_schema}"."{table}"'))
                    print(f"{table}: {result.scalar()} rows")
                    print(f"Batch inserting from {table} into {final_table}")
                    offset = 0
                    chunk_no = 0
                    while True:
                        select_clause = ', '.join(
                            f'CAST("{col}" AS {type_map[col]}) AS "{col}"'
                            if col in table_cols[table]
                            else f'NULL::{"TEXT" if col not in type_map else type_map[col]} AS "{col}"'
                            for col in column_order
                        )
                        batch_query = text(f"""
                            INSERT INTO {final_table}
                            SELECT {select_clause}
                            FROM "{staging_schema}"."{table}"
                            ORDER BY ctid
                            OFFSET :offset LIMIT :limit
                        """)
                        result = conn.execute(batch_query, {"offset": offset, "limit": batch_size})
                        if result.rowcount == 0:
                            break
                        offset += batch_size
                        print(f'{(chunk_no+1)*10000}/{result.scalar()}')
                
                result = conn.execute(text(f'SELECT COUNT(*) FROM {final_table}'))
                print(f"{final_table}: {result.scalar()} rows after insert")

                print(f"Deduplicating {final_table}")
                final_table_replaced = final_table.replace('"','')
                dedup_temp = f'"{final_table_replaced}_dedup"'
                conn.execute(text(f"""
                    CREATE TABLE {dedup_temp} AS
                    SELECT DISTINCT * FROM {final_table};
                """))
                conn.execute(text(f'DROP TABLE {final_table};'))
                conn.execute(text(f'ALTER TABLE {dedup_temp} RENAME TO "{base_name}";'))
                conn.execute(text(f'ALTER TABLE {base_name} SET SCHEMA {final_schema}'))

        print("Batch merge completed.")

    def merge(self,tablename=None, batch_size: int = 10000):
        if batch_size:
            self.batch_merge_staging_tables(tablename=tablename,batch_size=batch_size)
        else:
            self.merge_staging_tables(tablename=tablename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    with open('config/config.json','r') as file:
        conf = json.load(file)
    parser.add_argument("--schema", default = None, required=False, help="Schema name (e.g., ccd or cis)")
    parser.add_argument("--tablename", default = None, required=False, help="Table name")
    parser.add_argument("--batchsize", default = conf['chunksize'], required=False, help="Batch size used for batch processing")
    args = parser.parse_args()
    args.batchsize = int(args.batchsize)
    if args.schema is None:
        print('Merging all schema:')
        for dataset in conf['datasets']:
            try:
                m = Merge(dataset)
                m.merge(tablename=args.tablename,batch_size=args.batchsize)
            except:
                print('Failed! Attempting batch merge with default batchsize of 10000')
                m = Merge(dataset)
                m.merge(tablename=args.tablename,batch_size=10000)
    else:
        m = Merge(args.schema)
        m.merge(tablename=args.tablename,batch_size=args.batchsize)