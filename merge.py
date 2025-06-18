import argparse
from sqlalchemy import create_engine, inspect, text
import yaml
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

    def merge_staging_tables(self):
        staging_schema = f"staging_{self.dataset}"
        final_schema = self.dataset

        engine = get_engine()
        inspector = inspect(engine)

        table_names = inspector.get_table_names(schema=staging_schema)

        # Group tables by base name
        grouped = defaultdict(list)
        for t in table_names:
            regex = r'[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,4}'
            base = re.split(regex,t)[0]
            base = base[:-1]
            base = base.split('.')[-1]
            grouped[base].append(t)

        with engine.begin() as conn:
            for base_name, tables in grouped.items():
                print(f"Merging tables for base name '{base_name}': {tables}")
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
                #Consider refactoring to work on id and datetime column subsets
                ###SELECT * FROM (
                ###    SELECT *, ROW_NUMBER() OVER (PARTITION BY patient_id, visit_date ORDER BY updated_at DESC NULLS LAST) AS rn
                ###    FROM ({union_query}) AS sub
                ###) AS filtered WHERE rn = 1
                final_table = f'"{final_schema}"."{base_name}"'

                print(f"Creating {final_table} with deduplicated rows and harmonized types.")
                conn.execute(text(f'DROP TABLE IF EXISTS {final_table};'))
                conn.execute(text(f'CREATE TABLE {final_table} AS {dedup_query};'))

        print("Merge completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    with open('config/config.yml','r') as file:
        conf = yaml.safe_load(file)
    parser.add_argument("--dataset", default = None, required=False, help="Dataset name (e.g., ccd or cis)")
    args = parser.parse_args()
    if args.dataset is None:
        print('Merging all datasets:')
        for dataset in conf['datasets']:
            m = Merge(dataset)
            m.merge_staging_tables()
    else:
        m = Merge(args.dataset)
        m.merge_staging_tables()