import argparse
from sqlalchemy import create_engine, inspect, text
import yaml
from pathlib import Path
import sys
import re
from collections import defaultdict

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

def get_engine(config_path="csv/config/db_credentials.yml"):
    config_path = Path(__file__).parent.parent / "config" / "db_credentials.yml"
    with open(config_path, "r") as file:
        cfg = yaml.safe_load(file)
    db_url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(db_url)

def sanitize_table_name(fname):
    return re.sub(r"\.csv$", "", fname.lower()).replace(" ", "_").replace("-","_")

def get_column_types(engine, schema, table_list):
    inspector = inspect(engine)
    table_cols = {}

    for table in table_list:
        columns = inspector.get_columns(table, schema=schema)
        table_cols[table] = {col["name"]: str(col["type"]).upper() for col in columns}

    # Intersect to get common column names
    common_cols = set.intersection(*(set(cols.keys()) for cols in table_cols.values()))
    column_order = list(common_cols)

    # Resolve common types
    def resolve_type(colname):
        types = {table_cols[table][colname] for table in table_list}
        # Priority: TEXT > DOUBLE PRECISION > INTEGER
        if any(t.startswith("TEXT") or "CHAR" in t for t in types):
            return "TEXT"
        elif any("DOUBLE" in t or "FLOAT" in t or "NUMERIC" in t for t in types):
            return "DOUBLE PRECISION"
        elif any("INT" in t for t in types):
            return "INTEGER"
        else:
            return list(types)[0]  # fallback

    type_map = {col: resolve_type(col) for col in column_order}
    return column_order, type_map

def merge_staging_tables(dataset):
    staging_schema = f"staging_{dataset}"
    final_schema = dataset

    engine = get_engine()
    inspector = inspect(engine)

    table_names = [sanitize_table_name(t) for t in inspector.get_table_names(schema=staging_schema)]

    # Group tables by base name
    grouped = defaultdict(list)
    for t in table_names:
        base = t.split('_')[0]
        grouped[base].append(t)

    with engine.begin() as conn:
        for base_name, tables in grouped.items():
            print(f"Merging tables for base name '{base_name}': {tables}")

            column_order, type_map = get_column_types(engine, staging_schema, tables)
            if not column_order:
                print(f"Skipping {base_name}: no common columns")
                continue

            # Generate SELECTs with CASTs
            selects = []
            for table in tables:
                select_clause = ', '.join(
                    f'CAST("{col}" AS {type_map[col]}) AS "{col}"' for col in column_order
                )
                selects.append(f'SELECT {select_clause} FROM "{staging_schema}"."{table}"')

            union_query = " UNION ALL ".join(selects)
            final_table = f'"{final_schema}"."{base_name}"'

            print(f"Creating {final_table} with harmonized types.")
            conn.execute(text(f'DROP TABLE IF EXISTS {final_table};'))
            conn.execute(text(f'CREATE TABLE {final_table} AS {union_query};'))

    print("Merge completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., ccd or cis)")
    args = parser.parse_args()
    merge_staging_tables(args.dataset)