
import argparse
from sqlalchemy import create_engine, text
import json

def get_engine(config_path="config/.env"):
    with open(config_path, "r") as f:
        cfg = json.load(f)
    db_url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(db_url,future=True)

def create_schemas(dataset):
    engine = get_engine()
    schemas = [f"staging_{dataset}", dataset]

    with engine.connect() as conn:
        for schema in schemas:
            print(f"Creating schema if not exists: {schema}")
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        conn.commit()
        print("Schemas created successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., cis or ccd)")
    args = parser.parse_args()
    create_schemas(args.dataset)
