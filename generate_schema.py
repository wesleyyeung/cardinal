import glob
import os
import yaml
import pandas as pd

files = glob.glob("../raw/*.csv")
files += glob.glob("../raw/*.xlsx")
files += glob.glob("../raw/*.parquet")

tables = [file.split('/')[-1].replace('.csv','').replace('.xlsx','').replace('.parquet','') for file in files]
table_dict = {}
df_dict = {}
for table,file in zip(tables,files):
    table_dict[table] = file
    ext = os.path.splitext(file)[1].lower()
    if ext == '.csv':
        df_dict[table] = pd.read_csv(file,nrows=0)
    elif ext in ['.xls', '.xlsx']:
        df_dict[table] = pd.read_excel(file,nrows=0)
    elif ext == '.parquet':
        df_dict[table] = pd.read_parquet(file)

with open('config/canonical_tablenames.yml') as file:
    canonical_tablenames = yaml.safe_load(file)

check = [file for file in list(set(list(df_dict.keys()))) if file not in list(set(list(canonical_tablenames.keys())))]
if check:
    raise ValueError(f"The following tables have not been mapped to a canonical table: {check}")
    
schema = {}
for k,v in canonical_tablenames.items():
    try:
        schema[v] += df_dict[k].columns.tolist()
        schema[v] = list(dict.fromkeys(schema[v]))
    except:
        schema[v] = df_dict[k].columns.tolist()

with open('config/schema.yml','w') as file:
    yaml.dump(schema,file)