from collections import defaultdict
import hashlib
import re
import json
from sqlalchemy import create_engine, text
import pandas as pd

def get_engine(config_path="config/.env"):
        with open(config_path, "r") as file:
            cfg = json.load(file)
        db_url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        return create_engine(db_url)

engine = get_engine()

def query(sql: str = '', engine=engine,return_df=True):
    if return_df:
        with engine.connect() as conn:
            return pd.read_sql(sql, conn)
    else:
        with engine.begin() as conn:
            conn.execute(text(sql))
    
# Precompile patterns as a single regex for performance
NRIC_REGEX = re.compile(r'^[a-zA-Z]{1,2}[0-9]{7}[a-zA-Z]?$')

def redact_nric(cell):
    s = str(cell)
    if NRIC_REGEX.match(s):
        return '[REDACTED]'
    return cell

def sanitize_table_name(fname: str) -> str:
    return re.sub(r"\.csv$", "", fname.lower()).replace(" ", "_").replace("-","_")

def flatten(xss):
    return [x for xs in xss for x in xs]

def safe_load_yml_to_dict(yaml_path: str) -> dict:
    """
    Load the coros_mapper_dict from a YAML file.
    
    Parameters:
        yaml_path (str): Path to the YAML file.
    
    Returns:
        dict: Dictionary loaded from the YAML file.
    """
    with open(yaml_path, 'r') as f:
        output_dict = json.load(f)
    return output_dict 

def build_reverse_mapper(mapper: dict) -> dict:
    """
    Build a reverse lookup dictionary from values to keys.

    For example:
    {"LM": ["a", "b"], "RCA": ["b", "c"]}
    becomes
    {"a": ["LM"], "b": ["LM", "RCA"], "c": ["RCA"]}
    """
    reverse_mapper = defaultdict(list)
    for key, values in mapper.items():
        for value in values:
            reverse_mapper[value].append(key)
    return reverse_mapper

def remap_value_fast(input_string: str, reverse_mapper: dict) -> list:
    """
    Use the reverse mapper to get all keys associated with the input value.
    """
    return reverse_mapper.get(input_string, [])

class ColumnNameSanitizer():
     
    def __init__(self,abbreviations: dict = {}):
        self.ABBREVIATIONS = abbreviations

    @staticmethod
    def sanitize_column_name_for_sql(name: str) -> str:
        #Sanitise column names 
        # Lowercase, replace spaces/periods with underscore, remove special characters
        new_column_name = name.strip().lower()
        new_column_name = re.sub(r'#', 'no', new_column_name) 
        new_column_name = re.sub(r"[ .]+", "_", new_column_name)
        new_column_name = re.sub(r"[^a-z0-9_]", "", new_column_name)
        if new_column_name[-1] == '_':
            new_column_name = new_column_name[:-1]
        if 'right' in new_column_name:
            new_column_name = 'right_'
        return new_column_name

    def sanitise(self,column_names: list, simple = True) -> dict:
        if simple:
            new_column_names = {}
            for col in column_names:
                # Lowercase, replace spaces/periods with underscore, remove special characters
                new_column_name = col.strip().lower()
                new_column_name = re.sub(r"[ .]+", "_", new_column_name)
                new_column_name = re.sub(r"[^a-z0-9_]", "", new_column_name)
                if new_column_name[-1] == '_':
                    new_column_name = new_column_name[:-1]
                new_column_names[col] = new_column_name
        else:
           new_column_names = self.shorten_column_names(column_names)
        for k,v in new_column_names.items():
            new_column_names[k] = self.sanitize_column_name_for_sql(v)
        return new_column_names

    def tokenize(self, name):
        # Split camelCase and underscores
        s1 = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        tokens = re.split(r'[_\\/.]', s1.lower())
        return [t for t in tokens if t]

    def clean_tokens(self, tokens):
        tokens = [token.strip().lower() for token in tokens]
        tokens = [re.sub(r"[ .]+", "_", token) for token in tokens]
        tokens = [re.sub(r"[^a-z0-9_]", "", token) for token in tokens]
        return tokens

    def dedup(self, tokens):
        result = []
        for t in tokens:
            if not result or result[-1] != t:
                result.append(t)
        return result

    def abbreviate(self, tokens, abbreviation_map):
        return [abbreviation_map.get(t, t) for t in tokens]

    def prune(self, tokens, keep_first=3):
        # Remove tokens like 'data', 'entry' after the first few
        return tokens[:keep_first] + [t for t in tokens[keep_first:] if t]

    def ensure_unique(self, name, used_names):
        if name not in used_names:
            used_names.add(name)
            return name
        suffix = hashlib.md5(name.encode()).hexdigest()[:4]
        new_name = f"{name}_{suffix}"
        used_names.add(new_name)
        return new_name

    def shorten_column_names(self, column_names: list) -> dict:
        used_names = set()
        new_names = {}
        for col in column_names:
            tokens = self.tokenize(col)
            tokens = self.clean_tokens(tokens)
            tokens = self.dedup(tokens)
            tokens = self.abbreviate(tokens, self.ABBREVIATIONS)
            tokens = self.dedup(tokens)
            tokens = self.prune(tokens)
            short_name = '_'.join(filter(None, tokens))
            unique_name = self.ensure_unique(short_name, used_names)
            new_names[col] = unique_name
        return new_names

def infer_dataset_tablename(input_str: str) -> tuple:
    lst = input_str.split('.')
    dataset = lst[0]
    tablename = lst[-1]
    return dataset, tablename

def has_any_code(code_str, target_set):
    code_list = code_str.split(',')
    if code_list: #check if list is not empty
        codes = set(code_list)
        return 1 if not codes.isdisjoint(target_set) else 0
    else:
        return 0

def get_unique(input: str) -> str:
    return '|'.join(list(set(list(input.split('|')))))    

def hash_csv_content(path):
    df = pd.read_csv(path, dtype=str).fillna("")  # Read all as strings
    df = df.reindex(sorted(df.columns), axis=1)   # Sort columns alphabetically
    df = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)  # Sort rows
    normalized = df.to_csv(index=False, line_terminator='\n').encode("utf-8")
    return hashlib.md5(normalized).hexdigest()

def parse_medication_instruction(instruction: str):
    instruction = instruction.strip()

    # Normalize parentheses like "(mg)" → "mg"
    instruction = re.sub(r'\((.*?)\)', r'\1', instruction)

    # Expanded set of verbs
    verbs = r"(Take|Inject|Infuse|Apply|Insert|Chew|Place|Swallow|Spray|Drink|Put|Give|Administer|Inhale)?"

    # Expanded dose units
    dose_units = (
        r"(mg|g|mL|L|litre(s)?|UNIT|units|mcg|µg|IU|drop(s)?|application(s)?|tablet(s)?|capsule(s)?|scoop(s)?|"
        r"feed(s)?|sachet(s)?|lozenge(s)?|puff(s)?|plaster(s)?|patch(es)?|suppositor(y|ies)?|each|dose|spray(s)?|"
        r"inhalation(s)?)"
    )

    # Dose pattern with optional verb
    dose_pattern = rf"{verbs}\s*([\d,\.]+\s*{dose_units})"

    # Frequency pattern includes:
    # - numeric patterns (e.g. "3 times a day", "2 feeds a day")
    # - common phrases (e.g. "at bedtime", "before meals")
    # - medical abbreviations (OD, BD, TDS, etc.)
    # - weekday abbreviations (Mon-Sun)
    frequency_pattern = (
        r"(every\s+(other\s+)?(hour|morning|night|day|\d+\s*(hours?|days?|weeks?)|"
        r"Mon|Tue|Wed|Thu|Fri|Sat|Sun)|"
        r"once\s+(only|a\s+day|daily)?|twice\s+(a\s+day|daily)?|"
        r"\d+\s+times\s+a\s+(day|week)|"
        r"\d+\s+feeds\s+a\s+day|"
        r"at\s+bedtime|"
        r"when required|as needed|prn|"
        r"(before\s+(meals?|dialysis|procedure))|"
        r"on\s+non-dialysis\s+days|"
        r"\([^)]+\)\s*(OM|ON)|OM|ON|PM|OD|BD|TDS|QDS|QID|"
        r"(once\s+only\s+on\s+\d{1,2}\s+\w+))"
    )

    # Duration pattern
    duration_pattern = (
        r"(for\s+(up\s+to\s+)?\d+\s+(days?|weeks?)|"
        r"once\s+for\s+\d+\s+dose)"
    )

    # Apply patterns
    dose_match = re.search(dose_pattern, instruction, re.IGNORECASE)
    freq_match = re.search(frequency_pattern, instruction, re.IGNORECASE)
    dur_match = re.search(duration_pattern, instruction, re.IGNORECASE)

    # Extract values
    dose = dose_match.group(2).replace(",", "") if dose_match else None
    if not dose and "apply to affected area" in instruction.lower():
        dose = "topical (unspecified)"

    frequency = freq_match.group(0).strip() if freq_match else None
    duration = dur_match.group(0).replace("for ", "").strip() if dur_match else None

    return {
        "instruction": instruction,
        "dose": dose,
        "frequency": frequency,
        "duration": duration
    }

def medication_type_cleaner(input_str):
    if 'IP' in input_str or 'Inpatient' in input_str:
        return 'Inpatient'
    elif 'discharge' in input_str or 'Discharge' in input_str:
        return 'Discharge'
    elif 'AE' in input_str or 'Emergency' in input_str:
        return 'Emergency'
    elif 'DS' in input_str:
        return 'Day Surgery'
    elif 'OP' in input_str or 'Outpatient' in input_str:
        return 'Outpatient'

def elementwise_nonmissing(df_list):
    anchor_df = df_list[0] #base source of truth
    assert len(set([df.shape for df in df_list])) == 1 
    for df in df_list[1:]:
        for row_idx in range(len(df)):
            for col_idx in range(len(list(df))):
                if pd.isnull(df.iloc[row_idx,col_idx])==False: 
                    if pd.isnull(anchor_df.iloc[row_idx,col_idx]):
                        anchor_df.iloc[row_idx,col_idx] =  df.iloc[row_idx,col_idx]
    return anchor_df