from collections import defaultdict
import hashlib
import re
import yaml

def sanitize_table_name(fname):
    return re.sub(r"\.csv$", "", fname.lower()).replace(" ", "_").replace("-","_")

def hash_sha1(input_string,start_idx=20,output_length=10):
    return hashlib.sha1(str.encode(input_string)).hexdigest()[start_idx:start_idx+output_length]

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
        output_dict = yaml.safe_load(f)
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
     
    def __init__(self,abbreviations=True):
        if abbreviations:
            self.ABBREVIATIONS = {
            'resting':'',
            'ecg':'',
            'interpretation': 'interp',
            'interpretations': 'interp',
            'statement': 'stmt',
            'components': 'comps',
            'coded': 'cd',
            'structure': 'struct',
            'datastructure': 'struct',
            'restingecgdata': 'restecg',
            'value': 'val',
            'entry': '',
            'data': '',
            'generalpatientdata': '',
            'information': 'info',
            'interpretationdatastructure': '',
            'statementcomponents':'stmtcomp',
            'reportinfo':'',
            'reportformat':'',
            'dataacquisition':'datacq',
            'signalcharacteristics':'sigcharct',
            'patientmedicaldata':'meddat',
            'bloodpressure':'bp',
            'internalmeasurements':'intmeas',
            'crossleadmeasurements':'xleadmeas',
            'groupmeasurements':'grpmeas',
            'groupmeasurement':'grpmeas',
            'groupnumber':'grpno',
            'codedstatement':'codedstmt',
            'unparsedstatement':'unparstmt',
            'statementnumber':'stmtno',
            'qualitystatement':'qualstmt',
            'variables':'var',
            'listof':'lstof',
            'numericvalue':'numvar',
            'serialcomparison':'srlcomp',
            'previousecg':'prevecg',
            'modifiers':'mod',
            'modifier':'mod',
            'globalmeasurements':'globmeas',
            'ecgmeasurements':'ecgmeas',
            'ecgsample':'ecgsamp'
        }
        else:
            self.ABBREVIATIONS = {}

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