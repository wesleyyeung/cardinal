import hashlib
import json

with open('config/.secret','r') as f:
    conf = json.load(f)

def hash_sha1(input_string,start_idx=conf['start_idx'],output_length=conf['length'], salt = conf['salt']):
    combined_string = input_string + salt
    return hashlib.sha1(str.encode(combined_string)).hexdigest()[start_idx:start_idx+output_length]
