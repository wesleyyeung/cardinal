import argparse
import os
from utils import query

class PostProcess:

    def __init__(self,path: str):
        self.files = os.listdir(path)
        self.sql_files = [file for file in files if '.sql' in file]

    def postprocess(self):
        for sql_file in sql_files:
            with open(sql_file,'r') as file:
                print(f'Reading {sql_file}')
                sql = file.readlines()
                print('Executing query')
                query(sql=sql,return_df=False)
                print('Complete')

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default = 'postprocess/', required=False, help="Path to SQL scripts")
    args = parser.parse_args()
    pp = PostProcess(args.path)
    pp.postprocess()