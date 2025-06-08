import argparse
import csv
import datetime
import glob
import os 
import warnings
warnings.filterwarnings('ignore', message="^Columns.*")
import yaml
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from tableinferer import TableNameInferer
from utils import redact_nric
import openpyxl
    
class Preprocess:
    """
    Purpose: 
        1. Read raw file directory and obtain file list
        2. Use TableNameInferer to infer tablename for each file
        3. Remove PHI columns where known (e.g. from CIS tables)
        4. Check if there are any residual strings that match ID numbers and replace with [REDACTED]
        5. Create new filename of format: schema.tablename_date.csv  
        6. Save files to preprocesed directory
    """
    def __init__(self, raw_path: str, preprocessed_path: str):
        self.threshold = 0.90
        self.raw_path = raw_path
        self.preprocessed_path = preprocessed_path
        self.filelist = glob.glob(raw_path+"/*.csv") #read all csv files
        self.filelist += glob.glob(raw_path+"/*.xlsx") #read all xlsx files
        self.filelist += glob.glob(raw_path+"/*.parquet") #read all parquet files
        self.preprocessed_files = []
        self.chunksize = 500000
        with open('config/schema.yml','r') as file:
            self.known_schemas = yaml.safe_load(file)
        with open('config/phi_columns.yml','r') as file:
            self.phi_columns = yaml.safe_load(file)
        try:
            with open('logs/preprocess.csv','r') as csvfile:
                reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
                for row in reader:
                    self.preprocessed_files += [row[0]]
        except:
            pass
 
    @staticmethod
    def get_max_datetime(df: pd.DataFrame) -> pd.Timestamp:
        now = pd.Timestamp.now()
        # Flatten and drop nulls
        flat = df.values.ravel()
        flat = flat[pd.notnull(flat)]
        # Filter likely date strings — very lightweight (no regex)
        is_str = np.vectorize(lambda x: isinstance(x, str))
        flat_str = flat[is_str(flat)]
        # Keep only strings with at least 2 slashes or 2 dashes
        date_like_mask = (
            np.char.count(flat_str.astype(str), '-') >= 2
        ) | (
            np.char.count(flat_str.astype(str), '/') >= 2
        )
        date_like = flat_str[date_like_mask]
        # Combine with any native datetime/datetime64 objects
        flat_dt = flat[~is_str(flat)]
        all_candidates = np.concatenate([flat_dt, date_like])
        # Convert all at once, suppress warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            dt_series = pd.to_datetime(all_candidates, errors='coerce')
        dt_series = dt_series[dt_series <= now]
        return dt_series.max()

    def infer_export_date(self, df: pd.DataFrame) -> str:
        max_dt = self.get_max_datetime(df)
        try: 
            return max_dt.strftime(format='%d-%m-%Y') 
        except BaseException as e:
            warnings.warn(f'Unable to infer date due to {e}')
            return 'unknowndate' + datetime.datetime.now().strftime(format='%d-%m-%Y') 

    def remove_phi_columns(self,df: pd.DataFrame,tablename: str) -> pd.DataFrame:
        drop_list = self.phi_columns.get(tablename,[])
        if drop_list:
            return df.drop(columns=drop_list,errors='ignore')
        else:
            return df

    @staticmethod
    def count_rows_csv(filepath: str) -> int:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for _ in f) - 1  # subtract 1 for header

    @staticmethod
    def count_rows_excel(filepath: str) -> int:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        sheet = wb.active
        return sheet.max_row - 1  # type: ignore # subtract 1 for header

    @staticmethod
    def count_rows_parquet(filepath: str) -> int:
        parquet_file = pq.ParquetFile(filepath)
        return parquet_file.metadata.num_rows

    def get_row_count(self, filepath: str, ext: str) -> int:
        if ext == '.csv':
            return self.count_rows_csv(filepath)
        elif ext in ['.xls', '.xlsx']:
            return self.count_rows_excel(filepath)
        elif ext == '.parquet':
            return self.count_rows_parquet(filepath)
        else:
            raise ValueError(f"Unsupported file type for row count: {filepath}")

    def preprocess_df(self, df: pd.DataFrame, suffix: str = '') -> bool:
        #2. Use TableTypeInferer to infer tablename for each file
        tablename, score = TableNameInferer(known_schemas=self.known_schemas,threshold=self.threshold).infer(test_cols=df.columns.tolist())
        if tablename == 'unknown':
            print(f"Unable to match {self.file} with any known tablename, closet match is {tablename} with {score*100}% match, skipping...")
            return False
        print(f"Matched {self.file} with {tablename} with {score*100}% match")    
        self.tablename = tablename
        #3. Remove PHI columns where known (e.g. from CIS tables)
        df = self.remove_phi_columns(df,tablename)
        #4. Check if there are any residual strings that match ID numbers and replace with [REDACTED]
        obj_cols = []
        for col in df.select_dtypes(include='object'):
            df[col] = df[col].apply(redact_nric)
            obj_cols += [col]
        #5. Create new filename of format: schema.tablename_date.csv  
        export_date = self.infer_export_date(df[obj_cols])
        output_filename = tablename+'_'+export_date+suffix+'.csv'
        df.to_csv(self.preprocessed_path+'/'+output_filename,index=False)
        del df
        return True

    def preprocess(self):
        #1. Read raw file directory and obtain file list
        for file in self.filelist:
            if file in self.preprocessed_files:
                print(f'{file} exists in logs, skipping..')
                continue
            print(f'Reading "{file}"')
            self.file = file
            ext = os.path.splitext(file)[1].lower()
            row_count = self.get_row_count(file, ext)
            if row_count > self.chunksize and ext == '.csv':
                print(f"{file} is large ({row_count} rows and csv). Using chunked processing.")
                chunk_iter = pd.read_csv(file, chunksize=self.chunksize)
                flag = []
                num_chunks = int(np.ceil(row_count/self.chunksize))
                for num, chunk in enumerate(chunk_iter):
                    print(f"Processing chunk {num+1} of {num_chunks}")
                    flag += [self.preprocess_df(chunk,'part'+str(num))]
                flag = all(flag)
            else:
                print(f"{file} is small with ({row_count} rows or non-csv). Loading fully.")
                if ext == '.csv':
                    df = pd.read_csv(file)
                elif ext in ['.xls', '.xlsx']:
                    df = pd.read_excel(file)
                elif ext == '.parquet':
                    df = pd.read_parquet(file)
                else:
                    warnings.warn(f"Unsupported file type: {file}, skipping")
                    continue
                flag = self.preprocess_df(df)
            
            if flag:
                #Log successful processing of file
                with open('logs/preprocess.csv','a') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    csvwriter.writerow([file , self.tablename, str(pd.Timestamp.now())])
            else:
                warnings.warn("Failed to preprocess {file}, skipping...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    with open('config/config.yml','r') as file:
        conf = yaml.safe_load(file)
    parser.add_argument("--raw_path",default=conf['raw_path'], required=False, help="Path to directory containing raw files")
    parser.add_argument("--preprocessed_path", default=conf['preprocessed_path'], required=False, help="Path to directory for preprocessed files")
    args = parser.parse_args()

    p = Preprocess(args.raw_path,args.preprocessed_path)
    p.preprocess()