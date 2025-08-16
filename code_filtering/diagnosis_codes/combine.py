import pandas as pd
import glob

lst = []
for file in glob.glob('*.xlsx'):
    lst += [pd.read_excel(file,dtype='object')]

lst[0] = lst[0].rename(columns={'CLINICAL_DIAGNOSIS_KEY':'diagnosis_code','CODE_SYSTEM':'code_system','DISPLAY_TEXT':'diagnosis_text'})
lst[1]['code_system'] = 'SNOMED-CT'
lst[2]['code_system'] = 'SNOMED-CT'
lst[2] = lst[2].rename(columns={'diagnosis':'diagnosis_text'})

lst[0]['db_source'] = 'edw'
lst[1]['db_source'] = 'dts'
lst[2]['db_source'] = 'dts'

for idx, _ in enumerate(lst):
    lst[idx] = lst[idx][['db_source','diagnosis_code','code_system','diagnosis_text']]
    lst[idx] = lst[idx].ffill()
    lst[idx] = lst[idx].dropna(how='any')

combined_diagnosis_codes = pd.concat(lst)

combined_diagnosis_codes.to_csv('ehr_diagnosis_codes.csv',index=False)