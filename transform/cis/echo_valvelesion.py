import pandas as pd
from transform.base_transform import BaseTransform

class EchoValveTextParser:
    
    def __init__(self):
        self.patterns = ['§','¤','. ']
        self.eval_matrix = {}
        self.eval_matrix['valve_type'] = {
            'nonexclusive':{},
            'exclusive':{
                'mechanical':{
                'on x',
                'tilting',
                'starr edward',
                'st jude',
                'bileaflet',
                'mechanical'
                },
                'bioprosthetic':{
                    'bioprosthesis'
                },
                'tavr':{
                    'sapien',
                    'evolut',
                    'corevalv'
                    'tavi',
                    'tavr'
                },
                'mitraclip':{
                    'mitraclip'
                },
                'triclip':{
                    'triclip'
                }
            }
        }
        self.eval_matrix['anatomical_valve_types'] = {
            'nonexclusive':{
                'aortic':{
                    'aortic'
                },
                'mitral':{
                    'mitral'
                },
                'tricuspid':{
                    'tricuspid'
                },
                'pulmonary':{
                    'pulmonary'
                    'pulmonic'
                }
            },
            'exclusive':{}
        }
        self.eval_matrix['lesion_type'] = {
            'exclusive':{
                'stenosis':{
                    'stenosis'
                },
                'regurgitation':{
                    'regurg',
                },
                'prolapse':{
                    'prolapse',
                    'vp',
                    'vp',
                    'flail'
                },
                'bicuspid_valve':{
                    'bicuspid',
                    'bav',
                    'opening line'
                },
                'annular_calcification':{
                    'annular calcification'
                },
                'systolic_anterior_motion':{
                    'sam',
                    'systolic anterior motion'
                },
                'vegetation':{
                    'vegetation'
                },
                'mass':{
                    'mass'
                }
            },
            'nonexclusive':{}
        }
        self.eval_matrix['severity'] = {
            'nonexclusive':{},
            'exclusive':{
                '_severe':{
                    'severe'
                },
                '_moderate':{
                    'mod',
                    'moderate'
                },
                '_mild':{
                    'mild'
                },
                '_trivial':{
                    'trivial'
                }
            }
        }

    def flatten(self, input_list: list) -> list:
        return [sublst for sublsts in input_list for sublst in sublsts]
    
    def split_statements_by_pattern(self,input_string: str, pattern: str) -> list:
        return input_string.split(pattern)

    def recursive_split(self,x,patterns):
        if not patterns:
            return x
        else:
            pattern = patterns.pop()
            if isinstance(x,list):
                new_x = []
                for item in x:
                    new_x += [self.split_statements_by_pattern(item,pattern)]
                x = self.flatten(new_x)
            elif isinstance(x,str):
                x = self.split_statements_by_pattern(x,pattern)
            else:
                raise
            return self.recursive_split(x,patterns)

    def key_from_value_partial_string_match(self,final_key,input_dict,input_string):
        for key,sub_dict in input_dict.items():
            if not sub_dict: #sub_dict is empty
                continue
            if key == 'nonexclusive':
                for subkey,values in sub_dict.items():
                    if any([value in input_string for value in values]):
                        final_key = subkey if final_key == '' else final_key + '_' + subkey
                return final_key
            elif key == 'exclusive':
                for subkey,values in sub_dict.items():
                    if any([value in input_string for value in values]):
                        if final_key == '':
                            return subkey
                        else: 
                            return final_key + '_' + subkey
                return ''
            else:
                raise ValueError(f'{key} not nonexclusive or exclusive!')

    def evaluate(self,input_string):
        final_key = ''
        for dictionary in self.eval_matrix.values():
            final_key = self.key_from_value_partial_string_match(final_key,dictionary,input_string)
        return final_key
    
    def classify_anatomical_valve_type(self,input_string):
        output_list = []
        valves = self.eval_matrix['anatomical_valve_types']['nonexclusive'].keys()
        stem = input_string
        for valve in valves:
            stem = stem.replace(valve,'')
        for valve in valves:
            if valve in input_string:
                output_list += [(valve+'_'+stem)]
        return output_list

    def reshape_list(self,input_list):
        output_list = []
        for item in input_list:
            item = item.replace('____','__')
            location,lesion,severity = item.split('__')
            key_name = location+'_'+lesion
            key_name = key_name.replace('__','_')
            output_list += [{key_name:severity}]

        output_dict = {}
        for d in output_list:
            output_dict.update(d)
        return output_dict
    
    def parse(self, input):
        self.attributes = {}
        output = str(input).lower().replace('-',' ').replace('_',' ')
        output = self.recursive_split(output,self.patterns.copy())
        output = [self.evaluate(item) for item in output]
        output = [self.classify_anatomical_valve_type(item) for item in output]
        output = self.flatten(output)
        output = [item for item in output if item != '']
        output = self.reshape_list(output)
        return output

class CISEchoValveLesion(BaseTransform):
    
    def __init__(self):
        super().__init__()

    def custom_transform(self, df: pd.DataFrame) -> tuple:
        df = df[['idref','study_num','procedure_date','finaldxcollated']]
        evtp = EchoValveTextParser()
        valve_lesions = df['finaldxcollated'].apply(lambda row: evtp.parse(row)).tolist()
        valve_lesions = pd.DataFrame(valve_lesions)
        df = df.merge(valve_lesions,left_index=True,right_index=True)
        return df, "valvelesion"
        


