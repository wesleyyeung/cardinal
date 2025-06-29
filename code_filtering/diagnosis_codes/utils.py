import pandas as pd
from fuzzywuzzy import fuzz

class PatternMatcher:

    def __init__(self,keywords: list, target_series: pd.Series, fuzzy: bool = True, threshold: int = 95):
        self.keywords = [str(keyword).lower() for keyword in keywords]
        self.series = target_series
        self.target_list = target_series.tolist()
        self.fuzzy = fuzzy
        self.output_list = []
        self.threshold = threshold

    def recursive_fuzzy_search(self, patterns: list, candidates: list, threshold: int, pruning_threshold: float = 0.5, matched: list = [], iteration_count: int = 0,verbose: int = 1) -> list:
        if not patterns: #when there are no more patterns to match, return sorted matched list
            return sorted(matched)
        else:
            iteration_count += 1
            if verbose >= 1:
                print(f'Iteration: {iteration_count}, patterns: {len(patterns)}, candidates: {len(candidates)}, matched: {len(matched)}')
            new_matches = [] #list of matches from this iteration
            remove = []
            for pattern in patterns:
                for candidate in candidates:
                    if pattern in candidate.lower():
                        new_matches += [candidate]
                    score = fuzz.partial_ratio(pattern, candidate.lower()) 
                    if score > threshold:
                        if verbose == 2:
                            print(f'Testing {pattern} against {candidate}; score = {score}')
                        new_matches += [candidate]
                    if score < (threshold * pruning_threshold):
                        remove += [candidate]
            matched  = matched + new_matches
            matched = list(set(matched)) #maintain unique set of matches
            candidates = [candidate for candidate in candidates if candidate not in matched+remove] #remove candidates from matches
            patterns = list(set(new_matches)) #use new_matches as the seed for the next iteration
        return self.recursive_fuzzy_search(patterns,candidates,threshold,pruning_threshold,matched,iteration_count,verbose)
    
    def match(self,verbose: bool = False) -> list:
        if self.fuzzy:
            return self.recursive_fuzzy_search(patterns=self.keywords,candidates=self.target_list,threshold =self.threshold)
        else:
            for keyword in self.keywords: 
                self.output_list += self.series[self.series.str.lower().str.contains(keyword)].tolist()
            return self.output_list