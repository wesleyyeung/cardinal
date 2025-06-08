from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher

class TableNameInferer:
    def __init__(self, known_schemas: Dict[str, List[str]], threshold: float = 0.95):
        self.known_schemas = known_schemas
        self.threshold = threshold

    def match_score(self, ref_cols, test_cols):
        matched = 0
        for ref in ref_cols:
            try:
                score = []
                for test in test_cols:
                    score += [SequenceMatcher(None, ref, test).ratio()]
                best = max(score, default=0)
            except:
                print(f"Test: {test}")
                print(f"Ref: {ref}")
                raise
            if best >= self.threshold:
                matched += 1
        return matched / len(ref_cols)

    def infer_by_fuzzy_ratio(self, test_cols: List[str]) -> Tuple[str, float]:
        best_match, best_score = 'unknown', 0
        for name, ref_cols in self.known_schemas.items():
            score = self.match_score(ref_cols, test_cols)
            if score > best_score:
                best_match, best_score = name, score
        return (best_match, best_score) if best_score >= self.threshold else ('unknown', best_score)

    def infer(self, test_cols: List[str], method: str = 'fuzzy') -> Tuple[str, float]:
        methods = {
            'fuzzy': self.infer_by_fuzzy_ratio,
        }
        if method not in methods:
            raise ValueError(f"Unknown method '{method}'. Choose from: {list(methods.keys())}")
        return methods[method](test_cols)
