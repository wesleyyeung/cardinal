from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher
from rapidfuzz import fuzz, process
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class TableTypeInferer:
    def __init__(self, known_schemas: Dict[str, List[str]], threshold: float = 0.95):
        self.known_schemas = known_schemas
        self.threshold = threshold

    @staticmethod
    def _normalize(col: str) -> str:
        return col.lower().replace("_", "")

    def infer_by_fuzzy_ratio(self, test_cols: List[str]) -> Tuple[str, float]:
        def match_score(ref_cols, test_cols):
            matched = 0
            for ref in ref_cols:
                best = max((SequenceMatcher(None, ref, test).ratio() for test in test_cols), default=0)
                if best >= self.threshold:
                    matched += 1
            return matched / len(ref_cols)

        best_match, best_score = 'unknown', 0
        for name, ref_cols in self.known_schemas.items():
            score = match_score(ref_cols, test_cols)
            if score > best_score:
                best_match, best_score = name, score
        return (best_match, best_score) if best_score >= self.threshold else ('unknown', best_score)

    def infer_by_jaccard(self, test_cols: List[str]) -> Tuple[str, float]:
        def jaccard(set1, set2):
            return len(set1 & set2) / len(set1 | set2)

        test_set = set(self._normalize(c) for c in test_cols)
        scores = {
            name: jaccard(set(self._normalize(c) for c in cols), test_set)
            for name, cols in self.known_schemas.items()
        }
        best = max(scores.items(), key=lambda x: x[1])
        return best if best[1] >= self.threshold else ('unknown', best[1])

    def infer_by_tfidf(self, test_cols: List[str]) -> Tuple[str, float]:
        schema_names = list(self.known_schemas.keys())
        corpus = [' '.join(cols) for cols in self.known_schemas.values()] + [' '.join(test_cols)]
        vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 4))
        tfidf = vectorizer.fit_transform(corpus)
        sims = cosine_similarity(tfidf[-1], tfidf[:-1]).flatten()
        best_idx = sims.argmax()
        best_score = sims[best_idx]
        return (schema_names[best_idx], best_score) if best_score >= self.threshold else ('unknown', best_score)

    def infer_by_fuzzy_token(self, test_cols: List[str]) -> Tuple[str, float]:
        def avg_score(ref, test):
            return sum(process.extractOne(col, test, scorer=fuzz.ratio)[1] for col in ref) / len(ref)

        best_match, best_score = 'unknown', 0
        for name, ref_cols in self.known_schemas.items():
            score = avg_score(ref_cols, test_cols)
            if score > best_score:
                best_match, best_score = name, score
        return (best_match, best_score / 100) if best_score / 100 >= self.threshold else ('unknown', best_score / 100)

    def infer(self, test_cols: List[str], method: str = 'fuzzy') -> Tuple[str, float]:
        methods = {
            'fuzzy': self.infer_by_fuzzy_ratio,
            'jaccard': self.infer_by_jaccard,
            'tfidf': self.infer_by_tfidf,
            'fuzzy_token': self.infer_by_fuzzy_token,
        }
        if method not in methods:
            raise ValueError(f"Unknown method '{method}'. Choose from: {list(methods.keys())}")
        return methods[method](test_cols)
