from collections import defaultdict, Counter
import math
import logging
import pickle
from typing import List, Optional, Iterator
from Data_Processing.Clean_Comments import CommentCleaner
from Database.CommentDatabase import video_hotness_map
from Webapp.models.words import get_all_words


"""
This version will sent params to XGBoost model directly. Not for training the model. 
"""

# set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('FindWords4XG')

DEFAULT_CONFIG = {
    'min_freq': 5,         
    'min_pmi': 3.0,          
    'min_entropy': 2.0,    
    'max_ngram': 6,             
    'min_word_length': 2,  
    'max_word_length': 6, 
}

class FindWords4XG:
    """
    stream new words discoverer class.
    
    usage example:
    discoverer = FindWords4XG()
    
    # add comments in batches
    for batch in comment_batches:
        discoverer.add_comments(batch)
    
    # get results
    results = discoverer.get_results()
    
    # save and load state
    discoverer.save_state("discoverer_state.pkl")
    discoverer2 = FindWords4XG.load_state("discoverer_state.pkl")
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        initialize the discoverer 
        
        :param config: optional
        """
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._init_accumulators()
        self.total_comments = 0
        self.total_chars = 0
        self.cleaner=CommentCleaner()

        # New data structures for TF-IDF
        self.aid_set = set()  # Set of all aids (video IDs)
        self.document_frequency = defaultdict(int)  # Document frequency for each word
        self.term_frequency = defaultdict(lambda: defaultdict(int))  # Term frequency per word per aid
        self.aid_by_word = defaultdict(set)  # Record which aids contain each word

        self.video_hot_map = video_hotness_map() # Record video of its hotness. key=aid, value=0/1

        self.found_words = get_all_words() # load found words to filter out

        logger.info("NewWordDiscoverer initialized with config: %s", self.config)

    def _init_accumulators(self):
        """initialize accumulators"""
        self.char_count = defaultdict(int)
        self.ngram_counts = defaultdict(lambda: defaultdict(int))
        self.left_neighbors = defaultdict(lambda: defaultdict(Counter))
        self.right_neighbors = defaultdict(lambda: defaultdict(Counter))
        self.sample_comments = defaultdict(list)
    
    def add_comments(self, comments_with_aid: List[tuple]):
        """
        Add comments along with their aid (video ID).
        
        :param comments_with_aid: List of tuples (comment, aid)
        """
        if not comments_with_aid:
            return
            
        self.total_comments += len(comments_with_aid)
        
        for item in comments_with_aid:
            if isinstance(item, tuple) and len(item) == 2:
                comment, aid = item
            else:
                # For backward compatibility, if only comment is provided, aid is None
                comment, aid = item, None

            cleaned_comment = self.cleaner.clean_comment(comment)
            if not cleaned_comment:
                continue
                
            self._process_comment(cleaned_comment, aid)
        
        logger.info(f"Processed {len(comments_with_aid)} comments, total: {self.total_comments}")
    
    def _process_comment(self, comment: str, aid: Optional[str] = None):
        """process single comment with optional aid"""
        n = len(comment)
        self.total_chars += n

        # Add aid to set if provided
        if aid is not None:
            self.aid_set.add(aid)
    
        for i in range(n):
            char = comment[i]
            self.char_count[char] += 1
            
            # go through all n-grams 
            for word_len in range(
                self.config['min_word_length'], 
                min(self.config['max_ngram'] + 1, n - i + 1)
            ):
                word = comment[i:i + word_len]

                if word.endswith(' ') or word.startswith(' '):
                    continue
                self.ngram_counts[word_len][word] += 1

                left_idx = max(0, i-20)
                right_idx = min(n, i+word_len+20)
                if len(self.sample_comments[word]) <= 5:
                    self.sample_comments[word].append([comment[left_idx:right_idx], i-left_idx, i-left_idx+word_len])
                
                if i > 0:
                    left_char = comment[i - 1]
                    self.left_neighbors[word_len][word][left_char] += 1
                
                if i + word_len < n:
                    right_char = comment[i + word_len]
                    self.right_neighbors[word_len][word][right_char] += 1

                # Update TF-IDF statistics if aid is provided
                if aid is not None:
                    # If this aid hasn't been recorded for this word, update document frequency
                    if aid not in self.aid_by_word[word]:
                        self.aid_by_word[word].add(aid)
                        self.document_frequency[word] += 1
                    self.term_frequency[word][aid] += 1


    def get_results(self, min_freq: Optional[int] = None, 
                min_pmi: Optional[float] = None, 
                min_entropy: Optional[float] = None) -> Iterator[List[dict]]:

        min_freq = min_freq or self.config['min_freq']
        min_pmi = min_pmi or self.config['min_pmi']
        min_entropy = min_entropy or self.config['min_entropy']

        logger.info("Generating results with thresholds: freq=%d, pmi=%.1f, entropy=%.1f", 
                min_freq, min_pmi, min_entropy)

        candidates = []  
        to_remove = set()
        N = len(self.aid_set)  # Total number of videos
        for word_len in range(self.config['min_word_length'], self.config['max_ngram'] + 1):
            for word, freq in self.ngram_counts[word_len].items():
                if word in self.found_words:
                    to_remove.add(word)
                    continue

                left_ent = self._calculate_entropy(self.left_neighbors[word_len].get(word, Counter()))
                right_ent = self._calculate_entropy(self.right_neighbors[word_len].get(word, Counter()))
                pmi = self._calculate_pmi(word, word_len)

                if freq < min_freq:
                    to_remove.add(word)
                    continue
                if pmi < min_pmi:
                    to_remove.add(word)
                    continue
                if left_ent == 0 or right_ent == 0:
                    to_remove.add(word)
                    continue
                if max(left_ent, right_ent) < min_entropy:
                    to_remove.add(word)
                    continue

                # Calculate TF-IDF
                if N > 0:
                    df = self.document_frequency.get(word, 0)
                    idf = math.log(N / (df + 1))  # Add-one smoothing
                    max_tf = max(self.term_frequency[word].values()) if word in self.term_frequency else 0
                    tfidf_value = max_tf * idf
                else:
                    tfidf_value = 0
                
                # Calculate hot_video_ratio
                total = len(self.aid_by_word[word])
                num_of_hot_video = sum(self.video_hot_map.get(aid, 0) for aid in self.aid_by_word[word])
                hot_video_ratio = num_of_hot_video / total if total > 0 else 0

                candidates.append({
                    'word': word,
                    'Length': word_len,
                    'log_freq': math.log2(freq),
                    'PMI': round(pmi, 5),
                    'LeftEnt': round(left_ent, 5),
                    'RightEnt': round(right_ent, 5),
                    'tfidf': round(tfidf_value, 5),
                    'hot_video_ratio':round(hot_video_ratio, 5),
                    'sample': self.sample_comments.get(word, [])
                })

        for word in to_remove:
            for d in (self.ngram_counts, self.left_neighbors, self.right_neighbors):
                for sub_d in d.values():
                    sub_d.pop(word, None)
            self.sample_comments.pop(word, None)

        logger.info("Generated %d candidate words before XGB", len(candidates))
        return candidates

    def _calculate_pmi(self, word: str, word_len: int) -> float:
        """calculate PMI for a given word"""
        if self.total_chars == 0:
            return float('-inf')
        
        product = 1
        for char in word:
            char_freq = self.char_count.get(char, 0)
            if char_freq == 0:
                return float('-inf')
            product *= char_freq
        
        if product == 0:
            return float('-inf')
        
        # PMI calculation
        try:
            return math.log2(
                (self.ngram_counts[word_len][word] * (self.total_chars ** (word_len - 1)) 
                / product
            ))
        except (ValueError, ZeroDivisionError):
            return float('-inf')
    
    def _calculate_entropy(self, counter: Counter) -> float:
        total = sum(counter.values())
        if total <= 1:  
            return 0.0
        
        entropy = 0.0
        for count in counter.values():
            p = count / total
            if p > 0:
                entropy -= p * math.log2(p)
                
        return entropy
    
    def save_state(self, file_path: str):
        state = {
            'config': self.config,
            'char_count': dict(self.char_count),
            'ngram_counts': {k: dict(v) for k, v in self.ngram_counts.items()},
            'left_neighbors': {
                k: {wk: dict(wv) for wk, wv in v.items()} 
                for k, v in self.left_neighbors.items()
            },
            'right_neighbors': {
                k: {wk: dict(wv) for wk, wv in v.items()} 
                for k, v in self.right_neighbors.items()
            },
            'total_comments': self.total_comments,
            'total_chars': self.total_chars,
            # Save TF-IDF related data
            'aid_set': list(self.aid_set),
            'document_frequency': dict(self.document_frequency),
            'term_frequency': {word: dict(aid_dict) for word, aid_dict in self.term_frequency.items()},
            'aid_by_word': {word: list(aid_list) for word, aid_list in self.aid_by_word.items()}
        }
        
        with open(file_path, 'wb') as f:
            pickle.dump(state, f)
        logger.info("Saved state to %s", file_path)
    
    @classmethod
    def load_state(cls, file_path: str) :
        with open(file_path, 'rb') as f:
            state = pickle.load(f)
        
        discoverer = cls(config=state.get('config'))
        discoverer.char_count = defaultdict(int, state['char_count'])
        
        discoverer.ngram_counts = defaultdict(lambda: defaultdict(int))
        for word_len, counts in state['ngram_counts'].items():
            discoverer.ngram_counts[word_len] = defaultdict(int, counts)
        
        discoverer.left_neighbors = defaultdict(lambda: defaultdict(Counter))
        for word_len, words in state['left_neighbors'].items():
            for word, neighbors in words.items():
                discoverer.left_neighbors[word_len][word] = Counter(neighbors)
        
        discoverer.right_neighbors = defaultdict(lambda: defaultdict(Counter))
        for word_len, words in state['right_neighbors'].items():
            for word, neighbors in words.items():
                discoverer.right_neighbors[word_len][word] = Counter(neighbors)
        
        discoverer.total_comments = state['total_comments']
        discoverer.total_chars = state['total_chars']
        
        # Load TF-IDF related data
        discoverer.aid_set = set(state.get('aid_set', []))
        discoverer.document_frequency = defaultdict(int, state.get('document_frequency', {}))
        discoverer.term_frequency = defaultdict(lambda: defaultdict(int))
        for word, aid_dict in state.get('term_frequency', {}).items():
            discoverer.term_frequency[word] = defaultdict(int, aid_dict)
        discoverer.aid_by_word = defaultdict(set)
        for word, aid_list in state.get('aid_by_word', {}).items():
            discoverer.aid_by_word[word] = set(aid_list)
        
        logger.info("Loaded state from %s with %d comments processed", 
                   file_path, discoverer.total_comments)
        return discoverer
    
