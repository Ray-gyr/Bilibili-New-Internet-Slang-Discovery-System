import re
import os 

base_dir=os.path.dirname(os.path.abspath(__file__))
emoticons_file=os.path.join(base_dir,"emoticons.txt")

class CommentCleaner:
    def __init__(self, emoticons_file=emoticons_file, min_length=2):
        """
        emoticons_file: 存储颜文字的 txt，每行一个
        min_length: 去掉过短评论
        """
        self.min_length = min_length
        self.pattern = None

        if emoticons_file:
            self.load_emoticons(emoticons_file)

        # 通用 emoji 正则
        self.emoji_pattern = re.compile(
            "["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map
            u"\U0001F700-\U0001F77F"  # alchemical symbols
            u"\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
            u"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
            u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            u"\U0001FA00-\U0001FA6F"  # Chess Symbols etc.
            u"\U0001FA70-\U0001FAFF"  # Symbols & Pictographs Extended-A
            u"\U00002702-\U000027B0"
            u"\U0001F170-\U0001F251"
            u"\U0001f926-\U0001f937"
            "]+", 
            flags=re.UNICODE
        )
        self.en_num_pattern = re.compile(r"[A-Za-z0-9]")
        

    def load_emoticons(self, file_path):
        """从 txt 文件加载颜文字库，生成正则"""
        with open(file_path, "r", encoding="utf-8") as f:
            emoticons = [line.strip() for line in f if line.strip()]
        # 按长度降序，避免部分匹配问题
        emoticons.sort(key=len, reverse=True)
        self.pattern = re.compile('|'.join(re.escape(e) for e in emoticons))

    def clean_comment(self, text):
        """清理单条评论"""
        if not text:
            return ''

        # Remove comments that have over 70% english or number
        en_num_count=len(self.en_num_pattern.findall(text))
        if en_num_count/len(text)>0.7:
            return ''

        # Remove "回复@xxx:" 前缀
        text = re.sub(r'^回复\s*@[^:：]+[:：]', '', text).strip()

        # Remove bilibili style emojis like [表情]
        text = re.sub(r'\[.*?\]', '', text)

        # Remove URLs
        text = re.sub(r'(https?://\S+|www\.\S+)', '', text)

        # Remove mentions
        text = re.sub(r'@[^\s@]+', '', text)

        # Remove unicode emojis
        text = self.emoji_pattern.sub('', text)

        # Remove emoticons from loaded txt
        if self.pattern:
            text = self.pattern.sub('', text)
        
        # Remove punctuations
        text=re.sub(r'[^\w\s]', '', text)

        # Remove underline
        text = re.sub(r'__+', '', text)

        # Merge multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()

        # Remove too short text
        if text and len(text) < self.min_length:
            return ''

        return text
