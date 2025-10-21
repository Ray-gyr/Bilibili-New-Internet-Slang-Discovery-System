from Webapp.models.db import get_db
import json

def create_words_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT,
            sentences TEXT,          -- json list of example sentences
            length INTEGER GENERATED ALWAYS AS (LENGTH(word)) VIRTUAL,
            log_freq REAL,
            pmi REAL,
            tfidf REAL,
            leftent REAL,
            rightent REAL,
            hot_video_ratio REAL,
            machine_score REAL DEFAULT 0,
            machine_label INTEGER DEFAULT 0,
            human_score REAL DEFAULT 0,
            human_label INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending'    -- 'pending', 'approved', 'rejected'
        )
    ''')
    conn.commit()
    conn.close()


def insert_word(word, sentences, log_freq, pmi, tfidf, leftent, rightent, hot_video_ratio,
                machine_score=0, machine_label=0):
    """
    insert single word
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO words (
            word, sentences, log_freq, pmi, tfidf, leftent, rightent, hot_video_ratio,
            machine_score, machine_label
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (word, json.dumps(sentences, ensure_ascii=False), log_freq, pmi, tfidf,
          leftent, rightent, hot_video_ratio, machine_score, machine_label))
    conn.commit()
    conn.close()


def insert_words_batch(words_list):
    """
    insert batch of words
    words_list: [(word, sentences_json, log_freq, pmi, tfidf, leftent, rightent, hot_video_ratio, machine_score, machine_label, status), ...]
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO words (
            word, sentences, log_freq, pmi, tfidf, leftent, rightent, hot_video_ratio,
            machine_score, machine_label, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', words_list)
    conn.commit()
    conn.close()


def get_words_for_user(user_id, batch_size=20, max_votes=3):
    """
    get unlabeled words for a user (not exceeding max_votes limit)
    prioritize words with higher machine scores to ensure the quality of labeling
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT w.id, w.word, w.sentences, w.log_freq, w.pmi, w.tfidf,
               w.leftent, w.rightent, w.hot_video_ratio, w.machine_score,
               w.machine_label, w.human_score, w.human_label, w.status
        FROM words w 
        LEFT JOIN (
            SELECT word_id, COUNT(*) AS vote_count
            FROM labels
            GROUP BY word_id
        ) l ON w.id = l.word_id
        WHERE w.id NOT IN (
            SELECT word_id FROM labels WHERE user_id=?
        )
        AND (l.vote_count IS NULL OR l.vote_count < ?)
        AND w.status = 'pending'
        ORDER BY 
            -- 优先选择模型最不确定的样本（概率接近0.5）
            ABS(w.machine_score - 0.5) ASC,
            -- 其次考虑尚未被标注或标注次数少的样本
            COALESCE(l.vote_count, 0) ASC,
            -- 最后随机排序，增加多样性
            RANDOM()
        LIMIT ?
    """, (user_id, max_votes, batch_size))

    rows = cursor.fetchall()
    cols=[c[0] for c in cursor.description]
    conn.close()

    result = []
    for r in rows:
        d = dict(zip(cols, r))
        d["sentences"] = json.loads(d["sentences"])
        result.append(d)
    return result

def get_word_by_id(word_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM words WHERE id=?", (word_id,)).fetchone()
    conn.close()
    if row:
        d = dict(row)
        d["sentences"] = json.loads(d["sentences"])
        return d
    return None

def update_word_status(word_id, status):
    """
    status: 'pending', 'approved', 'rejected'
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE words SET status=? WHERE id=?", (status, word_id))
    conn.commit()
    conn.close()

def batch_update_words_status(word_ids, status):
    """
    批量更新词语状态
    """
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        placeholders = ','.join('?' for _ in word_ids)
        query = f"UPDATE words SET status = ? WHERE id IN ({placeholders})"
        cursor.execute(query, [status] + word_ids)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
        
def get_accepted_words():
    """
    get all words with status 'approved' or 'rejected'
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM words WHERE status='approved' or status='rejected'")
    rows = cursor.fetchall()
    conn.close()

    result = set(w[1] for w in rows) 
    return result

def get_all_words():
    """
    get all words in the database
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM words")
    rows = cursor.fetchall()
    conn.close()

    result = set(w[1] for w in rows) 
    return result

def get_everything_from_words():
    """
    get all columns for all words
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM words
                   ORDER BY 
                    -- 优先选择模型最不确定的样本（概率接近0.5）
                    ABS(machine_score - 0.5) ASC""")
    rows = cursor.fetchall()
    cols=[c[0] for c in cursor.description]
    conn.close()

    result = []
    for r in rows:
        d = dict(zip(cols, r))
        d["sentences"] = json.loads(d["sentences"])
        result.append(d)
    return result