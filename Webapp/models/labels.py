from Webapp.models.db import get_db

def create_labels_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            word_id INTEGER,
            label BOOLEAN,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (word_id) REFERENCES words(id)
        )
    ''')
    conn.commit()
    conn.close()

def submit_label_safe(word_id, user_id, label, max_votes=3):
    """
    concurrently safe
    
    one vote per user per word,
    statistics of human_score and human_label,
    status remains 'pending', to be updated by backend review
    """
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN TRANSACTION")

        # ignore duplicate votes from same user
        cursor.execute("""
            INSERT OR IGNORE INTO labels (word_id, user_id, label)
            VALUES (?, ?, ?)
        """, (word_id, user_id, label))

        # statistics
        cursor.execute("""
            SELECT label FROM labels
            WHERE word_id=?
            ORDER BY timestamp ASC
        """, (word_id,))
        rows = cursor.fetchall()

        if rows:
            labels = [r[0] for r in rows]
            if len(labels) >= max_votes:
                # update info when at least 3 votes
                labels_to_count = labels[:max_votes]
                human_score = sum(labels_to_count) / len(labels_to_count)
                human_label = 1 if human_score >= 0.5 else 0

                cursor.execute("""
                    UPDATE words
                    SET human_score=?, human_label=?
                    WHERE id=?
                """, (human_score, human_label, word_id))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_word_vote_stats(word_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS positive_count,
            SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) AS negative_count
        FROM labels
        WHERE word_id = ?
    """, (word_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return {'positive': result[0] or 0, 'negative': result[1] or 0}
    return {'positive': 0, 'negative': 0}

def get_user_labeled_words(user_id):
    conn = get_db()
    rows = conn.execute("""
        SELECT word_id FROM labels WHERE user_id=?
    """, (user_id,)).fetchall()
    conn.close()
    return [r["word_id"] for r in rows]

def get_label_stats(user_id=None):
    """
    get statistics of labels
    user_id: if provided, get stats for the specific user; otherwise get global stats
    """
    conn = get_db()
    cursor = conn.cursor()
    
    if user_id:
        # 用户个人统计
        cursor.execute("""
            SELECT 
                COUNT(*) as total_labels,
                SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) as positive_labels,
                SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) as negative_labels
            FROM labels
            WHERE user_id = ?
        """, (user_id,))
    else:
        # 全局统计
        cursor.execute("""
            SELECT 
                COUNT(*) as total_labels,
                SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) as positive_labels,
                SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) as negative_labels
            FROM labels
        """)
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'total_labels': result[0] or 0,
            'positive_labels': result[1] or 0,
            'negative_labels': result[2] or 0,
            'positive_ratio': result[1] / result[0] if result[0] > 0 else 0
        }
    return {
        'total_labels': 0,
        'positive_labels': 0,
        'negative_labels': 0,
        'positive_ratio': 0
    }

def get_today_words_labeled_count(user_id=None):
    """
    get count of words labeled today
    user_id: if provided, get count for the specific user; otherwise get global count
    """
    conn = get_db()
    cursor = conn.cursor()
    
    if user_id:
        cursor.execute("""
            SELECT COUNT(DISTINCT word_id) 
            FROM labels 
            WHERE user_id = ? 
              AND DATE(timestamp) = DATE('now', 'localtime')
        """, (user_id,))
    else:
        cursor.execute("""
            SELECT COUNT(DISTINCT word_id) 
            FROM labels 
            WHERE DATE(timestamp) = DATE('now', 'localtime')
        """)
    
    result = cursor.fetchone()
    conn.close()
    
    return int(result[0]) if result else 0