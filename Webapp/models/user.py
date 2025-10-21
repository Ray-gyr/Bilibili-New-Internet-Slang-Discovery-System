from Webapp.models.db import get_db


def create_user_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin BOOLEAN DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def add_user(username, password_hash, is_admin=False):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)', 
                      (username, password_hash, 1 if is_admin else 0))
        conn.commit()
    finally:
        conn.close()
    return True

def get_user_by_username(username):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
    user = cursor.fetchone()
    conn.close()
    return user

def delete_user(username):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM users WHERE username = ?', (username,))
    conn.commit()
    conn.close()    

def get_user_by_id(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = cursor.fetchone()
    conn.close()
    return user

def update_user_password(user_id, new_password_hash):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE users SET password_hash = ? WHERE id = ?', (new_password_hash, user_id))
        conn.commit()
    finally:
        conn.close()
    return True

def is_user_admin(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT is_admin FROM users WHERE id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else False