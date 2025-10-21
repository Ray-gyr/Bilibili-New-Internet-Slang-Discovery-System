import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'data', 'labeling.db')

def get_db(db_path=DB_FILE):
    conn = sqlite3.connect(db_path)
    return conn

