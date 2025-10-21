import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),"Webapp"))

from werkzeug.security import generate_password_hash
from Webapp.models.db import get_db

def delete_all_users():
    """删除所有用户（谨慎使用）"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users")
    conn.commit()
    conn.close()
    print("所有用户已被删除。")

def create_admin_user():
    """创建管理员用户"""
    username = "admin"
    password = "Asdf1234"
    
    # 检查用户是否已存在
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    existing_user = cursor.fetchone()
    
    if existing_user:
        print(f"用户 '{username}' 已存在，更新为管理员...")
        # 更新现有用户为管理员
        cursor.execute("UPDATE users SET is_admin = 1, password_hash = ? WHERE username = ?", 
                      (generate_password_hash(password), username))
    else:
        print(f"创建管理员用户 '{username}'...")
        # 创建新管理员用户
        cursor.execute("INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)", 
                      (username, generate_password_hash(password), 1))
    
    conn.commit()
    conn.close()
    print("管理员用户创建/更新成功！")

if __name__ == "__main__":
    delete_all_users()
    create_admin_user()