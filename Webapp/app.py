from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from functools import wraps
from Webapp.models.db import get_db
from Webapp.models.words import get_words_for_user, get_word_by_id, insert_words_batch, create_words_table, update_word_status, get_accepted_words, batch_update_words_status
from Webapp.models.labels import submit_label_safe, get_user_labeled_words, get_word_vote_stats, create_labels_table, get_label_stats, get_today_words_labeled_count
from Webapp.models.user import create_user_table, add_user, get_user_by_username, get_user_by_id, update_user_password, is_user_admin
from Webapp.config import BATCH_SIZE, MAX_VOTES_PER_WORD, RAW_DATA_PATH
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio
from Data_Collection.SmartBiliCrawler import MultiCategoryHotCrawler, CommentDatabase
from Webapp.xgbFindWords import FindWords4XG
from Database.CommentDatabase import load_comments_batch_return_comment_oid
from xgbModel.xgbModel import xgbModel
import secrets
from werkzeug.security import generate_password_hash, check_password_hash
import json


app = Flask(__name__)
app.secret_key = secrets.token_hex(16)  
app.config['SESSION_COOKIE_NAME'] = 'slang_session'
app.config['SESSION_COOKIE_SECURE'] = False
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# initialize database tables
create_words_table()
create_labels_table()
create_user_table()

# 添加上下文处理器，使 is_user_admin 函数在所有模板中可用
@app.context_processor
def utility_processor():
    def check_user_admin():
        user_id = session.get("user_id")
        if not user_id:
            return False
        return is_user_admin(user_id)
    
    return dict(is_user_admin=check_user_admin)

# 管理员中间件检查
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = session.get("user_id")
        if not user_id or not is_user_admin(user_id):
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# index page
@app.route("/")
def index():
    user_id = session.get("user_id")
    username = session.get("username")
    return render_template("index.html", user_id=user_id, username=username)

# for stats on index page
@app.route("/stats/overview")
def get_stats_overview():
    conn = get_db()
    cursor = conn.cursor()
    
    # 获取候选词数量
    cursor.execute("SELECT COUNT(*) FROM words")
    candidate_words = cursor.fetchone()[0] or 0
    
    # 获取标签应用数量
    cursor.execute("SELECT COUNT(*) FROM labels")
    labels_applied = cursor.fetchone()[0] or 0
    
    # 获取活跃贡献者数量（至少标注过一次的用户）
    cursor.execute("SELECT COUNT(DISTINCT user_id) FROM labels")
    active_contributors = cursor.fetchone()[0] or 0
    
    # 获取已验证术语数量（状态为approved的词语）
    cursor.execute("SELECT COUNT(*) FROM words WHERE status='approved'")
    validated_terms = cursor.fetchone()[0] or 0
    
    conn.close()
    
    return jsonify({
        'candidate_words': candidate_words,
        'labels_applied': labels_applied,
        'active_contributors': active_contributors,
        'validated_terms': validated_terms
    })

# 登录页面
@app.route("/login")
def login_page():
    return render_template("login.html")

# 登录API
@app.route("/login", methods=["POST"])
def login():
    username = request.form.get("username")
    password = request.form.get("password")
    
    # 验证用户
    user = get_user_by_username(username)
    if user and check_password_hash(user[2], password):
        session["user_id"] = user[0]
        session["username"] = username
        session["is_admin"] = is_user_admin(user[0])  # 存储管理员状态到session
        return jsonify({"success": True, "message": "Login successful"})
    else:
        return jsonify({"success": False, "message": "Invalid username or password"}), 401

# 注册页面
@app.route("/register")
def register_page():
    return render_template("register.html")

@app.route("/register", methods=["POST"])
def register():
    username = request.form.get("username")
    password = request.form.get("password")
    
    # 检查用户名是否已存在
    if get_user_by_username(username):
        return jsonify({"success": False, "message": "Username already exists"}), 400
    
    add_user(username, generate_password_hash(password))
    return jsonify({"success": True, "message": "Registration successful"})

@app.route("/labeling")
def labeling():
    return render_template("labeling.html", active_page='labeling')

# 获取词语批次API - 修改为按机器分数优先级返回
@app.route("/words/batch", methods=["GET"])
def get_batch_words():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "Not logged in"}), 401

    # 优先获取机器分数高的词语
    batch = get_words_for_user(user_id, batch_size=BATCH_SIZE, max_votes=MAX_VOTES_PER_WORD)
    return jsonify(batch)

# 获取已标注词语API
@app.route("/words/stats", methods=["GET"])
def get_user_stats():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "Not logged in"}), 401
    
    # 获取用户标注统计
    user_stats = get_label_stats(user_id)
    today_count = get_today_words_labeled_count(user_id)
    total_count = user_stats.get('total_labels', 0)
    
    return jsonify({
        "today_count": today_count,
        "total_count": total_count
    })

# 提交标注API
@app.route("/words/submit", methods=["POST"])
def submit_word_label():
    print("Received label submission")
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "Not logged in"}), 401

    data = request.get_json()
    word_id = data.get("word_id")
    label = data.get("label")  # 1 / 0
    print(f"User {user_id} labeled word {word_id} with label {label}")

    if word_id is None or label is None:
        return jsonify({"error": "Lack params"}), 400

    submit_label_safe(word_id, user_id, int(label))
    vote_stats = get_word_vote_stats(word_id)
    
    today_count = get_today_words_labeled_count(user_id)
    total_count = get_label_stats(user_id).get('total_labels', 0)

    return jsonify({"success":True, 
                    "vote_stats": {
                        "yes_count": vote_stats['positive'], 
                        "negative_count": vote_stats['negative']},
                    "today_count": today_count,
                    "total_count": total_count})

# statistics page
@app.route("/statistics")
def statistics():
    user_id = session.get("user_id")
    
    # 获取全局统计
    global_stats = get_label_stats()
    
    # 如果用户已登录，获取用户统计
    user_stats = get_label_stats(user_id) if user_id else None
    
    return render_template("statistics.html", 
                          active_page='statistics',
                          user_stats=user_stats,
                          global_stats=global_stats,
                          is_logged_in=bool(user_id))

# 设置页面
@app.route("/settings")
def settings(): 
    return render_template("settings.html", active_page='settings')

# 修改密码API
@app.route("/change_password", methods=["POST"])
def change_password():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"success": False, "message": "Not logged in"}), 401
    
    data = request.get_json()
    current_password = data.get("currentPassword")
    new_password = data.get("newPassword")

    if current_password == new_password:
        return jsonify({"success": False, "message": "New password cannot be the same as the current password"}), 400
    
    if not current_password or not new_password:
        return jsonify({"success": False, "message": "Missing required fields"}), 400
    
    # 获取用户信息
    user = get_user_by_id(user_id)
    if not user:
        return jsonify({"success": False, "message": "User not found"}), 404
    
    # 验证当前密码
    if not check_password_hash(user[2], current_password):
        return jsonify({"success": False, "message": "Current password is incorrect"}), 401
    
    # 更新密码
    update_user_password(user_id, generate_password_hash(new_password))
    
    return jsonify({"success": True, "message": "Password updated successfully"})

@app.route("/logout")
def logout():
    session.pop("user_id", None)
    session.pop("username", None)
    session.pop("is_admin", None)  # 清除管理员状态
    return redirect(url_for('index'))

# 管理员审核页面
@app.route("/admin/review")
@admin_required
def admin_review():
    # 获取搜索参数
    search = request.args.get('search', '')
    status_filter = request.args.get('status', 'all')
    min_votes = int(request.args.get('min_votes', 3))
    
    page = int(request.args.get('page', 1))
    per_page = 20
    
    # 构建查询
    query = """
        SELECT w.*, 
               COUNT(l.id) as vote_count,
               SUM(CASE WHEN l.label = 1 THEN 1 ELSE 0 END) as yes_votes,
               SUM(CASE WHEN l.label = 0 THEN 1 ELSE 0 END) as no_votes
        FROM words w
        LEFT JOIN labels l ON w.id = l.word_id
        WHERE 1=1
    """
    params = []
    
    if search:
        query += " AND w.word LIKE ?"
        params.append(f"%{search}%")
    
    if status_filter != 'all':
        query += " AND w.status = ?"
        params.append(status_filter)
    
    # 添加投票数筛选
    query += " GROUP BY w.id HAVING vote_count >= ?"
    params.append(min_votes)
    
    # 获取总数
    conn = get_db()
    cursor = conn.cursor()
    
    # 先获取总数
    count_query = f"SELECT COUNT(*) FROM ({query})"
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()[0] or 0
    
    # 添加分页
    query += " ORDER BY vote_count DESC, w.id DESC LIMIT ? OFFSET ?"
    params.extend([per_page, (page-1)*per_page])
    
    cursor.execute(query, params)
    cols = [c[0] for c in cursor.description]
    words = [dict(zip(cols, row)) for row in cursor.fetchall()]
    conn.close()
    
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    
    # 确保页码在有效范围内
    if page > total_pages:
        page = total_pages
    if page < 1:
        page = 1
    
    return render_template("admin_review.html", 
                         words=words, 
                         page=page, 
                         total_pages=total_pages,
                         search=search,
                         status_filter=status_filter,
                         min_votes=min_votes)

# 批量更新词语状态
@app.route("/admin/batch_update", methods=["POST"])
@admin_required
def batch_update_words():
    data = request.get_json()
    word_ids = data.get('word_ids', [])
    action = data.get('action')  # 'approve' or 'reject'
    
    if not word_ids or not action:
        return jsonify({"success": False, "message": "缺少参数"})
    
    status = "approved" if action == "approve" else "rejected"
    
    conn = get_db()
    try:
        # 使用参数化查询防止SQL注入
        placeholders = ','.join('?' for _ in word_ids)
        query = f"UPDATE words SET status = ? WHERE id IN ({placeholders})"
        conn.execute(query, [status] + word_ids)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "message": str(e)})
    finally:
        conn.close()
    
    return jsonify({"success": True})

# 获取词语详情（用于模态框）
@app.route("/admin/word/<int:word_id>")
@admin_required
def get_word_detail(word_id):
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # 获取词语信息
        cursor.execute("SELECT * FROM words WHERE id = ?", (word_id,))
        row = cursor.fetchone()

        if not row:
            return jsonify({"error": "词语不存在"}), 404

        # 转换结果为字典
        cols = [c[0] for c in cursor.description]
        word_dict = dict(zip(cols, row))

        # 处理 sentences 字段
        try:
            if word_dict.get("sentences"):
                word_dict["sentences"] = json.loads(word_dict["sentences"])
        except json.JSONDecodeError:
            word_dict["sentences"] = [word_dict["sentences"]] if word_dict.get("sentences") else []

        # 获取投票详情
        cursor.execute("""
            SELECT l.*, u.username 
            FROM labels l 
            JOIN users u ON l.user_id = u.id 
            WHERE l.word_id = ?
            ORDER BY l.timestamp DESC
        """, (word_id,))
        labels = cursor.fetchall()
        label_cols = [c[0] for c in cursor.description]

        labels_list = [dict(zip(label_cols, r)) for r in labels]

        conn.close()

        return jsonify({
            "word": word_dict,
            "labels": labels_list
        })
        
    except Exception as e:
        print(f"Error in get_word_detail: {e}")
        return jsonify({"error": "服务器内部错误"}), 500

# Browse Dictionary page
@app.route("/dictionary")
def browse_dictionary():
    # 获取搜索参数
    search = request.args.get('search', '')
    sort_by = request.args.get('sort_by', 'word')
    sort_order = request.args.get('sort_order', 'asc')
    
    page = int(request.args.get('page', 1))
    per_page = 20
    
    # 构建查询 - 只获取已批准的词汇
    query = """
        SELECT w.*
        FROM words w
        WHERE w.status = 'approved'
    """
    params = []
    
    if search:
        query += " AND w.word LIKE ?"
        params.append(f"%{search}%")
    
    # 添加排序
    valid_sort_columns = ['word', 'machine_score', 'human_score', 'tfidf', 'pmi']
    if sort_by in valid_sort_columns:
        query += f" ORDER BY w.{sort_by} {sort_order.upper()}"
    
    # 获取总数
    conn = get_db()
    cursor = conn.cursor()
    
    # 先获取总数
    count_query = f"SELECT COUNT(*) FROM ({query})"
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()[0] or 0
    
    # 添加分页
    query += " LIMIT ? OFFSET ?"
    params.extend([per_page, (page-1)*per_page])
    
    cursor.execute(query, params)
    cols = [c[0] for c in cursor.description]
    words = [dict(zip(cols, row)) for row in cursor.fetchall()]
    conn.close()
    
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    
    # 确保页码在有效范围内
    if page > total_pages:
        page = total_pages
    if page < 1:
        page = 1
    
    return render_template("dictionary.html", 
                         words=words, 
                         page=page, 
                         total_pages=total_pages,
                         search=search,
                         sort_by=sort_by,
                         sort_order=sort_order)

# Trending Slang page
@app.route("/trending")
def trending_slang():
    return render_template("trending.html", active_page='trending')

# Trending slang API
@app.route("/api/trending_words")
def get_trending_words():
    # Get query parameters
    max_words = request.args.get('max_words', 100, type=int)
    min_freq = request.args.get('min_freq', 0.0, type=float)
    
    # Validate parameters
    max_words = min(max_words, 200)  # Cap at 200 words
    max_words = max(max_words, 10)   # Minimum 10 words
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Get trending words based on log_freq
    cursor.execute("""
        SELECT word, log_freq 
        FROM words 
        WHERE status = 'approved' 
        AND log_freq IS NOT NULL 
        AND log_freq >= ?
        ORDER BY log_freq DESC 
        LIMIT ?
    """, (min_freq, max_words))
    
    words = cursor.fetchall()
    conn.close()
    
    # Convert to list of dictionaries
    word_list = [{"word": row[0], "log_freq": row[1]} for row in words]
    
    return jsonify({
        "words": word_list,
        "count": len(word_list)
    })

# 每日任务 - 获取新词语
def daily_job():
    print("Daily job executed")
    create_words_table()
    create_labels_table()

    # 爬取新评论并处理
    async def main():
        # Step 1: Crawl new comments
        async def crawl_comments(): 
            db = CommentDatabase()
            db.show_information()
            crawler = MultiCategoryHotCrawler(db, max_comments_per_video=50)
            
            try:
                await crawler.crawl_strategically(total_videos=1)
                print("Smart crawling completed")
            except Exception as e:
                print(f"Error: {e}")
            finally:
                await crawler.close_session()
        
        # Step 2: Process comments to find candidate words
        async def find_new_words():
            db_path = RAW_DATA_PATH
            comments_batch = load_comments_batch_return_comment_oid(size=5000, db_path=db_path)
        
            discoverer=FindWords4XG()
            model=xgbModel()
            for comment_oid in comments_batch:
                discoverer.add_comments(comment_oid)
            results = discoverer.get_results()
            # 使用模型筛选结果
            model.predict(results, 0.27)
            words_list = model.return_tuple_list()
            print(f"Identified {len(words_list)} candidate words")
            
            # 将候选词插入数据库
            insert_words_batch(words_list)
            print("Inserted candidate words into the database")

        # 执行任务
        await crawl_comments()
        await find_new_words() 

    asyncio.run(main())

# 设置定时任务
scheduler = BackgroundScheduler()
scheduler.add_job(daily_job, 'cron', hour=3)  # 每天3点执行
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True)