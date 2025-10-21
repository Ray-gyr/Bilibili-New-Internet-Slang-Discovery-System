# BiliCrawler.py
import asyncio
import random
import json
import httpx
import hashlib
import os
import time
import pickle
from datetime import datetime
from typing import Any, Iterable, List, Dict, Optional, Tuple, Generator
from urllib.parse import urlencode
from playwright.async_api import async_playwright, BrowserContext, Page
import sqlite3

def ensure_dir_exists(path: str):
    """确保目录存在，不存在则创建"""
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

BASEDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 数据库文件路径
DB_FILE = os.path.join(BASEDIR, "Database", "raw_bilibili_comments.db")

# 工具函数
def get_user_agent() -> str:
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def convert_cookies(cookies: List[Dict]) -> Tuple[str, Dict]:
    """将cookies列表转换为字符串和字典"""
    cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    cookie_dict = {c['name']: c['value'] for c in cookies}
    return cookie_str, cookie_dict

def get_unix_timestamp() -> int:
    """获取当前Unix时间戳"""
    return int(datetime.now().timestamp())

class DataFetchError(Exception):
    """数据获取错误异常"""

# 签名帮助类
class BilibiliSign:
    def __init__(self, img_key: str, sub_key: str):
        self.img_key = img_key
        self.sub_key = sub_key
        self.map_table = [
            46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
            33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
            61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
            36, 20, 34, 44, 52
        ]

    def get_salt(self) -> str:
        """获取加盐的key"""
        mixin_key = self.img_key + self.sub_key
        salt = ''.join(mixin_key[i] for i in self.map_table)
        return salt[:32]

    def sign(self, req_data: Dict) -> Dict:
        """请求参数签名"""
        current_ts = get_unix_timestamp()
        req_data.update({"wts": current_ts})
        sorted_data = dict(sorted(req_data.items()))
        
        # 过滤特殊字符
        filtered_data = {
            k: ''.join(ch for ch in str(v) if ch not in "!'()*")
            for k, v in sorted_data.items()
        }
        
        query = urlencode(filtered_data)
        salt = self.get_salt()
        
        # 使用hashlib计算MD5
        wbi_sign = hashlib.md5((query + salt).encode()).hexdigest()
        
        filtered_data['w_rid'] = wbi_sign
        return filtered_data

# B站登录类
class BilibiliLogin:
    def __init__(self, context_page: Page, browser_context: BrowserContext, crawler):
        self.context_page = context_page
        self.browser_context = browser_context
        self.login_timeout = 300  # 5分钟超时
        self.crawler = crawler  # 添加爬虫实例引用

    async def login_by_qrcode(self, cookies_file: str):
        """通过二维码登录B站"""
        print("Scan the qrcode to login...")
        
        # 等待登录成功
        await self.wait_for_login()
        print("Login success!")
        
        # 保存cookies（只保存一次）
        cookies = await self.browser_context.cookies()
        ensure_dir_exists(cookies_file)
        with open(cookies_file, "w") as f:
            json.dump(cookies, f)
        print(f"Cookies saved to {cookies_file}")

    async def wait_for_login(self):
        """等待用户完成登录"""
        start_time = time.time()
        
        while time.time() - start_time < self.login_timeout:
            try:
                # 使用爬虫的客户端检查登录状态
                if self.crawler.client and await self.crawler.client.pong():
                    print("通过 API 检测到登录成功!")
                    return
                
                # 检查页面元素
                if await self.context_page.is_visible("a.header-entry-mini") or \
                await self.context_page.is_visible("li.header-avatar-wrap") or \
                await self.context_page.is_visible("a.header-entry-avatar"):
                    print("通过页面元素检测到登录成功!")
                    return
                
                # 等待状态更新
                await asyncio.sleep(3)
                
            except Exception as e:
                print(f"登录检查错误: {str(e)}")
                await asyncio.sleep(3)
        
        raise TimeoutError("登录超时")

# 爬虫状态管理器
class CrawlerStateManager:
    def __init__(self, state_file: str):
        self.state_file = state_file
        ensure_dir_exists(state_file)
        self.state = self.load_state()
        
        # 初始化全局rpid集合
        self.global_rpid_set = self.state.get("global_rpid_set", set())
        
    def load_state(self):
        """加载爬虫状态"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"Load crawler status: {str(e)}")
        return {
            "global_rpid_set": set(),
            "video_progress": {}
        }
    
    def save_state(self):
        """保存爬虫状态"""
        self.state["global_rpid_set"] = self.global_rpid_set
        try:
            with open(self.state_file, "wb") as f:
                pickle.dump(self.state, f)
        except Exception as e:
            print(f"Fail to save crawler status: {str(e)}")
    
    def update_video_progress(self, aid: str, next_page: int, comment_count: int):
        """更新视频爬取进度"""
        self.state["video_progress"][aid] = {
            "next_page": next_page,
            "comment_count": comment_count,
            "last_updated": time.time()
        }
        self.save_state()
    
    def get_video_progress(self, aid: str) -> dict:
        """获取视频爬取进度"""
        return self.state["video_progress"].get(aid, {
            "next_page": 0,
            "comment_count": 0,
            "last_updated": 0
        })
    
    def add_rpid(self, rpid: int):
        """添加rpid到全局集合"""
        if rpid not in self.global_rpid_set:
            self.global_rpid_set.add(rpid)
            # 每添加100个rpid保存一次状态
            if len(self.global_rpid_set) % 100 == 0:
                self.save_state()
    
    def is_rpid_exists(self, rpid: int) -> bool:
        """检查rpid是否已存在"""
        return rpid in self.global_rpid_set

# B站客户端
class BilibiliClient:
    def __init__(
        self,
        timeout: int = 60,
        proxy: Optional[str] = None,
        *,
        headers: Dict[str, str],
        playwright_page: Page,
        cookie_dict: Dict[str, str],
    ):
        self.proxy = proxy
        self.timeout = timeout
        self.headers = headers
        self._host = "https://api.bilibili.com"
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict

    async def request(self, method: str, url: str, **kwargs) -> Any:
        """发送HTTP请求"""
        async with httpx.AsyncClient(proxy=self.proxy) as client:
            response = await client.request(
                method, url, timeout=self.timeout, **kwargs
            )
            
        try:
            data: Dict = response.json()
        except json.JSONDecodeError:
            raise DataFetchError(f"Failed to prase JSON. Content: {response.text}")
            
        if data.get("code") != 0:
            error_msg = data.get("message", "unknown error")
            raise DataFetchError(f"API error: {error_msg} (code: {data.get('code')})")
        return data.get("data", {})

    async def pre_request_data(self, req_data: Dict) -> Dict:
        """请求参数签名预处理"""
        if not req_data:
            return {}
        img_key, sub_key = await self.get_wbi_keys()
        return BilibiliSign(img_key, sub_key).sign(req_data)

    async def get_wbi_keys(self) -> Tuple[str, str]:
        """获取WBI签名密钥"""
        # 从localStorage获取密钥
        local_storage = await self.playwright_page.evaluate("() => window.localStorage")
        wbi_img_urls = local_storage.get("wbi_img_urls", "")
        
        if wbi_img_urls and "-" in wbi_img_urls:
            img_url, sub_url = wbi_img_urls.split("-")
        else:
            # 从API获取密钥
            try:
                resp = await self.request("GET", self._host + "/x/web-interface/nav")
                img_url: str = resp['wbi_img']['img_url']
                sub_url: str = resp['wbi_img']['sub_url']
            except DataFetchError as e:
                # 如果API请求失败，使用默认密钥
                img_url = "https://i0.hdslb.com/bfs/wbi/7cd084941338484aae1ad9425b84077c.png"
                sub_url = "https://i0.hdslb.com/bfs/wbi/4932caff0ff746eab6f01bf08b70ac45.png"
        
        img_key = img_url.rsplit('/', 1)[1].split('.')[0]
        sub_key = sub_url.rsplit('/', 1)[1].split('.')[0]
        return img_key, sub_key

    async def get(self, uri: str, params: Optional[Dict] = None, 
                 enable_params_sign: bool = True) -> Dict:
        """发送GET请求"""
        final_uri = uri
        if enable_params_sign and params:
            params = await self.pre_request_data(params)
        
        if params:
            final_uri = f"{uri}?{urlencode(params)}"
            
        return await self.request(
            "GET", f"{self._host}{final_uri}", headers=self.headers
        )

    async def pong(self) -> bool:
        """
        检查登录状态
        返回 True 表示已登录，False 表示未登录
        """
        try:
            # 调用 B 站的导航接口检查登录状态
            nav_data = await self.get("/x/web-interface/nav", enable_params_sign=False)
            return nav_data.get("isLogin", False)
        except Exception as e:
            print(f"检查登录状态失败: {e}")
            return False
    
    async def get_video_comments(
        self,
        video_id: str,
        next_page: int = 0,
    ) -> Dict:
        """获取视频评论"""
        uri = "/x/v2/reply/wbi/main"
        # 确保video_id是整数类型
        try:
            video_id_int = int(video_id)
        except (ValueError, TypeError):
            raise DataFetchError(f"Invalid video ID: {video_id}")
            
        post_data = {
            "oid": video_id_int,  # 使用整数类型的aid
            "mode": 0,  # 默认排序
            "type": 1, 
            "ps": 20, 
            "next": next_page
        }
        return await self.get(uri, post_data)

# 数据库操作类
class CommentDatabase:
    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        # 确保数据库目录存在
        db_dir = os.path.dirname(db_file)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        self.create_raw_comments_db()
        self.create_video_info_db()
    
    def create_raw_comments_db(self):
        """创建存储原始评论的数据库表"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_comments (
                rpid INTEGER PRIMARY KEY,
                aid INTEGER NOT NULL,
                comment TEXT NOT NULL,
                ctime INTEGER NOT NULL,
                category VARCHAR(50) Default 'Other',
                is_hot BOOLEAN DEFAULT FALSE
            )
        ''')
        # 创建索引以提高查询性能
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_aid ON raw_comments(aid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ctime ON raw_comments(ctime)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON raw_comments(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_hot ON raw_comments(is_hot)')
        conn.commit()
        conn.close()
        print("Raw comments database is set.")
    
    def create_video_info_db(self):
        """创建视频信息表"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS video_info (
                aid INTEGER PRIMARY KEY,
                title TEXT,
                category VARCHAR(50),
                is_hot BOOLEAN DEFAULT FALSE,
                hotness_score REAL DEFAULT 0,   
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')   #hotness_score 暂未使用
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_video_category ON video_info(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_video_hot ON video_info(is_hot)')
        conn.commit()
        conn.close()
        print("Video info database is set.")
    
    def save_comments_batch(self, comments_batch: List[Dict]):
        """将一批评论保存到数据库"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            # 准备批量插入数据
            data_to_insert = [
                (
                    comment['rpid'], comment['aid'], comment['comment'], 
                    comment['ctime'], comment.get('category', 'Other'),
                    comment.get('is_hot', False)
                )
                for comment in comments_batch
            ]
            
            # 执行批量插入
            cursor.executemany('''
                INSERT OR IGNORE INTO raw_comments 
                (rpid, aid, comment, ctime, category, is_hot)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            print(f"Save {len(comments_batch)} comments to database")
        except sqlite3.Error as e:
            print(f"数据库操作错误: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_video_info(self, video_data: Dict):
        """保存视频信息"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO video_info 
                (aid, title, category, is_hot, hotness_score, view_count, like_count, comment_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                video_data['aid'],
                video_data.get('title', ''),
                video_data.get('category', 'unknown'),
                video_data.get('is_hot', False),
                video_data.get('hotness_score', 0),
                video_data.get('view_count', 0),
                video_data.get('like_count', 0),
                video_data.get('comment_count', 0)
            ))
            
            conn.commit()
            print(f"Save video info for aid: {video_data['aid']}")
        except sqlite3.Error as e:
            print(f"数据库操作错误: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_video_count_by_category(self):
        """获取各分区的视频数量"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT category, COUNT(*) as count 
                FROM video_info 
                GROUP BY category
            ''')
            result = dict(cursor.fetchall())
            return result
        except sqlite3.Error as e:
            print(f"数据库操作错误: {e}")
            return {}
        finally:
            conn.close()
    
    def get_existing_aids(self):
        """获取已存在的视频aid列表"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT aid FROM video_info')
            result = {row[0] for row in cursor.fetchall()}
            return result
        except sqlite3.Error as e:
            print(f"数据库操作错误: {e}")
            return set()
        finally:
            conn.close()
    
    def show_information(self):
        conn=sqlite3.connect(self.db_file)
        cursor=conn.cursor()
        try:
            cursor.execute("""select count(*) from raw_comments""")
            count=cursor.fetchone()[0]
            print(f"There are {count} comments in the database.")
        except sqlite3.Error as e:
            print(f"数据库操作错误: {e}")
        finally:
            conn.commit()
            conn.close()

        result=self.get_video_count_by_category()
        total_value=sum(result.values())
        print('Ratio of each category:')
        for key in result:
            print(f'{key}:{round(result[key]/total_value,2)*100}%')

# 评论爬虫主类
class BilibiliCommentCrawler:
    def __init__(self, aid_list: List[str], max_comments_per_video: int = 50):
        """
        初始化B站评论爬虫
        
        :param aid_list: 视频aid列表
        :param max_comments_per_video: 每个视频最多爬取的评论数
        """
        self.aid_list = aid_list
        self.max_comments_per_video = max_comments_per_video
        self.browser_context: Optional[BrowserContext] = None
        self.context_page: Optional[Page] = None
        self.client: Optional[BilibiliClient] = None
        self.playwright = None
        self.logged_in = False
        self.extra_video_info = {}  # 存储额外的视频信息（分类和热门标记）

        # 统一文件路径管理
        self.data_dir = os.path.join(BASEDIR, "Data_Collection")
        self.cookies_file = os.path.join(self.data_dir, "bilibili_cookies.json")
        self.state_file = os.path.join(self.data_dir, "crawler_state.pkl")
        
        # 确保目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 确保数据库目录存在
        db_dir = os.path.dirname(DB_FILE)
        os.makedirs(db_dir, exist_ok=True)
        
        # 确保状态文件目录存在
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        
        # 初始化状态管理器
        self.state_manager = CrawlerStateManager(self.state_file)
        self.db = CommentDatabase()  # 初始化数据库操作对象

    async def setup_browser(self):
        """设置浏览器环境"""
        self.playwright = await async_playwright().start()
        browser = await self.playwright.chromium.launch(
            headless=False,  # 显示浏览器以便扫码
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--start-maximized"
            ]
        )
        self.browser_context = await browser.new_context(
            user_agent=get_user_agent(),
            viewport={"width": 1200, "height": 800},
            java_script_enabled=True,
            ignore_https_errors=True
        )
        self.context_page = await self.browser_context.new_page()
        
        # 加载反检测脚本
        await self.context_page.add_init_script("""
            delete navigator.__proto__.webdriver;
            Object.defineProperty(navigator, 'webdriver', {get: () => false});
        """)
        
        # 尝试加载保存的cookies
        if os.path.exists(self.cookies_file):
            try:
                with open(self.cookies_file, "r") as f:
                    cookies = json.load(f)
                await self.browser_context.add_cookies(cookies)
                print("Saved Cookies are loaded")
                self.logged_in = True
            except Exception as e:
                print(f"Failed to load Cookies: {str(e)}")
        
        await self.context_page.goto("https://www.bilibili.com", wait_until="networkidle")
        
    async def login_if_needed(self):
        """如果需要则进行登录"""
        # 先创建客户端，因为我们需要用客户端检查登录状态
        if self.client is None:
            await self.create_client()
        
        # 使用 API 检查登录状态
        try:
            if await self.client.pong():
                print("通过 API 检查确认已登录")
                self.logged_in = True
                return
        except Exception as e:
            print(f"API 登录检查失败: {e}")
        
        # 如果 API 检查失败或未登录，尝试使用页面元素检查
        try:
            # 检查是否已登录
            header_entry = await self.context_page.query_selector("a.header-entry-mini")
            avatar_wrap = await self.context_page.query_selector("li.header-avatar-wrap")
            entry_avatar = await self.context_page.query_selector("a.header-entry-avatar")
            if header_entry or avatar_wrap or entry_avatar:
                print("通过页面元素检查确认已登录")
                self.logged_in = True
                return
        except Exception as e:
            print(f"页面元素登录检查失败: {e}")
        
        # 如果两种方法都检查失败或未登录，进行二维码登录
        print("未检测到登录状态，开始二维码登录...")
        login_manager = BilibiliLogin(self.context_page, self.browser_context, self)
        await login_manager.login_by_qrcode(self.cookies_file)
        self.logged_in = True
        
        # 登录后更新客户端的 cookies
        await self.create_client()
        
        # 再次验证登录状态
        try:
            if await self.client.pong():
                print("登录成功并验证通过")
            else:
                print("登录后验证失败")
        except Exception as e:
            print(f"登录后验证失败: {e}")

    async def create_client(self):
        """创建API客户端"""
        # 创建客户端
        cookies = await self.browser_context.cookies()
        cookie_str, cookie_dict = convert_cookies(cookies)
        
        self.client = BilibiliClient(
            proxy=None,
            headers={
                "User-Agent": get_user_agent(),
                "Cookie": cookie_str,
                "Origin": "https://www.bilibili.com",
                "Referer": "https://www.bilibili.com",
                "Content-Type": "application/json;charset=UTF-8",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict
        )

    async def close(self):
        """关闭浏览器资源"""
        try:
            if self.browser_context:
                await self.browser_context.close()
            if self.playwright:
                await self.playwright.stop()
            # 保存最终状态
            self.state_manager.save_state()
        except Exception as e:
            print(f"Error while closing resources: {e}")

    async def ensure_browser_alive(self):
        """确保浏览器环境存活，如果已关闭则重新初始化"""
        try:
            # 检查浏览器上下文是否仍然有效
            if not self.browser_context:
                print("Browser context is closed, reinitializing...")
                await self.setup_browser()
                await self.login_if_needed()
                await self.create_client()
                return True
            return False
        except Exception as e:
            print(f"Error checking browser status: {e}")
            # 尝试重新初始化
            try:
                await self.setup_browser()
                await self.login_if_needed()
                await self.create_client()
                return True
            except Exception as e2:
                print(f"Failed to reinitialize browser: {e2}")
                raise e2

    async def crawl_comments(self) -> List[Dict]:
        """爬取所有视频的评论"""
        await self.setup_browser()
        await self.login_if_needed()
        await self.create_client()
        
        results = []
        for aid in self.aid_list:
            try:
                # 检查是否已完成爬取
                progress = self.state_manager.get_video_progress(aid)
                if progress["comment_count"] >= self.max_comments_per_video:
                    print(f"Video {aid} has been fetched.Skip")
                    continue
                    
                print(f"Start fetching comments from {aid}...")
                
                # 确保从正确的页码开始
                comments = await self.get_video_comments(aid)
                
                # 保存当前视频的评论到数据库
                if comments:
                    self.db.save_comments_batch(comments)
                    results.extend(comments)
                
                print(f"Successfully fetched {len(comments)} comments from {aid}")
                
                # 随机延迟防止请求过快
                delay = random.uniform(2.0, 5.0)
                print(f"Wait for {delay:.2f} seconds and continue...")
                await asyncio.sleep(delay)
            except DataFetchError as e:
                print(f"Failed to fetch commments from {aid} : {e}")
            except Exception as e:
                print(f"Error when processing {aid}: {str(e)}")
        
        return results

    async def get_video_comments(self, aid: str, start_page: int = 0) -> List[Dict]:
        """获取单个视频的评论，支持断点续爬"""
        # 确保浏览器环境存活
        await self.ensure_browser_alive()
        
        # 从状态管理器中获取进度
        progress = self.state_manager.get_video_progress(aid)
        next_page = progress["next_page"]  # 使用保存的下一页页码
        current_count = progress["comment_count"]
        
        # 如果已爬取数量超过限制，直接返回空列表
        if current_count >= self.max_comments_per_video:
            print(f"Reach maximum comments per video ({self.max_comments_per_video}) for {aid}. Skip")
            return []
        
        comments = []
        is_end = False
        max_retries = 3
        retry_count = 0
        
        print(f" {aid} : Start page={next_page}, # of comments obtained ={current_count}/{self.max_comments_per_video}")
        
        # 添加标志位控制是否达到限制
        reached_max = False
        
        while not is_end and not reached_max:
            try:
                # 显示当前爬取进度
                print(f"Start fetching page {next_page} from {aid} ...")
                
                comments_res = await self.client.get_video_comments(
                    video_id=aid, 
                    next_page=next_page
                )
                retry_count = 0  # 重置重试计数
                
                cursor_info = comments_res.get("cursor", {})
                is_end = cursor_info.get("is_end", True)
                next_page = cursor_info.get("next", 0)
                
                # 提取评论数据
                reply_list = comments_res.get("replies", [])
                if not isinstance(reply_list, list):
                    print(f"Format error for {aid}, skip")
                    break
                    
                # 处理本页评论
                page_comments = []
                for comment in reply_list:
                    rpid = comment.get("rpid")
                    if not rpid:
                        continue
                    # 检查是否已爬取过
                    if self.state_manager.is_rpid_exists(rpid):
                        continue
                        
                    # 创建评论数据
                    comment_data = {
                        "rpid": rpid,
                        "aid": aid,
                        "comment": comment.get("content", {}).get("message", ""),
                        "ctime": comment.get("ctime", 0),
                        "category": self.extra_video_info.get('category', 'unknown'),
                        "is_hot": self.extra_video_info.get('is_hot', False)
                    }
                    
                    # 添加到结果
                    if all([comment_data["rpid"], comment_data["aid"], comment_data["comment"]]):
                        page_comments.append(comment_data)
                        self.state_manager.add_rpid(rpid)
                        current_count += 1
                    
                    # 检查是否达到最大限制 - 设置标志位
                    if current_count >= self.max_comments_per_video:
                        print(f"Reach maximum comments per video ({self.max_comments_per_video}) for {aid}. Skip")
                        reached_max = True
                        break  # 跳出评论处理循环
                
                # 添加本页评论到总结果
                comments.extend(page_comments)
                
                # 显示进度
                print(f"Video {aid} Page {next_page}: obtain {len(page_comments)} comments. Total: {current_count}/{self.max_comments_per_video}")
                
                # 保存当前进度
                self.state_manager.update_video_progress(aid, next_page, current_count)
                
                # 如果达到限制，跳出主循环
                if reached_max:
                    break
                
                # 随机延迟
                delay = random.uniform(0.8, 1.5)
                await asyncio.sleep(delay)
                
            except DataFetchError as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"Request failed when fetching comments of {aid}. Skip this video: {str(e)}")
                    break
                
                # 指数退避重试
                delay = 2 ** retry_count + random.uniform(0, 1)
                print(f"Request failed. Retry after{delay:.2f} seconds (Try {retry_count}/{max_retries})...")
                await asyncio.sleep(delay)
            except Exception as e:
                print(f"Unexpected error when fetching comments: {e}")
                # 尝试重新初始化浏览器
                try:
                    await self.ensure_browser_alive()
                except Exception as e2:
                    print(f"Failed to reinitialize browser: {e2}")
                    break
        
        # 如果已结束，标记为完成
        if is_end and not reached_max:
            print(f"Fetched all comments from {aid} ({current_count} in total)")
            self.state_manager.update_video_progress(aid, 0, current_count)
        
        return comments

# 多分区热门视频采集器
class MultiCategoryHotCrawler:
    def __init__(self, db_connection, max_comments_per_video=50):
        self.db = db_connection
        self.max_comments_per_video = max_comments_per_video
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.bilibili.com'
        }
        self.crawler = None  # 延迟初始化
        
        # 定义目标分区及其比例
        self.target_ratios = {
            'kichiku': 0.45,    
            'music': 0.10,     
            'game': 0.10,      
            'life': 0.10,      
            'knowledge': 0.10, 
            'film': 0.10,
            'other':0.05
        }
        
        self.category_tid_map = {
            'kichiku': [119],    # 鬼畜区
            'music': [3],        # 音乐区
            'game': [4],         # 游戏区
            'life': [160],       # 生活区
            'knowledge': [36,188],   # 知识区
            'film': [167,117,13,11,23], #影视区
            'other': [0]
        }

        # 分区ID映射
        self.category_subtid_map = {
            # 生活区
            160: "life", 138: "life", 239: "life", 161: "life", 162: "life", 21: "life",

            # 游戏区
            4: "game", 17: "game", 65: "game", 172: "game",
            171: "game", 173: "game", 136: "game", 121: "game", 19: "game",

            # 音乐区
            3: "music", 130: "music", 29: "music", 59: "music",
            31: "music", 193: "music", 30: "music", 194: "music", 28: "music",

            # 知识区 + 科技整合
            36: "knowledge", 201: "knowledge", 124: "knowledge", 228: "knowledge",
            207: "knowledge", 208: "knowledge", 209: "knowledge", 229: "knowledge",
            122: "knowledge", 95: "knowledge", 230: "knowledge", 231: "knowledge",
            232: "knowledge", 233: "knowledge",

            # 影视类（合并番剧、国创、电视剧、纪录片、电影）
            51: "film", 32: "film", 33: "film", 153: "film", 168: "film", 169: "film",
            170: "film", 195: "film", 185: "film", 187: "film", 23: "film", 83: "film",
            145: "film", 146: "film", 147: "film", 177: "film", 182: "film", 37: "film",

            # 鬼畜区（单独）
            119: "kichiku", 22: "kichiku", 26: "kichiku", 126: "kichiku",
            216: "kichiku", 127: "kichiku"

        }
        
        # 热门视频API端点
        self.hot_video_apis = {
            'popular': 'https://api.bilibili.com/x/web-interface/popular',
            'weekly': 'https://api.bilibili.com/x/web-interface/popular/series/one',
            'rank_all': 'https://api.bilibili.com/x/web-interface/ranking/v2',
        }

        self.existing_aids = set()
    
    async def init_session(self):
        """初始化HTTP会话"""
        self.session = httpx.AsyncClient(headers=self.headers, timeout=30.0)
    
    async def close_session(self):
        """关闭HTTP会话"""
        if self.session:
            await self.session.aclose()
    
    def calculate_next_category(self, current_counts):
        """根据当前比例计算下一个应该采集的分区"""
        total_videos = sum(current_counts.values())
        if total_videos == 0:
            return random.choice(list(self.target_ratios.keys()))
        
        # 计算各分区的当前比例
        current_ratios = {}
        for category, target_ratio in self.target_ratios.items():
            current_count = current_counts.get(category, 0)
            current_ratios[category] = current_count / total_videos
        
        # 找出比例最低于目标的分区
        deficit_categories = []
        for category, target_ratio in self.target_ratios.items():
            deficit = target_ratio - current_ratios.get(category, 0)
            if deficit > 0:
                deficit_categories.append((category, deficit))
        
        # 按赤字大小排序，优先采集赤字最大的分区
        if deficit_categories:
            deficit_categories.sort(key=lambda x: x[1], reverse=True)
            return deficit_categories[0][0]
        
        # 如果所有分区都达到或超过目标，随机选择一个
        return random.choice(list(self.target_ratios.keys()))
    
    async def get_hot_videos(self, source_type='popular', count=20):
        """获取热门视频列表"""
        if source_type not in self.hot_video_apis:
            raise ValueError(f"Unknown source type: {source_type}")
        
        url = self.hot_video_apis[source_type]
        params = {'ps': count} if source_type == 'popular' else {}
        
        try:
            response = await self.session.get(url, params=params)
            data = response.json()
            
            if data.get('code') == 0:
                videos = data.get('data', {}).get('list', []) or data.get('data', {}).get('archives', [])
                return videos
            else:
                print(f"API Error: {data.get('message')}")
                return []
        except Exception as e:
            print(f"Error fetching hot videos: {e}")
            return []
    
    async def get_category_videos(self, category, count=20):
        """获取指定分区的视频列表"""
        if category not in self.category_tid_map:
            raise ValueError(f"Unknown category: {category}")
        
        tid = random.choice(self.category_tid_map[category])
        url = "https://api.bilibili.com/x/web-interface/dynamic/region"
        params = {'rid': tid, 'ps': count}
        
        try:
            response = await self.session.get(url, params=params)
            data = response.json()
            
            if data.get('code') == 0:
                videos = data.get('data', {}).get('archives', [])
                return videos
            else:
                print(f"API Error: {data.get('message')}")
                return []
        except Exception as e:
            print(f"Error fetching category videos: {e}")
            return []
    
    async def get_other_videos(self, count=20):
        """处理"其他"分区的视频"""
        sources = [
            self.get_hot_videos('popular', count*2),
            self.get_hot_videos('rank_all', count*2),
            self.get_category_videos('game', count),  # 从游戏区获取
            self.get_category_videos('life', count),  # 从生活区获取
        ]
        
        # 并行获取所有来源的视频
        results = await asyncio.gather(*sources, return_exceptions=True)
        
        # 合并所有视频
        all_videos = []
        for result in results:
            if isinstance(result, list):
                all_videos.extend(result)
        
        # 随机打乱视频顺序
        random.shuffle(all_videos)
        
        other_videos = []
        for video in all_videos:
            if len(other_videos) >= count:
                break
                
            aid = video['aid']
            if aid in self.existing_aids:
                continue

            # 获取视频详细信息
            video_detail = await self.get_video_detail(aid)
            if not video_detail:  # 如果获取失败，跳过
                continue
                
            # 检测分区
            detected_category = self.detect_category(video_detail)
            
            # 如果不是已知分区，则归类为"other"
            if detected_category == 'other':
                other_videos.append(video)
        
        # 如果仍然没有找到足够的"other"视频，返回一些热门视频
        if len(other_videos) < count:
            hot_videos = await self.get_hot_videos('popular', count)
            for video in hot_videos:
                if len(other_videos) >= count:
                    break
                if video['aid'] not in self.existing_aids:
                    other_videos.append(video)
        
        return other_videos
    
    async def get_video_detail(self, aid):
        """获取视频详细信息"""
        url = f"https://api.bilibili.com/x/web-interface/view?aid={aid}"
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = await self.session.get(url)
                # 检查响应状态和内容类型
                if response.status_code != 200:
                    print(f"Error: Received status code {response.status_code} for aid {aid}")
                    retry_count += 1
                    await asyncio.sleep(2 ** retry_count)  # 指数退避
                    continue
                    
                # 检查响应内容是否为JSON
                content_type = response.headers.get('content-type', '')
                if 'application/json' not in content_type:
                    print(f"Error: Non-JSON response for aid {aid}, content-type: {content_type}")
                    retry_count += 1
                    await asyncio.sleep(2 ** retry_count)
                    continue
                
                data = response.json()
                
                if data.get('code') == 0:
                    return data.get('data', {})
                else:
                    print(f"API Error for aid {aid}: {data.get('message')}")
                    return {}
            except json.JSONDecodeError:
                print(f"JSON decode error for aid {aid}, retrying...")
                retry_count += 1
                await asyncio.sleep(2 ** retry_count)
            except Exception as e:
                print(f"Error fetching video detail for aid {aid}: {e}")
                retry_count += 1
                await asyncio.sleep(2 ** retry_count)
        
        print(f"Failed to get video detail for aid {aid} after {max_retries} retries")
        return {}
    
    def detect_category(self, video_data):
        """根据视频信息推断分区"""
        subtid = video_data.get('tid', 0)
        category = self.category_subtid_map.get(subtid, 'other')
        return category
    
    def is_hot_video(self, video_data):
        """判断视频是否为热门视频"""
        # 根据播放量、点赞数、评论数等指标判断
        stat = video_data.get('stat', {})
        view = stat.get('view', 0)
        like = stat.get('like', 0)
        reply = stat.get('reply', 0)
        
        # 简单的热门判断逻辑
        return view > 100000 or like > 1000 or reply > 500

    async def crawl_strategically(self, total_videos=100):
        """智能策略采集视频，保持各分区比例"""
        await self.init_session()
        
        try:
            # 获取当前各分区的视频数量
            current_counts = self.db.get_video_count_by_category()
            existing_aids = self.db.get_existing_aids()
            self.existing_aids = existing_aids  # 更新实例变量
            
            # 只创建一个crawler实例
            if self.crawler is None:
                self.crawler = BilibiliCommentCrawler([], self.max_comments_per_video)
                await self.crawler.setup_browser()
                await self.crawler.login_if_needed()
                await self.crawler.create_client()
            
            collected_count = 0
            batch_size = 10
            consecutive_failures = 0  # 连续失败计数器
            
            while collected_count < total_videos and consecutive_failures < 10:  # 添加失败限制
                # 确定下一个要采集的分区
                next_category = self.calculate_next_category(current_counts)
                print(f"Next category to crawl: {next_category}")
                
                # 采集该分区的视频
                if next_category == 'kichiku' and current_counts.get('kichiku', 0) > total_videos * 0.4:
                    # 如果鬼畜区已经超过40%，采集热门视频
                    videos = await self.get_hot_videos('popular', batch_size)
                    is_hot = True
                elif next_category == 'other':
                    # 如果下一个分区是other，尝试获取其他分区的视频
                    videos = await self.get_other_videos(batch_size)
                    is_hot = True
                else:
                    # 采集指定分区的视频
                    videos = await self.get_category_videos(next_category, batch_size)
                    is_hot = False
                
                # 如果没有获取到视频，增加失败计数并跳过
                if not videos:
                    consecutive_failures += 1
                    print(f"No videos found for category {next_category}. Consecutive failures: {consecutive_failures}")
                    # 随机延迟后继续
                    await asyncio.sleep(random.uniform(2.0, 5.0))
                    continue
                
                # 重置连续失败计数
                consecutive_failures = 0
                
                # 处理视频
                for video in videos:
                    if collected_count >= total_videos:
                        break
                    
                    aid = video['aid']
                    if aid in existing_aids:
                        continue
                    
                    # 获取视频详细信息
                    video_detail = await self.get_video_detail(aid)
                    if not video_detail:
                        continue
                    
                    # 检测分区和热门状态
                    detected_category = self.detect_category(video_detail)
                    detected_hot = self.is_hot_video(video_detail) or is_hot
                    
                    # 保存视频信息
                    video_info = {
                        'aid': aid,
                        'title': video_detail.get('title', ''),
                        'category': detected_category,
                        'is_hot': detected_hot,
                        'view_count': video_detail.get('stat', {}).get('view', 0),
                        'like_count': video_detail.get('stat', {}).get('like', 0),
                        'comment_count': video_detail.get('stat', {}).get('reply', 0),
                    }
                    self.db.save_video_info(video_info)
                    
                    # 设置额外的视频信息，用于在保存评论时添加分类和热门标记
                    self.crawler.extra_video_info = {
                        'category': detected_category,
                        'is_hot': detected_hot
                    }
                    
                    try:
                        # 爬取评论
                        comments = await self.crawler.get_video_comments(aid)
                        
                        # 保存评论到数据库
                        if comments:
                            self.db.save_comments_batch(comments)
                    except Exception as e:
                        print(f"Error fetching comments for video {aid}: {e}")
                        # 检查是否是浏览器关闭的错误
                        if "Target page, context or browser has been closed" in str(e):
                            print("Browser closed unexpectedly. Reinitializing...")
                            # 重新初始化crawler
                            await self.crawler.close()
                            self.crawler = BilibiliCommentCrawler([], self.max_comments_per_video)
                            await self.crawler.setup_browser()
                            await self.crawler.login_if_needed()
                            await self.crawler.create_client()
                    
                    # 更新计数
                    current_counts[detected_category] = current_counts.get(detected_category, 0) + 1
                    collected_count += 1
                    existing_aids.add(aid)
                    
                    print(f"Collected {collected_count}/{total_videos} videos")
                    
                    # 随机延迟
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                
                # 更新当前计数
                current_counts = self.db.get_video_count_by_category()
        
        finally:
            await self.close_session()
            if self.crawler:
                await self.crawler.close()