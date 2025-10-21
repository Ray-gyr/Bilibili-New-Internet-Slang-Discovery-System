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
import sqlite3  # 导入SQLite模块

def ensure_dir_exists(path: str):
    """确保目录存在，不存在则创建"""
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

BASEDIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 数据库文件路径
DB_FILE = os.path.join(BASEDIR,"Database","raw_bilibili_comments.db")

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
        logged_in = False
        
        while time.time() - start_time < self.login_timeout and not logged_in:
            try:
            # 检查登录状态
                if await self.context_page.is_visible("li.header-avatar-wrap "):
                    print("Login detected!")
                    return
        
                # 等待状态更新
                await asyncio.sleep(3)
                
            except Exception as e:
                print(f"Login error: {str(e)}")
                await asyncio.sleep(3)
        
        if not logged_in:
            raise TimeoutError("Login timeout")

# 爬虫状态管理器
class CrawlerStateManager:
    def __init__(self, state_file:str):
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

    async def get_video_comments(
        self,
        video_id: str,
        next_page: int = 0,
    ) -> Dict:
        """获取视频评论"""
        uri = "/x/v2/reply/wbi/main"
        post_data = {
            "oid": video_id, 
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
    
    def create_raw_comments_db(self):
        """创建存储原始评论的数据库表"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_comments (
                rpid INTEGER PRIMARY KEY,
                oid INTEGER NOT NULL,
                comment TEXT NOT NULL,
                ctime INTEGER NOT NULL
            )
        ''')
        # 创建索引以提高查询性能
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_oid ON raw_comments(oid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ctime ON raw_comments(ctime)')
        conn.commit()
        conn.close()
        print("Database is set.")
    
    def save_comments_batch(self, comments_batch: List[Dict]):
        """将一批评论保存到数据库"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            # 准备批量插入数据
            data_to_insert = [
                (comment['rpid'], comment['oid'], comment['comment'], comment['ctime'])
                for comment in comments_batch
            ]
            
            # 执行批量插入
            cursor.executemany('''
                INSERT OR IGNORE INTO raw_comments (rpid, oid, comment, ctime)
                VALUES (?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            print(f"Save {len(comments_batch)} comments to database")
        except sqlite3.Error as e:
            print(f"数据库操作错误: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def comment_generator(self, all_comments: Iterable[Dict], batch_size: int = 1000) -> Generator[List[Dict], None, None]:
        """生成器：分批产生评论数据
        
        Args:
            all_comments: 可迭代的评论数据集合
            batch_size: 每批处理的评论数量
        
        Yields:
            每批最多batch_size条评论
        """
        batch = []
        for comment in all_comments:
            # 确保评论数据包含所需字段
            if not all(key in comment for key in ['rpid', 'oid', 'comment', 'ctime']):
                print(f"Skip : {comment}")
                continue
                
            batch.append(comment)
            
            # 当批次达到指定大小时返回
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # 返回最后一批（可能不足batch_size条）
        if batch:
            yield batch
    
    def save_all_comments(self, all_comments: Iterable[Dict], batch_size: int = 1000):
        """将评论数据分批存储到数据库
        
        Args:
            all_comments: 可迭代的评论数据集合
            batch_size: 每批处理的评论数量
        """
        total_saved = 0
        for batch in self.comment_generator(all_comments, batch_size):
            self.save_comments_batch(batch)
            total_saved += len(batch)
        
        print(f"All comments are saved. There are a total of {total_saved}.")

# 评论爬虫主类
class BilibiliCommentCrawler:
    def __init__(self, aid_list: List[str], max_comments_per_video: int = 100):
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
        if self.logged_in:
            return
            
        try:
            # 检查是否已登录
            nav_button = await self.context_page.query_selector("div.header-avatar-wrap")
            if nav_button:
                print("Already logged in ")
                self.logged_in = True
                return
        except:
            pass
            
        # 进行二维码登录（传递爬虫实例）
        login_manager = BilibiliLogin(self.context_page, self.browser_context, self)
        await login_manager.login_by_qrcode(self.cookies_file)
        self.logged_in = True

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
        if self.browser_context:
            await self.browser_context.close()
        if self.playwright:
            await self.playwright.stop()
        # 保存最终状态
        self.state_manager.save_state()

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
                    self.db.save_all_comments(comments)
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
                        "oid": aid,
                        "comment": comment.get("content", {}).get("message", ""),
                        "ctime": comment.get("ctime", 0)
                    }
                    
                    # 添加到结果
                    if all(comment_data.values()):
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
        
        # 如果已结束，标记为完成
        if is_end and not reached_max:
            print(f"Fetched all comments from {aid} ({current_count} in total)")
            self.state_manager.update_video_progress(aid, 0, current_count)
        
        return comments


