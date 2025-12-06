import logging
import sys
import os
import time
import yaml
import re
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime

DEFAULT_CONFIG = {"log_level": "INFO", "request_delay": 1}
logger = logging.getLogger(__name__)

def setup_driver():
    options = Options()
    options.page_load_strategy = 'eager'
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    chromedriver_path = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
    try:
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"})
        logger.info("成功启动 WebDriver (已应用基础反检测设置)。")
        return driver
    except Exception as e:
        logger.error(f"启动 WebDriver 失败: {e}")
        sys.exit(1)

def setup_logging(log_level_str, site_name, log_prefix="script"):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    site_part = f"{site_name}_" if site_name else ""
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    log_filename = f"{log_prefix}_{site_part}{timestamp}.log"
    log_file_path = os.path.join(logs_dir, log_filename)
    numeric_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(level=numeric_level, format="[%(asctime)s] [%(levelname)s] %(message)s", handlers=[logging.FileHandler(log_file_path, encoding='utf-8'), logging.StreamHandler(sys.stdout)])
    logger.info(f"日志已配置。级别: {log_level_str}, 文件: {log_file_path}")

def load_config(site_name):
    if not site_name:
        print("错误: 必须通过 --site <site_name> 参数指定一个网站配置。")
        sys.exit(1)
    
    # 1. 确定项目根目录 (utils.py 在 scripts/ 下，所以往上两级)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    # 2. 读取 YAML
    config_path = os.path.join(project_root, 'configs', f"{site_name}.yaml")
    if not os.path.exists(config_path):
        print(f"错误: 配置文件未找到: {config_path}")
        sys.exit(1)
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            site_config = yaml.safe_load(f)
    except Exception as e:
        print(f"错误: 加载或解析配置文件 {config_path} 失败: {e}")
        sys.exit(1)
        
    config = {**DEFAULT_CONFIG, **site_config}
    config['site_name'] = site_name

    # --- 【核心修改：智能数据库路径处理】 ---
    # 定义标准的数据库存放目录: project_root/database
    db_root_dir = os.path.join(project_root, 'database')
    os.makedirs(db_root_dir, exist_ok=True) # 自动创建目录
    
    raw_db_name = config.get('database_file', 'default.db')
    
    # 判断用户填的是 "xxx.db" 还是 "folder/xxx.db"
    if os.path.dirname(raw_db_name):
        # 如果包含路径（为了兼容旧配置），则认为它是相对于项目根目录的
        # 例如: "test/old.db" -> "/app/test/old.db"
        config['database_file'] = os.path.join(project_root, raw_db_name)
    else:
        # 如果只是文件名 (推荐)，自动放入 database 目录
        # 例如: "javbee.db" -> "/app/database/javbee.db"
        config['database_file'] = os.path.join(db_root_dir, raw_db_name)
    # ------------------------------------

    return config

def normalize_date(date_str):
    """
    尝试解析多种常见的日期格式（包括Unix时间戳秒/毫秒），并将其标准化为 'YYYY-MM-DD HH:MM:SS'。
    """
    if not date_str:
        return None

    # 转换为字符串并去除首尾空白
    # s = str(date_str).strip()
    s = ' '.join(str(date_str).split())
    
    # --- 1. 尝试解析数字时间戳 ---
    # 使用正则判断是否纯数字，比 try-except int() 更快且更安全
    if re.match(r'^\d+$', s):
        try:
            ts = int(s)
            # 判定标准：
            # 秒级时间戳(10位): 2001年(1e9) ~ 2286年(1e10)
            # 毫秒级时间戳(13位): 2001年(1e12) ~ 2286年(1e13)
            
            # 情况 A: 秒级时间戳
            if 1000000000 < ts < 10000000000:
                return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            
            # 情况 B: 毫秒级时间戳 (常见于 JS/Java 后端)
            elif 1000000000000 < ts < 10000000000000:
                return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                
        except (ValueError, TypeError):
            pass

    # --- 2. 尝试常见文本格式 ---
    formats_to_try = [
        '%Y-%m-%d %H:%M:%S',  # 2025-09-08 07:38:36
        '%Y-%m-%d %H:%M',     # 2025-09-08 07:38
        '%Y-%m-%d',           # 2025-09-08
        
        '%Y.%m.%d %H:%M:%S',  # 2025.09.08 07:38:36 (常见于亚洲站点)
        '%Y.%m.%d',           # 2025.09.08
        
        '%Y/%m/%d %H:%M:%S',  # 2025/09/08 07:38:36
        '%Y/%m/%d',           # 2025/09/08
        
        '%b. %d, %Y',         # Sep. 20, 2025 (英文格式)
        '%d %b %Y',           # 20 Sep 2025
        
        '%Y%m%d',             # 20250920 (紧凑格式)
    ]
    
    for fmt in formats_to_try:
        try:
            dt_obj = datetime.strptime(s, fmt)
            # 如果解析出的时间没有时分秒（如仅日期），默认补全为 00:00:00
            # strftime 会自动处理，但如果是 %Y-%m-%d，时间部分就是 00:00:00
            return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue

    # --- 3. 兜底处理 ---
    # 如果都解析失败，记录警告并返回原始字符串，防止报错中断程序
    logger.warning(f"无法解析的日期格式: '{date_str}'")
    return date_str


def parse_tags_from_title(title, tag_rules):
    found_tags = set()
    if not title or not tag_rules: return []
    lower_title = title.lower()
    for standard_tag, keywords in tag_rules.items():
        for keyword in keywords:
            clean_keyword = re.sub(r'[\[\]【】]', '', keyword).lower()
            if clean_keyword in lower_title:
                found_tags.add(standard_tag)
                break
    return list(found_tags)
