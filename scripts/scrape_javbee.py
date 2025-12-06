import argparse
import os
import re
import requests
import time
import logging
import json
import sys
import calendar
import hashlib
import bencodepy
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta
from pathlib import Path

import database
from utils import setup_logging, load_config, normalize_date, parse_tags_from_title

logger = logging.getLogger(__name__)

# 修改说明：移除了 Referer 的硬编码，只保留通用的 User-Agent
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7'
}

class JavbeeDownloader:
    def __init__(self, config):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.config = config
        self.base_url = config['base_url'].rstrip('/')
        
        # 修改说明：根据配置动态设置 Referer，方便后续支持其他网站
        self.session.headers.update({'Referer': self.base_url})
        
        self.tag_rules = config.get('tag_rules', {})
        self.download_dir = "torrent_downloads"
        Path(self.download_dir).mkdir(parents=True, exist_ok=True)

    def extract_torrent_info(self, card_element, tag_rules):
        info = {}
        # 获取配置中的选择器，如果没有则使用默认（兼容旧配置）
        sels = self.config.get('selectors', {})
        
        # 定义辅助函数：安全获取文本
        def get_text_safe(selector):
            if not selector: return ""
            el = card_element.select_one(selector)
            return el.get_text(strip=True) if el else ""

        # 定义辅助函数：安全获取属性
        def get_attr_safe(selector, attr):
            if not selector: return None
            el = card_element.select_one(selector)
            return el.get(attr, '') if el else None

        # 1. 标题提取
        info['title'] = get_text_safe(sels.get('title_link', 'h5.title.is-4.is-spaced a'))
        
        # 2. 原文链接 (post_url)
        title_el = card_element.select_one(sels.get('title_link', 'h5.title.is-4.is-spaced a'))
        href = title_el.get('href', '') if title_el else ""
        info['post_url'] = urljoin(self.base_url, href) if href else ""

        # 3. 大小提取
        info['size'] = get_text_safe(sels.get('size', 'h5.title span.is-size-6'))
        
        # 4. 日期提取
        raw_date_str = get_text_safe(sels.get('date', 'p.subtitle.is-6 a'))
        # 如果选择器没取到，尝试取 title 属性 (有些网站日期在 title 里)
        if not raw_date_str and sels.get('date'):
            el = card_element.select_one(sels['date'])
            if el and el.get('title'):
                raw_date_str = el.get('title')
        info['date'] = normalize_date(raw_date_str)
        
        # 5. 编号提取 (保持原有的双重匹配逻辑)
        standard_match = re.search(r'([A-Z0-9]+(?:-[A-Z0-9]+)*-\d+)', info['title'], re.IGNORECASE)
        if standard_match:
            info['item_number'] = standard_match.group(1).upper()
        else:
            compact_match = re.search(r'([A-Z]+)(\d{3,})', info['title'], re.IGNORECASE)
            if compact_match:
                prefix = compact_match.group(1).upper()
                suffix = compact_match.group(2)
                info['item_number'] = f"{prefix}-{suffix}"
            elif len(info['title']) < 15: 
                info['item_number'] = info['title'].strip().upper()
            else:
                info['item_number'] = ''

        # 6. 链接提取 (Magnet & Torrent)
        magnet_sel = sels.get('magnet', 'a[title="Download Magnet"]')
        torrent_sel = sels.get('torrent', 'a[title="Download .torrent"]')
        
        info['magnet_link'] = get_attr_safe(magnet_sel, 'href')
        
        raw_torrent_url = get_attr_safe(torrent_sel, 'href')
        info['torrent_url'] = urljoin(self.base_url, raw_torrent_url) if raw_torrent_url else None
        
        # 7. 图片提取
        img_sel = sels.get('image', 'img.image.lazy')
        img_el = card_element.select_one(img_sel) if img_sel else None
        if img_el:
            # 优先尝试配置的属性 (如 data-src)，如果为空则尝试 src
            target_attr = sels.get('image_attr', 'data-src')
            img_src = img_el.get(target_attr) or img_el.get('src') or ''
            info['cover_image_url'] = urljoin(self.base_url, img_src)
        else:
            info['cover_image_url'] = ""

        # 解析标签
        tags = parse_tags_from_title(info['title'], self.tag_rules)
        
        logger.info(f"成功提取数据: 编号={info['item_number'] or 'N/A'}")
        return info, tags

    def torrent_to_magnet(self, torrent_path):
        # ... (保持原样，未修改) ...
        try:
            with open(torrent_path, 'rb') as f:
                torrent_data = f.read()
            metadata = bencodepy.decode(torrent_data)
            info_data = metadata.get(b'info')
            info_encoded = bencodepy.encode(info_data)
            info_hash = hashlib.sha1(info_encoded).hexdigest()
            
            name = b''
            if b'name' in info_data:
                name = info_data[b'name']
            try:
                name_str = name.decode('utf-8')
            except:
                name_str = "Unknown"

            magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={name_str}"
            return magnet
        except Exception as e:
            logger.error(f"转换 torrent 到 magnet 失败: {e}")
            return None

    def process_item(self, info, tags):
        # ... (保持原样，未修改) ...
        filepath = None
        try:
            if not info.get('magnet_link') and info.get('torrent_url'):
                clean_title = re.sub(r'[\\/*?:"<>|]', '_', info['title'][:50])
                filename = f"{clean_title}_{int(time.time())}.torrent"
                filepath = os.path.join(self.download_dir, filename)
                
                logger.info(f"正在下载 .torrent 文件: {info['title']}")
                try:
                    
                    sleep_time = self.config.get('download_delay', 1) 
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                        
                    response = self.session.get(info['torrent_url'], timeout=30)
                    response.raise_for_status()
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    info['magnet_link'] = self.torrent_to_magnet(filepath)
                except requests.RequestException as e:
                    logger.error(f"下载 .torrent 文件失败 for {info['title']}: {e}")
                    return 'FAILED'

            if info.get('magnet_link'):
                return database.add_processed_post_with_tags(self.config['database_file'], self.config['site_name'], info, tags)
            else:
                logger.warning(f"最终未能获取 magnet 链接，跳过: {info['title']}")
                return 'FAILED'

        finally:
            if filepath and os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except OSError as e:
                    logger.warning(f"删除临时文件失败 {filepath}: {e}")

    def scrape_page(self, url, tag_rules, stats_counter):
        try:
            logger.info(f"正在抓取页面: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 修改说明：从配置中读取卡片选择器
            card_selector = self.config.get('selectors', {}).get('card', 'div.card.mb-3')
            cards = soup.select(card_selector)
            
            if not cards:
                logger.warning(f"页面上未找到种子信息卡片: {url}")
                return "NO_CONTENT"
            
            stats_counter['total_found'] += len(cards)
            logger.info(f"在页面 {url} 找到 {len(cards)} 个种子信息")
            
            consecutive_duplicates = 0
            stop_threshold = self.config.get('stop_on_consecutive_duplicates', 10)

            for card in cards:
                try:
                    info, tags = self.extract_torrent_info(card, tag_rules)
                    result = self.process_item(info, tags)
                    if result in stats_counter: 
                        stats_counter[result] += 1
                    if result == 'DUPLICATE':
                        consecutive_duplicates += 1
                    else:
                        consecutive_duplicates = 0
                    if consecutive_duplicates >= stop_threshold:
                        logger.info(f"已连续检测到 {stop_threshold} 个重复记录，终止抓取当前页面。")
                        return "STOP_SIGNAL"
                except Exception as e:
                    logger.error(f"处理单个卡片时出错: {e}")
                    stats_counter['FAILED'] += 1
                    consecutive_duplicates = 0
                
            time.sleep(self.config.get('request_delay', 1))
            return "CONTINUE"
        except requests.RequestException as e:
            logger.error(f"请求页面时出错 {url}: {e}")
            return "PAGE_ERROR"
        except Exception as e:
            logger.error(f"处理页面时出现未知错误 {url}: {e}")
            return "PAGE_ERROR"

    def scrape_series(self, path_suffix, start_page, stats_counter):
        # ... (保持原样，未修改) ...
        page = start_page
        tag_rules = self.config.get('tag_rules', {})
        consecutive_failure_count = 0
        CONSECUTIVE_FAILURE_THRESHOLD = 2
        
        logger.info(f"开始抓取系列: {self.base_url}/{path_suffix} (起始页: {page})")

        while True:
            url = f"{self.base_url}/{path_suffix}?page={page}"
            page_result = self.scrape_page(url, tag_rules, stats_counter)
            
            if page_result == "CONTINUE" or page_result == "STOP_SIGNAL":
                consecutive_failure_count = 0
            else: 
                consecutive_failure_count += 1
                logger.warning(f"抓取第 {page} 页失败或为空，连续失败次数: {consecutive_failure_count}/{CONSECUTIVE_FAILURE_THRESHOLD}")

            if page_result == "STOP_SIGNAL":
                logger.info(f"在第 {page} 页遇到“旧数据之墙”，系列 {path_suffix} 处理完毕。")
                break
            if consecutive_failure_count >= CONSECUTIVE_FAILURE_THRESHOLD:
                logger.info(f"已连续 {CONSECUTIVE_FAILURE_THRESHOLD} 次抓取页面失败或为空，系列 {path_suffix} 处理完毕。")
                break
            
            page += 1
            
        logger.info(f"系列 {path_suffix} 的所有页面处理完成。")

# ... (后续 validate_date_format 和 main 函数保持原样，未修改) ...

def validate_date_format(date_str):
    # 尝试 YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            datetime.strptime(date_str, fmt)
            return "day", fmt # 返回类型和匹配的格式
        except ValueError:
            pass
            
    # 尝试 YYYY-MM, YYYY/MM, YYYYMM
    for fmt in ("%Y-%m", "%Y/%m", "%Y%m"):
        try:
            datetime.strptime(date_str, fmt)
            return "month", fmt
        except ValueError:
            pass
            
    return None, None

def main():
    parser = argparse.ArgumentParser(description='按日期或标签下载种子信息并存入数据库')
    parser.add_argument("--site", "-s", required=True, help="网站标识")
    # 提示：现在支持 2020/12/05 或 2020/12 这种格式了
    parser.add_argument('--date', type=str, help='日期，格式 YYYY-MM-DD, YYYY/MM/DD 等')
    parser.add_argument('--tag', type=str, help='标签/关键词')
    parser.add_argument('--start-page', type=int, default=1, help='起始页码 (默认: 1)')
    args = parser.parse_args()

    config = load_config(args.site)
    db_path = config['database_file']
    setup_logging(config['log_level'], config['site_name'], "scrape_javbee")
    database.init_db(db_path)
    
    # [新增] 获取配置中的 URL 日期格式，默认为 YYYY-MM-DD
    target_url_fmt = config.get('url_date_format', "%Y-%m-%d")

    stats = {'ADDED': 0, 'DUPLICATE': 0, 'FAILED': 0, 'total_found': 0}
    start_time = time.time()

    try:
        downloader = JavbeeDownloader(config)
        
        if args.tag:
            logger.info(f"--- 开始处理标签任务: {args.tag} ---")
            path_suffix = f"tag/{args.tag}"
            downloader.scrape_series(path_suffix, args.start_page, stats)

        elif args.date:
            date_input = args.date
            date_type, input_fmt = validate_date_format(date_input)

            if date_type == "day":
                # 解析用户输入的日期
                dt = datetime.strptime(date_input, input_fmt)
                # 转换为网站需要的格式 (例如用户输 2020-12-05, 但网站需要 2020/12/05)
                url_date_str = dt.strftime(target_url_fmt)
                
                logger.info(f"--- 开始处理单日任务: {url_date_str} ---")
                downloader.scrape_series(f"date/{url_date_str}", args.start_page, stats)
            
            elif date_type == "month":
                # 解析用户输入的月份
                dt = datetime.strptime(date_input, input_fmt)
                year, month = dt.year, dt.month
                    
                _, days_in_month = calendar.monthrange(year, month)
                logger.info(f"--- 开始处理整月任务: {year}-{month:02d} (共 {days_in_month} 天) ---")
                
                for day in range(1, days_in_month + 1):
                    # 构造每一天的 datetime 对象
                    day_dt = datetime(year, month, day)
                    # [关键] 使用配置中的格式转换为字符串 (支持 / 或 -)
                    url_date_str = day_dt.strftime(target_url_fmt)
                    
                    logger.info(f"--- 正在处理 {url_date_str} (第 {day}/{days_in_month} 天) ---")
                    downloader.scrape_series(f"date/{url_date_str}", 1, stats)
                    
                    if day < days_in_month:
                        time.sleep(config.get('request_delay', 1) * 2)
            else:
                logger.error(f"日期格式错误: '{date_input}'")
                sys.exit(1)
        
        else:
            # 默认抓取昨天，同样适配格式
            yesterday = datetime.now() - timedelta(days=1)
            url_date_str = yesterday.strftime(target_url_fmt)
            logger.info(f"未提供日期，默认抓取昨天: {url_date_str}")
            downloader.scrape_series(f"date/{url_date_str}", 1, stats)

    finally:
        # ... (结尾总结代码保持不变) ...
        end_time = time.time()
        duration = end_time - start_time
        total_in_db = database.get_total_count(db_path)
        
        width = 62
        title = " 任务总结 "
        top_line = f"{title:=^{width}}"
        bottom_line = "=" * width
        summary = f"""
        \n{top_line}
        - 目标网站: {config['site_name']}
        - 开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}
        - 结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}
        - 总耗时: {time.strftime('%H时%M分%S秒', time.gmtime(duration))}

        --- 处理结果 ---
        - 页面发现总数: {stats['total_found']}
        - ✅ 成功新增记录: {stats['ADDED']}
        - ⏩ 检测到重复记录: {stats['DUPLICATE']}
        - ❌ 处理失败记录: {stats['FAILED']}

        --- 数据库状态 ---
        - 数据库文件: {db_path}
        - 数据库总记录数: {total_in_db}
        \n{bottom_line}
        """
        logger.info(summary)

if __name__ == "__main__":
    main()
