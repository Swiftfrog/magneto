import argparse
import os
import re
import requests
import time
import logging
import json
import sys
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

import database
from utils import setup_logging, load_config, normalize_date, parse_tags_from_title

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
}

class NyaaScraper:
    def __init__(self, config):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.config = config
        self.base_url = config['base_url']
        self.tag_rules = config.get('tag_rules', {})
        self.selectors = config.get('selectors', {})
        if not self.selectors:
            logger.error("配置文件中缺少 'selectors' 部分！")
            sys.exit(1)

    def extract_item_info(self, item_row):
        details = {}
        title_tag = item_row.select_one(self.selectors['title'])
        details['title'] = title_tag.get_text(strip=True) if title_tag else ''
        post_url_tag = item_row.select_one(self.selectors['post_url'])
        details['post_url'] = urljoin(self.base_url, post_url_tag['href']) if post_url_tag and post_url_tag.get('href') else ''
        magnet_tag = item_row.select_one(self.selectors['magnet_link'])
        details['magnet_link'] = magnet_tag['href'] if magnet_tag and magnet_tag.get('href') else ''
        size_tag = item_row.select_one(self.selectors['file_size'])
        details['size'] = size_tag.get_text(strip=True) if size_tag else ''
        date_tag = item_row.select_one(self.selectors['publish_date'])
        raw_date_str = date_tag.get('data-timestamp') if date_tag and date_tag.get('data-timestamp') else (date_tag.get_text(strip=True) if date_tag else None)
        details['date'] = normalize_date(raw_date_str)
        number_match = re.search(r'([A-Z0-9]+(?:-[A-Z0-9]+)*-\d+)', details['title'], re.IGNORECASE)
        details['item_number'] = number_match.group(1).upper() if number_match else ''
        details['cover_image_url'] = ''
        tags = parse_tags_from_title(details['title'], self.tag_rules)
        logger.info(f"成功提取数据: 编号={details['item_number'] or 'N/A'}")
        logger.debug(f"完整提取数据: {json.dumps(details, indent=2, ensure_ascii=False)}")
        logger.debug(f"解析出的标签: {tags}")
        return details, tags

    def scrape_page(self, page_num, stats_counter):
        base = self.base_url.strip().rstrip('/')
        url = f"{base}?p={page_num}"
        page_stats = {'found': 0, 'added': 0}
        try:
            logger.info(f"正在抓取页面: {url}")
            self.session.headers['Referer'] = f"{base}?p={page_num - 1}" if page_num > 1 else self.base_url
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            item_rows = soup.select(self.selectors['item_row'])
            if not item_rows:
                logger.warning(f"页面上未找到信息条目: {url}")
                return page_stats

            page_stats['found'] = len(item_rows)
            stats_counter['total_found'] += page_stats['found']
            logger.info(f"在页面 {url} 找到 {len(item_rows)} 条信息")

            for row in item_rows:
                try:
                    details, tags = self.extract_item_info(row)
                    # ---【核心修正：直接调用数据库函数，不再经过 process_item】---
                    result = database.add_processed_post_with_tags(self.config['database_file'], self.config['site_name'], details, tags)
                    if result in stats_counter: 
                        stats_counter[result] += 1
                        if result == 'ADDED':
                            page_stats['added'] += 1
                except Exception as e:
                    logger.error(f"处理单个条目时出错: {e}")
                    stats_counter['FAILED'] += 1
            return page_stats
        except Exception as e:
            logger.error(f"处理页面时出错 {url}: {e}")
            return None

    def run(self, start_page, end_page_str):
        stats = {'ADDED': 0, 'DUPLICATE': 0, 'FAILED': 0, 'total_found': 0}
        start_time = time.time()
        is_auto_mode = (str(end_page_str).lower() == 'auto')
        end_page = float('inf') if is_auto_mode else int(end_page_str)
        page_num = start_page
        consecutive_duplicate_pages = 0
        stop_threshold = self.config.get('stop_on_consecutive_duplicates', 2)
        
        try:
            while page_num <= end_page:
                logger.info(f"--- 开始处理第 {page_num} 页 ---")
                page_stats = self.scrape_page(page_num, stats)
                
                if page_stats is None or page_stats['found'] == 0:
                    logger.info(f"第 {page_num} 页抓取失败或没有内容，任务结束。")
                    break

                is_fully_duplicate = (page_stats['found'] > 0 and page_stats['added'] == 0)
                if is_fully_duplicate:
                    consecutive_duplicate_pages += 1
                    logger.info(f"页面 {page_num} 的所有内容均重复，连续重复页面计数: {consecutive_duplicate_pages}/{stop_threshold}")
                else:
                    consecutive_duplicate_pages = 0

                if consecutive_duplicate_pages >= stop_threshold:
                    logger.info(f"已连续遇到 {stop_threshold} 个完全重复的页面，自动终止抓取。")
                    break
                page_num += 1
                time.sleep(self.config.get('request_delay', 1))
        finally:
            end_time = time.time()
            duration = end_time - start_time
            db_path = self.config['database_file']
            total_in_db = database.get_total_count(db_path)
            
            width = 62
            title = " 任务总结 "
            top_line = f"{title:=^{width}}"
            bottom_line = "=" * width
            summary = f"""
            \n{top_line}
            - 目标网站: {self.config['site_name']}
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
    parser = argparse.ArgumentParser(description='从 nyaa.si 这样的列表页网站抓取种子信息。')
    parser.add_argument("--site", "-s", required=True, help="网站标识")
    parser.add_argument('--start-page', type=int, default=1, help='起始页码 (默认: 1)')
    parser.add_argument('--end-page', type=str, default='auto', help="结束页码或 'auto' (默认: 'auto')")
    args = parser.parse_args()

    config = load_config(args.site)
    setup_logging(config['log_level'], config['site_name'], "scrape_nyaa")
    database.init_db(config['database_file'])

    scraper = NyaaScraper(config)
    scraper.run(args.start_page, args.end_page)

    logger.info("所有任务处理完毕。")
