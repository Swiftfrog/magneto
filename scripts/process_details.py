import argparse
import logging
import re
import os
import time
import psutil
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils import setup_driver, setup_logging, load_config, normalize_date, parse_tags_from_title
import database

logger = logging.getLogger(__name__)

def extract_data(html_content, url, selectors, base_url, tag_rules):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # --- Naming Optimization ---
        # Changed 'LINK' to 'post_url' for consistency with the database schema
        details = {'post_url': url, 'date': None, 'item_number': 'N/A', 'title': 'N/A', 'magnet_link': 'N/A', 'size': 'N/A', 'type': 'N/A', 'cover_image_url': ''}
        # --- End Optimization ---

        time_em_tag = soup.select_one(selectors['publish_time'])
        if time_em_tag:
            time_span_tag = time_em_tag.find("span", title=True)
            raw_date_str = time_span_tag['title'].strip() if time_span_tag else (re.search(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})', time_em_tag.get_text(strip=True)) or [None])[0]
            details['date'] = normalize_date(raw_date_str)
        
        meta_tag = soup.select_one(selectors['meta_keywords'])
        if meta_tag and meta_tag.get('content'):
            content = meta_tag.get('content').strip()
            match = re.match(r'^([A-Za-z0-9\-]+)\s*', content)
            details['item_number'], details['title'] = (match.group(1), content[match.end():].strip()) if match else ('N/A', content)
        
        magnet_container = soup.select_one(selectors['magnet_link'])
        
        if magnet_container:
            # 2. 获取容器内所有文本
            container_text = magnet_container.get_text(separator=" ", strip=True)
            
            magnet_match = re.search(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]{32,40})', container_text)
            
            if magnet_match:
                details['magnet_link'] = magnet_match.group(1)
            else:
                pass 
        
        cover_selector = selectors.get('cover_image')
        if cover_selector:
            img_tag = soup.select_one(cover_selector)
            if img_tag:
                img_src = img_tag.get('file') or img_tag.get('zoomfile') or img_tag.get('data-src') or img_tag.get('src')
                if img_src: details['cover_image_url'] = urljoin(base_url, img_src)
        
        container = soup.select_one(selectors.get('post_content_container'))
        if container:
            lines = str(container).split('<br/>')
            size_keywords = selectors.get('size_keyword')
            type_keywords = selectors.get('type_keyword')
            for line in lines:
                plain_line = BeautifulSoup(line, 'html.parser').get_text(strip=True)
                if details['size'] == 'N/A' and size_keywords and re.search(size_keywords, plain_line):
                    match = re.search(r'[：:]\s*(.*)', plain_line)
                    if match: details['size'] = match.group(1).strip()
                if details['type'] == 'N/A' and type_keywords and re.search(type_keywords, plain_line):
                    match = re.search(r'[：:]\s*(.*)', plain_line)
                    if match: details['type'] = match.group(1).strip()
        
        tags = parse_tags_from_title(details['title'], tag_rules)
        logger.info(f"成功提取数据: 编号={details['item_number']}")
        logger.debug(f"完整提取数据: {json.dumps(details, indent=2, ensure_ascii=False)}")
        logger.debug(f"解析出的标签: {tags}")
        return details, tags
    except Exception as e:
        logger.error(f"提取数据失败: {url} - {e}", exc_info=True)
        return None, None

def fetch_html_selenium(url, driver, selectors):
    try:
        driver.get(url)
        time.sleep(0.5)
        
        # 处理进入按钮（如满18岁确认）
        if 'enter_button' in selectors and selectors['enter_button']:
            buttons = driver.find_elements(By.CSS_SELECTOR, selectors['enter_button'])
            if buttons:
                try:
                    driver.execute_script("arguments[0].click();", buttons[0])
                    time.sleep(1)
                except Exception as e:
                    logger.warning(f"JS click failed: {e}")
        
        # 等待内容加载
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors['post_content_container'])))
        return driver.page_source

    except Exception as e:
        logger.error(f"[Selenium] 失败: {url} - {e}")
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        
        # 1. 保存截图
        try:
            img_path = os.path.join("logs", f"failed_process_{timestamp}.png")
            driver.save_screenshot(img_path)
            logger.error(f"失败截图已保存: {img_path}")
        except Exception as se:
            logger.error(f"保存截图失败: {se}")
            
        # 2. 【新增】保存 HTML 源码用于调试
        try:
            html_path = os.path.join("logs", f"failed_source_{timestamp}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.error(f"失败页面源码已保存: {html_path}")
        except Exception as se:
            logger.error(f"保存源码失败: {se}")
            
        return None

def main():
    BROWSER_RESTART_INTERVAL = 25
    parser = argparse.ArgumentParser(description="从数据库读取URL并抓取详情。")
    parser.add_argument("--site", "-s", required=True, help="网站标识")
    parser.add_argument("--retry-failed", action="store_true", help="专门重试之前处理失败的任务")
    args = parser.parse_args()

    config = load_config(args.site)
    db_path = config['database_file']
    setup_logging(config['log_level'], config['site_name'], "process_details")
    database.init_db(db_path)
    
    if args.retry_failed:
        logger.info(f"开始为 '{config['site_name']}' [重试失败任务], 数据存入 '{db_path}'")
        urls_to_process = database.get_failed_urls(db_path, config['site_name'])
        if not urls_to_process:
            logger.info("数据库中没有需要重试的失败任务。")
            return
        logger.info(f"发现 {len(urls_to_process)} 个失败任务需要重试。")
    else:
        logger.info(f"开始为 '{config['site_name']}' [处理新任务], 数据存入 '{db_path}'")
        urls_to_process = database.get_unprocessed_urls(db_path, config['site_name'])
        if not urls_to_process:
            logger.info("数据库中没有待处理的新任务。")
            return
        logger.info(f"发现 {len(urls_to_process)} 个待处理的新任务。")

    stats = {'UPDATED': 0, 'DUPLICATE': 0, 'FAILED': 0}
    start_time = time.time()
    
    selectors = config['selectors']['process_details']
    tag_rules = config.get('tag_rules', {})
    driver = None
    processed_in_session = 0 
    parent_process = psutil.Process(os.getpid())

    try:
        for i, url in enumerate(urls_to_process):
            if driver is None or processed_in_session >= BROWSER_RESTART_INTERVAL:
                if driver: driver.quit()
                driver = setup_driver()
                processed_in_session = 0
            
            logger.info(f"--- 处理进度 ({i+1}/{len(urls_to_process)}) ---")
            html = fetch_html_selenium(url, driver, selectors)
            processed_in_session += 1
            
            if not html:
                database.mark_url_failed(db_path, url, config['site_name'])
                stats['FAILED'] += 1
                driver.quit(); driver = None
                continue
            
            details, tags = extract_data(html, url, selectors, config['base_url'], tag_rules)
            if details and details.get('magnet_link') and details['magnet_link'] != 'N/A':
                result = database.update_post_with_tags(db_path, url, config['site_name'], details, tags)
                if result in stats: 
                    stats[result] += 1
            else:
                database.mark_url_failed(db_path, url, config['site_name'])
                stats['FAILED'] += 1
    finally:
        if driver: driver.quit()
        for child in parent_process.children(recursive=True):
            try: child.kill()
            except psutil.NoSuchProcess: pass
        logger.info("残留进程清理完毕。")

        end_time = time.time()
        duration = end_time - start_time
        total_in_db = database.get_total_count(db_path)
        
        width = 62
        summary_title_text = "失败任务重试总结" if args.retry_failed else "任务总结"
        top_line = f"{' ' + summary_title_text + ' ':=^{width}}"
        bottom_line = "=" * width
        
        summary = f"""
        \n{top_line}
        - 目标网站: {config['site_name']}
        - 开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}
        - 结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}
        - 总耗时: {time.strftime('%H时%M分%S秒', time.gmtime(duration))}

        --- 处理结果 ---
        - 计划处理URL: {len(urls_to_process)}
        - ✅ 成功更新记录: {stats['UPDATED']}
        - ⏩ 检测到重复记录: {stats['DUPLICATE']}
        - ❌ 处理失败记录: {stats['FAILED']}

        --- 数据库状态 ---
        - 数据库文件: {db_path}
        - 数据库总记录数: {total_in_db}
        \n{bottom_line}
        """
        logger.info(summary)

if __name__ == '__main__':
    main()
