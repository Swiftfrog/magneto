import argparse, os, logging, time
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils import setup_driver, setup_logging, load_config
import database

logger = logging.getLogger(__name__)

def extract_unique_urls(html_content, base_url, selectors):
    soup = BeautifulSoup(html_content, 'html.parser')
    thread_items = soup.select(selectors['thread_list_item'])
    relative_links = [item.select_one(selectors['thread_link'])['href'] for item in thread_items if item.select_one(selectors['thread_link'])]
    unique_links = set(relative_links)
    base = base_url.rstrip('/')
    return [f"{base}/{link.lstrip('/')}" for link in unique_links]

def extract_max_page(html_content, selectors):
    if not html_content: return 1
    soup = BeautifulSoup(html_content, 'html.parser')
    try:
        last_page_tag = soup.select_one(selectors['max_page_link'])
        if last_page_tag and "forum-" in last_page_tag.get("href", ""):
            return int(last_page_tag["href"].split("-")[-1].split(".")[0])
    except Exception: pass
    try:
        span_tag = soup.select_one(selectors['max_page_span'])
        if span_tag:
            return int(span_tag.get("title", "").split("共")[-1].split("页")[0].strip())
    except Exception: pass
    return 1
    
def fetch_html_with_selenium(url, driver, selectors):
    try:
        driver.get(url)
        time.sleep(0.5)
        if 'enter_button' in selectors and selectors['enter_button']:
            buttons = driver.find_elements(By.CSS_SELECTOR, selectors['enter_button'])
            if buttons:
                try: buttons[0].click()
                except Exception: pass
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors['thread_list_item'])))
        return driver.page_source
    except Exception as e:
        logger.error(f"页面加载失败: {url} - {e}")
        return None

class Orchestrator:
    def __init__(self, config, page_ranges, incremental_mode):
        self.config = config
        self.page_ranges = page_ranges
        self.incremental_mode = incremental_mode
        self.driver = None
        self.selectors = config['selectors']['fetch_urls']
    def run(self):
        try:
            self.driver = setup_driver()
            if self.page_ranges: self._process_pages(self.page_ranges)
            elif self.incremental_mode: self._process_pages([1])
            else:
                first_page_url = f"{self.config['base_url']}/forum.php?mod=forumdisplay&fid={self.config.get('fid')}&page=1"
                first_page_html = fetch_html_with_selenium(first_page_url, self.driver, self.selectors)
                max_pages = extract_max_page(first_page_html, self.selectors)
                logger.info(f"确定最大页码为 {max_pages}")
                self._process_pages(list(range(max_pages, 0, -1)))
        finally:
            if self.driver: self.driver.quit()
    def _process_pages(self, page_list):
        all_urls_batch = []
        db_path = self.config['database_file']
        for i, page_num in enumerate(sorted(list(page_list), reverse=True)):
            target_url = f"{self.config['base_url']}/forum.php?mod=forumdisplay&fid={self.config.get('fid')}&page={page_num}"
            logger.info(f"正在抓取页面 ({i+1}/{len(page_list)}): {target_url}")
            html = fetch_html_with_selenium(target_url, self.driver, self.selectors)
            if html: all_urls_batch.extend(extract_unique_urls(html, self.config['base_url'], self.selectors))
            if (i + 1) % self.config.get('batch_pages', 10) == 0 or (i + 1 == len(page_list)):
                if all_urls_batch:
                    database.add_urls(db_path, all_urls_batch, self.config['site_name'])
                    all_urls_batch = []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="抓取帖子 URL 并保存到指定数据库。")
    parser.add_argument("--site", "-s", required=True, help="网站标识")
    parser.add_argument("--page", help="指定页面范围")
    parser.add_argument("--incremental", action="store_true", help="增量模式")
    args = parser.parse_args()

    config = load_config(args.site)
    db_path = config['database_file']
    setup_logging(config['log_level'], config['site_name'], "fetch_urls")
    database.init_db(db_path)
    
    def parse_page_range(page_str):
        if not page_str: return None
        pages = set()
        for part in page_str.split(','):
            part = part.strip()
            if '-' in part:
                try:
                    start, end = map(int, part.split('-'))
                    pages.update(range(start, end + 1))
                except ValueError: logger.warning(f"无法解析: '{part}'")
            else:
                try: pages.add(int(part))
                except ValueError: logger.warning(f"无法解析: '{part}'")
        return list(pages)

    page_ranges = parse_page_range(args.page)
    orchestrator = Orchestrator(config=config, page_ranges=page_ranges, incremental_mode=args.incremental)
    orchestrator.run()
    logger.info("URL 抓取任务完成。")
