import sqlite3
import logging
import re
from datetime import datetime
import os

logger = logging.getLogger(__name__)
###

def parse_size_str_to_bytes(size_str):
    """
    将文件大小字符串 (如 '1.2 GiB', '500 MB') 转换为整数字节。
    支持格式: TiB/GiB/MiB/KiB (1024进制) 和 G/M/K (1000进制)
    """
    if not size_str:
        return 0
    
    s = size_str.strip().upper()
    # 移除可能存在的逗号
    s = s.replace(',', '')
    
    try:
        # 匹配数字部分
        match = re.match(r'^([\d\.]+)\s*([A-Z]+)?', s)
        if not match:
            return 0
            
        number = float(match.group(1))
        unit = match.group(2) if match.group(2) else ''
        
        # 1024 进制 (IEC 标准)
        if 'TIB' in unit: return int(number * 1024**4)
        if 'GIB' in unit: return int(number * 1024**3)
        if 'MIB' in unit: return int(number * 1024**2)
        if 'KIB' in unit: return int(number * 1024)
        
        # 1000 进制 (通常用作简写，或者你可以根据需求统一按1024处理)
        # 这里为了兼容你之前的SQL逻辑，保留混合处理，或者你可以统一改为 1024
        if 'G' in unit: return int(number * 1000**3) # 或者 1024**3
        if 'M' in unit: return int(number * 1000**2) # 或者 1024**2
        if 'K' in unit: return int(number * 1000)    # 或者 1024
        
        return int(number)
    except Exception:
        return 0

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # --- 【核心简化】---
    # 不再检查和修改旧表，直接创建包含所有字段的最终版表结构
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            source TEXT NOT NULL, 
            post_url TEXT NOT NULL, 
            status TEXT NOT NULL DEFAULT 'NEW', 
            info_hash TEXT, 
            title TEXT, 
            publish_date TEXT, 
            file_size TEXT, 
            file_size_bytes INTEGER DEFAULT 0,
            item_number TEXT, 
            magnet_link TEXT, 
            cover_url TEXT, 
            added_at TEXT NOT NULL, 
            processed_at TEXT,
            workflow_status TEXT DEFAULT 'pending', -- 使用英文并设置默认值
            UNIQUE(source, post_url), 
            UNIQUE(info_hash)
        )
    ''')
    # --- 【简化结束】---

    cursor.execute('CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS media_tags (
            media_id INTEGER, tag_id INTEGER,
            FOREIGN KEY(media_id) REFERENCES media(id) ON DELETE CASCADE,
            FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY(media_id, tag_id)
        )
    ''')
    conn.commit()
    conn.close()
    logger.info(f"数据库 '{db_path}' 初始化成功。")

def batch_update_workflow_status(db_path, ids, new_status):
    if not ids:
        return 0
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    placeholders = ', '.join('?' for _ in ids)
    query = f"UPDATE media SET workflow_status = ? WHERE id IN ({placeholders})"
    params = [new_status] + ids
    cursor.execute(query, params)
    count = cursor.rowcount
    conn.commit()
    conn.close()
    logger.info(f"成功更新了 {count} 条记录的状态为 '{new_status}'。")
    return count

###
def add_urls(db_path, urls, source):
    if not urls: return
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    new_urls_data = [(source, url, 'NEW', datetime.now().isoformat()) for url in urls]
    cursor.executemany('INSERT OR IGNORE INTO media (source, post_url, status, added_at) VALUES (?, ?, ?, ?)', new_urls_data)
    if cursor.rowcount > 0: logger.info(f"成功向数据库 '{db_path}' 添加了 {cursor.rowcount} 个新URL。")
    conn.commit()
    conn.close()

def get_unprocessed_urls(db_path, source):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT post_url FROM media WHERE source = ? AND status = "NEW"', (source,))
    urls = [row[0] for row in cursor.fetchall()]
    conn.close()
    return urls

def get_failed_urls(db_path, source):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT post_url FROM media WHERE source = ? AND status = "FAILED"', (source,))
    urls = [row[0] for row in cursor.fetchall()]
    conn.close()
    return urls

def mark_url_failed(db_path, post_url, source):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE media SET status = 'FAILED' WHERE source = ? AND post_url = ?", (source, post_url))
    conn.commit()
    conn.close()
    logger.warning(f"已将URL标记为失败: {post_url}")

def _execute_tag_update(cursor, media_id, tags_list):
    cursor.execute("DELETE FROM media_tags WHERE media_id = ?", (media_id,))
    if not tags_list: return
    for tag_name in tags_list:
        cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
        cursor.execute("SELECT id FROM tags WHERE name = ?", (tag_name,))
        tag_id_result = cursor.fetchone()
        if tag_id_result:
            cursor.execute("INSERT OR IGNORE INTO media_tags (media_id, tag_id) VALUES (?, ?)", (media_id, tag_id_result[0]))

def update_post_with_tags(db_path, post_url, source, details, tags_list):
    magnet = details.get('magnet_link')
    info_hash = None
    if magnet and 'btih:' in magnet:
        match = re.search(r'btih:([a-fA-F0-9]+)', magnet)
        if match: info_hash = match.group(1).lower()
            
    if not info_hash:
        mark_url_failed(db_path, post_url, source)
        return 'FAILED'

    size_str = details.get('size', '')
    size_bytes = parse_size_str_to_bytes(size_str)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM media WHERE info_hash = ? AND post_url != ?", (info_hash, post_url))
        if cursor.fetchone():
            logger.warning(f"Info hash for {post_url} 已存在，删除此重复任务。")
            cursor.execute("DELETE FROM media WHERE source = ? AND post_url = ?", (source, post_url))
            conn.commit()
            return 'DUPLICATE'
        else:
            cursor.execute('''
                UPDATE media SET status = ?, info_hash = ?, title = ?, publish_date = ?, 
                file_size = ?, file_size_bytes = ?, item_number = ?, magnet_link = ?, 
                cover_url = ?, processed_at = ?
                WHERE source = ? AND post_url = ?
            ''', (
                'PROCESSED', info_hash, details.get('title'), details.get('date'), 
                size_str, size_bytes, # 对应 file_size 和 file_size_bytes
                details.get('item_number'), magnet, details.get('cover_image_url', ''), 
                datetime.now().isoformat(),
                source, post_url
            ))
            cursor.execute("SELECT id FROM media WHERE post_url = ? AND source = ?", (post_url, source))
            media_id_result = cursor.fetchone()
            if media_id_result: _execute_tag_update(cursor, media_id_result[0], tags_list)
            conn.commit()
            logger.info(f"已更新URL: {post_url}")
            return 'UPDATED'
    except sqlite3.IntegrityError:
        logger.warning(f"更新 {post_url} 时 info_hash 已存在，删除此重复任务。")
        cursor.execute("DELETE FROM media WHERE source = ? AND post_url = ?", (source, post_url))
        conn.commit()
        return 'DUPLICATE'
    finally:
        conn.close()

def add_processed_post_with_tags(db_path, source, details, tags_list):
    magnet = details.get('magnet_link')
    info_hash = None
    if magnet and 'btih:' in magnet:
        match = re.search(r'btih:([a-fA-F0-9]+)', magnet)
        if match: info_hash = match.group(1).lower()
    if not info_hash:
        logger.warning(f"缺少 info_hash，跳过记录: {details.get('title')}")
        return 'FAILED'

    size_str = details.get('size', '')
    size_bytes = parse_size_str_to_bytes(size_str)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    try:
        cursor.execute('''
            INSERT INTO media (source, post_url, status, info_hash, title, publish_date,
            file_size, file_size_bytes, item_number, magnet_link, cover_url, added_at, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            source, details.get('post_url'), 'PROCESSED', info_hash, details.get('title'), 
            details.get('date'), size_str, size_bytes, # 插入 size_str 和 size_bytes
            details.get('item_number'), magnet, 
            details.get('cover_image_url'), now, now
        ))
        media_id = cursor.lastrowid
        _execute_tag_update(cursor, media_id, tags_list)
        logger.info(f"成功添加新记录: {details.get('title')}")
        conn.commit()
        return 'ADDED'
    except sqlite3.IntegrityError:
        logger.info(f"Info hash {info_hash} 或 URL 已存在，跳过。")
        return 'DUPLICATE'
    finally:
        conn.close()

def get_total_count(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM media")
    count = cursor.fetchone()[0]
    conn.close()
    return count
    
def get_all_media_for_retag(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT id, title FROM media')
    items = cursor.fetchall()
    conn.close()
    return items

def update_tags_for_media_id(db_path, media_id, tags_list):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    _execute_tag_update(cursor, media_id, tags_list)
    conn.commit()
    conn.close()

def get_all_tags(db_path):
    """从数据库获取所有不重复的 tag 列表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM tags ORDER BY name")
        tags = [row[0] for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        tags = [] # 如果表不存在或为空
    finally:
        conn.close()
    return tags

def delete_media_by_ids(db_path, ids):
    """根据 ID 列表批量删除记录"""
    if not ids:
        return 0
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 动态构建 SQL 语句: DELETE FROM media WHERE id IN (?, ?, ...)
    placeholders = ', '.join('?' for _ in ids)
    query = f"DELETE FROM media WHERE id IN ({placeholders})"
    
    try:
        cursor.execute(query, ids)
        count = cursor.rowcount
        conn.commit()
        logger.info(f"成功删除了 {count} 条记录。")
        return count
    except Exception as e:
        logger.error(f"删除记录失败: {e}")
        return 0
    finally:
        conn.close()