import sqlite3
import os
import sys
import glob # 用于查找配置文件
import subprocess
import time
from flask import Flask, render_template, request, g, redirect, url_for, flash

# --- 定时任务库 ---
from flask_apscheduler import APScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

# --- 添加 scripts 目录到路径，以便导入 database ---
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
import database

# --- 基础配置 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 修正数据库路径，指向当前目录下的 test 文件夹
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
CONFIG_DIR = os.path.join(BASE_DIR, 'configs')
PER_PAGE = 100 

app = Flask(__name__)
app.secret_key = 'your_very_secret_and_random_key_for_flask'

# --- 1. 配置 Scheduler (定时任务) ---
class Config:
    SCHEDULER_API_ENABLED = True
    # 持久化存储：把任务存到 scheduler.db 文件里，重启 Docker 不丢失
    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(url='sqlite:///scheduler.db')
    }

app.config.from_object(Config())

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# --- 数据库辅助函数 ---
def get_db(db_name):
    db_path = os.path.join(DATABASE_DIR, db_name)
    if not os.path.exists(db_path):
        return None
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# ==========================================
#               Admin & Config 路由
# ==========================================

@app.route('/admin')
def admin():
    # 获取所有配置文件列表
    config_files = []
    if os.path.exists(CONFIG_DIR):
        files = glob.glob(os.path.join(CONFIG_DIR, "*.yaml"))
        config_files = [os.path.basename(f) for f in files]
    return render_template('admin.html', config_files=config_files)

@app.route('/api/get_config')
def get_config():
    filename = request.args.get('filename')
    if not filename or not filename.endswith('.yaml'):
        return "无效的文件名", 400
    
    file_path = os.path.join(CONFIG_DIR, filename)
    if not os.path.exists(file_path):
        return "文件不存在", 404
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        return str(e), 500

@app.route('/api/save_config', methods=['POST'])
def save_config():
    filename = request.form.get('filename')
    content = request.form.get('content')
    
    if not filename or not filename.endswith('.yaml'):
        return "无效的文件名", 400
        
    file_path = os.path.join(CONFIG_DIR, filename)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return "保存成功", 200
    except Exception as e:
        return str(e), 500
        
@app.route('/api/delete_config', methods=['POST'])
def delete_config():
    filename = request.form.get('filename')
    
    if not filename or not filename.endswith('.yaml'):
        return "无效的文件名", 400
        
    file_path = os.path.join(CONFIG_DIR, filename)
    
    if not os.path.exists(file_path):
        return "文件不存在", 404
    
    try:
        os.remove(file_path)
        return "删除成功", 200
    except Exception as e:
        return str(e), 500

# ==========================================
#               任务执行路由
# ==========================================

@app.route('/run_advanced_task', methods=['POST'])
def run_advanced_task():
    # 1. 获取通用参数
    task_type = request.form.get('task_type')
    site_name = request.form.get('param1')  # 统一从下拉菜单获取 site (不含 .yaml)
    
    # 基础命令: python run_task.py [javbee/sehuatang/...]
    cmd = [sys.executable, 'run_task.py', task_type]

    # 2. 根据任务类型组装参数
    if task_type == 'javbee':
        # --- 修改开始 ---
        date_val = request.form.get('param_jav_date', '').strip()
        tag_val = request.form.get('param_jav_tag', '').strip()
        start_page = request.form.get('param_jav_start', '1').strip()
        
        cmd.extend(['--site', site_name])
        
        # 逻辑：如果有 Tag 就用 Tag，否则看日期，否则默认
        if tag_val:
            cmd.extend(['--tag', tag_val])
            if start_page and start_page != '1':
                cmd.extend(['--start-page', start_page])
        elif date_val and date_val != 'auto':
            cmd.extend(['--date', date_val])
        # --- 修改结束 ---
            
    elif task_type == 'sehuatang':
        page_val = request.form.get('param_sech_page', '').strip()
        cmd.extend(['--site', site_name])
        if page_val:
            if not page_val.startswith('-'):
                cmd.extend(['--page', page_val])
            else:
                cmd.extend(page_val.split())
            
    elif task_type == 'nyaa':
        start_page = request.form.get('param_nyaa_start', '1').strip()
        end_page = request.form.get('param_nyaa_end', 'auto').strip()
        cmd.extend(['--site', site_name])
        cmd.extend(['--start-page', start_page])
        cmd.extend(['--end-page', end_page])

    elif task_type == 'retag':
        # Retag 命令格式特殊: run_task.py retag [site_name]
        cmd.append(site_name)

    try:
        print(f"Executing: {' '.join(cmd)}")
        subprocess.Popen(cmd)
        flash(f"🚀 任务已启动 [{site_name}]: {' '.join(cmd)}", "success")
    except Exception as e:
        flash(f"启动失败: {str(e)}", "error")
        
    return redirect(url_for('admin'))

@app.route('/run_update', methods=['POST'])
def run_update():
    """旧的一键更新入口，保留以兼容旧代码"""
    try:
        subprocess.Popen([sys.executable, 'run_task.py', 'sehuatang'])
        flash("🚀 后台更新任务已启动！请稍后查看日志。", "success")
    except Exception as e:
        flash(f"启动失败: {e}", "error")
    return redirect(url_for('index'))

# ==========================================
#           APScheduler (定时任务) API
# ==========================================

@app.route('/api/jobs')
def get_jobs():
    """获取所有定时任务"""
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else '暂停'
        jobs.append({
            'id': job.id,
            'name': job.name,
            'trigger': str(job.trigger),
            'next_run': next_run
        })
    return {'jobs': jobs}

@app.route('/api/add_job', methods=['POST'])
def add_job():
    """添加新的定时任务 (逻辑升级版)"""
    import time
    
    # 1. 获取通用参数
    task_type = request.form.get('task_type')
    site_name = request.form.get('param1') # 配置文件名
    cron_exp = request.form.get('cron_expression') 
    
    # 构造要执行的命令参数
    # 基础命令: python run_task.py [task_type]
    job_args = [sys.executable, 'run_task.py', task_type]
    job_name = f"Task: {task_type}"

    # 2. 根据任务类型解析专用参数 (复用 run_advanced_task 的逻辑)
    if task_type == 'javbee':
        # --- 修改开始 ---
        date_val = request.form.get('param_jav_date', '').strip()
        tag_val = request.form.get('param_jav_tag', '').strip()
        
        job_args.extend(['--site', param1])
        
        if tag_val:
            job_args.extend(['--tag', tag_val])
            job_name += f" (Tag: {tag_val})"
        elif date_val and date_val != 'auto':
            job_args.extend(['--date', date_val])
            job_name += f" (Date: {date_val})"
        else:
            job_name += f" (Auto Date)"
        # --- 修改结束 ---
        
    elif task_type == 'sehuatang':
        page_val = request.form.get('param_sech_page', '').strip()
        job_args.extend(['--site', site_name])
        if page_val:
            if not page_val.startswith('-'):
                job_args.extend(['--page', page_val])
            else:
                job_args.extend(page_val.split()) # 支持 --retry-failed 等
        job_name += f" ({site_name})"

    elif task_type == 'nyaa':
        start_page = request.form.get('param_nyaa_start', '1').strip()
        end_page = request.form.get('param_nyaa_end', 'auto').strip()
        job_args.extend(['--site', site_name])
        job_args.extend(['--start-page', start_page])
        job_args.extend(['--end-page', end_page])
        job_name += f" ({site_name})"

    elif task_type == 'retag':
        job_args.append(site_name)
        job_name += f" ({site_name})"

    try:
        # 解析 cron 表达式
        if not cron_exp:
            raise ValueError("Cron 表达式不能为空")
            
        minute, hour, day, month, week = cron_exp.split()
        job_id = f"job_{int(time.time())}"
        
        scheduler.add_job(
            id=job_id,
            func=subprocess.run,
            args=[job_args],
            trigger='cron',
            minute=minute, hour=hour, day=day, month=month, day_of_week=week,
            name=job_name,
            replace_existing=True
        )
        flash(f"✅ 定时任务已添加: {job_name} @ {cron_exp}", "success")
    except Exception as e:
        flash(f"❌ 添加失败: {str(e)}", "error")
        
    return redirect(url_for('admin'))
    
@app.route('/api/delete_job/<job_id>')
def delete_job(job_id):
    """删除定时任务"""
    try:
        scheduler.remove_job(job_id)
        flash(f"任务 {job_id} 已删除", "success")
    except Exception as e:
        flash(f"删除失败: {str(e)}", "error")
    return redirect(url_for('admin'))

# ==========================================
#               日志 & 首页路由
# ==========================================

@app.route('/logs')
def list_logs():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        return "日志目录不存在"
    # 按修改时间倒序排列
    files = sorted(os.listdir(log_dir), key=lambda x: os.path.getmtime(os.path.join(log_dir, x)), reverse=True)
    
    html = "<h1>系统日志</h1><ul>"
    for f in files:
        if f.endswith('.log'):
            html += f'<li><a href="/logs/{f}">{f}</a></li>'
    html += "</ul><a href='/admin'>返回后台</a> <a href='/'>返回首页</a>"
    return html

@app.route('/logs/<filename>')
def view_log(filename):
    log_dir = 'logs'
    try:
        with open(os.path.join(log_dir, filename), 'r', encoding='utf-8') as f:
            content = f.read()
        return f"<pre>{content}</pre>"
    except Exception as e:
        return f"读取日志失败: {e}"

def get_all_sources(conn):
    if not conn: return []
    try:
        sources = conn.execute("SELECT DISTINCT source FROM media ORDER BY source").fetchall()
        return [row['source'] for row in sources]
    except sqlite3.OperationalError:
        return []

@app.route('/', methods=['GET'])
def index():
    # 1. 先扫描目录，看看有哪些数据库文件
    try:
        available_dbs = sorted([f for f in os.listdir(DATABASE_DIR) if f.endswith('.db')])
    except FileNotFoundError:
        available_dbs = []

    # 2. 智能决定当前使用哪个数据库
    # 优先级: URL参数指定 > 列表里的第一个 > None (全空)
    db_name = request.args.get('db')
    
    if not db_name:
        if available_dbs:
            db_name = available_dbs[0]  # 默认自动选中第一个
        else:
            db_name = None  # 彻底没有数据库

    # 3. 定义默认的空值（防止后面变量未定义报错）
    items = []
    total_pages = 0
    page = 1
    # 默认空筛选参数
    search_term = search_scope = filter_source = filter_tag = filter_workflow_status = start_date = end_date = ''
    sort_by = 'publish_date'
    sort_order = 'DESC'
    all_sources = []
    all_tags = []

    # 4. 如果确定有数据库名，才去连接
    if db_name:
        conn = get_db(db_name)
        if conn:
            db_path = os.path.join(DATABASE_DIR, db_name)
            
            # --- 原有的筛选、排序、查询逻辑全部放在这里面 ---
            # 获取参数
            search_term = request.args.get('q_term', '').strip()
            search_scope = request.args.get('q_scope', 'all')
            filter_source = request.args.get('f_source', '')
            filter_tag = request.args.get('f_tag', '')
            filter_workflow_status = request.args.get('f_wstatus', '')
            start_date = request.args.get('start_date', '').strip()
            end_date = request.args.get('end_date', '').strip()
            sort_by = request.args.get('sort_by', 'publish_date')
            sort_order = request.args.get('sort_order', 'DESC')

            all_sources = get_all_sources(conn)
            all_tags = database.get_all_tags(db_path)

            # 构建查询
            query = "SELECT * FROM media WHERE 1=1"
            params = []
            
            if search_term:
                if search_scope == 'title':
                    query += " AND title LIKE ?"
                    params.append(f"%{search_term}%")
                elif search_scope == 'item_number':
                    query += " AND item_number LIKE ?"
                    params.append(f"%{search_term}%")
                else:
                    query += " AND (title LIKE ? OR item_number LIKE ?)"
                    params.extend([f"%{search_term}%", f"%{search_term}%"])

            if filter_source:
                query += " AND source = ?"
                params.append(filter_source)
            
            if filter_tag:
                query += " AND id IN (SELECT media_id FROM media_tags JOIN tags ON tags.id = media_tags.tag_id WHERE tags.name = ?)"
                params.append(filter_tag)

            if filter_workflow_status:
                query += " AND workflow_status = ?"
                params.append(filter_workflow_status)

            if start_date:
                query += " AND date(publish_date) >= date(?)"
                params.append(start_date)
            if end_date:
                query += " AND date(publish_date) <= date(?)"
                params.append(end_date)

            valid_sort_columns = ['publish_date', 'added_at', 'file_size', 'item_number', 'title', 'source', 'workflow_status']
            if sort_by not in valid_sort_columns: sort_by = 'publish_date'
            if sort_order.upper() not in ['ASC', 'DESC']: sort_order = 'DESC'
            
            if sort_by == 'file_size':
                query += f" ORDER BY file_size_bytes {sort_order}"
            else:
                query += f" ORDER BY {sort_by} {sort_order}"
            
            page = request.args.get('page', 1, type=int)
            offset = (page - 1) * PER_PAGE

            total_query = query.replace("SELECT *", "SELECT COUNT(*)")
            total_items = conn.execute(total_query, params).fetchone()[0]
            total_pages = (total_items + PER_PAGE - 1) // PER_PAGE if total_items > 0 else 1

            query += f" LIMIT {PER_PAGE} OFFSET {offset}"
            items = conn.execute(query, params).fetchall()
            conn.close()
        else:
             # 有文件名但文件打不开（极少见）
             flash(f"警告: 无法连接数据库 '{db_name}'", 'error')

    # 5. 渲染页面（即使 items 为空也能正常显示页面框架）
    return render_template(
        'index.html', items=items, page=page, total_pages=total_pages,
        search_term=search_term, search_scope=search_scope,
        filter_source=filter_source, all_sources=all_sources,
        filter_tag=filter_tag, all_tags=all_tags,
        filter_workflow_status=filter_workflow_status,
        start_date=start_date, end_date=end_date,
        sort_by=sort_by, sort_order=sort_order,
        available_dbs=available_dbs, current_db=db_name
    )
    
@app.route('/batch_update', methods=['POST'])
def batch_update():
    db_name = request.form.get('db_name')
    new_status = request.form.get('new_status')
    selected_ids = request.form.getlist('selected_ids')
    if not db_name or not new_status or not selected_ids:
        flash("操作失败：缺少必要参数。", 'error')
    else:
        db_path = os.path.join(DATABASE_DIR, db_name)
        if os.path.exists(db_path):
            count = database.batch_update_workflow_status(db_path, selected_ids, new_status)
            flash(f"成功更新了 {count} 条记录的状态为 '{new_status}'。", 'success')
        else:
            flash(f"操作失败：数据库 '{db_name}' 不存在。", 'error')
    return redirect(request.referrer or url_for('index'))

@app.route('/batch_delete', methods=['POST'])
def batch_delete():
    db_name = request.form.get('db_name')
    selected_ids = request.form.getlist('selected_ids')
    
    # 如果是单条删除（通过 URL 参数传来的，下面前端代码会用到）
    if not selected_ids:
        single_id = request.args.get('id')
        if single_id:
            selected_ids = [single_id]

    if not db_name or not selected_ids:
        flash("操作失败：未选择任何记录或缺少数据库参数。", 'error')
    else:
        db_path = os.path.join(DATABASE_DIR, db_name)
        if os.path.exists(db_path):
            count = database.delete_media_by_ids(db_path, selected_ids)
            if count > 0:
                flash(f"🗑️ 成功删除了 {count} 条记录。", 'success')
            else:
                flash("删除失败或未找到记录。", 'error')
        else:
            flash(f"操作失败：数据库 '{db_name}' 不存在。", 'error')
            
    return redirect(request.referrer or url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=6246)
