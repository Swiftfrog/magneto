import logging
import subprocess
import sys
import os

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PYTHON_EXEC = sys.executable

def run_script(script_path, args):
    """运行脚本并等待结束"""
    if not os.path.exists(script_path) and not os.path.dirname(script_path):
        potential_path = os.path.join("scripts", script_path)
        if os.path.exists(potential_path):
            script_path = potential_path

    cmd = [PYTHON_EXEC, script_path] + args
    logger.info(f"🚀 开始运行: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        logger.info(f"✅ {script_path} 执行完毕。")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {script_path} 执行失败，退出码: {e.returncode}")
        return False

def get_site_from_args(args, default_site):
    """辅助函数：从参数列表中提取 --site 的值"""
    if args and "--site" in args:
        try:
            index = args.index("--site")
            if index + 1 < len(args):
                return args[index + 1]
        except ValueError:
            pass
    return default_site

def task_sehuatang_update(extra_args=None):
    """色花堂更新"""
    # 1. 确定目标站点名称
    current_site = get_site_from_args(extra_args, "sehuatang")
    
    # 2. 判断是否是【重试模式】
    # 只有 process_details.py 支持 --retry-failed
    is_retry_mode = extra_args and "--retry-failed" in extra_args

    # --- 阶段 1: 抓取 URL ---
    if is_retry_mode:
        logger.info(">>> 检测到重试模式 (--retry-failed)，跳过阶段 1 (抓取列表)。")
    else:
        # 准备 fetch_urls 参数
        if extra_args:
            fetch_args = extra_args
            if "--site" not in fetch_args:
                fetch_args = ["--site", current_site] + fetch_args
        else:
            fetch_args = ["--site", current_site, "--page", "1-2"]

        logger.info(f">>> 阶段 1: 抓取 URL (Site: {current_site})")
        success = run_script("scripts/fetch_urls.py", fetch_args)
        
        if not success:
            logger.error("阶段 1 失败，终止后续任务。")
            return

    # --- 阶段 2: 处理详情/重试 ---
    if is_retry_mode:
        logger.info(f">>> 阶段 2: 开始重试失败任务 (Site: {current_site})")
        # 构造重试参数
        process_args = ["--site", current_site, "--retry-failed"]
    else:
        logger.info(f">>> 阶段 2: 处理新发现的任务 (Site: {current_site})")
        process_args = ["--site", current_site]
    
    run_script("scripts/process_details.py", process_args)

def task_javbee_update(extra_args=None):
    """Javbee 更新"""
    current_site = get_site_from_args(extra_args, "javbee")
    cmd_args = ["--site", current_site]
    if extra_args:
        if "--site" in extra_args:
            cmd_args = extra_args
        else:
            cmd_args.extend(extra_args)
    run_script("scripts/scrape_javbee.py", cmd_args) 

def task_nyaa_update(extra_args=None):
    """Nyaa 更新"""
    if not extra_args:
        extra_args = ["--site", "nyaa", "--start-page", "1", "--end-page", "auto"]
    run_script("scripts/scrape_nyaa.py", extra_args)

def task_retag(site_name):
    """运行标签重新解析任务"""
    logger.info(f">>> 开始对 {site_name} 进行标签重整 (Retag)...")
    run_script("scripts/retag.py", ["--site", site_name])

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        extra_args = sys.argv[2:] 
        
        if command == "sehuatang":
            task_sehuatang_update(extra_args)
        elif command == "javbee":
            task_javbee_update(extra_args)
        elif command == "nyaa":
            task_nyaa_update(extra_args)
        elif command == "retag":
            target_site = sys.argv[2] if len(sys.argv) > 2 else "javbee"
            task_retag(target_site)
        else:
            logger.error(f"未知的命令: {command}")
    else:
        logger.info("未提供参数，默认执行 Sehuatang 更新任务...")
        task_sehuatang_update()