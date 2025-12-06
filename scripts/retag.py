import argparse, logging
from utils import load_config, setup_logging, parse_tags_from_title
import database

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="根据最新规则，重新处理数据库中所有记录的标签。")
    parser.add_argument("--site", "-s", required=True, help="网站标识，用于加载配置文件和确定数据库。")
    args = parser.parse_args()

    config = load_config(args.site)
    db_path = config['database_file']
    tag_rules = config.get('tag_rules', {})
    
    if not tag_rules:
        print(f"错误: 配置文件 'configs/{args.site}.yaml' 中未找到 'tag_rules'。")
        return

    setup_logging(config['log_level'], config['site_name'], "retag")
    database.init_db(db_path)
    
    logger.info(f"开始为数据库 '{db_path}' 进行标签回填...")
    
    all_media = database.get_all_media_for_retag(db_path)
    if not all_media:
        logger.info("数据库中没有需要处理的记录。")
        return
        
    logger.info(f"找到 {len(all_media)} 条记录，开始重新解析标签...")
    
    count = 0
    for media_id, title in all_media:
        if not title: continue
        new_tags = parse_tags_from_title(title, tag_rules)
        database.update_tags_for_media_id(db_path, media_id, new_tags)
        count += 1
        if count % 100 == 0:
            logger.info(f"已处理 {count}/{len(all_media)} 条记录...")

    logger.info(f"所有 {len(all_media)} 条记录的标签已根据最新规则更新完毕！")

if __name__ == "__main__":
    main()
