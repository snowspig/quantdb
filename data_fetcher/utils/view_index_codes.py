#!/usr/bin/env python
"""
查看指数代码列表 - 显示MongoDB中保存的指数代码列表

该脚本用于查看MongoDB数据库中index_code_list集合中保存的指数代码列表。

使用方法：
    python view_index_codes.py                  # 显示前10条记录
    python view_index_codes.py --limit 20       # 显示前20条记录
    python view_index_codes.py --all            # 显示所有记录
"""
import sys
import os
import argparse
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

try:
    # 导入MongoDB处理器
    from core.mongodb_handler import init_mongodb_handler
except ImportError:
    logger.error("无法导入必要的模块，请确保您在正确的环境中运行")
    sys.exit(1)

def view_index_codes(limit=10, show_all=False, db_name=None):
    """
    查看指数代码列表
    
    Args:
        limit: 显示的记录数量
        show_all: 是否显示所有记录
        db_name: 数据库名称
    """
    try:
        # 初始化MongoDB处理器
        handler = init_mongodb_handler()
        if handler is None:
            logger.error("初始化MongoDB处理器失败")
            return
        
        # 设置数据库名称（如果指定了）
        if db_name:
            handler.db = handler.client[db_name]
        
        # 获取数据库名称
        db_name = handler.db.name
        logger.info(f"当前数据库: {db_name}")
        
        # 检查集合是否存在
        collections = handler.db.list_collection_names()
        if "index_code_list" not in collections:
            logger.error("index_code_list集合不存在")
            return
        
        # 获取记录总数
        total_count = handler.db.index_code_list.count_documents({})
        logger.info(f"总记录数: {total_count}")
        
        # 查询记录
        if show_all:
            results = list(handler.db.index_code_list.find())
            logger.info(f"显示所有记录 ({len(results)} 条)")
        else:
            results = list(handler.db.index_code_list.find().limit(limit))
            logger.info(f"显示前 {len(results)} 条记录")
        
        # 显示记录
        for i, record in enumerate(results, 1):
            index_code = record.get("index_code", "N/A")
            update_time = record.get("update_time", "N/A")
            print(f"{i}. 指数代码: {index_code}, 更新时间: {update_time}")
            
    except Exception as e:
        logger.error(f"查看指数代码列表时发生错误: {str(e)}")
        import traceback
        logger.debug(f"详细错误信息: {traceback.format_exc()}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='查看MongoDB中保存的指数代码列表')
    parser.add_argument('--limit', type=int, default=10, help='显示的记录数量')
    parser.add_argument('--all', action='store_true', help='显示所有记录')
    parser.add_argument('--db-name', help='数据库名称')
    
    args = parser.parse_args()
    
    # 调用查看函数
    view_index_codes(
        limit=args.limit,
        show_all=args.all,
        db_name=args.db_name
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
        sys.exit(130)  # 使用标准的SIGINT退出码 