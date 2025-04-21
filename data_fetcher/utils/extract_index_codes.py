#!/usr/bin/env python
"""
提取指数代码工具 - 从index_members集合中提取所有不重复的指数代码

该脚本从MongoDB数据库的index_members集合中提取所有不重复的index_code值，
并将它们保存到一个新的集合index_code_list中，方便其他程序查询。

使用方法：
    python extract_index_codes.py                   # 使用默认设置
    python extract_index_codes.py --verbose         # 输出详细日志
    python extract_index_codes.py --db-name tushare_data  # 指定数据库名称
"""
import sys
import os
import argparse
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
from loguru import logger
import pymongo

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

try:
    # 尝试导入MongoDB处理器
    from core.mongodb_handler import MongoDBHandler, init_mongodb_handler
except ImportError:
    logger.error("无法导入必要的模块，请确保您在正确的环境中运行")
    sys.exit(1)

def extract_index_codes(
    db_name: str = None,
    source_collection: str = "index_members",
    target_collection: str = "index_code_list",
    verbose: bool = False,
    mongo_handler: MongoDBHandler = None
) -> bool:
    """
    从index_members集合中提取所有不重复的指数代码并保存到新集合
    
    Args:
        db_name: MongoDB数据库名称，如果为None则使用默认值
        source_collection: 源集合名称
        target_collection: 目标集合名称
        verbose: 是否输出详细日志
        mongo_handler: MongoDB处理器实例，如果为None则创建新实例
    
    Returns:
        bool: 操作是否成功
    """
    try:
        # 初始化MongoDB处理器
        if mongo_handler is None:
            if verbose:
                logger.info("初始化MongoDB处理器...")
            mongo_handler = init_mongodb_handler()
            if mongo_handler is None:
                logger.error("初始化MongoDB处理器失败")
                return False
            
            # 设置数据库名称（如果指定了）
            if db_name:
                logger.info(f"设置数据库名称为: {db_name}")
                mongo_handler.db = mongo_handler.client[db_name]
        
        # 检查连接状态
        if not mongo_handler.is_connected():
            logger.warning("MongoDB未连接，尝试连接...")
            if not mongo_handler.connect():
                logger.error("连接MongoDB失败")
                return False
        
        # 获取当前数据库名称
        try:
            # 尝试获取数据库名称
            current_db_name = mongo_handler.db.name
            logger.info(f"当前数据库: {current_db_name}")
        except Exception as e:
            logger.warning(f"无法获取数据库名称: {str(e)}")
            if db_name:
                logger.info(f"使用参数指定的数据库名称: {db_name}")
                current_db_name = db_name
            else:
                logger.error("未指定数据库名称，无法继续")
                return False
        
        # 检查源集合是否存在
        try:
            # 获取集合列表
            collections = mongo_handler.db.list_collection_names()
            if source_collection not in collections:
                logger.error(f"源集合 {source_collection} 不存在")
                return False
        except Exception as e:
            logger.warning(f"无法获取集合列表: {str(e)}")
            # 继续执行，假设集合存在
            logger.info("假设源集合存在并继续执行")
        
        # 从源集合提取不重复的index_code
        logger.info(f"从 {source_collection} 集合提取不重复的指数代码...")
        
        try:
            # 使用MongoDB的distinct函数获取不重复的index_code
            distinct_index_codes = mongo_handler.db[source_collection].distinct("index_code")
            
            if not distinct_index_codes:
                logger.warning(f"未找到任何指数代码")
                return False
            
            # 对结果进行排序
            distinct_index_codes.sort()
            
            # 输出统计信息
            logger.info(f"成功提取 {len(distinct_index_codes)} 个不重复的指数代码")
            
            if verbose:
                # 在详细模式下，输出前10个指数代码
                logger.debug(f"前10个指数代码示例: {distinct_index_codes[:10]}")
            
            # 准备保存到目标集合
            logger.info(f"准备保存到 {target_collection} 集合...")
            
            # 检查目标集合是否存在，如果存在则清空
            try:
                if target_collection in collections:
                    logger.warning(f"目标集合 {target_collection} 已存在，将被清空")
                    mongo_handler.db[target_collection].drop()
            except Exception as e:
                logger.warning(f"检查目标集合时出错: {str(e)}")
                # 尝试直接删除集合
                try:
                    mongo_handler.db[target_collection].drop()
                except Exception as e:
                    logger.warning(f"删除目标集合时出错: {str(e)}")
                    # 继续执行，尝试直接插入
            
            # 创建文档列表
            documents = []
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for index_code in distinct_index_codes:
                doc = {
                    "index_code": index_code,
                    "update_time": current_time
                }
                documents.append(doc)
            
            # 批量插入文档
            result = mongo_handler.db[target_collection].insert_many(documents)
            
            # 创建索引
            mongo_handler.db[target_collection].create_index("index_code", unique=True)
            
            logger.success(f"成功保存 {len(result.inserted_ids)} 个指数代码到 {target_collection} 集合")
            logger.info(f"已创建索引: index_code (unique)")
            
            return True
        except Exception as e:
            logger.error(f"处理指数代码时发生错误: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
            
    except Exception as e:
        logger.error(f"提取指数代码时发生错误: {str(e)}")
        import traceback
        logger.debug(f"详细错误信息: {traceback.format_exc()}")
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='从index_members集合中提取所有不重复的指数代码')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--source', default='index_members', help='源集合名称')
    parser.add_argument('--target', default='index_code_list', help='目标集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    
    args = parser.parse_args()
    
    # 根据verbose参数设置日志级别
    if not args.verbose:
        # 非详细模式下，设置日志级别为INFO，不显示DEBUG消息
        logger.remove()  # 移除所有处理器
        logger.add(sys.stderr, level="INFO")  # 添加标准错误输出处理器，级别为INFO
    
    # 执行提取操作
    success = extract_index_codes(
        db_name=args.db_name,
        source_collection=args.source,
        target_collection=args.target,
        verbose=args.verbose
    )
    
    if success:
        logger.success("指数代码提取和保存成功")
        return 0
    else:
        logger.error("指数代码提取或保存失败")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
        sys.exit(130)  # 使用标准的SIGINT退出码 