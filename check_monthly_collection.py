#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查MongoDB中的monthly集合数据
"""

import sys
import os
import logging
from pymongo import MongoClient
from bson import json_util
import json
import yaml
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_path="config/config.yaml"):
    """加载配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        return None

def connect_mongodb(config):
    """连接到MongoDB数据库"""
    try:
        # 获取MongoDB配置
        mongo_config = config.get('mongodb', {})
        uri = mongo_config.get('uri')
        
        if uri:
            client = MongoClient(uri)
        else:
            host = mongo_config.get('host', 'localhost')
            port = mongo_config.get('port', 27017)
            username = mongo_config.get('username')
            password = mongo_config.get('password')
            auth_db = mongo_config.get('auth_source')
            
            if username and password:
                client = MongoClient(host, port, username=username, password=password, authSource=auth_db)
            else:
                client = MongoClient(host, port)
        
        db_name = mongo_config.get('db_name', 'tushare_data')
        return client[db_name]
    except Exception as e:
        logger.error(f"连接MongoDB失败: {e}")
        return None

def check_monthly_collection(db):
    """检查monthly集合的数据情况"""
    try:
        # 获取集合
        collection = db['monthly']
        
        # 获取文档数量
        doc_count = collection.count_documents({})
        logger.info(f"monthly集合中共有 {doc_count} 条文档")
        
        # 获取一个样本文档
        if doc_count > 0:
            sample_doc = collection.find_one()
            if sample_doc:
                # 转换为可读的JSON格式
                sample_json = json.dumps(json_util.dumps(sample_doc), indent=2, ensure_ascii=False)
                logger.info(f"样本文档:\n{sample_json}")
                
                # 显示字段信息
                logger.info(f"字段列表: {', '.join(sample_doc.keys())}")
                
                # 获取交易日期范围
                min_date = collection.find_one({}, sort=[('trade_date', 1)])
                max_date = collection.find_one({}, sort=[('trade_date', -1)])
                
                if min_date and max_date:
                    logger.info(f"日期范围: 从 {min_date.get('trade_date')} 到 {max_date.get('trade_date')}")
                
                # 获取股票代码数量
                ts_codes = collection.distinct('ts_code')
                logger.info(f"共有 {len(ts_codes)} 个不同的股票代码")
                
                # 展示部分股票代码
                if len(ts_codes) > 0:
                    logger.info(f"部分股票代码: {', '.join(ts_codes[:10])}")
            else:
                logger.warning("未能获取样本文档")
        else:
            logger.warning("monthly集合中没有数据")
    except Exception as e:
        logger.error(f"检查monthly集合时出错: {e}")

def main():
    """主函数"""
    # 加载配置
    config = load_config()
    if not config:
        logger.error("配置加载失败，退出程序")
        sys.exit(1)
    
    # 连接MongoDB
    db = connect_mongodb(config)
    if db is None:
        logger.error("MongoDB连接失败，退出程序")
        sys.exit(1)
    
    # 检查monthly集合
    check_monthly_collection(db)

if __name__ == "__main__":
    main()