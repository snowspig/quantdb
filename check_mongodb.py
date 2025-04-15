#!/usr/bin/env python
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# 导入MongoDB处理模块
from core import mongodb_handler
# 导入初始化函数
from core.mongodb_handler import init_mongodb_handler

# 计算配置文件路径
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "config.yaml")
print(f"使用配置文件: {config_path}")

# 初始化MongoDB处理器
print("正在初始化MongoDB处理器...")
mongodb_handler = init_mongodb_handler(config_path)

# 确保mongodb_handler不为None
if mongodb_handler is None:
    print("MongoDB处理器初始化失败")
    sys.exit(1)

# 连接MongoDB
if not mongodb_handler.is_connected():
    print("正在连接MongoDB...")
    mongodb_handler.connect()

if not mongodb_handler.is_connected():
    print("无法连接到MongoDB，请检查服务是否运行")
    sys.exit(1)

# 使用daily_basic_ts集合
collection_name = "daily_basic_ts"

# 查询记录总数
total_count = mongodb_handler.count_documents(collection_name)
print(f"daily_basic_ts集合总记录数: {total_count}")

# 查询不同交易日期的记录
print("\n获取交易日期数据:")

# 使用聚合查询按交易日期分组
pipeline = [
    {"$group": {"_id": "$trade_date", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]

# 直接使用PyMongo进行聚合查询
db = mongodb_handler.get_database()
collection = db[collection_name]
result = list(collection.aggregate(pipeline))

if result:
    print("按交易日期统计:")
    for item in result:
        print(f"交易日 {item['_id']}: {item['count']} 条记录")
else:
    print("未找到交易日期数据")

# 获取最近几条记录
print("\n最新3条记录:")
recent_docs = mongodb_handler.find_documents(collection_name, {})[:3]
for doc in recent_docs:
    # 只打印关键字段，完整数据太长
    print(f"交易日: {doc.get('trade_date')}, 股票代码: {doc.get('ts_code')}, 收盘价: {doc.get('close')}") 