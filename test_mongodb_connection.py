#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试MongoDB连接
验证优化后的MongoDB连接处理器
"""
import os
import sys
import time
import logging

# 设置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_mongodb_connection")

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入MongoDB处理器
from core.mongodb_handler import mongodb_handler, init_mongodb_handler

def test_connection():
    """测试MongoDB连接"""
    logger.info("开始测试MongoDB连接")
    
    # 确保MongoDB处理器已初始化
    handler = init_mongodb_handler()
    
    # 测试连接
    is_connected = handler.connect()
    logger.info(f"连接状态: {'成功' if is_connected else '失败'}")
    
    if is_connected:
        # 测试基本操作
        db = handler.get_database()
        logger.info(f"成功连接到数据库: {db.name}")
        
        # 尝试获取集合列表
        collection_names = db.list_collection_names()
        logger.info(f"数据库中的集合: {collection_names}")
        
        # 尝试插入和查询测试文档
        test_collection = "connection_test"
        test_document = {
            "test_id": "test_conn_" + time.strftime("%Y%m%d%H%M%S"),
            "timestamp": time.time(),
            "message": "测试MongoDB连接和操作"
        }
        
        try:
            # 插入测试文档
            inserted_id = handler.insert_document(test_collection, test_document)
            logger.info(f"测试文档插入成功，ID: {inserted_id}")
            
            # 查询刚插入的文档
            found_doc = handler.find_document(test_collection, {"_id": inserted_id})
            if found_doc:
                logger.info(f"测试文档查询成功: {found_doc['test_id']}")
            else:
                logger.warning("测试文档查询失败，未找到插入的文档")
                
            # 测试计数
            count = handler.count_documents(test_collection, {"test_id": test_document["test_id"]})
            logger.info(f"文档计数: {count}")
            
            # 测试更新
            update_result = handler.update_document(
                test_collection, 
                {"_id": inserted_id},
                {"$set": {"updated": True, "update_time": time.time()}}
            )
            logger.info(f"更新操作影响了 {update_result} 个文档")
            
            # 删除测试文档
            delete_result = handler.delete_document(test_collection, {"_id": inserted_id})
            logger.info(f"删除操作删除了 {delete_result} 个文档")
            
            logger.info("MongoDB连接和基本操作测试全部通过")
            return True
            
        except Exception as e:
            logger.error(f"测试过程中发生错误: {e}")
            return False
    else:
        logger.error("MongoDB连接失败，无法进行进一步测试")
        return False

def test_reconnection():
    """测试重连机制"""
    logger.info("\n开始测试重连机制")
    handler = mongodb_handler
    
    # 关闭现有连接
    if handler.is_connected():
        handler.close_connection()
        logger.info("已关闭当前连接以测试重连机制")
    
    # 尝试获取数据库(此操作应触发重连)
    try:
        db = handler.get_database()
        logger.info(f"重连成功，连接到数据库: {db.name}")
        return True
    except Exception as e:
        logger.error(f"重连失败: {e}")
        return False

def test_multiple_operations():
    """测试多次操作的稳定性"""
    logger.info("\n开始测试多次连续操作的稳定性")
    handler = mongodb_handler
    
    success_count = 0
    fail_count = 0
    iterations = 5
    
    for i in range(iterations):
        try:
            # 确认连接
            if not handler.is_connected():
                handler.connect()
                
            # 进行查询
            collection_names = handler.get_database().list_collection_names()
            logger.info(f"迭代 {i+1}/{iterations}: 查询到 {len(collection_names)} 个集合")
            
            # 添加短暂延迟
            time.sleep(0.5)
            success_count += 1
            
        except Exception as e:
            logger.error(f"迭代 {i+1}/{iterations} 失败: {e}")
            fail_count += 1
    
    logger.info(f"多次操作测试完成: 成功 {success_count}/{iterations}, 失败 {fail_count}/{iterations}")
    return fail_count == 0

def main():
    """主函数"""
    logger.info("MongoDB连接优化测试程序启动")
    
    # 运行测试
    connection_test = test_connection()
    reconnection_test = test_reconnection()
    multiple_operations_test = test_multiple_operations()
    
    # 汇总结果
    logger.info("\n测试结果汇总:")
    logger.info(f"- 基本连接和操作测试: {'通过' if connection_test else '失败'}")
    logger.info(f"- 重连机制测试: {'通过' if reconnection_test else '失败'}")
    logger.info(f"- 多次操作稳定性测试: {'通过' if multiple_operations_test else '失败'}")
    
    # 最终结论
    if connection_test and reconnection_test and multiple_operations_test:
        logger.info("所有测试通过，MongoDB连接处理器工作正常!")
        return 0
    else:
        logger.error("部分测试未通过，请检查日志了解详情")
        return 1

if __name__ == "__main__":
    sys.exit(main())
