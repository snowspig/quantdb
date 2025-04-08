#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
示例数据获取器，使用ConfigStateManager来避免重复配置验证

此示例演示了如何使用ConfigStateManager来检查是否已经进行了配置验证，
从而避免在多个程序执行时重复进行MongoDB和Tushare API的连接测试。
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# 设置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ExampleFetcher')

# 导入ConfigStateManager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from core.config_state_manager import ConfigStateManager


class ExampleFetcher:
    """示例数据获取器，使用ConfigStateManager来优化配置验证流程"""
    
    def __init__(self, use_config_state=True):
        """
        初始化数据获取器
        
        Args:
            use_config_state: 是否使用配置状态管理器
        """
        self.use_config_state = use_config_state
        logger.info("初始化示例数据获取器")
        
    def initialize(self):
        """
        初始化连接和配置
        """
        start_time = time.time()
        logger.info("开始初始化...")
        
        # 验证MongoDB连接
        mongo_ok = self._setup_mongodb()
        if not mongo_ok:
            logger.error("MongoDB连接失败，无法继续")
            return False
        
        # 验证Tushare连接
        tushare_ok = self._setup_tushare()
        if not tushare_ok:
            logger.error("Tushare API连接失败，无法继续")
            return False
            
        # 验证WAN网络接口
        wan_ok = self._setup_wan_interfaces()
        if not wan_ok:
            logger.warning("WAN网络接口验证失败，将使用默认网络")
            # 继续执行，因为可以使用默认网络
        
        elapsed_time = time.time() - start_time
        logger.info(f"初始化完成，耗时: {elapsed_time:.2f}秒")
        return True
        
    def _setup_mongodb(self):
        """
        设置MongoDB连接
        
        优先使用ConfigStateManager检查是否已经验证过MongoDB连接
        
        Returns:
            bool: 连接是否成功
        """
        if self.use_config_state:
            # 使用ConfigStateManager检查MongoDB是否已验证
            config_state = ConfigStateManager()
            is_validated, details = config_state.is_mongodb_validated()
            
            if is_validated:
                logger.info("使用已验证的MongoDB连接配置")
                return True
            else:
                logger.info(f"MongoDB配置未验证或已过期: {details.get('error', '未知错误')}")
        
        logger.info("执行MongoDB连接验证...")
        # 这里会执行实际的MongoDB连接验证
        # 此处只是示例，实际使用时请替换为真实的连接验证代码
        try:
            # 模拟连接验证
            time.sleep(1)  # 模拟连接延迟
            
            # 假设连接成功
            if self.use_config_state:
                # 更新ConfigStateManager状态
                ConfigStateManager().set_mongodb_validated(
                    True, 
                    {"host": "localhost", "port": 27017, "database": "quantdb"},
                    None  # 无错误
                )
            return True
        except Exception as e:
            logger.error(f"MongoDB连接验证失败: {str(e)}")
            if self.use_config_state:
                # 记录验证失败
                ConfigStateManager().set_mongodb_validated(False, None, str(e))
            return False
            
    def _setup_tushare(self):
        """
        设置Tushare API连接
        
        优先使用ConfigStateManager检查是否已经验证过Tushare连接
        
        Returns:
            bool: 连接是否成功
        """
        if self.use_config_state:
            # 使用ConfigStateManager检查Tushare是否已验证
            config_state = ConfigStateManager()
            is_validated, details = config_state.is_tushare_validated()
            
            if is_validated:
                logger.info("使用已验证的Tushare API配置")
                return True
            else:
                logger.info(f"Tushare配置未验证或已过期: {details.get('error', '未知错误')}")
        
        logger.info("执行Tushare API连接验证...")
        # 这里会执行实际的Tushare API连接验证
        # 此处只是示例，实际使用时请替换为真实的连接验证代码
        try:
            # 模拟API连接验证
            time.sleep(1)  # 模拟连接延迟
            
            # 假设连接成功
            if self.use_config_state:
                # 更新ConfigStateManager状态
                ConfigStateManager().set_tushare_validated(
                    True, 
                    {"token": "*****", "version": "1.0"},
                    None  # 无错误
                )
            return True
        except Exception as e:
            logger.error(f"Tushare API连接验证失败: {str(e)}")
            if self.use_config_state:
                # 记录验证失败
                ConfigStateManager().set_tushare_validated(False, None, str(e))
            return False
    
    def _setup_wan_interfaces(self):
        """
        设置WAN网络接口
        
        优先使用ConfigStateManager检查是否已经验证过WAN接口
        
        Returns:
            bool: 验证是否成功
        """
        if self.use_config_state:
            # 使用ConfigStateManager检查WAN是否已验证
            config_state = ConfigStateManager()
            is_validated, details = config_state.is_wan_validated()
            
            if is_validated:
                logger.info("使用已验证的WAN网络接口配置")
                return True
            else:
                logger.info(f"WAN配置未验证或已过期: {details.get('error', '未知错误')}")
        
        logger.info("执行WAN网络接口验证...")
        # 这里会执行实际的WAN接口验证
        # 此处只是示例，实际使用时请替换为真实的WAN接口验证代码
        try:
            # 模拟WAN接口验证
            time.sleep(2)  # 模拟验证延迟
            
            # 假设验证成功，发现了3个接口
            interfaces = [
                {"name": "wan1", "port_range": [50001, 51000], "status": "active"},
                {"name": "wan2", "port_range": [51001, 52000], "status": "active"},
                {"name": "wan3", "port_range": [52001, 53000], "status": "active"}
            ]
            
            if self.use_config_state:
                # 更新ConfigStateManager状态
                ConfigStateManager().set_wan_validated(True, interfaces, None)
            return True
        except Exception as e:
            logger.error(f"WAN网络接口验证失败: {str(e)}")
            if self.use_config_state:
                # 记录验证失败
                ConfigStateManager().set_wan_validated(False, None, str(e))
            return False
    
    def fetch_data(self, data_type, **kwargs):
        """
        获取数据示例方法
        
        Args:
            data_type: 数据类型
            **kwargs: 其他参数
            
        Returns:
            dict: 获取的数据
        """
        logger.info(f"获取数据: {data_type}, 参数: {kwargs}")
        # 此处为示例，实际应根据data_type获取不同类型的数据
        return {"status": "success", "data_type": data_type, "timestamp": datetime.now().isoformat()}


def main():
    """示例入口函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="使用ConfigStateManager的示例数据获取器")
    parser.add_argument("--no-config-state", action="store_true", help="不使用配置状态管理器")
    parser.add_argument("--reset", action="store_true", help="重置所有配置验证状态")
    
    args = parser.parse_args()
    
    # 重置配置状态（如果需要）
    if args.reset:
        logger.info("重置所有配置验证状态")
        ConfigStateManager().reset_validations()
    
    # 创建数据获取器实例
    fetcher = ExampleFetcher(use_config_state=not args.no_config_state)
    
    # 初始化
    if not fetcher.initialize():
        logger.error("初始化失败，退出")
        return 1
    
    # 获取示例数据
    data = fetcher.fetch_data("daily", code="000001.SZ")
    logger.info(f"获取数据结果: {data}")
    
    # 打印配置状态摘要
    summary = ConfigStateManager().get_validation_summary()
    logger.info(f"配置状态摘要: {summary}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
