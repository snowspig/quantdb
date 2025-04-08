#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置状态管理器 (ConfigStateManager)

用于避免gorepy.py在执行多个程序时重复进行MongoDB、Tushare和多WAN口配置的验证。
系统启动时进行一次全面的配置验证，将结果缓存到文件中，并在后续调用中重用这些配置。
"""

import json
import os
import sys
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ConfigStateManager')

class ConfigStateManager:
    """配置状态管理器，用于缓存和重用已验证的配置"""
    
    CONFIG_STATE_FILE = "config_state.json"
    DEFAULT_VALIDITY_HOURS = 24
    
    @classmethod
    def verify_and_store_config(cls, force_check=False) -> Dict[str, Any]:
        """验证所有配置并存储结果
        
        Args:
            force_check: 是否强制重新验证
            
        Returns:
            dict: 包含全部配置状态的字典
        """
        start_time = time.time()
        logger.info("开始验证系统配置...")
        
        # 检查是否需要重新验证
        if not force_check and cls.is_config_valid():
            logger.info("使用缓存的配置状态")
            return cls.load_config_state()
        
        logger.info("进行全新配置验证")
        
        # 运行验证测试
        try:
            # MongoDB连接验证
            logger.info("验证MongoDB连接...")
            mongo_status = cls._verify_mongo_connection()
            
            # Tushare连接验证
            logger.info("验证Tushare API连接...")
            tushare_status = cls._verify_tushare_connection()
            
            # 多WAN口验证
            logger.info("验证WAN网络接口...")
            wan_info = cls._verify_wan_interfaces()
            
            # 创建配置状态
            current_time = datetime.now()
            state = {
                "timestamp": current_time.isoformat(),
                "valid_until": (current_time + timedelta(hours=cls.DEFAULT_VALIDITY_HOURS)).isoformat(),
                "mongo": mongo_status,
                "tushare": tushare_status,
                "wan_interfaces": wan_info
            }
            
            # 保存状态到文件
            try:
                with open(cls.CONFIG_STATE_FILE, 'w') as f:
                    json.dump(state, f, indent=2)
                logger.info(f"配置状态已保存到 {cls.CONFIG_STATE_FILE}")
            except Exception as e:
                logger.error(f"保存配置状态失败: {str(e)}")
            
            elapsed_time = time.time() - start_time
            logger.info(f"配置验证完成，耗时: {elapsed_time:.2f}秒")
            return state
            
        except Exception as e:
            logger.error(f"配置验证过程中出错: {str(e)}")
            # 创建最小化的错误状态
            error_state = {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "valid_until": datetime.now().isoformat()  # 立即过期
            }
            return error_state
    
    @classmethod
    def is_config_valid(cls) -> bool:
        """检查缓存配置是否存在且有效
        
        Returns:
            bool: 配置是否有效
        """
        if not os.path.exists(cls.CONFIG_STATE_FILE):
            logger.info("配置状态文件不存在")
            return False
        
        try:
            with open(cls.CONFIG_STATE_FILE, 'r') as f:
                state = json.load(f)
            
            # 检查是否有错误字段
            if "error" in state:
                logger.info("缓存的配置状态包含错误信息")
                return False
            
            # 检查有效期
            valid_until = datetime.fromisoformat(state.get("valid_until", "2000-01-01T00:00:00"))
            is_valid = datetime.now() < valid_until
            
            if is_valid:
                logger.info("缓存配置有效，有效期至: " + state.get("valid_until"))
            else:
                logger.info("缓存配置已过期")
                
            return is_valid
        except json.JSONDecodeError:
            logger.error("配置状态文件格式无效")
            return False
        except Exception as e:
            logger.error(f"检查配置有效性时出错: {str(e)}")
            return False
    
    @classmethod
    def load_config_state(cls) -> Dict[str, Any]:
        """加载缓存的配置状态
        
        Returns:
            dict: 配置状态字典
        """
        try:
            with open(cls.CONFIG_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置状态失败: {str(e)}")
            return {"error": f"加载配置状态失败: {str(e)}"}
    
    @staticmethod
    def _verify_mongo_connection() -> Dict[str, Any]:
        """验证MongoDB连接
        
        Returns:
            dict: MongoDB连接状态信息
        """
        try:
            # 导入现有的MongoDB连接测试模块
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from test_mongodb_connection import test_connection
            
            result = test_connection()
            
            if result.get("status") == "success":
                # 从成功的测试结果中提取连接参数
                mongo_info = {
                    "status": "connected",
                    "server_info": result.get("server_info", {}),
                    "connection_params": result.get("connection_params", {})
                }
                return mongo_info
            else:
                return {"status": "error", "message": result.get("message", "未知错误")}
                
        except ImportError:
            logger.error("无法导入MongoDB连接测试模块")
            return {"status": "error", "message": "无法导入MongoDB连接测试模块"}
        except Exception as e:
            logger.error(f"MongoDB连接验证失败: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @staticmethod
    def _verify_tushare_connection() -> Dict[str, Any]:
        """验证Tushare API连接
        
        Returns:
            dict: Tushare连接状态信息
        """
        try:
            # 导入Tushare客户端
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from data_fetcher.tushare_client import TushareClient
            
            # 导入配置管理器获取Tushare token
            from config_manager import ConfigManager
            config = ConfigManager().get_config()
            
            # 创建Tushare客户端并测试连接
            tushare_client = TushareClient(token=config.get('tushare', {}).get('token', ''))
            is_connected = tushare_client.test_connection()
            
            if is_connected:
                return {
                    "status": "connected",
                    "token_valid": True,
                    "connection_params": {
                        "token": config.get('tushare', {}).get('token', '')
                    }
                }
            else:
                return {"status": "error", "message": "Tushare API连接失败"}
                
        except ImportError as e:
            logger.error(f"无法导入Tushare客户端: {str(e)}")
            return {"status": "error", "message": f"无法导入Tushare客户端: {str(e)}"}
        except Exception as e:
            logger.error(f"Tushare连接验证失败: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    @staticmethod
    def _verify_wan_interfaces() -> list:
        """验证多WAN接口状态
        
        Returns:
            list: WAN接口信息列表
        """
        try:
            # 导入网络管理器
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from network_manager import NetworkManager
            
            # 定义要测试的端口范围
            port_ranges = [
                (50001, 51000),
                (51001, 52000),
                (52001, 53000)
            ]
            
            # 测试URL
            test_url = "http://106.14.185.239:29990/test"
            
            wan_interfaces = []
            
            # 对每个端口范围测试WAN连接
            for port_range in port_ranges:
                start_port, end_port = port_range
                
                try:
                    # 创建网络管理器实例
                    network_mgr = NetworkManager(start_port=start_port, end_port=end_port)
                    
                    # 测试连接并获取响应
                    response = network_mgr.request(test_url)
                    
                    if response and hasattr(response, 'json'):
                        response_data = response.json()
                        detected_ip = response_data.get('your_ip', 'unknown')
                        
                        wan_interfaces.append({
                            "port_range": list(port_range),
                            "detected_ip": detected_ip,
                            "status": "active" if detected_ip != "unknown" else "error"
                        })
                    else:
                        wan_interfaces.append({
                            "port_range": list(port_range),
                            "detected_ip": "unknown",
                            "status": "error",
                            "message": "无法获取有效响应"
                        })
                        
                except Exception as e:
                    logger.error(f"端口范围 {port_range} 的WAN接口测试失败: {str(e)}")
                    wan_interfaces.append({
                        "port_range": list(port_range),
                        "detected_ip": "unknown",
                        "status": "error",
                        "message": str(e)
                    })
            
            return wan_interfaces
            
        except ImportError as e:
            logger.error(f"无法导入NetworkManager: {str(e)}")
            return [{"status": "error", "message": f"无法导入NetworkManager: {str(e)}"}]
        except Exception as e:
            logger.error(f"WAN接口验证失败: {str(e)}")
            return [{"status": "error", "message": str(e)}]


def main():
    """命令行入口函数，用于测试ConfigStateManager"""
    import argparse
    
    parser = argparse.ArgumentParser(description="配置状态管理器")
    parser.add_argument("--force", "-f", action="store_true", help="强制重新验证配置")
    parser.add_argument("--show", "-s", action="store_true", help="显示当前配置状态")
    
    args = parser.parse_args()
    
    if args.show and not args.force and ConfigStateManager.is_config_valid():
        # 只显示当前配置
        config = ConfigStateManager.load_config_state()
        print(json.dumps(config, indent=2, ensure_ascii=False))
    else:
        # 验证并存储配置
        config = ConfigStateManager.verify_and_store_config(force_check=args.force)
        print(json.dumps(config, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
