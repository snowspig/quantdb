#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置管理模块
负责读取、解析和管理系统配置
"""
import os
import sys

import yaml
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

# 默认配置目录和文件
DEFAULT_CONFIG_DIR = "config"
DEFAULT_CONFIG_FILE = "config.yaml"
DEFAULT_INTERFACE_DIR = "config/interfaces"

class ConfigManager:
    """
    配置管理器类
    提供统一的配置读取和管理功能，包括：
    1. 读取主配置文件（YAML格式）
    2. 读取接口配置文件（JSON格式）
    3. 获取特定配置项
    4. 验证配置并缓存结果（避免重复验证）
    """
    
    _instance = None
    _initialized_log = False # 新增日志标志
    
    # 配置状态文件
    CONFIG_STATE_FILE = "config_state.json"
    DEFAULT_VALIDITY_HOURS = 24
    
    @classmethod
    def get_instance(cls, config_path=None):
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls(config_path)
        return cls._instance
    
    def __new__(cls, config_path=None):
        """
        单例模式实现
        
        Args:
            config_path: 配置文件路径
        """
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            instance = cls._instance
            instance.config_path = config_path or os.path.join(DEFAULT_CONFIG_DIR, DEFAULT_CONFIG_FILE)
            instance.interface_dir = os.path.join(os.path.dirname(instance.config_path), "interfaces")
            
            # 先设置日志记录器
            instance._setup_logging()
            
            # 加载配置
            instance.config = instance._load_config()
            instance.interface_configs = {}
            
            # 确保日志只打印一次
            if not cls._initialized_log:
                instance.logger.info(f"配置管理器单例初始化完成，配置文件: {instance.config_path}")
                cls._initialized_log = True
        return cls._instance
    
    def __init__(self, config_path=None):
        """
        初始化配置管理器 - __new__ 负责实际的一次性初始化
        __init__ 不应再包含重复的初始化逻辑。
        """
        # 清空 __init__ 的内容，避免重复初始化和日志记录
        pass
        
    def _setup_logging(self):
        """
        设置日志记录 - 依赖根日志配置
        """
        self.logger = logging.getLogger("core.ConfigManager")
        # 移除显式的处理器添加和级别设置
        # 因为 main.py 中已经使用 basicConfig(force=True) 配置了根 logger
        # 子 logger 会自动继承配置
        
        # 如果需要，可以保留此处的日志级别设置，但通常继承即可
        # log_level = os.environ.get("QUANTDB_LOG_LEVEL", "INFO").upper()
        # self.logger.setLevel(getattr(logging, log_level, logging.INFO))
        
        # 确保 logger 存在
        if self.logger is None:
             print("ERROR: Failed to get logger for ConfigManager", file=sys.stderr)
             self.logger = logging.getLogger("core.ConfigManager.fallback") # 创建备用
    
    def _load_config(self) -> Dict:
        """
        加载配置文件
        
        Returns:
            Dict: 配置字典
        """
        try:
            # 检查配置文件是否存在
            if not os.path.isfile(self.config_path):
                self.logger.warning(f"配置文件不存在: {self.config_path}")
                return {}
                
            # 读取配置文件
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
            return config or {}
            
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {str(e)}")
            return {}
    
    def _load_interface_config(self, interface_name: str) -> Dict:
        """
        加载接口配置文件
        
        Args:
            interface_name: 接口名称
            
        Returns:
            Dict: 接口配置字典
        """
        try:
            # 构建接口配置文件路径
            interface_path = os.path.join(self.interface_dir, f"{interface_name}.json")
            
            # 检查接口配置文件是否存在
            if not os.path.isfile(interface_path):
                self.logger.warning(f"接口配置文件不存在: {interface_path}")
                return {}
                
            # 读取接口配置文件
            with open(interface_path, 'r', encoding='utf-8') as f:
                interface_config = json.load(f)
                
            return interface_config or {}
            
        except Exception as e:
            self.logger.error(f"加载接口配置文件失败: {str(e)}")
            return {}
    
    def get_all_config(self) -> Dict:
        """
        获取所有配置
        
        Returns:
            Dict: 配置字典
        """
        return self.config
        
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取指定键的配置值
        支持多级嵌套的配置项，如 'mongodb.uri'
        
        Args:
            key: 配置键（可以是多级路径，以点分隔）
            default: 默认值，未找到指定键时返回
            
        Returns:
            Any: 配置值
        """
        parts = key.split('.')
        value = self.config
        
        # 逐层查找配置
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
                
        return value
    
    def set(self, key: str, value: Any):
        """
        设置指定键的配置值
        支持多级嵌套的配置项，如 'mongodb.uri'
        
        Args:
            key: 配置键（可以是多级路径，以点分隔）
            value: 配置值
        """
        parts = key.split('.')
        target = self.config
        
        # 逐层查找配置，并创建缺失的层级
        for i, part in enumerate(parts[:-1]):
            if part not in target:
                target[part] = {}
            elif not isinstance(target[part], dict):
                target[part] = {}
                
            target = target[part]
            
        # 设置最后一层的值
        target[parts[-1]] = value
    
    def get_interface_config(self, interface_name: str) -> Dict:
        """
        获取接口配置
        
        Args:
            interface_name: 接口名称
            
        Returns:
            Dict: 接口配置字典
        """
        # 缓存接口配置
        if interface_name not in self.interface_configs:
            self.interface_configs[interface_name] = self._load_interface_config(interface_name)
            
        return self.interface_configs[interface_name]
    
    def get_all_interface_names(self) -> List[str]:
        """
        获取所有可用的接口名称
        
        Returns:
            List[str]: 接口名称列表
        """
        try:
            if not os.path.isdir(self.interface_dir):
                self.logger.warning(f"接口目录不存在: {self.interface_dir}")
                return []
                
            interface_files = [f for f in os.listdir(self.interface_dir) if f.endswith('.json')]
            interface_names = [f.rsplit('.', 1)[0] for f in interface_files]
            return interface_names
            
        except Exception as e:
            self.logger.error(f"获取接口名称列表失败: {str(e)}")
            return []
    
    def save_config(self, config_path: str = None) -> bool:
        """
        保存配置到文件
        
        Args:
            config_path: 配置文件路径，不提供则使用当前配置文件路径
            
        Returns:
            bool: 是否成功保存
        """
        config_path = config_path or self.config_path
        
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            
            # 保存配置文件
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
                
            self.logger.info(f"配置已保存: {config_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"保存配置失败: {str(e)}")
            return False
    
    def reload(self):
        """
        重新加载配置
        
        Returns:
            Dict: 加载的配置字典
        """
        self.config = self._load_config()
        self.interface_configs = {}  # 清空接口配置缓存
        
        self.logger.info(f"配置已重新加载: {self.config_path}")
        return self.config
    
    def get_mongodb_config(self) -> Dict:
        """
        获取 MongoDB 配置
        
        Returns:
            Dict: MongoDB 配置字典
        """
        return self.get('mongodb', {})
    
    def get_tushare_config(self) -> Dict:
        """
        获取 Tushare 配置
        
        Returns:
            Dict: Tushare 配置字典
        """
        return self.get('tushare', {})
    
    def get_log_config(self) -> Dict:
        """
        获取日志配置
        
        Returns:
            Dict: 日志配置字典
        """
        return self.get('log', {'level': 'INFO', 'file': 'logs/quantdb.log'})
    
    def get_wan_config(self) -> Dict:
        """
        获取多WAN口配置
        
        Returns:
            Dict: WAN配置字典
        """
        return self.get('wan', {})
    
    def is_wan_enabled(self) -> bool:
        """
        检查是否启用多WAN口
        
        Returns:
            bool: 是否启用多WAN口
        """
        wan_config = self.get_wan_config()
        return wan_config.get('enabled', False)
    
    def get_wan_interfaces(self) -> List[Dict]:
        """
        获取WAN接口列表
        
        Returns:
            List[Dict]: WAN接口列表
        """
        wan_config = self.get_wan_config()
        return wan_config.get('interfaces', [])
    
    def get_fetch_config(self, fetcher_name: str) -> Dict:
        """
        获取指定获取器的配置
        
        Args:
            fetcher_name: 获取器名称
            
        Returns:
            Dict: 获取器配置
        """
        fetchers_config = self.get('fetchers', {})
        return fetchers_config.get(fetcher_name, {})

    def get_default_fetcher_config(self) -> Dict:
        """
        获取默认的获取器配置
        
        Returns:
            Dict: 默认获取器配置
        """
        return {
            'thread_count': 5,
            'batch_size': 100,
            'retry_count': 3,
            'retry_interval': 1,
            'timeout': 30
        }
    
    # 以下是从config_state_manager.py合并的方法
    
    def verify_and_store_config(self, force_check=False) -> Dict[str, Any]:
        """
        验证所有配置并存储结果
        
        Args:
            force_check: 是否强制重新验证
            
        Returns:
            dict: 包含全部配置状态的字典
        """
        start_time = time.time()
        self.logger.info("开始验证系统配置...")
        
        # 检查是否需要重新验证
        if not force_check and self.is_config_valid():
            self.logger.info("使用缓存的配置状态")
            return self.load_config_state()
        
        self.logger.info("进行全新配置验证")
        
        # 运行验证测试
        try:
            # MongoDB连接验证
            self.logger.info("验证MongoDB连接...")
            mongo_status = self._verify_mongo_connection()
            
            # Tushare连接验证
            self.logger.info("验证Tushare API连接...")
            tushare_status = self._verify_tushare_connection()
            
            # 多WAN口验证
            self.logger.info("验证WAN网络接口...")
            wan_info = self._verify_wan_interfaces()
            
            # 创建配置状态
            current_time = datetime.now()
            state = {
                "timestamp": current_time.isoformat(),
                "valid_until": (current_time + timedelta(hours=self.DEFAULT_VALIDITY_HOURS)).isoformat(),
                "mongo": mongo_status,
                "tushare": tushare_status,
                "wan_interfaces": wan_info
            }
            
            # 保存状态到文件
            try:
                with open(self.CONFIG_STATE_FILE, 'w') as f:
                    json.dump(state, f, indent=2)
                self.logger.info(f"配置状态已保存到 {self.CONFIG_STATE_FILE}")
            except Exception as e:
                self.logger.error(f"保存配置状态失败: {str(e)}")
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"配置验证完成，耗时: {elapsed_time:.2f}秒")
            return state
            
        except Exception as e:
            self.logger.error(f"配置验证过程中出错: {str(e)}")
            # 创建最小化的错误状态
            error_state = {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "valid_until": datetime.now().isoformat()  # 立即过期
            }
            return error_state
    
    def is_config_valid(self) -> bool:
        """
        检查缓存配置是否存在且有效
        
        Returns:
            bool: 配置是否有效
        """
        if not os.path.exists(self.CONFIG_STATE_FILE):
            self.logger.info("配置状态文件不存在")
            return False
        
        try:
            with open(self.CONFIG_STATE_FILE, 'r') as f:
                state = json.load(f)
            
            # 检查是否有错误字段
            if "error" in state:
                self.logger.info("缓存的配置状态包含错误信息")
                return False
            
            # 检查有效期
            valid_until = datetime.fromisoformat(state.get("valid_until", "2000-01-01T00:00:00"))
            is_valid = datetime.now() < valid_until
            
            if is_valid:
                self.logger.info("缓存配置有效，有效期至: " + state.get("valid_until"))
            else:
                self.logger.info("缓存配置已过期")
                
            return is_valid
        except json.JSONDecodeError:
            self.logger.error("配置状态文件格式无效")
            return False
        except Exception as e:
            self.logger.error(f"检查配置有效性时出错: {str(e)}")
            return False
    
    def load_config_state(self) -> Dict[str, Any]:
        """
        加载缓存的配置状态
        
        Returns:
            dict: 配置状态字典
        """
        try:
            with open(self.CONFIG_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"加载配置状态失败: {str(e)}")
            return {"error": f"加载配置状态失败: {str(e)}"}
    
    def _verify_mongo_connection(self) -> Dict[str, Any]:
        """
        验证MongoDB连接
        
        Returns:
            dict: MongoDB连接状态信息
        """
        try:
            # 导入MongoDB处理器 - 改为绝对导入
            # from .mongodb_handler import mongodb_handler
            from core.mongodb_handler import mongodb_handler
            
            # 尝试连接
            if mongodb_handler.connect():
                # 获取服务器信息
                server_info = {}
                try:
                    info = mongodb_handler.client.server_info()
                    server_info = {
                        "version": info.get("version", "unknown"),
                        "gitVersion": info.get("gitVersion", "unknown")
                    }
                except:
                    pass
                
                # 获取连接参数
                connection_params = {
                    "host": mongodb_handler.config.get("host", "localhost"),
                    "port": mongodb_handler.config.get("port", 27017),
                    "db_name": mongodb_handler.config.get("db_name", "admin")
                }
                
                mongo_info = {
                    "status": "connected",
                    "server_info": server_info,
                    "connection_params": connection_params
                }
                return mongo_info
            else:
                return {"status": "error", "message": "无法连接到MongoDB服务器"}
        except ImportError:
            self.logger.error("无法导入MongoDB处理器")
            return {"status": "error", "message": "无法导入MongoDB处理器"}
        except Exception as e:
            self.logger.error(f"MongoDB连接验证失败: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _verify_tushare_connection(self) -> Dict[str, Any]:
        """
        验证Tushare API连接
        
        Returns:
            dict: Tushare连接状态信息
        """
        try:
            # 导入Tushare客户端
            from core.tushare_client_wan import TushareClientWAN
            
            # 获取Tushare配置
            tushare_config = self.get_tushare_config()
            token = tushare_config.get('token', '')
            
            if not token:
                return {"status": "error", "message": "未配置Tushare API令牌"}
            
            # 测试连接
            client = TushareClientWAN(token=token)
            try:
                # 尝试获取简单数据测试连接
                df = client.get_data('trade_cal', {'exchange': 'SSE', 'start_date': '20230101', 'end_date': '20230105'})
                if df is not None:
                    return {
                        "status": "connected",
                        "token_valid": True,
                        "connection_params": {
                            "token": token[:4] + '*****' + token[-4:] if len(token) > 8 else "****"
                        }
                    }
                else:
                    return {"status": "error", "message": "Tushare API连接失败，无法获取数据"}
            except Exception as e:
                return {"status": "error", "message": f"Tushare API请求失败: {str(e)}"}
        except ImportError as e:
            self.logger.error(f"无法导入TushareClientWAN: {str(e)}")
            return {"status": "error", "message": f"无法导入TushareClientWAN: {str(e)}"}
        except Exception as e:
            self.logger.error(f"Tushare连接验证失败: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _verify_wan_interfaces(self) -> list:
        """
        验证多WAN接口状态
        
        Returns:
            list: WAN接口信息列表
        """
        try:
            # 测试URL
            test_url = "http://106.14.185.239:29990/test"
            
            # 导入检查工具
            from core.wan_manager.wan_port_pool import WanPortPool
            wan_port_pool = WanPortPool.get_instance()
            
            wan_interfaces = []
            
            # 获取所有可用WAN索引
            wan_indices = wan_port_pool.get_available_wan_indices()
            
            # 检查每个WAN接口
            for wan_idx in wan_indices:
                try:
                    # 获取端口范围
                    port_range = wan_port_pool.wan_port_ranges.get(wan_idx, (0, 0))
                    
                    # 分配一个端口
                    port = wan_port_pool.allocate_port(wan_idx)
                    
                    if port:
                        try:
                            # 使用socket尝试连接测试URL
                            import socket
                            import urllib.parse
                            
                            parsed = urllib.parse.urlparse(test_url)
                            host = parsed.netloc.split(':')[0]
                            port_url = parsed.port or (443 if parsed.scheme == 'https' else 80)
                            
                            # 创建socket并绑定本地端口
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(5)
                            sock.bind(('0.0.0.0', port))
                            
                            # 连接测试URL
                            sock.connect((host, port_url))
                            sock.close()
                            
                            wan_interfaces.append({
                                "wan_idx": wan_idx,
                                "port_range": list(port_range),
                                "status": "active",
                                "test_port": port
                            })
                        except Exception as e:
                            wan_interfaces.append({
                                "wan_idx": wan_idx,
                                "port_range": list(port_range),
                                "status": "error",
                                "message": f"连接测试失败: {str(e)}",
                                "test_port": port
                            })
                        finally:
                            # 释放端口
                            wan_port_pool.release_port(wan_idx, port)
                    else:
                        wan_interfaces.append({
                            "wan_idx": wan_idx,
                            "port_range": list(port_range),
                            "status": "error",
                            "message": "无法分配端口"
                        })
                except Exception as e:
                    wan_interfaces.append({
                        "wan_idx": wan_idx,
                        "status": "error",
                        "message": f"WAN接口测试失败: {str(e)}"
                    })
            
            return wan_interfaces
        except ImportError as e:
            self.logger.error(f"无法导入WanPortPool: {str(e)}")
            return [{"status": "error", "message": f"无法导入WanPortPool: {str(e)}"}]
        except Exception as e:
            self.logger.error(f"WAN接口验证失败: {str(e)}")
            return [{"status": "error", "message": str(e)}]
    
    def get_validation_status(self) -> Dict[str, bool]:
        """
        获取各项配置的验证状态
        
        Returns:
            Dict[str, bool]: 验证状态字典
        """
        state = self.load_config_state()
        
        return {
            "mongo": state.get("mongo", {}).get("status") == "connected",
            "tushare": state.get("tushare", {}).get("status") == "connected",
            "wan": any(wan.get("status") == "active" for wan in state.get("wan_interfaces", []))
        }