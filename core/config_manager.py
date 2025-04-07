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
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

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
    """
    
    _instance = None
    
    def __new__(cls, config_path=None):
        """
        单例模式实现
        
        Args:
            config_path: 配置文件路径
        """
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path=None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径，不提供则使用默认路径
        """
        # 避免重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        # 配置文件路径
        self.config_path = config_path or os.path.join(DEFAULT_CONFIG_DIR, DEFAULT_CONFIG_FILE)
        self.interface_dir = os.path.join(os.path.dirname(self.config_path), "interfaces")
        
        # 初始化日志
        self._setup_logging()
        
        # 加载配置
        self.config = self._load_config()
        self.interface_configs = {}
        
        # 标记为已初始化
        self._initialized = True
        
        self.logger.info(f"配置管理器初始化完成，配置文件: {self.config_path}")
        
    def _setup_logging(self):
        """
        设置日志记录
        """
        self.logger = logging.getLogger("core.ConfigManager")
        
        if not self.logger.handlers:
            # 避免重复添加处理程序
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(console_handler)
            
            # 设置日志级别
            log_level = os.environ.get("QUANTDB_LOG_LEVEL", "INFO").upper()
            self.logger.setLevel(getattr(logging, log_level, logging.INFO))
    
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
    
    def is_wan_enabled(self) -> bool:
        """
        检查是否启用多WAN口
        
        Returns:
            bool: 是否启用多WAN口
        """
        network_config = self.get('network', {})
        wan_config = network_config.get('wan', {})
        return wan_config.get('enabled', False)
    
    def get_wan_config(self) -> Dict:
        """
        获取多WAN口配置
        
        Returns:
            Dict: WAN配置字典
        """
        network_config = self.get('network', {})
        return network_config.get('wan', {})
    
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


# 创建全局配置管理器实例
config_manager = ConfigManager()