"""
配置管理模块,用于加载和管理平台配置
"""
import os
import yaml
from loguru import logger
from typing import Any, Dict, Optional

# 全局标志，标记配置是否已加载，避免重复输出日志
CONFIG_LOADED = False

class ConfigManager:
    """配置管理类,负责加载和访问配置"""
    
    def __init__(self, config_path: str = None, silent: bool = False):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径,如果为None则使用默认路径
            silent: 静默模式，不输出日志
        """
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'config', 'config.yaml'
        )
        self.silent = silent
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        从YAML文件加载配置
        
        Returns:
            加载的配置字典
        """
        global CONFIG_LOADED
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                # 只有在非静默模式且首次加载时才输出日志
                if not self.silent and not CONFIG_LOADED:
                    logger.info(f"成功从 {self.config_path} 加载配置")
                    CONFIG_LOADED = True
                return config
        except Exception as e:
            # 错误总是需要记录
            logger.error(f"加载配置失败: {str(e)}")
            raise
    
    def get(self, section: str, key: Optional[str] = None, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            section: 配置部分名称
            key: 配置键名,如果为None则返回整个部分
            default: 如果键不存在,返回的默认值
            
        Returns:
            配置值或默认值
        """
        if section not in self.config:
            logger.warning(f"配置部分 '{section}' 不存在,返回默认值 {default}")
            return default
        
        if key is None:
            return self.config[section]
        
        if not isinstance(key, (str, int)) or key not in self.config[section]:
            logger.warning(f"配置键 '{section}.{key}' 不存在,返回默认值 {default}")
            return default
            
        return self.config[section][key]
    
    def get_mongodb_uri(self) -> str:
        """
        获取MongoDB连接URI
        
        Returns:
            MongoDB连接URI字符串
        """
        return self.get('mongodb', 'uri')
    
    def get_mongodb_db(self) -> str:
        """
        获取MongoDB数据库名称
        
        Returns:
            MongoDB数据库名称
        """
        return self.get('mongodb', 'db')
    
    def get_tushare_api_key(self) -> str:
        """
        获取Tushare API密钥
        
        Returns:
            Tushare API密钥
        """
        return self.get('tushare', 'api_key')
    
    def get_tushare_api_url(self) -> str:
        """
        获取Tushare API URL
        
        Returns:
            Tushare API URL
        """
        return self.get('tushare', 'api_url')
    
    def get_wan_config(self) -> Dict[str, Any]:
        """
        获取WAN配置
        
        Returns:
            WAN配置字典
        """
        return self.get('wan')
    
    def get_update_config(self) -> Dict[str, Any]:
        """
        获取更新配置
        
        Returns:
            更新配置字典
        """
        return self.get('update')
    
    def is_wan_enabled(self) -> bool:
        """
        检查是否启用多WAN功能
        
        Returns:
            是否启用多WAN
        """
        return self.get('wan', 'enabled', False)
    
    def is_daily_update_enabled(self) -> bool:
        """
        检查是否启用每日更新
        
        Returns:
            是否启用每日更新
        """
        return self.get('update', 'daily_update_enabled', False)
    
    def get_daily_update_time(self) -> str:
        """
        获取每日更新时间
        
        Returns:
            每日更新时间字符串 (HH:MM)
        """
        return self.get('update', 'daily_update_time', "16:30")
    
    def set(self, section: str, key: str, value: Any) -> None:
        """
        设置配置值（仅在内存中）
        
        Args:
            section: 配置部分名称
            key: 配置键名
            value: 要设置的值
        """
        # 确保部分存在
        if section not in self.config:
            self.config[section] = {}
        
        # 设置值
        self.config[section][key] = value
        logger.debug(f"已设置配置项 {section}.{key} = {value}")


# 创建全局配置管理器实例，默认非静默
config_manager = ConfigManager() 