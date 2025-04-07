"""
日志管理模块，提供统一的日志记录功能
"""
import os
import sys
import time
from typing import Dict, Any, Optional
from loguru import logger
import json


class Logger:
    """
    日志记录器，提供统一的日志管理和记录功能
    """
    
    def __init__(self, name: str = None, verbose: bool = False):
        """
        初始化日志记录器
        
        Args:
            name: 日志名称，用于标识不同的日志记录器
            verbose: 是否启用详细日志模式
        """
        self.name = name or "data_fetcher"
        self.verbose = verbose
        
        # 创建日志目录
        self.log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        os.makedirs(self.log_dir, exist_ok=True)
        
        # 设置日志文件路径
        self.log_file = os.path.join(self.log_dir, f"{self.name}_{time.strftime('%Y%m%d')}.log")
        
        # 配置日志记录器
        self._configure_logger()
        
        logger.info(f"日志记录器 '{self.name}' 初始化完成，{'详细' if self.verbose else '标准'}模式")
    
    def _configure_logger(self):
        """配置日志记录器的格式和处理器"""
        # 移除所有已有的处理器
        logger.remove()
        
        # 添加控制台处理器
        console_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        logger.add(sys.stderr, format=console_format, level="DEBUG" if self.verbose else "INFO")
        
        # 添加文件处理器
        file_format = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
        logger.add(self.log_file, format=file_format, level="DEBUG", rotation="500 MB", encoding="utf-8")
    
    def set_verbose(self, verbose: bool):
        """
        设置是否启用详细日志模式
        
        Args:
            verbose: 是否启用详细日志
        """
        if self.verbose != verbose:
            self.verbose = verbose
            self._configure_logger()
            logger.info(f"切换到{'详细' if self.verbose else '标准'}日志模式")
    
    def info(self, message: str):
        """
        记录信息级别的日志
        
        Args:
            message: 日志消息
        """
        logger.info(message)
    
    def debug(self, message: str):
        """
        记录调试级别的日志
        
        Args:
            message: 日志消息
        """
        logger.debug(message)
    
    def warning(self, message: str):
        """
        记录警告级别的日志
        
        Args:
            message: 日志消息
        """
        logger.warning(message)
    
    def error(self, message: str):
        """
        记录错误级别的日志
        
        Args:
            message: 日志消息
        """
        logger.error(message)
    
    def success(self, message: str):
        """
        记录成功级别的日志
        
        Args:
            message: 日志消息
        """
        logger.success(message)
    
    def stats(self, stats_dict: Dict[str, Any], title: Optional[str] = None):
        """
        记录统计信息
        
        Args:
            stats_dict: 统计信息字典
            title: 统计信息标题，可选
        """
        if title:
            logger.info(f"===== {title} =====")
        
        for key, value in stats_dict.items():
            if isinstance(value, dict):
                logger.info(f"{key}:")
                for sub_key, sub_value in value.items():
                    logger.info(f"  {sub_key}: {sub_value}")
            else:
                logger.info(f"{key}: {value}")
    
    def pretty_dict(self, data: Dict[str, Any], title: Optional[str] = None):
        """
        美观地打印字典内容
        
        Args:
            data: 要打印的字典
            title: 标题，可选
        """
        if title:
            logger.info(f"===== {title} =====")
        try:
            formatted = json.dumps(data, ensure_ascii=False, indent=2)
            for line in formatted.split('\n'):
                logger.info(line)
        except:
            logger.info(str(data))


# 创建全局日志记录器实例
data_logger = Logger()