#!/usr/bin/env python
"""
Tushare客户端 - WAN绑定版本
支持多WAN口绑定请求的Tushare API客户端
"""
import os
import sys
import time
import json
import pandas as pd
import requests
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger

# 导入WAN绑定适配器
class SourceAddressAdapter(requests.adapters.HTTPAdapter):
    """HTTP适配器，支持设置源地址"""
    
    def __init__(self, source_address, **kwargs):
        self.source_address = source_address
        super(SourceAddressAdapter, self).__init__(**kwargs)
        
    def init_poolmanager(self, *args, **kwargs):
        kwargs['source_address'] = self.source_address
        super(SourceAddressAdapter, self).init_poolmanager(*args, **kwargs)


class TushareClientWAN:
    """
    专用于WAN绑定的Tushare客户端
    """
    
    def __init__(self, token: str, timeout: int = 60, api_url: str = None):
        """初始化WAN绑定的Tushare客户端"""
        self.token = token
        self.timeout = timeout
        
        # 使用传入的API URL或默认为湘财Tushare API地址
        self.url = api_url or "http://api.waditu.com"
        
        self.headers = {
            "Content-Type": "application/json",
        }
        self.proxies = None
        self.local_addr = None
        
        # 验证token
        mask_token = token[:4] + '*' * (len(token) - 8) + token[-4:] if len(token) > 8 else '***'
        logger.debug(f"TushareClientWAN初始化: {mask_token} (长度: {len(token)}), API URL: {self.url}")
    
    def set_local_address(self, host: str, port: int):
        """设置本地地址绑定"""
        self.local_addr = (host, port)
        logger.debug(f"已设置本地地址绑定: {host}:{port}")
    
    def reset_local_address(self):
        """重置本地地址绑定"""
        self.local_addr = None
        logger.debug("已重置本地地址绑定")
    
    def set_timeout(self, timeout: int):
        """设置请求超时"""
        self.timeout = timeout
        logger.debug(f"已设置请求超时: {timeout}秒")
    
    def get_data(self, api_name: str, params: dict, fields: list = None):
        """
        获取API数据
        
        Args:
            api_name: API名称
            params: 请求参数
            fields: 返回字段列表
            
        Returns:
            DataFrame格式的数据
        """
        try:
            # 创建请求数据 - 与原始TushareClient请求格式保持一致
            req_params = {
                "api_name": api_name,
                "token": self.token,
                "params": params or {},
                "fields": fields or ""
            }
            
            logger.debug(f"请求URL: {self.url}, API: {api_name}, Token长度: {len(self.token)}")
            
            # 使用requests发送请求
            start_time = time.time()
            
            # 支持本地地址绑定的请求
            s = requests.Session()
            if self.local_addr:
                # 设置source_address
                s.mount('http://', SourceAddressAdapter(self.local_addr))
                s.mount('https://', SourceAddressAdapter(self.local_addr))
            
            response = s.post(
                self.url,
                json=req_params,
                headers=self.headers,
                timeout=self.timeout,
                proxies=self.proxies
            )
            
            elapsed = time.time() - start_time
            logger.debug(f"API请求耗时: {elapsed:.2f}s")
            
            # 检查响应状态
            if response.status_code != 200:
                logger.error(f"API请求错误: {response.status_code} - {response.text}")
                return None
                
            # 解析响应
            result = response.json()
            if result.get('code') != 0:
                logger.error(f"API返回错误: {result.get('code')} - {result.get('msg')}")
                return None
                
            # 转换为DataFrame
            data = result.get('data')
            if not data or not data.get('items'):
                logger.debug("API返回空数据")
                return pd.DataFrame()
                
            items = data.get('items')
            columns = data.get('fields')
            
            # 创建DataFrame
            df = pd.DataFrame(items, columns=columns)
            return df
            
        except Exception as e:
            logger.error(f"获取API数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return None 