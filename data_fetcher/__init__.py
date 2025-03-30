"""
数据获取模块，负责从Tushare API获取金融数据
"""
from data_fetcher.tushare_client import TushareClient, tushare_client

__all__ = ['TushareClient', 'tushare_client'] 