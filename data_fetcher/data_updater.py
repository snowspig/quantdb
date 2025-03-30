"""
数据更新模块，负责从Tushare获取数据并更新到MongoDB
"""
import os
import sys
import argparse
import threading
import time
from typing import Dict, Any, List, Optional, Union, Set
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

# 添加项目根目录到路径，确保可以导入项目内的其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_fetcher.interface_processor import InterfaceProcessor, interface_manager
from storage.mongodb_client import mongodb_client
from config import config_manager
from wan_manager.port_allocator import port_allocator


class StockBasicFetcher:
    """
    股票基本信息数据获取器，专门用于获取stock_basic接口数据
    并按要求过滤和存储数据
    """

    def __init__(self, verbose: bool = False):
        """
        初始化股票基本信息获取器

        Args:
            verbose: 是否显示详细日志
        """
        self.verbose = verbose
        self.setup_logging()
        self.interface_name = 'stock_basic'
        self.processor = interface_manager.get_processor(self.interface_name)
        
        if not self.processor:
            logger.error(f"无法获取接口处理器: {self.interface_name}")
            raise ValueError(f"接口 {self.interface_name} 不可用")
            
        # 要保留的市场代码
        self.target_market_codes = {'00', '30', '60', '68'}
        
        logger.info("股票基本信息获取器初始化成功")
        if self.verbose:
            logger.debug(f"目标市场代码: {self.target_market_codes}")

    def setup_logging(self):
        """配置日志级别"""
        # 移除默认处理器
        logger.remove()
        
        # 根据verbose参数设置日志级别
        log_level = "DEBUG" if self.verbose else "INFO"
        
        # 添加控制台处理器
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        
        if self.verbose:
            logger.debug("详细日志模式已启用")
        else:
            logger.info("默认日志模式已启用")

    def filter_stock_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤股票数据，只保留指定市场代码的股票

        Args:
            df: 股票基本信息数据

        Returns:
            过滤后的数据
        """
        if df.empty:
            logger.warning("没有股票数据可过滤")
            return df

        if self.verbose:
            logger.debug(f"过滤前股票数量: {len(df)}")

        # 提取市场代码（股票代码的前两位）
        if 'symbol' in df.columns:
            df['market_code'] = df['symbol'].str[:2]
            # 过滤市场代码
            filtered_df = df[df['market_code'].isin(self.target_market_codes)]
            # 删除临时列
            filtered_df = filtered_df.drop(columns=['market_code'])
            
            if self.verbose:
                logger.debug(f"过滤后股票数量: {len(filtered_df)}")
                for code in sorted(self.target_market_codes):
                    count = len(df[df['symbol'].str[:2] == code])
                    logger.debug(f"市场 {code} 股票数量: {count}")
                
            return filtered_df
        else:
            logger.warning("数据中没有symbol列，无法过滤市场代码")
            return df

    def fetch_and_store(self, wan_index: int = None) -> Dict[str, int]:
        """
        获取并存储股票基本信息

        Args:
            wan_index: 指定WAN索引，用于多WAN负载均衡

        Returns:
            处理结果统计
        """
        start_time = time.time()
        logger.info(f"开始获取股票基本信息 (接口: {self.interface_name})")
        
        try:
            # 获取数据
            if self.verbose:
                logger.debug(f"从Tushare获取 {self.interface_name} 数据")
                
            df = self.processor.fetch_data(wan_index=wan_index)
            
            if df.empty:
                logger.warning("未获取到股票基本信息数据")
                return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
                
            # 过滤数据
            if self.verbose:
                logger.debug("过滤股票数据，只保留00、30、60、68市场代码的股票")
                
            filtered_df = self.filter_stock_data(df)
            
            if filtered_df.empty:
                logger.warning("过滤后没有符合条件的股票数据")
                return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
                
            # 存储到MongoDB
            if self.verbose:
                logger.debug(f"保存 {len(filtered_df)} 条股票数据到MongoDB集合: {self.interface_name}")
                
            result = self.processor.save_to_mongodb(filtered_df)
            
            elapsed = time.time() - start_time
            logger.info(f"股票基本信息获取并存储完成，耗时: {elapsed:.2f}秒")
            logger.info(f"处理结果: 总计 {result.get('total', 0)} 条记录, "
                      f"新增 {result.get('new', 0)}, "
                      f"更新 {result.get('updated', 0)}, "
                      f"未变 {result.get('unchanged', 0)}")
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"获取股票基本信息失败，耗时: {elapsed:.2f}秒，错误: {str(e)}")
            if self.verbose:
                import traceback
                logger.debug(f"错误详情: {traceback.format_exc()}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}


def main():
    """主函数，处理命令行参数并执行数据获取"""
    parser = argparse.ArgumentParser(description='获取股票基本信息数据并存储到MongoDB')
    parser.add_argument('--verbose', action='store_true', help='显示详细日志')
    parser.add_argument('--wan', type=int, help='指定使用的WAN口索引')
    args = parser.parse_args()
    
    # 初始化获取器
    fetcher = StockBasicFetcher(verbose=args.verbose)
    
    # 获取可用WAN索引
    wan_index = None
    if args.wan is not None and config_manager.is_wan_enabled():
        available_wan = port_allocator.get_available_wan_indices()
        if available_wan and args.wan in available_wan:
            wan_index = args.wan
            logger.info(f"使用WAN {wan_index} 获取数据")
        else:
            logger.warning(f"指定的WAN {args.wan} 不可用，将使用默认网络连接")
    
    # 执行获取和存储
    fetcher.fetch_and_store(wan_index)


if __name__ == '__main__':
    main()