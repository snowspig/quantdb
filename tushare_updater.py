#!/usr/bin/env python
"""
全量Tushare接口数据更新器，负责获取所有接口数据并存储到MongoDB
"""
import os
import sys
import argparse
import time
import random
import socket
import threading
import queue
from typing import Dict, Any, List, Optional, Union, Set, Tuple
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

# 添加项目根目录到路径，确保可以导入项目内的其他模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_fetcher.interface_processor import InterfaceProcessor, interface_manager
from data_fetcher.tushare_client import tushare_client
from storage.mongodb_client import mongodb_client
from config import config_manager
from wan_manager.port_allocator import port_allocator


class TushareDataUpdater:
    """
    Tushare数据更新器，用于获取所有接口数据并存储到MongoDB
    """
    
    def __init__(self, verbose: bool = False):
        """
        初始化数据更新器
        
        Args:
            verbose: 是否显示详细日志
        """
        self.verbose = verbose
        self.setup_logging()
        self.wan_enabled = config_manager.is_wan_enabled()
        
        # 获取可用WAN口
        self.available_wan_indices = []
        if self.wan_enabled:
            self.available_wan_indices = port_allocator.get_available_wan_indices()
            logger.info(f"多WAN支持已启用，可用WAN口: {self.available_wan_indices}")
        else:
            logger.info("多WAN支持未启用")
        
        # 线程数配置
        self.max_threads = config_manager.get("update", "threads", 4)
        self.batch_size = config_manager.get("update", "batch_size", 500)
        
        # 记录每个接口的处理结果
        self.results: Dict[str, Dict[str, int]] = {}
        
        logger.info(f"Tushare数据更新器初始化成功，最大线程数: {self.max_threads}")
        
    def setup_logging(self):
        """配置日志级别和格式"""
        # 移除默认处理器
        logger.remove()
        
        # 根据verbose参数设置日志级别
        log_level = "DEBUG" if self.verbose else "INFO"
        log_format = config_manager.get("logging", "format", "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        
        # 添加控制台处理器
        logger.add(sys.stderr, level=log_level, format=log_format)
        
        # 如果配置了日志文件，添加文件处理器
        log_file_enabled = config_manager.get("logging", "file", {}).get("enabled", False)
        if log_file_enabled:
            log_path = config_manager.get("logging", "file", {}).get("path", "logs/tushare_updater.log")
            log_rotation = config_manager.get("logging", "file", {}).get("rotation", "10 MB")
            log_retention = config_manager.get("logging", "file", {}).get("retention", "1 month")
            
            # 确保日志目录存在
            log_dir = os.path.dirname(log_path)
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
                
            logger.add(log_path, level=log_level, format=log_format, 
                       rotation=log_rotation, retention=log_retention)
            logger.info(f"日志文件已配置: {log_path}")
        
        if self.verbose:
            logger.debug("详细日志模式已启用")
        else:
            logger.info("默认日志模式已启用")

    def get_wan_index(self) -> Optional[int]:
        """
        获取一个可用的WAN索引
        
        Returns:
            WAN索引，如果没有可用WAN则返回None
        """
        if not self.wan_enabled or not self.available_wan_indices:
            return None
            
        # 随机选择一个WAN口
        return random.choice(self.available_wan_indices)

    def filter_stock_data_by_market(self, df: pd.DataFrame, target_markets: Set[str]) -> pd.DataFrame:
        """
        按市场代码过滤股票数据
        
        Args:
            df: 数据
            target_markets: 目标市场代码集合
            
        Returns:
            过滤后的数据
        """
        if df.empty or 'symbol' not in df.columns:
            return df
            
        # 提取市场代码（股票代码的前两位）
        df['market_code'] = df['symbol'].str[:2]
        
        # 过滤市场代码
        filtered_df = df[df['market_code'].isin(target_markets)]
        
        # 删除临时列
        filtered_df = filtered_df.drop(columns=['market_code'])
        
        if self.verbose:
            logger.debug(f"过滤前股票数量: {len(df)}, 过滤后: {len(filtered_df)}")
            for code in sorted(target_markets):
                count = len(df[df['symbol'].str[:2] == code])
                logger.debug(f"市场 {code} 股票数量: {count}")
                
        return filtered_df

    def update_stock_basic(self, target_markets: Set[str] = None, wan_index: int = None) -> Dict[str, int]:
        """
        更新股票基本信息，可选择性过滤特定市场
        
        Args:
            target_markets: 目标市场代码集合，如果为None则不过滤
            wan_index: 指定WAN索引，用于多WAN负载均衡
            
        Returns:
            处理结果统计
        """
        interface_name = 'stock_basic'
        processor = interface_manager.get_processor(interface_name)
        
        if not processor:
            logger.error(f"无法获取接口处理器: {interface_name}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
            
        start_time = time.time()
        logger.info(f"开始更新股票基本信息 (接口: {interface_name})")
        
        try:
            # 获取数据
            df = processor.fetch_data(wan_index=wan_index)
            
            if df.empty:
                logger.warning("未获取到股票基本信息数据")
                return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
                
            # 如果指定了目标市场，进行过滤
            if target_markets:
                if self.verbose:
                    logger.debug(f"过滤股票数据，只保留 {target_markets} 市场代码的股票")
                df = self.filter_stock_data_by_market(df, target_markets)
                
            if df.empty:
                logger.warning("过滤后没有符合条件的股票数据")
                return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
                
            # 存储到MongoDB
            if self.verbose:
                logger.debug(f"保存 {len(df)} 条股票数据到MongoDB集合: {interface_name}")
                
            result = processor.save_to_mongodb(df)
            
            elapsed = time.time() - start_time
            logger.info(f"股票基本信息更新完成，耗时: {elapsed:.2f}秒")
            logger.info(f"处理结果: 总计 {result.get('total', 0)} 条记录, "
                      f"新增 {result.get('new', 0)}, "
                      f"更新 {result.get('updated', 0)}, "
                      f"未变 {result.get('unchanged', 0)}")
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"更新股票基本信息失败，耗时: {elapsed:.2f}秒，错误: {str(e)}")
            if self.verbose:
                import traceback
                logger.debug(f"错误详情: {traceback.format_exc()}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}

    def update_interface(self, interface_name: str, start_date: str = None, end_date: str = None, 
                         ts_codes: List[str] = None, wan_index: int = None) -> Dict[str, int]:
        """
        更新单个接口数据
        
        Args:
            interface_name: 接口名称
            start_date: 开始日期
            end_date: 结束日期
            ts_codes: 股票代码列表
            wan_index: 指定WAN索引
            
        Returns:
            处理结果统计
        """
        processor = interface_manager.get_processor(interface_name)
        if not processor:
            logger.error(f"无法获取接口处理器: {interface_name}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
        
        start_time = time.time()
        logger.info(f"开始更新 {interface_name} 数据")
        
        try:
            result = processor.update_data(start_date, end_date, ts_codes, wan_index)
            
            elapsed = time.time() - start_time
            logger.info(f"{interface_name} 数据更新完成，耗时: {elapsed:.2f}秒")
            logger.info(f"处理结果: 总计 {result.get('total', 0)} 条记录, "
                      f"新增 {result.get('new', 0)}, "
                      f"更新 {result.get('updated', 0)}, "
                      f"未变 {result.get('unchanged', 0)}")
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"更新 {interface_name} 数据失败，耗时: {elapsed:.2f}秒，错误: {str(e)}")
            if self.verbose:
                import traceback
                logger.debug(f"错误详情: {traceback.format_exc()}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}

    def worker(self, work_queue: queue.Queue, results: Dict[str, Dict[str, int]]):
        """
        工作线程函数，处理队列中的任务
        
        Args:
            work_queue: 工作队列
            results: 结果字典，用于存储处理结果
        """
        while True:
            try:
                # 获取任务
                task = work_queue.get(block=False)
                
                if task is None:
                    # None表示终止信号
                    work_queue.task_done()
                    break
                
                interface_name, start_date, end_date, ts_codes = task
                
                # 获取WAN口
                wan_index = self.get_wan_index()
                
                # 更新数据
                result = self.update_interface(interface_name, start_date, end_date, ts_codes, wan_index)
                
                # 保存结果
                results[interface_name] = result
                
            except queue.Empty:
                # 队列为空，退出
                break
            except Exception as e:
                logger.error(f"工作线程处理任务时出错: {str(e)}")
                if self.verbose:
                    import traceback
                    logger.debug(f"线程错误详情: {traceback.format_exc()}")
            finally:
                work_queue.task_done()

    def update_all_interfaces(self, start_date: str = None, end_date: str = None, 
                             specific_interfaces: List[str] = None, 
                             skip_interfaces: List[str] = None) -> Dict[str, Dict[str, int]]:
        """
        更新所有接口数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            specific_interfaces: 指定要更新的接口列表，如果为None则更新所有接口
            skip_interfaces: 需要跳过的接口列表
            
        Returns:
            处理结果统计，按接口名组织
        """
        # 初始化结果字典
        self.results = {}
        
        # 获取股票基本信息，这是后续获取其他数据的基础
        basic_result = self.update_stock_basic(target_markets={'00', '30', '60', '68'})
        self.results['stock_basic'] = basic_result
        
        # 如果股票基本信息获取失败，其他接口可能也无法正常工作
        if basic_result.get('total', 0) == 0:
            logger.warning("股票基本信息获取失败，其他接口数据可能无法正确获取")
            
        # 获取所有接口，按优先级排序
        interfaces = interface_manager.get_interfaces_by_priority()
        
        # 过滤接口
        filtered_interfaces = []
        for name, priority in interfaces:
            if name == 'stock_basic':
                # 股票基本信息已处理
                continue
                
            if specific_interfaces and name not in specific_interfaces:
                # 如果指定了接口列表但当前接口不在其中，跳过
                continue
                
            if skip_interfaces and name in skip_interfaces:
                # 如果当前接口在跳过列表中，跳过
                continue
                
            filtered_interfaces.append((name, priority))
        
        total_interfaces = len(filtered_interfaces)
        logger.info(f"共有 {total_interfaces} 个接口需要更新")
        
        if total_interfaces == 0:
            return self.results
        
        # 根据优先级排序
        filtered_interfaces.sort(key=lambda x: x[1])
        
        # 创建工作队列
        work_queue = queue.Queue()
        
        # 填充队列
        for name, _ in filtered_interfaces:
            work_queue.put((name, start_date, end_date, None))
        
        # 添加终止信号
        for _ in range(self.max_threads):
            work_queue.put(None)
        
        # 创建工作线程
        threads = []
        for i in range(min(self.max_threads, total_interfaces)):
            thread = threading.Thread(target=self.worker, args=(work_queue, self.results))
            thread.start()
            threads.append(thread)
            
        # 等待所有任务完成
        work_queue.join()
        
        # 确保所有线程已终止
        for thread in threads:
            thread.join()
            
        # 计算总结果
        total_result = {
            "total": sum(result.get("total", 0) for result in self.results.values()),
            "new": sum(result.get("new", 0) for result in self.results.values()),
            "updated": sum(result.get("updated", 0) for result in self.results.values()),
            "unchanged": sum(result.get("unchanged", 0) for result in self.results.values())
        }
        
        # 输出统计信息
        logger.info("所有接口数据更新完成")
        logger.info(f"总计: {total_result['total']} 条记录, "
                  f"新增: {total_result['new']}, "
                  f"更新: {total_result['updated']}, "
                  f"未变: {total_result['unchanged']}")
        
        # 按新增记录数量排序接口，输出详情
        if self.verbose:
            sorted_results = sorted(self.results.items(), 
                                   key=lambda x: x[1].get('new', 0) + x[1].get('updated', 0), 
                                   reverse=True)
            
            logger.debug("各接口处理结果:")
            for name, result in sorted_results:
                logger.debug(f"{name}: 总计 {result.get('total', 0)} 条记录, "
                           f"新增 {result.get('new', 0)}, "
                           f"更新 {result.get('updated', 0)}, "
                           f"未变 {result.get('unchanged', 0)}")
                
        return self.results


def main():
    """主函数，处理命令行参数并执行数据更新"""
    parser = argparse.ArgumentParser(description='Tushare数据更新工具')
    parser.add_argument('--verbose', action='store_true', help='显示详细日志')
    parser.add_argument('--start-date', help='起始日期，格式YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式YYYYMMDD')
    parser.add_argument('--interface', action='append', help='指定要更新的接口，可多次使用')
    parser.add_argument('--skip', action='append', help='指定要跳过的接口，可多次使用')
    parser.add_argument('--stock-basic-only', action='store_true', help='仅更新股票基本信息')
    parser.add_argument('--market-filter', action='store_true', 
                        help='过滤股票基本信息，只保留00、30、60、68板块的股票')
    
    args = parser.parse_args()
    
    # 初始化更新器
    updater = TushareDataUpdater(verbose=args.verbose)
    
    # 执行更新
    if args.stock_basic_only:
        # 只更新股票基本信息
        if args.market_filter:
            # 只保留特定市场代码的股票
            updater.update_stock_basic(target_markets={'00', '30', '60', '68'})
        else:
            # 获取所有股票
            updater.update_stock_basic()
    else:
        # 更新所有或指定接口
        updater.update_all_interfaces(
            start_date=args.start_date,
            end_date=args.end_date,
            specific_interfaces=args.interface,
            skip_interfaces=args.skip
        )


if __name__ == '__main__':
    main()