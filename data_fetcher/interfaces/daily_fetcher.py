#!/usr/bin/env python
"""
日线行情数据获取器V3 - 获取日线行情数据并保存到MongoDB

该脚本用于从湘财Tushare获取日线行情数据，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，通过分时间段获取和多WAN接口并行处理，解决大量数据获取问题
修复了get_stock_basic和get_trade_calendar方法中的MongoDB访问问题

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=27

使用方法：
    python daily_fetcher.py              # 默认使用recent模式获取最近一周的数据更新
    python daily_fetcher.py --full        # 获取完整历史数据而非默认的最近一周数据
    python daily_fetcher.py --verbose     # 使用详细日志模式
    python daily_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python daily_fetcher.py --ts-code 000001.SZ  # 获取特定股票的数据
"""

import sys
import time
import pandas as pd
import pymongo
from datetime import datetime, timedelta
from typing import List, Set, Optional
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core.tushare_fetcher import TushareFetcher
from core import mongodb_handler

class DailyFetcherV3(TushareFetcher):
    """
    日线行情数据获取器V3
    
    该类用于从Tushare获取日线行情数据并保存到MongoDB数据库
    优化点：
    1. 支持按时间段分批获取数据，避免一次获取超过10000条数据限制
    2. 多WAN接口并行获取，提高数据获取效率
    3. 增加数据获取重试机制，提高稳定性
    4. 支持recent模式、full模式以及指定日期范围模式
    5. 修复了MongoDB访问方式，使用全局mongodb_handler处理
    """

    def __init__(
        self,
        target_market_codes: Set[str] = {"00", "30", "60", "68"},  # 默认只保存00 30 60 68四个板块的股票数据
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "daily.json",
        db_name: str = None,
        collection_name: str = "daily",
        verbose: bool = False,
        max_workers: int = 3,  # 并行工作线程数
        retry_count: int = 3,  # 数据获取重试次数
        retry_delay: int = 5   # 重试延迟时间(秒)
    ):
        """
        初始化日线行情数据获取器
        
        Args:
            target_market_codes: 目标市场代码集合，只保存这些板块的股票数据
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
            max_workers: 并行工作线程数
            retry_count: 数据获取重试次数
            retry_delay: 重试延迟时间(秒)
        """
        # 调用父类初始化方法
        super().__init__(
            config_path=config_path,
            interface_dir=interface_dir,
            interface_name=interface_name,
            db_name=db_name,
            collection_name=collection_name,
            verbose=verbose
        )
        
        self.target_market_codes = target_market_codes
        self.max_workers = max_workers
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        
        # 运行时统计信息
        self.stats = {
            "total_records": 0,
            "success_count": 0,
            "failure_count": 0,
            "skipped_count": 0
        }
        
        # 记录最后一次操作的统计信息
        self.last_operation_stats = None
        
        # 日志输出
        logger.info(f"日线数据获取器初始化完成，目标市场代码: {', '.join(self.target_market_codes)}，最大并行数: {self.max_workers}")
        
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从Tushare获取日线行情数据
        
        支持以下调用方式：
        1. trade_date参数：获取特定交易日的全部股票日线数据
        2. ts_code参数：获取特定股票的日线数据，可选传入start_date和end_date
        3. start_date和end_date参数：获取特定日期范围内的数据（需配合ts_code使用）
        
        Args:
            **kwargs: 查询参数
                - trade_date: 交易日期
                - ts_code: 股票代码
                - start_date: 开始日期
                - end_date: 结束日期
                - offset: 用于分页查询的偏移量
                - limit: 每页记录数
                
        Returns:
            pd.DataFrame: 日线行情数据，如果失败则返回None
        """
        try:
            # 解析参数
            trade_date = kwargs.get('trade_date')
            ts_code = kwargs.get('ts_code')
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')
            offset = kwargs.get('offset', 0)
            limit = kwargs.get('limit', 10000)  # 默认每次获取10000条记录
            
            # 准备API参数
            api_name = "daily"
            params = {}
            
            # 根据参数设置API调用方式
            if trade_date:
                params["trade_date"] = trade_date
                logger.info(f"获取交易日 {trade_date} 的日线数据")
            elif ts_code:
                params["ts_code"] = ts_code
                if start_date and end_date:
                    params["start_date"] = start_date
                    params["end_date"] = end_date
                    logger.info(f"获取股票 {ts_code} 从 {start_date} 至 {end_date} 的日线数据")
                else:
                    logger.info(f"获取股票 {ts_code} 的日线数据")
            else:
                logger.error("缺少必要参数，至少需要提供trade_date或ts_code")
                return None
                
            # 添加分页参数，处理大数据量问题
            params["offset"] = offset
            params["limit"] = limit
            
            # 使用接口配置中的available_fields作为请求字段
            fields = self.available_fields
            if not fields:
                logger.warning("接口配置中未定义available_fields，将获取所有字段")
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket()
            use_wan = wan_info is not None
            
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            start_time = time.time()
            
            # 增加超时，设置为120秒
            self.client.set_timeout(120)
            logger.debug(f"增加API请求超时时间为120秒，提高网络可靠性")
            
            # 添加重试机制
            retry = 0
            df = None
            last_error = None
            
            while retry <= self.retry_count and df is None:
                try:
                    # 如果使用WAN接口，设置本地地址绑定
                    if use_wan:
                        wan_idx, port = wan_info
                        self.client.set_local_address('0.0.0.0', port)
                    
                    df = self.client.get_data(api_name=api_name, params=params, fields=fields)
                    
                    # 重置客户端设置
                    if use_wan:
                        self.client.reset_local_address()
                        
                    # 如果获取到数据，跳出重试循环
                    if df is not None and not df.empty:
                        break
                    else:
                        # 数据为空，可能需要重试
                        if retry < self.retry_count:
                            retry += 1
                            logger.warning(f"获取数据为空，第{retry}次重试...")
                            time.sleep(self.retry_delay)
                        else:
                            logger.warning(f"已达到最大重试次数({self.retry_count})，获取数据为空")
                            break
                        
                except Exception as e:
                    last_error = str(e)
                    if retry < self.retry_count:
                        retry += 1
                        logger.warning(f"获取数据出错: {str(e)}，第{retry}次重试...")
                        time.sleep(self.retry_delay)
                    else:
                        logger.error(f"获取数据失败，已达到最大重试次数: {str(e)}")
                        import traceback
                        logger.debug(f"异常详情: {traceback.format_exc()}")
                        break
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                self.port_allocator.release_port(wan_idx, port)
            
            # 数据验证和处理
            if df is not None and not df.empty:
                elapsed = time.time() - start_time
                logger.success(f"成功获取 {len(df)} 条日线数据，耗时 {elapsed:.2f}s")
                
                # 检查返回的数据包含的字段
                if self.verbose:
                    logger.debug(f"API返回的字段: {list(df.columns)}")
                    logger.debug(f"数据示例：\n{df.head(3)}")
                
                return df
            else:
                if last_error:
                    logger.error(f"获取数据失败: {last_error}")
                else:
                    logger.error("获取数据失败，数据为空")
                return None
                
        except Exception as e:
            logger.error(f"获取日线数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 如果有WAN信息，确保释放端口
            try:
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
            except Exception as release_error:
                logger.warning(f"释放WAN端口时出错: {str(release_error)}")
                
            return None
            
    def process_data(self, df: pd.DataFrame, skip_market_filter: bool = False) -> pd.DataFrame:
        """
        处理获取的日线数据
        
        主要功能：
        1. 过滤出目标市场的股票数据（除非设置skip_market_filter为True）
        
        Args:
            df: 原始日线数据
            skip_market_filter: 是否跳过市场代码过滤，当从stock_basic已经过滤过时设为True
            
        Returns:
            处理后的数据
        """
        if df is None or df.empty:
            logger.warning("没有数据可处理")
            return pd.DataFrame()
            
        # 只在需要时进行市场代码过滤
        if not skip_market_filter and 'ts_code' in df.columns and self.target_market_codes:
            before_filter = len(df)
            # 提取ts_code的前两位作为市场代码
            df['market_code'] = df['ts_code'].str[:2]
            df_filtered = df[df['market_code'].isin(self.target_market_codes)].copy()
            # 删除临时列
            if 'market_code' in df_filtered.columns:
                df_filtered.drop('market_code', axis=1, inplace=True)
                
            after_filter = len(df_filtered)
            logger.info(f"市场代码过滤: {before_filter} -> {after_filter} 条记录")
            
            # 详细日志
            if self.verbose and not df_filtered.empty:
                # 统计市场分布
                market_stats = {}
                for code in self.target_market_codes:
                    count = len(df[df['ts_code'].str[:2] == code])
                    market_stats[code] = count
                    
                logger.debug("各市场数据统计:")
                for code, count in market_stats.items():
                    logger.debug(f"  {code}: {count} 条记录")
            
            return df_filtered
        else:
            if skip_market_filter:
                logger.debug("跳过市场代码过滤，已在获取股票列表时过滤")
            return df
    
    def get_stock_basic(self) -> Set[str]:
        """
        从MongoDB获取股票基本信息中的目标板块股票代码
        
        Returns:
            目标板块股票代码集合
        """
        try:
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return set()
                    
            # 查询stock_basic集合中符合条件的股票代码
            logger.info(f"从stock_basic集合查询目标板块 {self.target_market_codes} 的股票代码")
            
            # 构建查询条件
            # 方法1：使用ts_code字段前两位进行过滤（更准确）
            query_conditions = []
            for market_code in self.target_market_codes:
                # 直接匹配ts_code前两位
                query_conditions.append({"ts_code": {"$regex": f"^.{{2}}\\.{market_code}$"}})
                
            # 方法2：使用symbol字段匹配（如果symbol格式是市场代码开头）
            for market_code in self.target_market_codes:
                query_conditions.append({"symbol": {"$regex": f"^{market_code}"}})
                
            # 使用$or操作符组合多个条件
            query = {"$or": query_conditions} if query_conditions else {}
            
            # 查询股票代码
            result = mongodb_handler.find_documents("stock_basic", query)
            
            # 提取ts_code集合
            ts_codes = set()
            for doc in result:
                if "ts_code" in doc:
                    # 再次确认ts_code符合目标市场代码
                    for market_code in self.target_market_codes:
                        # 提取股票代码后缀（SH、SZ等）
                        code_suffix = doc["ts_code"].split(".")[-1] if "." in doc["ts_code"] else ""
                        # 转换为标准两位数代码
                        std_market_code = self._convert_to_standard_market_code(code_suffix)
                        if std_market_code in self.target_market_codes:
                            ts_codes.add(doc["ts_code"])
                            break
            
            logger.success(f"从stock_basic集合获取到 {len(ts_codes)} 个目标股票代码")
            
            # 如果没有获取到数据，提供警告
            if not ts_codes:
                logger.warning("未获取到任何目标板块的股票代码，请确保stock_basic集合中有数据")
            
            return ts_codes
                
        except Exception as e:
            logger.error(f"查询stock_basic集合失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return set()
    
    def _convert_to_standard_market_code(self, code_suffix: str) -> str:
        """
        将股票代码后缀转换为标准的市场代码
        
        Args:
            code_suffix: 股票代码后缀（如SH、SZ）
            
        Returns:
            标准的市场代码（如60、00）
        """
        # 常见的市场代码映射
        market_code_map = {
            "SH": "60",  # 上交所主板
            "SZ": "00",  # 深交所主板
            "BJ": "80",  # 北交所
            "": ""       # 空后缀
        }
        
        # 特殊映射规则
        if code_suffix == "SH" and self.target_market_codes and "68" in self.target_market_codes:
            return "68"  # 如果目标包含科创板，则SH可能对应科创板
        elif code_suffix == "SZ" and self.target_market_codes and "30" in self.target_market_codes:
            return "30"  # 如果目标包含创业板，则SZ可能对应创业板
            
        return market_code_map.get(code_suffix, "")
    
    def get_trade_calendar(self, start_date: str, end_date: str) -> List[str]:
        """
        从MongoDB获取交易日历
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表
        """
        try:
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return []
            
            logger.info(f"获取日期范围 {start_date} 至 {end_date} 的交易日")
            
            # 构建查询条件
            query = {
                "trade_date": {"$gte": start_date, "$lte": end_date}
            }
            
            # 查询结果 - 正确传递参数
            result = mongodb_handler.find_documents("trade_cal", query)
            
            # 提取日期列表
            trade_dates = []
            for doc in result:
                if "trade_date" in doc:
                    trade_dates.append(doc["trade_date"])
            
            trade_dates.sort()  # 确保日期有序
            
            if trade_dates:
                logger.info(f"从MongoDB获取到 {len(trade_dates)} 个交易日")
                return trade_dates
            else:
                logger.warning(f"MongoDB中未找到交易日数据，尝试通过TradeCalFetcher获取")
                
                # 尝试调用trade_cal_fetcher获取交易日历
                try:
                    from data_fetcher.interfaces.trade_cal_fetcher import TradeCalFetcher
                    trade_cal_fetcher = TradeCalFetcher(
                        start_date=start_date,
                        end_date=end_date,
                        verbose=self.verbose
                    )
                    
                    # 获取交易日并保存到MongoDB
                    df = trade_cal_fetcher.fetch_data()
                    if df is not None and not df.empty:                     
                        # 保存到MongoDB
                        trade_cal_fetcher.save_to_mongodb(df)
                        
                        # 提取交易日列表
                        if "trade_date" in df.columns:
                            trade_dates = df["trade_date"].tolist()
                            trade_dates.sort()
                            logger.success(f"通过TradeCalFetcher获取到 {len(trade_dates)} 个交易日")
                            return trade_dates
                except Exception as e:
                    logger.error(f"通过TradeCalFetcher获取交易日历失败: {str(e)}")
                    import traceback
                    logger.debug(f"异常详情: {traceback.format_exc()}")
                
                # 如果API获取失败，生成日期范围内的所有日期作为备选
                logger.warning(f"未能通过任何方式获取交易日，将使用日期范围内的所有日期作为备选")
                
                # 生成日期范围内的所有日期作为备选
                start_date_obj = datetime.strptime(start_date, '%Y%m%d')
                end_date_obj = datetime.strptime(end_date, '%Y%m%d')
                
                current_date = start_date_obj
                all_dates = []
                while current_date <= end_date_obj:
                    all_dates.append(current_date.strftime('%Y%m%d'))
                    current_date += timedelta(days=1)
                
                logger.info(f"使用日期范围内的所有日期作为备选，共 {len(all_dates)} 个日期")
                return all_dates
                
        except Exception as e:
            logger.error(f"获取交易日历失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 出错时生成日期范围内的所有日期作为备选
            start_date_obj = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            
            trade_dates = []
            current_date = start_date_obj
            while current_date <= end_date_obj:
                trade_dates.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)
            
            logger.info(f"使用日期范围内的所有日期作为备选，共 {len(trade_dates)} 个日期")
            return trade_dates
    
    def fetch_by_date_range(self, start_date: str, end_date: str, use_parallel: bool = True) -> bool:
        """
        获取指定日期范围内的所有交易日数据
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            use_parallel: 是否使用并行处理
            
        Returns:
            是否成功
        """
        # 获取日期范围内的交易日
        trade_dates = self.get_trade_calendar(start_date, end_date)
        if not trade_dates:
            logger.error(f"日期范围 {start_date} 至 {end_date} 内无交易日")
            return False
            
        logger.info(f"获取到 {len(trade_dates)} 个交易日，开始抓取日线数据")
        
        # 确保集合和索引
        self._ensure_collection_and_indexes()
        
        # 根据是否并行处理选择不同的处理方式
        if use_parallel and self.port_allocator and len(self.port_allocator.get_available_wan_indices()) > 0:
            logger.info("使用并行模式处理多个交易日")
            return self._fetch_dates_parallel(trade_dates)
        else:
            logger.info("使用串行模式处理多个交易日")
            return self._fetch_dates_sequential(trade_dates)
    
    def _fetch_dates_parallel(self, trade_dates: List[str]) -> bool:
        """
        并行获取多个交易日的数据
        
        Args:
            trade_dates: 交易日期列表
            
        Returns:
            是否成功
        """
        import threading
        import queue
        
        total_days = len(trade_dates)
        # 设置最大并发数
        available_wans = self.port_allocator.get_available_wan_indices()
        max_workers = min(len(available_wans), total_days, self.max_workers)
        
        result_queue = queue.Queue()
        threads = []
        
        # 线程锁用于日志和进度更新
        log_lock = threading.Lock()
        
        processed_days = 0
        success_days = 0
        total_records = 0
        
        # 线程函数
        def process_date(date_str, thread_idx):
            try:
                with log_lock:
                    logger.debug(f"线程 {thread_idx} 开始处理日期 {date_str}")
                
                # 获取WAN端口
                wan_idx = available_wans[thread_idx % len(available_wans)]
                wan_info = self._get_wan_socket(wan_idx)
                
                success = False
                records_count = 0
                
                try:
                    # 获取当日数据
                    df = self.fetch_data(trade_date=date_str)
                    
                    if df is not None and not df.empty:
                        # 处理数据
                        df_processed = self.process_data(df)
                        
                        # 保存到MongoDB
                        if df_processed is not None and not df_processed.empty:
                            self.save_to_mongodb(df_processed)
                            records_count = len(df_processed)
                            success = True
                finally:
                    # 释放WAN端口
                    if wan_info:
                        self.port_allocator.release_port(wan_info[0], wan_info[1])
                
                # 放入结果队列
                result_queue.put((date_str, success, records_count))
                
                with log_lock:
                    if success:
                        logger.success(f"线程 {thread_idx}(WAN{wan_idx}) 成功处理日期 {date_str}，共 {records_count} 条记录")
                    else:
                        logger.warning(f"线程 {thread_idx} 处理日期 {date_str} 失败或无数据")
                        
            except Exception as e:
                with log_lock:
                    logger.error(f"线程 {thread_idx} 处理日期 {date_str} 出错: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    try:
                        self.port_allocator.release_port(wan_info[0], wan_info[1])
                    except:
                        pass
                        
                result_queue.put((date_str, False, 0))
        
        # 启动线程
        start_time = time.time()
        
        # 创建并启动所有线程
        for i, date_str in enumerate(trade_dates):
            thread = threading.Thread(
                target=process_date,
                args=(date_str, i % max_workers)
            )
            thread.start()
            threads.append(thread)
            
            # 控制同时运行的线程数
            if len(threads) >= max_workers:
                # 等待一个线程完成
                while result_queue.empty():
                    time.sleep(0.1)
                
                # 处理结果
                date_str, success, records_count = result_queue.get()
                processed_days += 1
                if success:
                    success_days += 1
                    total_records += records_count
                
                # 更新进度
                progress = processed_days / total_days * 100
                elapsed = time.time() - start_time
                remaining = elapsed / processed_days * (total_days - processed_days) if processed_days > 0 else 0
                logger.info(f"进度: {processed_days}/{total_days} ({progress:.1f}%)，"
                           f"成功: {success_days}，"
                           f"已耗时: {elapsed:.1f}s，"
                           f"预估剩余: {remaining:.1f}s")
                
                # 清理已完成的线程
                threads = [t for t in threads if t.is_alive()]
                
                # 控制启动新线程的间隔
                time.sleep(0.5)
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 处理剩余结果
        while not result_queue.empty():
            date_str, success, records_count = result_queue.get()
            processed_days += 1
            if success:
                success_days += 1
                total_records += records_count
        
        # 记录最终结果
        elapsed_total = time.time() - start_time
        logger.success(f"并行处理完成，成功处理 {success_days}/{total_days} 个交易日，"
                       f"共获取 {total_records} 条记录，"
                       f"总耗时: {elapsed_total:.1f}s")
        
        return success_days > 0
    
    def _fetch_dates_sequential(self, trade_dates: List[str]) -> bool:
        """
        串行获取多个交易日的数据
        
        Args:
            trade_dates: 交易日期列表
            
        Returns:
            是否成功
        """
        total_days = len(trade_dates)
        processed_days = 0
        success_days = 0
        total_records = 0
        
        start_time = time.time()
        
        for date_str in trade_dates:
            logger.info(f"处理交易日 {date_str}")
            
            # 获取数据
            df = self.fetch_data(trade_date=date_str)
            
            if df is not None and not df.empty:
                # 处理数据
                df_processed = self.process_data(df)
                
                # 保存到MongoDB
                if df_processed is not None and not df_processed.empty:
                    success = self.save_to_mongodb(df_processed)
                    if success:
                        success_days += 1
                        total_records += len(df_processed)
                        logger.success(f"成功处理交易日 {date_str}，共 {len(df_processed)} 条记录")
                    else:
                        logger.warning(f"保存交易日 {date_str} 数据失败")
                else:
                    logger.warning(f"交易日 {date_str} 处理后无有效数据")
            else:
                logger.warning(f"交易日 {date_str} 无数据")
            
            # 更新进度
            processed_days += 1
            progress = processed_days / total_days * 100
            elapsed = time.time() - start_time
            remaining = elapsed / processed_days * (total_days - processed_days) if processed_days > 0 else 0
            logger.info(f"进度: {processed_days}/{total_days} ({progress:.1f}%)，"
                       f"成功: {success_days}，"
                       f"已耗时: {elapsed:.1f}s，"
                       f"预估剩余: {remaining:.1f}s")
            
            # 间隔一段时间再处理下一个日期
            time.sleep(0.5)
        
        # 记录最终结果
        elapsed_total = time.time() - start_time
        logger.success(f"串行处理完成，成功处理 {success_days}/{total_days} 个交易日，"
                       f"共获取 {total_records} 条记录，"
                       f"总耗时: {elapsed_total:.1f}s")
        
        return success_days > 0
    
    def fetch_by_stock(self, ts_code: str, start_date: str = None, end_date: str = None) -> bool:
        """
        获取指定股票在日期范围内的数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            是否成功
        """
        # 设置默认日期范围
        if not start_date or not end_date:
            today = datetime.now()
            if not end_date:
                end_date = today.strftime('%Y%m%d')
            if not start_date:
                # 默认获取最近一年的数据
                start_date = (today - timedelta(days=365)).strftime('%Y%m%d')
        
        logger.info(f"获取股票 {ts_code} 在 {start_date} 至 {end_date} 期间的日线数据")
        
        # 确保集合和索引
        self._ensure_collection_and_indexes()
        
        # 获取数据
        df = self.fetch_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is not None and not df.empty:
            # 处理数据
            df_processed = self.process_data(df)
            
            # 保存到MongoDB
            if df_processed is not None and not df_processed.empty:
                success = self.save_to_mongodb(df_processed)
                if success:
                    logger.success(f"成功获取并保存股票 {ts_code} 的日线数据，共 {len(df_processed)} 条记录")
                    return True
                else:
                    logger.error(f"保存股票 {ts_code} 的日线数据失败")
            else:
                logger.warning(f"股票 {ts_code} 处理后无有效数据")
        else:
            logger.error(f"获取股票 {ts_code} 的日线数据失败")
        
        return False
    
    def fetch_by_recent(self, days: int = 7) -> bool:
        """
        获取最近几天的数据
        
        Args:
            days: 最近的天数
            
        Returns:
            是否成功
        """
        today = datetime.now()
        end_date = today.strftime('%Y%m%d')
        start_date = (today - timedelta(days=days)).strftime('%Y%m%d')
        
        logger.info(f"获取最近 {days} 天的日线数据（{start_date} 至 {end_date}）")
        
        return self.fetch_by_date_range(start_date, end_date)
    
    def fetch_full_history(self) -> bool:
        """
        获取完整历史数据
        
        采用按股票代码逐个抓取的方式，而非按日期抓取
        优点是可以并行处理，充分利用多WAN接口
        
        Returns:
            是否成功
        """
        start_date = "19900101"  # 从1990年开始
        end_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"获取完整历史日线数据（{start_date} 至 {end_date}）")
        
        # 第一步：检查并确保MongoDB集合和索引存在
        logger.info("第一步: 检查并确保MongoDB集合和索引存在")
        if not self._ensure_collection_and_indexes():
            logger.error("MongoDB集合和索引检查失败，退出程序")
            return False
        
        # 第二步：获取所有股票的列表
        logger.info("第二步: 获取所有股票的代码列表")
        stock_codes = self.get_stock_basic()
        if not stock_codes:
            logger.error("无法获取股票代码列表，请确保stock_basic集合中有数据")
            return False
        
        logger.info(f"获取到 {len(stock_codes)} 个股票代码，开始抓取历史日线数据")
        
        # 第三步：逐个股票抓取历史数据
        logger.info("第三步: 按股票代码逐个抓取历史数据")
        return self._fetch_stocks_parallel(list(stock_codes), start_date, end_date)
    
    def _fetch_stocks_parallel(self, stock_codes: List[str], start_date: str, end_date: str) -> bool:
        """
        并行获取多个股票的历史数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            是否成功
        """
        import threading
        import queue
        
        total_stocks = len(stock_codes)
        # 设置最大并发数
        available_wans = self.port_allocator.get_available_wan_indices()
        max_workers = min(len(available_wans), total_stocks, self.max_workers)
        
        result_queue = queue.Queue()
        threads = []
        
        # 线程锁用于日志和进度更新
        log_lock = threading.Lock()
        
        processed_stocks = 0
        success_stocks = 0
        total_records = 0
        
        # 线程函数
        def process_stock(ts_code, thread_idx):
            try:
                with log_lock:
                    logger.debug(f"线程 {thread_idx}(WAN{thread_idx % len(available_wans)}) 开始处理股票 {ts_code}")
                
                # 获取WAN端口
                wan_idx = available_wans[thread_idx % len(available_wans)]
                wan_info = self._get_wan_socket(wan_idx)
                
                success = False
                records_count = 0
                
                try:
                    # 获取股票历史数据
                    df = self.fetch_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
                    
                    if df is not None and not df.empty:
                        # 处理数据 - 跳过市场代码过滤，因为在获取股票列表时已经过滤过
                        df_processed = self.process_data(df, skip_market_filter=True)
                        
                        # 保存到MongoDB
                        if df_processed is not None and not df_processed.empty:
                            self.save_to_mongodb(df_processed)
                            records_count = len(df_processed)
                            success = True
                finally:
                    # 释放WAN端口
                    if wan_info:
                        self.port_allocator.release_port(wan_info[0], wan_info[1])
                
                # 放入结果队列
                result_queue.put((ts_code, success, records_count))
                
                with log_lock:
                    if success:
                        logger.success(f"线程 {thread_idx}(WAN{wan_idx}) 成功处理股票 {ts_code}，共 {records_count} 条记录")
                    else:
                        logger.warning(f"线程 {thread_idx}(WAN{wan_idx}) 处理股票 {ts_code} 失败或无数据")
                        
            except Exception as e:
                with log_lock:
                    logger.error(f"线程 {thread_idx} 处理股票 {ts_code} 出错: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    try:
                        self.port_allocator.release_port(wan_info[0], wan_info[1])
                    except:
                        pass
                        
                result_queue.put((ts_code, False, 0))
        
        # 启动线程
        start_time = time.time()
        
        # 创建并启动所有线程
        for i, ts_code in enumerate(stock_codes):
            thread = threading.Thread(
                target=process_stock,
                args=(ts_code, i % max_workers)
            )
            thread.start()
            threads.append(thread)
            
            # 控制同时运行的线程数
            if len(threads) >= max_workers:
                # 等待一个线程完成
                while result_queue.empty():
                    time.sleep(0.1)
                
                # 处理结果
                ts_code, success, records_count = result_queue.get()
                processed_stocks += 1
                if success:
                    success_stocks += 1
                    total_records += records_count
                
                # 更新进度
                progress = processed_stocks / total_stocks * 100
                elapsed = time.time() - start_time
                remaining = elapsed / processed_stocks * (total_stocks - processed_stocks) if processed_stocks > 0 else 0
                logger.info(f"进度: {processed_stocks}/{total_stocks} ({progress:.1f}%)，"
                           f"成功: {success_stocks}，"
                           f"已获取记录: {total_records}，"
                           f"已耗时: {elapsed:.1f}s，"
                           f"预估剩余: {remaining:.1f}s")
                
                # 清理已完成的线程
                threads = [t for t in threads if t.is_alive()]
                
                # 控制启动新线程的间隔
                time.sleep(0.2)
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 处理剩余结果
        while not result_queue.empty():
            ts_code, success, records_count = result_queue.get()
            processed_stocks += 1
            if success:
                success_stocks += 1
                total_records += records_count
        
        # 记录最终结果
        elapsed_total = time.time() - start_time
        logger.success(f"并行处理完成，成功处理 {success_stocks}/{total_stocks} 个股票，"
                       f"共获取 {total_records} 条记录，"
                       f"总耗时: {elapsed_total:.1f}s")
        
        return success_stocks > 0
    
    def run(self, **kwargs) -> bool:
        """
        运行数据获取流程
        
        支持的运行模式：
        - recent: 获取最近数据
        - full: 获取完整历史数据
        - date_range: 获取指定日期范围的数据
        - stock: 获取指定股票的数据
        
        Args:
            **kwargs: 运行参数
                - mode: 运行模式，支持recent、full、date_range、stock
                - days: recent模式下的天数
                - start_date: date_range模式下的开始日期
                - end_date: date_range模式下的结束日期
                - ts_code: stock模式下的股票代码
                - use_parallel: 是否使用并行处理
                
        Returns:
            是否成功
        """
        mode = kwargs.get('mode', 'recent')
        use_parallel = kwargs.get('use_parallel', True)
        
        logger.info(f"开始运行日线数据获取器，模式: {mode}")
        
        # 对于非full模式，预先确保集合和索引存在
        if mode != 'full':
            if not self._ensure_collection_and_indexes():
                logger.error("集合和索引初始化失败")
                return False
            
        try:
            if mode == 'recent':
                days = kwargs.get('days', 7)
                return self.fetch_by_recent(days)
            elif mode == 'full':
                # full模式下使用按股票代码抓取的方式
                return self.fetch_full_history()
            elif mode == 'date_range':
                start_date = kwargs.get('start_date')
                end_date = kwargs.get('end_date')
                if not start_date or not end_date:
                    logger.error("date_range模式需要提供start_date和end_date参数")
                    return False
                return self.fetch_by_date_range(start_date, end_date, use_parallel)
            elif mode == 'stock':
                ts_code = kwargs.get('ts_code')
                if not ts_code:
                    logger.error("stock模式需要提供ts_code参数")
                    return False
                start_date = kwargs.get('start_date')
                end_date = kwargs.get('end_date')
                return self.fetch_by_stock(ts_code, start_date, end_date)
            else:
                logger.error(f"不支持的运行模式: {mode}")
                return False
                
        except Exception as e:
            logger.error(f"运行过程中发生异常: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _ensure_collection_and_indexes(self) -> bool:
        """
        确保集合和索引存在
        
        Returns:
            是否成功
        """
        try:
            # 第一步：检查集合是否存在
            logger.info(f"检查MongoDB集合 {self.collection_name} 是否存在")
            if not mongodb_handler.collection_exists(self.collection_name):
                logger.info(f"集合 {self.collection_name} 不存在，正在创建...")
                mongodb_handler.create_collection(self.collection_name)
                logger.info(f"集合 {self.collection_name} 创建成功")
            else:
                logger.info(f"集合 {self.collection_name} 已存在")
            
            # 第二步：检查索引是否正确
            logger.info(f"为集合 {self.collection_name} 设置索引字段: {self.index_fields}")
            
            # 获取当前集合的索引信息
            collection = mongodb_handler.get_collection(self.collection_name)
            index_info = collection.index_information()
            logger.debug(f"现有索引信息: {index_info}")
            
            # 检查是否存在复合索引
            compound_index_exists = False
            compound_index_name = None
            
            # 根据接口定义的配置文件，确定需要创建的索引
            index_fields = self.index_fields
            if not index_fields:
                logger.warning("接口配置中未定义index_fields，将使用默认索引字段：['ts_code', 'trade_date']")
                index_fields = ['ts_code', 'trade_date']
            
            # 检查是否存在复合索引
            for name, info in index_info.items():
                if name != '_id_':  # 跳过_id索引
                    keys = [key for key, _ in info['key']]
                    # 如果索引字段和顺序完全匹配
                    if keys == index_fields:
                        compound_index_exists = True
                        compound_index_name = name
                        logger.debug(f"复合索引 {index_fields} 已存在，索引名: {name}")
                        
                        # 检查是否为唯一索引
                        is_unique = info.get('unique', False)
                        logger.debug(f"复合索引 {name} 是否为唯一索引: {is_unique}")
                        break
            
            # 如果不存在复合索引，创建它
            if not compound_index_exists:
                logger.info(f"复合索引 {index_fields} 不存在，正在创建...")
                index_model = [(field, 1) for field in index_fields]
                collection.create_index(index_model, background=True, unique=True)
                logger.info(f"复合索引 {index_fields} 创建成功")
            else:
                logger.debug(f"复合索引 {index_fields} 已存在，检查是否为唯一索引")
            
            # 为每个字段创建单字段索引（如果不存在）
            for field in index_fields:
                field_index_exists = False
                for name, info in index_info.items():
                    if name != '_id_' and len(info['key']) == 1:
                        if info['key'][0][0] == field:
                            field_index_exists = True
                            logger.debug(f"字段 {field} 的单字段索引已存在，跳过创建")
                            break
                
                if not field_index_exists:
                    logger.info(f"为字段 {field} 创建单字段索引")
                    collection.create_index([(field, 1)], background=True)
                    logger.info(f"字段 {field} 的单字段索引创建成功")
                
            return True
            
        except Exception as e:
            logger.error(f"检查和创建索引失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        采用增量更新方式：
        1. 检查记录是否已存在
        2. 对于已存在的记录，检查是否需要更新
        3. 对于不存在的记录，批量插入
        
        Args:
            df: 要保存的数据
            
        Returns:
            是否成功
        """
        if df is None or df.empty:
            logger.warning("没有数据需要保存")
            return False
        
        try:
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return False
            
            start_time = time.time()
            logger.info(f"保存数据到MongoDB数据库：{mongodb_handler.db.name}，集合：{self.collection_name}")
            
            # 使用ts_code和trade_date作为唯一键
            key_fields = ['ts_code', 'trade_date']
            
            # 构建查询条件
            query_conditions = []
            for _, row in df.iterrows():
                if all(field in row.index for field in key_fields):
                    condition = {field: row[field] for field in key_fields}
                    query_conditions.append(condition)
            
            # 查询已存在的记录
            existing_records = []
            if query_conditions:
                existing_records = list(mongodb_handler.find_documents(
                    self.collection_name,
                    {"$or": query_conditions}
                ))
            
            logger.debug(f"找到 {len(existing_records)} 条已存在的记录")
            
            # 将已存在记录转为字典，以(ts_code, trade_date)为键
            existing_dict = {}
            for record in existing_records:
                if all(field in record for field in key_fields):
                    key = (record['ts_code'], record['trade_date'])
                    existing_dict[key] = record
            
            # 分类要处理的记录
            new_records = []
            update_records = []
            update_operations = []
            skip_records = []
            
            # 处理每一行数据
            for _, row in df.iterrows():
                if all(field in row.index for field in key_fields):
                    # 转换为字典
                    record = row.to_dict()
                    key = (record['ts_code'], record['trade_date'])
                    
                    # 如果记录已存在
                    if key in existing_dict:
                        existing = existing_dict[key]
                        
                        # 检查是否需要更新
                        need_update = False
                        for field, value in record.items():
                            if field not in existing or existing[field] != value:
                                need_update = True
                                break
                        
                        if need_update:
                            update_records.append(record)
                            filter_doc = {field: record[field] for field in key_fields}
                            update_doc = {"$set": record}
                            update_operations.append(
                                pymongo.UpdateOne(filter_doc, update_doc)
                            )
                        else:
                            skip_records.append(record)
                    else:
                        new_records.append(record)
                else:
                    logger.warning(f"记录缺少必要字段 {key_fields}，跳过")
            
            logger.debug(f"开始批量处理 {len(df)} 条记录...")
            
            # 统计信息
            stats = {
                "new": len(new_records),
                "update": len(update_records),
                "skip": len(skip_records),
                "fail": 0
            }
            
            logger.info(f"处理统计: 新记录:{stats['new']}条, 需更新:{stats['update']}条, 无变化跳过:{stats['skip']}条")
            
            # 批量插入新记录
            if new_records:
                try:
                    result_ids = mongodb_handler.insert_many_documents(self.collection_name, new_records, ordered=False)
                    logger.info(f"已批量插入 {len(result_ids)} 条新记录")
                except Exception as e:
                    logger.error(f"批量插入失败: {str(e)}")
                    stats["fail"] += len(new_records)
                    stats["new"] = 0
            
            # 批量更新记录
            if update_operations:
                try:
                    result = mongodb_handler.bulk_write(self.collection_name, update_operations, ordered=False)
                    logger.info(f"已批量更新 {result.modified_count} 条记录")
                    if result.modified_count != len(update_operations):
                        stats["fail"] += len(update_operations) - result.modified_count
                        stats["update"] = result.modified_count
                except Exception as e:
                    logger.error(f"批量更新失败: {str(e)}")
                    stats["fail"] += len(update_operations)
                    stats["update"] = 0
            
            # 查询总记录数
            count = mongodb_handler.count_documents(self.collection_name, {})
            logger.debug(f"查询到 {count} 条已存储的记录")
            
            elapsed = time.time() - start_time
            logger.info(f"数据存储操作完成，耗时: {elapsed:.2f}秒")
            
            # 记录总体结果
            logger.success(f"数据处理完成: 新插入 {stats['new']} 条记录，更新 {stats['update']} 条记录，"
                          f"跳过 {stats['skip']} 条记录，失败 {stats['fail']} 条记录")
            
            # 保存运行统计
            self.last_operation_stats = stats
            self.stats["total_records"] += len(df)
            self.stats["success_count"] += stats["new"] + stats["update"]
            self.stats["skipped_count"] += stats["skip"]
            self.stats["failure_count"] += stats["fail"]
            
            return True
            
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

def main():
    """主函数"""
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='获取日线行情数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--start-date', help='开始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式：YYYYMMDD')
    parser.add_argument('--ts-code', help='指定股票代码')
    parser.add_argument('--recent', action='store_true', help='获取最近一周的数据')
    parser.add_argument('--full', action='store_true', help='获取完整历史数据')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='daily', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--no-parallel', dest='use_parallel', action='store_false', help='禁用并行处理')
    parser.set_defaults(use_parallel=True)
    
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    try:
        # 创建获取器
        fetcher = DailyFetcherV3(
            config_path=args.config,
            interface_dir=args.interface_dir,
            target_market_codes=target_market_codes,
            db_name=args.db_name,
            collection_name=args.collection_name,
            verbose=args.verbose
        )
        
        # 确定运行模式和参数
        if args.ts_code:
            # stock模式
            success = fetcher.run(
                mode='stock',
                ts_code=args.ts_code,
                start_date=args.start_date,
                end_date=args.end_date
            )
        elif args.start_date and args.end_date:
            # date_range模式
            success = fetcher.run(
                mode='date_range',
                start_date=args.start_date,
                end_date=args.end_date,
                use_parallel=args.use_parallel
            )
        elif args.full:
            # full模式
            success = fetcher.run(
                mode='full',
                use_parallel=args.use_parallel
            )
        else:
            # recent模式（默认）
            success = fetcher.run(
                mode='recent',
                use_parallel=args.use_parallel
            )
        
        if success:
            logger.success("日线数据获取和保存成功")
            return 0
        else:
            logger.error("日线数据获取或保存失败")
            return 1
            
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 