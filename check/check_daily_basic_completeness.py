#!/usr/bin/env python
"""
日线基本面数据完整性检查工具

该脚本用于检查MongoDB中日线基本面数据的完整性，识别数据不完整的股票
可以设置检查的时间范围，如一周、一个月或自定义时间段

使用方法：
    python -m check.check_daily_basic_completeness --period week  # 检查最近一周数据完整性
    python -m check.check_daily_basic_completeness --period month  # 检查最近一个月数据完整性
    python -m check.check_daily_basic_completeness --start-date 20230101 --end-date 20230630  # 检查指定日期范围内的数据完整性
    python -m check.check_daily_basic_completeness --verbose  # 输出详细日志
    python -m check.check_daily_basic_completeness --output incomplete_stocks.csv  # 将不完整的股票列表保存到CSV文件
    python -m check.check_daily_basic_completeness --fetch-incomplete  # 检测并自动抓取不完整的股票数据
    python -m check.check_daily_basic_completeness --max-fetch 50  # 最多抓取50只不完整的股票数据
"""

import os
import sys
import yaml
import argparse
import pandas as pd
import pymongo
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional, Any
from pathlib import Path
from loguru import logger
import time

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
root_dir = current_dir.parent
sys.path.append(str(root_dir))

# 导入daily_basic_fetcher模块 (位于根目录)
from daily_basic_fetcher import DailyBasicFetcher
DAILY_BASIC_FETCHER_IMPORTABLE = True

class DailyBasicDataChecker:
    """
    日线基本面数据完整性检查器
    
    检查MongoDB中的日线基本面数据完整性，识别出数据不完整的股票
    """
    
    def __init__(
        self, 
        config_path: str = "config/config.yaml",
        db_name: str = None,
        collection_name: str = "daily_basic",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        verbose: bool = False
    ):
        """
        初始化日线基本面数据完整性检查器
        
        Args:
            config_path: 配置文件路径
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            target_market_codes: 目标市场代码集合，只检查这些板块的股票
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        # 如果配置路径不是绝对路径，则转换为相对于项目根目录的路径
        if not os.path.isabs(self.config_path):
            self.config_path = os.path.join(root_dir, self.config_path)
            
        self.collection_name = collection_name
        self.target_market_codes = target_market_codes
        self.verbose = verbose
        
        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        
        # 加载配置
        self.config = self._load_config()
        
        # 设置MongoDB连接参数
        mongodb_config = self.config.get("mongodb", {})
        self.db_name = db_name or mongodb_config.get("db_name", "tushare_data")
        
        # 初始化MongoDB客户端
        self.mongo_client = self._init_mongo_client()
        
        logger.info(f"MongoDB配置: 数据库名={self.db_name}, 集合名={self.collection_name}")
        logger.info(f"目标市场代码: {', '.join(self.target_market_codes)}")
    
    def _load_config(self) -> Dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.debug(f"成功加载配置文件: {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}
    
    def _init_mongo_client(self) -> pymongo.MongoClient:
        """初始化MongoDB客户端"""
        try:
            mongodb_config = self.config.get("mongodb", {})
            client = pymongo.MongoClient(
                host=mongodb_config.get("host", "localhost"),
                port=mongodb_config.get("port", 27017),
                username=mongodb_config.get("username"),
                password=mongodb_config.get("password"),
                authSource=mongodb_config.get("auth_source"),
                authMechanism=mongodb_config.get("auth_mechanism")
            )
            # 测试连接
            client.admin.command('ping')
            logger.success("成功连接MongoDB服务器")
            return client
        except Exception as e:
            logger.error(f"连接MongoDB失败: {str(e)}")
            raise
    
    def _get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        从MongoDB中获取指定日期范围内的交易日期
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表，格式为YYYYMMDD
        """
        try:
            db = self.mongo_client[self.db_name]
            # 尝试从trade_cal集合获取交易日历
            if "trade_cal" in db.list_collection_names():
                collection = db["trade_cal"]
                query = {
                    "cal_date": {"$gte": start_date, "$lte": end_date},
                    "is_open": 1  # 1表示交易日
                }
                result = collection.find(query, {"cal_date": 1, "_id": 0}).sort("cal_date", 1)
                trade_dates = [doc["cal_date"] for doc in result]
                
                if trade_dates:
                    logger.info(f"从交易日历获取到 {len(trade_dates)} 个交易日")
                    return trade_dates
            
            # 如果没有trade_cal集合或没有数据，尝试从daily_basic集合获取
            logger.info("未找到交易日历数据，尝试从daily_basic集合获取交易日期")
            collection = db[self.collection_name]
            result = collection.distinct("trade_date", {"trade_date": {"$gte": start_date, "$lte": end_date}})
            trade_dates = sorted(result)
            
            if trade_dates:
                logger.info(f"从daily_basic集合获取到 {len(trade_dates)} 个交易日")
                return trade_dates
            
            # 如果以上方法都失败，生成一个预估的交易日列表
            logger.warning("无法从数据库获取交易日信息，将生成估计的交易日")
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # 生成所有日期
            all_dates = []
            current_dt = start_dt
            while current_dt <= end_dt:
                # 如果不是周末，认为是交易日（简化处理）
                if current_dt.weekday() < 5:  # 0-4为周一至周五
                    all_dates.append(current_dt.strftime("%Y%m%d"))
                current_dt += timedelta(days=1)
            
            logger.info(f"生成了估计的 {len(all_dates)} 个交易日（不含周末）")
            return all_dates
            
        except Exception as e:
            logger.error(f"获取交易日期失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return []
    
    def _get_all_stocks(self) -> List[str]:
        """
        从stock_basic集合获取所有股票代码
        
        Returns:
            股票代码列表
        """
        try:
            db = self.mongo_client[self.db_name]
            
            # 首先尝试从stock_basic集合获取股票代码
            if "stock_basic" in db.list_collection_names():
                collection = db["stock_basic"]
                
                # 构建查询条件：只获取目标板块的股票
                query_conditions = []
                for market_code in self.target_market_codes:
                    # 使用正则表达式匹配symbol前两位
                    query_conditions.append({"symbol": {"$regex": f"^{market_code}"}})
                    
                # 使用$or操作符组合多个条件
                query = {"$or": query_conditions} if query_conditions else {}
                
                # 只查询ts_code字段
                projection = {"ts_code": 1, "_id": 0}
                
                # 执行查询
                result = collection.find(query, projection)
                stock_codes = [doc["ts_code"] for doc in result if "ts_code" in doc]
                
                if stock_codes:
                    logger.info(f"从stock_basic集合获取到 {len(stock_codes)} 只目标板块股票")
                    return stock_codes
                else:
                    logger.warning("从stock_basic集合未获取到股票数据，尝试从daily_basic集合获取")
            else:
                logger.warning("数据库中不存在stock_basic集合，尝试从daily_basic集合获取股票代码")
            
            # 如果从stock_basic获取失败，从daily_basic集合获取
            collection = db[self.collection_name]
            
            # 获取所有唯一的股票代码
            all_stock_codes = collection.distinct("ts_code")
            
            # 根据目标板块过滤股票代码
            stock_codes = []
            for ts_code in all_stock_codes:
                # 股票代码格式为XXXXXX.SZ或XXXXXX.SH，提取前两位进行比较
                symbol = ts_code.split('.')[0]
                prefix = symbol[:2] if len(symbol) >= 2 else ""
                if prefix in self.target_market_codes:
                    stock_codes.append(ts_code)
            
            logger.info(f"从daily_basic集合获取到 {len(stock_codes)} 只目标板块股票")
            return stock_codes
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return []
    
    def _get_stock_data_count(self, ts_code: str, start_date: str, end_date: str) -> int:
        """
        获取指定股票在指定日期范围内的数据量
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            数据条数
        """
        try:
            db = self.mongo_client[self.db_name]
            collection = db[self.collection_name]
            
            query = {
                "ts_code": ts_code,
                "trade_date": {"$gte": start_date, "$lte": end_date}
            }
            
            count = collection.count_documents(query)
            return count
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 数据量失败: {str(e)}")
            return 0
    
    def _get_stock_first_last_date(self, ts_code: str, start_date: str, end_date: str) -> Tuple[Optional[str], Optional[str]]:
        """
        获取指定股票在指定日期范围内的第一个和最后一个交易日
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            (第一个交易日, 最后一个交易日)，如果没有数据返回(None, None)
        """
        try:
            db = self.mongo_client[self.db_name]
            collection = db[self.collection_name]
            
            query = {
                "ts_code": ts_code,
                "trade_date": {"$gte": start_date, "$lte": end_date}
            }
            
            # 获取第一个交易日
            first_doc = collection.find(query).sort("trade_date", 1).limit(1)
            first_date = None
            for doc in first_doc:
                first_date = doc.get("trade_date")
            
            # 获取最后一个交易日
            last_doc = collection.find(query).sort("trade_date", -1).limit(1)
            last_date = None
            for doc in last_doc:
                last_date = doc.get("trade_date")
            
            return (first_date, last_date)
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 首末交易日失败: {str(e)}")
            return (None, None)
    
    def check_completeness(self, start_date: str, end_date: str, min_data_points: int = None) -> Dict:
        """
        检查指定日期范围内的数据完整性
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            min_data_points: 最小数据点数量，低于此数量的股票将被视为不完整
                            如果为None，则自动计算为交易日数量的80%
        
        Returns:
            包含完整性检查结果的字典
        """
        logger.info(f"开始检查数据完整性，日期范围: {start_date} 至 {end_date}")
        
        # 获取交易日期列表
        trade_dates = self._get_trade_dates(start_date, end_date)
        if not trade_dates:
            logger.error("获取交易日期失败，无法继续检查")
            return {
                "status": "error",
                "message": "获取交易日期失败",
                "total_trading_days": 0,
                "complete_stocks": [],
                "incomplete_stocks": []
            }
        
        total_trading_days = len(trade_dates)
        
        # 如果未指定最小数据点数量，则设为交易日数量的80%
        if min_data_points is None:
            min_data_points = int(total_trading_days * 0.8)
            logger.info(f"自动设置最小数据点数量为 {min_data_points} (交易日总数的80%)")
        
        # 获取所有股票代码
        stock_codes = self._get_all_stocks()
        if not stock_codes:
            logger.error("获取股票列表失败，无法继续检查")
            return {
                "status": "error",
                "message": "获取股票列表失败",
                "total_trading_days": total_trading_days,
                "complete_stocks": [],
                "incomplete_stocks": []
            }
        
        # 检查每只股票的数据完整性
        complete_stocks = []
        incomplete_stocks = []
        
        total_stocks = len(stock_codes)
        logger.info(f"开始检查 {total_stocks} 只股票的数据完整性...")
        
        # 用于统计
        zero_data_count = 0
        partial_data_count = 0
        
        for i, ts_code in enumerate(stock_codes):
            # 显示进度
            if i % 100 == 0:
                logger.info(f"进度: {i}/{total_stocks} ({i/total_stocks*100:.1f}%)")
            
            # 获取股票数据量
            data_count = self._get_stock_data_count(ts_code, start_date, end_date)
            
            # 获取首末交易日
            first_date, last_date = self._get_stock_first_last_date(ts_code, start_date, end_date)
            
            # 判断完整性
            is_complete = data_count >= min_data_points
            
            # 统计没有数据和不完整的股票
            if data_count == 0:
                zero_data_count += 1
            elif not is_complete:
                partial_data_count += 1
            
            # 收集结果
            stock_info = {
                "ts_code": ts_code,
                "data_count": data_count,
                "first_date": first_date,
                "last_date": last_date,
                "is_complete": is_complete,
                "expected_count": total_trading_days,
                "completeness_ratio": data_count / total_trading_days if total_trading_days > 0 else 0
            }
            
            if is_complete:
                complete_stocks.append(stock_info)
            else:
                incomplete_stocks.append(stock_info)
                if self.verbose:
                    logger.warning(f"股票 {ts_code} 数据不完整，有 {data_count}/{total_trading_days} 条数据，完整度: {stock_info['completeness_ratio']:.1%}")
        
        logger.success(f"完整性检查完成: 完整股票 {len(complete_stocks)}/{total_stocks} ({len(complete_stocks)/total_stocks*100:.1f}%)，"
                     f"不完整股票 {len(incomplete_stocks)}/{total_stocks} ({len(incomplete_stocks)/total_stocks*100:.1f}%)")
        
        # 输出详细统计
        logger.info(f"数据缺失情况: 完全无数据 {zero_data_count} 只，部分数据 {partial_data_count} 只")
        
        return {
            "status": "success",
            "message": "完整性检查完成",
            "total_trading_days": total_trading_days,
            "min_data_points": min_data_points,
            "complete_stocks": complete_stocks,
            "incomplete_stocks": incomplete_stocks,
            "zero_data_count": zero_data_count,
            "partial_data_count": partial_data_count
        }
    
    def export_results(self, results: Dict, output_file: str) -> None:
        """
        将结果导出到CSV文件
        
        Args:
            results: 完整性检查结果字典
            output_file: 输出文件路径
        """
        if results["status"] != "success":
            logger.error(f"结果状态为 {results['status']}，无法导出")
            return
        
        try:
            # 将不完整的股票列表转换为DataFrame
            incomplete_stocks = results["incomplete_stocks"]
            if not incomplete_stocks:
                logger.info("没有不完整的股票，跳过导出")
                return
            
            df = pd.DataFrame(incomplete_stocks)
            
            # 计算字段
            df["missing_days"] = df["expected_count"] - df["data_count"]
            df["completeness_pct"] = df["completeness_ratio"] * 100
            
            # 添加需要优先获取的标记
            df["priority"] = "低"
            # 完全没有数据的股票优先级最高
            df.loc[df["data_count"] == 0, "priority"] = "高"
            # 数据不到一半的股票优先级中等
            df.loc[(df["data_count"] > 0) & (df["completeness_pct"] < 50), "priority"] = "中"
            
            # 创建股票代码的批次建议
            batch_size = 10
            unique_ts_codes = df["ts_code"].unique()
            batch_count = (len(unique_ts_codes) + batch_size - 1) // batch_size
            
            # 对完全没有数据的股票生成获取命令
            zero_data_stocks = df[df["data_count"] == 0]["ts_code"].tolist()
            
            # 选择需要的列并排序
            df = df[["ts_code", "data_count", "expected_count", "missing_days", 
                     "completeness_pct", "first_date", "last_date", "priority"]]
            df = df.sort_values(by=["priority", "completeness_pct"], ascending=[False, True])
            
            # 保存到CSV
            df.to_csv(output_file, index=False)
            logger.success(f"已将 {len(incomplete_stocks)} 只不完整股票的信息导出到 {output_file}")
            
            # 生成并保存批次文件
            batch_file = output_file.replace(".csv", "_batches.txt")
            with open(batch_file, "w") as f:
                f.write(f"# 日线基本面数据获取批次建议 - 共{len(unique_ts_codes)}只股票，{batch_count}个批次\n\n")
                
                # 首先列出没有数据的股票
                if zero_data_stocks:
                    f.write("# 完全没有数据的股票（高优先级）\n")
                    zero_batches = [zero_data_stocks[i:i+batch_size] for i in range(0, len(zero_data_stocks), batch_size)]
                    for i, batch in enumerate(zero_batches):
                        f.write(f"\n# 批次 {i+1}/{len(zero_batches)} - {len(batch)} 只股票\n")
                        for j, code in enumerate(batch):
                            f.write(f"{code}")
                            if j < len(batch) - 1:
                                f.write(",")
                            if (j + 1) % 5 == 0 or j == len(batch) - 1:
                                f.write("\n")
                        
                        # 添加命令示例
                        codes_str = ",".join(batch)
                        f.write(f"\n# 命令示例：\n")
                        f.write(f"python -m daily_basic_fetcher --ts-code {codes_str}\n\n")
                
                # 然后列出数据不完整的股票
                incomplete_nonzero = df[(df["data_count"] > 0) & (df["completeness_pct"] < 80)].sort_values(by="completeness_pct")
                if not incomplete_nonzero.empty:
                    f.write("\n# 数据不完整的股票（中优先级）\n")
                    nonzero_codes = incomplete_nonzero["ts_code"].tolist()
                    nonzero_batches = [nonzero_codes[i:i+batch_size] for i in range(0, len(nonzero_codes), batch_size)]
                    for i, batch in enumerate(nonzero_batches):
                        if i >= 5:  # 只显示前5个批次示例
                            f.write(f"\n# ... 还有 {len(nonzero_batches) - 5} 个批次 ...\n")
                            break
                        f.write(f"\n# 批次 {i+1}/{len(nonzero_batches)} - {len(batch)} 只股票\n")
                        for j, code in enumerate(batch):
                            f.write(f"{code}")
                            if j < len(batch) - 1:
                                f.write(",")
                            if (j + 1) % 5 == 0 or j == len(batch) - 1:
                                f.write("\n")
            
            logger.success(f"已生成批次建议文件 {batch_file}")
            
        except Exception as e:
            logger.error(f"导出结果失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")

    def fetch_incomplete_stocks(self, results: Dict, start_date: str, end_date: str, max_stocks: int = None, batch_size: int = 10, use_imported: bool = True, use_parallel: bool = True, batch_delay: int = 5) -> bool:
        """
        抓取不完整的股票数据
        
        Args:
            results: 完整性检查结果字典
            start_date: 开始日期，格式为YYYYMMDD（仅用于检查，实际抓取默认从1990年开始）
            end_date: 结束日期，格式为YYYYMMDD（仅用于检查，实际抓取默认到当前日期）
            max_stocks: 最多抓取的股票数量，None表示不限制
            batch_size: 每批处理的股票数量
            use_imported: 是否使用导入的DailyBasicFetcher类
            use_parallel: 是否使用多WAN并行模式抓取
            batch_delay: 批次间延迟时间(秒)，避免API限制
            
        Returns:
            是否成功
        """
        if results["status"] != "success" or not results["incomplete_stocks"]:
            logger.error("没有需要抓取的不完整股票数据")
            return False
            
        # 获取不完整的股票代码列表
        incomplete_stocks = results["incomplete_stocks"]
        
        # 按完整度排序，优先抓取完全没有数据和完整度较低的股票
        incomplete_stocks.sort(key=lambda x: (1 if x["data_count"] > 0 else 0, x["completeness_ratio"]))
        
        if max_stocks:
            incomplete_stocks = incomplete_stocks[:max_stocks]
            
        ts_codes = [stock["ts_code"] for stock in incomplete_stocks]
        total_stocks = len(ts_codes)
        
        # 使用从1990年1月1日到今天的完整历史范围
        fetch_start_date = "19900101"
        fetch_end_date = datetime.now().strftime("%Y%m%d")
        
        logger.info(f"准备抓取 {total_stocks} 只不完整的股票数据，时间范围: {fetch_start_date} 至 {fetch_end_date}...")
        logger.info(f"并行模式: {'启用' if use_parallel else '禁用'}, 批次延迟: {batch_delay}秒")
        
        # 使用导入的DailyBasicFetcher类
        if use_imported and DAILY_BASIC_FETCHER_IMPORTABLE:
            return self._fetch_with_imported_fetcher(ts_codes, fetch_start_date, fetch_end_date, batch_size, use_parallel, batch_delay)
        # 使用命令行方式调用daily_basic_fetcher.py
        else:
            return self._fetch_with_command_line(ts_codes, fetch_start_date, fetch_end_date, batch_size, use_parallel, batch_delay)

    def _fetch_with_imported_fetcher(self, ts_codes: List[str], start_date: str, end_date: str, batch_size: int, use_parallel: bool = True, batch_delay: int = 5) -> bool:
        """
        使用导入的DailyBasicFetcher类抓取股票数据
        
        Args:
            ts_codes: 要抓取的股票代码列表
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            batch_size: 每批处理的股票数量
            use_parallel: 是否使用多WAN并行模式抓取
            batch_delay: 批次间延迟时间(秒)，避免API限制
            
        Returns:
            是否成功
        """
        try:
            # 创建DailyBasicFetcher实例
            fetcher = DailyBasicFetcher(
                config_path=self.config_path,
                target_market_codes=self.target_market_codes,
                verbose=self.verbose,
                db_name=self.db_name,
                collection_name=self.collection_name,
                batch_size=10000  # 设置API批量请求的数据最大数量
            )
            
            # 检查是否有可用的WAN接口
            has_wan = hasattr(fetcher, 'port_allocator') and fetcher.port_allocator
            wan_available = has_wan and fetcher.port_allocator.get_available_wan_indices()
            
            if use_parallel and has_wan and not wan_available:
                logger.warning("未检测到可用的WAN接口，将使用普通模式抓取数据")
                use_parallel = False
            
            if use_parallel and has_wan and wan_available:
                logger.info(f"使用多WAN并行模式抓取数据，可用WAN接口: {len(wan_available)}")
                # 使用多线程并行处理
                return self._fetch_with_wan_parallel(fetcher, ts_codes, start_date, end_date, batch_size, batch_delay)
            else:
                # 使用普通串行模式
                logger.info("使用普通串行模式抓取数据")
                return self._fetch_sequentially(fetcher, ts_codes, start_date, end_date, batch_delay)
            
        except Exception as e:
            logger.error(f"使用DailyBasicFetcher抓取数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def _fetch_with_wan_parallel(self, fetcher: Any, ts_codes: List[str], start_date: str, end_date: str, batch_size: int, batch_delay: int = 5) -> bool:
        """
        使用多WAN接口并行抓取多个股票的数据
        
        Args:
            fetcher: DailyBasicFetcher实例
            ts_codes: 要抓取的股票代码列表
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            batch_size: 每批处理的股票数量
            batch_delay: 批次间延迟时间(秒)，避免API限制
            
        Returns:
            是否成功
        """
        import threading
        import queue
        
        # 获取可用的WAN接口
        available_wans = fetcher.port_allocator.get_available_wan_indices()
        if not available_wans:
            logger.warning("没有可用的WAN接口，将使用普通模式抓取数据")
            return self._fetch_sequentially(fetcher, ts_codes, start_date, end_date, batch_delay)
        
        # 控制最大并发线程数 - 降低并发数，避免API限制
        max_workers = min(len(available_wans), 2)  # 每个WAN接口一个线程，但最多2个
        logger.info(f"并行模式: 使用 {max_workers} 个WAN接口并行抓取，每批次处理间隔 {batch_delay} 秒")
        
        # 创建队列和线程
        task_queue = queue.Queue()
        result_queue = queue.Queue()
        stop_event = threading.Event()
        
        # 创建一个线程锁用于日志和状态更新
        log_lock = threading.Lock()
        
        # 工作线程函数
        def worker(wan_idx, worker_id):
            # 记录上一次处理时间，用于控制批次间延迟
            last_process_time = 0
            
            while not stop_event.is_set():
                try:
                    # 控制API请求频率 - 确保批次间有足够的延迟
                    current_time = time.time()
                    time_since_last = current_time - last_process_time
                    if time_since_last < batch_delay and last_process_time > 0:
                        # 如果距离上次处理时间不足batch_delay秒，等待
                        sleep_time = batch_delay - time_since_last
                        time.sleep(sleep_time)
                    
                    # 从任务队列获取一个股票代码
                    try:
                        ts_code = task_queue.get(timeout=1)
                    except queue.Empty:
                        # 队列为空，继续等待
                        continue
                    
                    # 更新最后处理时间
                    last_process_time = time.time()
                    
                    with log_lock:
                        logger.info(f"工作线程 {worker_id} (WAN {wan_idx}) 开始处理股票 {ts_code}")
                    
                    # 获取WAN端口
                    try:
                        wan_info = fetcher._get_wan_socket(wan_idx)
                        if not wan_info:
                            with log_lock:
                                logger.warning(f"无法为WAN {wan_idx} 获取端口，跳过处理股票 {ts_code}")
                            result_queue.put((ts_code, False))
                            task_queue.task_done()
                            continue
                            
                        wan_idx, port = wan_info
                        
                        # 使用WAN接口获取数据
                        with log_lock:
                            logger.info(f"使用WAN {wan_idx}:{port} 抓取股票 {ts_code}")
                        
                        # 使用fetch_daily_basic_by_code_with_wan方法
                        result = fetcher.fetch_daily_basic_by_code_with_wan(
                            ts_code=ts_code, 
                            start_date=start_date, 
                            end_date=end_date, 
                            wan_info=wan_info,
                            max_count=10000  # 设置为最大值，提高单次获取效率
                        )
                        
                        if not result.empty:
                            # 保存数据
                            fetcher.store_data(result)
                            with log_lock:
                                logger.success(f"WAN {wan_idx} 成功获取并保存股票 {ts_code} 的数据，共 {len(result)} 条记录")
                            result_queue.put((ts_code, True))
                        else:
                            with log_lock:
                                logger.warning(f"WAN {wan_idx} 未获取到股票 {ts_code} 的数据")
                            result_queue.put((ts_code, False))
                    
                    finally:
                        # 确保释放端口
                        if 'wan_info' in locals() and wan_info:
                            # 添加短暂延迟，确保端口完全释放
                            time.sleep(1.0)  # 增加到1秒，确保端口充分释放
                            try:
                                fetcher.port_allocator.release_port(wan_idx, port)
                            except:
                                pass
                            
                            # 额外等待，确保API和端口冷却
                            time.sleep(2.0)  # 增加额外冷却时间
                        
                        # 完成任务
                        task_queue.task_done()
                    
                except Exception as e:
                    with log_lock:
                        logger.error(f"工作线程 {worker_id} 处理出错: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
                
                    # 确保无论如何都标记任务完成
                    if 'ts_code' in locals():
                        result_queue.put((ts_code, False))
                        task_queue.task_done()
        
        # 启动工作线程
        threads = []
        for i in range(max_workers):
            wan_idx = available_wans[i % len(available_wans)]
            t = threading.Thread(target=worker, args=(wan_idx, i), daemon=True)
            t.start()
            threads.append(t)
        
        # 添加所有任务到队列
        for ts_code in ts_codes:
            task_queue.put(ts_code)
        
        # 设置任务总数
        total_tasks = len(ts_codes)
        completed_tasks = 0
        success_count = 0
        
        # 显示进度和处理结果
        try:
            start_time = time.time()
            
            # 监控进度
            while completed_tasks < total_tasks:
                try:
                    # 从结果队列获取结果
                    ts_code, success = result_queue.get(timeout=60)  # 增加超时时间到1分钟
                    completed_tasks += 1
                    
                    if success:
                        success_count += 1
                    
                    # 更新进度
                    elapsed = time.time() - start_time
                    avg_time_per_task = elapsed / completed_tasks if completed_tasks > 0 else 0
                    remaining = (total_tasks - completed_tasks) * avg_time_per_task
                    progress = completed_tasks / total_tasks * 100
                    
                    logger.info(f"进度: {completed_tasks}/{total_tasks} ({progress:.1f}%)，成功: {success_count}，耗时: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
                    
                    # 标记结果处理完成
                    result_queue.task_done()
                    
                except queue.Empty:
                    # 检查任务队列是否为空且所有任务都已完成
                    if task_queue.empty() and task_queue.unfinished_tasks == 0:
                        break
                    
                    # 检查线程是否都已退出
                    if all(not t.is_alive() for t in threads):
                        logger.warning("所有工作线程已退出，但未完成所有任务")
                        break
                    
                    logger.debug("等待结果超时，继续等待...")
            
            # 等待队列中的所有任务完成
            task_queue.join()
            
            # 发送停止信号给所有线程
            stop_event.set()
            
            # 等待所有线程结束
            for t in threads:
                t.join(timeout=1)
            
            # 输出最终统计信息
            elapsed_total = time.time() - start_time
            logger.success(f"并行处理完成，成功获取 {success_count}/{total_tasks} 只股票的数据，耗时 {elapsed_total:.1f}s")
            
            return success_count > 0
        
        except KeyboardInterrupt:
            logger.warning("接收到中断信号，正在停止...")
            stop_event.set()
            for t in threads:
                t.join(timeout=1)
            raise
        
        except Exception as e:
            logger.error(f"并行处理出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 清理
            stop_event.set()
            for t in threads:
                t.join(timeout=1)
            
            return False

    def _fetch_sequentially(self, fetcher: Any, ts_codes: List[str], start_date: str, end_date: str, batch_delay: int = 5) -> bool:
        """
        使用串行方式逐个抓取股票数据
        
        Args:
            fetcher: DailyBasicFetcher实例
            ts_codes: 要抓取的股票代码列表
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            batch_delay: 批次间延迟时间(秒)，避免API限制
            
        Returns:
            是否成功
        """
        try:
            total_stocks = len(ts_codes)
            success_count = 0
            start_time = time.time()
            
            for i, ts_code in enumerate(ts_codes):
                # 显示进度
                logger.info(f"处理进度: {i+1}/{total_stocks} ({(i+1)/total_stocks*100:.1f}%)")
                logger.info(f"正在抓取股票 {ts_code} 的每日指标数据...")
                
                try:
                    # 获取数据
                    df = fetcher.fetch_daily_basic_by_code(
                        ts_code=ts_code, 
                        start_date=start_date, 
                        end_date=end_date
                    )
                    
                    if not df.empty:
                        # 保存数据
                        result = fetcher.store_data(df)
                        inserted = result.get("inserted", 0)
                        updated = result.get("updated", 0)
                        logger.success(f"成功获取并保存股票 {ts_code} 的数据，新增 {inserted} 条记录，更新 {updated} 条记录，共 {len(df)} 条记录")
                        success_count += 1
                    else:
                        logger.warning(f"未获取到股票 {ts_code} 的数据")
                except Exception as e:
                    logger.error(f"处理股票 {ts_code} 时出错: {str(e)}")
                
                # 每处理一只股票后等待一段时间，避免API调用过于频繁
                logger.debug(f"等待 {batch_delay} 秒后处理下一只股票...")
                time.sleep(batch_delay)
                
                # 每处理10只股票输出一次进度
                if (i + 1) % 10 == 0:
                    elapsed = time.time() - start_time
                    avg_time_per_stock = elapsed / (i + 1)
                    remaining = (total_stocks - (i + 1)) * avg_time_per_stock
                    logger.info(f"已处理 {i+1}/{total_stocks} 只股票，耗时: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            # 最终统计
            elapsed_total = time.time() - start_time
            logger.success(f"串行处理完成，成功获取 {success_count}/{total_stocks} 只股票的数据，耗时 {elapsed_total:.1f}s")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"串行处理出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def _fetch_with_command_line(self, ts_codes: List[str], start_date: str, end_date: str, batch_size: int, use_parallel: bool = True, batch_delay: int = 5) -> bool:
        """
        使用命令行方式调用daily_basic_fetcher.py抓取股票数据
        
        Args:
            ts_codes: 要抓取的股票代码列表
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            batch_size: 每批处理的股票数量
            use_parallel: 是否使用多WAN并行模式抓取
            batch_delay: 批次间延迟时间(秒)，避免API限制
            
        Returns:
            是否成功
        """
        try:
            # 分批处理
            total_batches = (len(ts_codes) + batch_size - 1) // batch_size
            success_count = 0
            
            for i in range(0, len(ts_codes), batch_size):
                # 获取当前批次的股票代码
                batch_codes = ts_codes[i:i+batch_size]
                batch_num = i // batch_size + 1
                logger.info(f"处理批次 {batch_num}/{total_batches}，共 {len(batch_codes)} 只股票")
                
                # 将批次进一步拆分，避免一次处理过多股票导致端口冲突
                sub_batch_size = 1  # 每个子批次只包含1只股票
                sub_batches = [batch_codes[j:j+sub_batch_size] for j in range(0, len(batch_codes), sub_batch_size)]
                
                for sub_batch_idx, sub_batch in enumerate(sub_batches):
                    # 构建命令
                    codes_str = ",".join(sub_batch)
                    cmd = [
                        "python", 
                        "-m",
                        "daily_basic_fetcher",  # 使用-m选项调用模块
                        "--ts-code", codes_str,
                        "--start-date", start_date,
                        "--end-date", end_date,
                        "--batch-size", "1"  # 设置为1，确保逐个处理
                    ]
                    
                    if self.verbose:
                        cmd.append("--verbose")
                    
                    # 添加并行模式参数
                    if not use_parallel:
                        cmd.append("--no-parallel")
                    
                    # 执行命令
                    logger.info(f"执行命令: {' '.join(cmd)}")
                    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                    
                    # 设置超时时间 (5分钟)
                    try:
                        stdout, stderr = process.communicate(timeout=300)
                        # 实时输出日志
                        for line in stdout.splitlines():
                            logger.debug(line)
                        
                        # 获取返回码
                        return_code = process.returncode
                    except subprocess.TimeoutExpired:
                        logger.error(f"命令执行超时，终止进程")
                        process.kill()
                        stdout, stderr = process.communicate()
                        return_code = -1
                    
                    if return_code == 0:
                        success_count += 1
                        logger.success(f"股票 {codes_str} 抓取成功")
                    else:
                        logger.warning(f"股票 {codes_str} 抓取失败，返回代码: {return_code}")
                        # 输出错误信息
                        for line in stderr.splitlines():
                            logger.error(line)
                
                    # 增加等待时间，确保端口完全释放
                    logger.debug(f"等待 {batch_delay} 秒后处理下一只股票...")
                    time.sleep(batch_delay)
                
                logger.info(f"完成批次 {batch_num}/{total_batches}，当前成功: {success_count}")
                # 批次间增加等待时间
                time.sleep(batch_delay * 2)  # 批次间等待时间加倍
            
            logger.info(f"数据抓取完成，成功获取 {success_count} 只股票的数据")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"调用daily_basic_fetcher.py失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="日线基本面数据完整性检查工具")
    parser.add_argument("--verbose", action="store_true", help="输出详细日志")
    parser.add_argument("--period", type=str, choices=["week", "month", "quarter", "year"], 
                      help="检查周期，可选值: week(一周)、month(一个月)、quarter(一季度)、year(一年)")
    parser.add_argument("--start-date", type=str, help="开始日期，格式为YYYYMMDD，如20230101")
    parser.add_argument("--end-date", type=str, help="结束日期，格式为YYYYMMDD，如20230630")
    parser.add_argument("--min-count", type=int, help="最小数据点数量，低于此数量的股票将被视为不完整")
    parser.add_argument("--output", type=str, default="incomplete_daily_basic_stocks.csv", help="输出文件路径，默认为incomplete_daily_basic_stocks.csv")
    parser.add_argument("--config", type=str, default="config/config.yaml", help="配置文件路径")
    parser.add_argument("--db-name", type=str, help="MongoDB数据库名称，默认从配置文件读取")
    parser.add_argument("--collection", type=str, default="daily_basic", help="MongoDB集合名称，默认为daily_basic")
    parser.add_argument("--market-codes", type=str, default="00,30,60,68", help="目标市场代码，用逗号分隔，默认为00,30,60,68")
    parser.add_argument("--fetch-incomplete", action="store_true", help="自动抓取不完整的股票数据")
    parser.add_argument("--max-fetch", type=int, help="最多抓取的不完整股票数量")
    parser.add_argument("--batch-size", type=int, default=1, help="每批处理的股票数量，默认为1")
    parser.add_argument("--batch-delay", type=int, default=5, help="批次间延迟时间(秒)，避免API限制，默认为5秒")
    parser.add_argument("--use-cmd", action="store_true", help="使用命令行方式调用daily_basic_fetcher.py，而不是直接导入")
    parser.add_argument("--no-parallel", dest="use_parallel", action="store_false", help="不使用并行处理")
    parser.set_defaults(use_parallel=True)
    
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 确定检查的日期范围
    today = datetime.now()
    end_date = today.strftime("%Y%m%d")
    
    if args.period:
        if args.period == "week":
            start_date = (today - timedelta(days=7)).strftime("%Y%m%d")
            logger.info(f"检查最近一周的数据完整性: {start_date} 至 {end_date}")
        elif args.period == "month":
            start_date = (today - timedelta(days=30)).strftime("%Y%m%d")
            logger.info(f"检查最近一个月的数据完整性: {start_date} 至 {end_date}")
        elif args.period == "quarter":
            start_date = (today - timedelta(days=90)).strftime("%Y%m%d")
            logger.info(f"检查最近一季度的数据完整性: {start_date} 至 {end_date}")
        elif args.period == "year":
            start_date = (today - timedelta(days=365)).strftime("%Y%m%d")
            logger.info(f"检查最近一年的数据完整性: {start_date} 至 {end_date}")
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
        logger.info(f"检查指定日期范围的数据完整性: {start_date} 至 {end_date}")
    else:
        # 默认检查最近一个月
        start_date = (today - timedelta(days=30)).strftime("%Y%m%d")
        logger.info(f"使用默认日期范围(最近一个月): {start_date} 至 {end_date}")
    
    # 如果配置路径不是绝对路径，则转换为相对于项目根目录的路径
    config_path = args.config
    if not os.path.isabs(config_path):
        config_path = os.path.join(root_dir, config_path)
    
    # 创建检查器
    checker = DailyBasicDataChecker(
        config_path=config_path,
        db_name=args.db_name,
        collection_name=args.collection,
        target_market_codes=target_market_codes,
        verbose=args.verbose
    )
    
    # 执行检查
    results = checker.check_completeness(start_date, end_date, args.min_count)
    
    if results["status"] == "success":
        # 显示统计信息
        complete_stocks = results["complete_stocks"]
        incomplete_stocks = results["incomplete_stocks"]
        total_stocks = len(complete_stocks) + len(incomplete_stocks)
        
        logger.info(f"总交易日数: {results['total_trading_days']}")
        logger.info(f"最小数据点要求: {results['min_data_points']}")
        logger.info(f"完整股票数: {len(complete_stocks)}/{total_stocks} ({len(complete_stocks)/total_stocks*100:.1f}%)")
        logger.info(f"不完整股票数: {len(incomplete_stocks)}/{total_stocks} ({len(incomplete_stocks)/total_stocks*100:.1f}%)")
        
        # 显示一些不完整的股票示例
        if incomplete_stocks:
            logger.info("部分不完整股票示例:")
            for i, stock in enumerate(sorted(incomplete_stocks, key=lambda x: x["data_count"])):
                if i >= 10:  # 只显示前10个
                    break
                logger.info(f"  {stock['ts_code']}: {stock['data_count']}/{results['total_trading_days']} 条数据 "
                           f"({stock['completeness_ratio']:.1%}), 日期范围: {stock['first_date']} ~ {stock['last_date']}")
        
        # 导出结果
        checker.export_results(results, args.output)
        
        # 如果指定了自动抓取，执行抓取操作
        if args.fetch_incomplete and incomplete_stocks:
            logger.info(f"开始自动抓取不完整的股票数据，批次延迟: {args.batch_delay}秒...")
            success = checker.fetch_incomplete_stocks(
                results=results, 
                start_date=start_date, 
                end_date=end_date, 
                max_stocks=args.max_fetch,
                batch_size=args.batch_size,
                use_imported=not args.use_cmd,
                use_parallel=args.use_parallel,
                batch_delay=args.batch_delay
            )
            
            if success:
                logger.success("不完整股票数据抓取完成")
            else:
                logger.error("不完整股票数据抓取失败")
    else:
        logger.error(f"检查失败: {results['message']}")

if __name__ == "__main__":
    main() 