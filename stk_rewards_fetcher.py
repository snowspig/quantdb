#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票管理层薪酬及持股数据获取器 - 获取管理层薪酬及持股数据并保存到MongoDB

该脚本用于从湘财Tushare获取股票管理层薪酬及持股数据，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=194

使用方法：
    python stk_rewards_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stk_rewards_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stk_rewards_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python stk_rewards_fetcher.py --full        # 获取所有历史数据（默认只获取最近一周数据）
"""
import os
import sys
import json
import yaml
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple, Union
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# 导入项目模块
from data_fetcher.tushare_client import TushareClient
from storage.mongodb_client import MongoDBClient
from wan_manager.port_allocator import PortAllocator

class StkRewardsFetcher:
    """
    股票管理层薪酬及持股数据获取器
    
    该类用于从Tushare获取股票管理层薪酬及持股数据并保存到MongoDB数据库，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stk_rewards.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        db_name: str = "tushare_data",
        collection_name: str = "stk_rewards",
        verbose: bool = False
    ):
        """
        初始化股票管理层薪酬及持股数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            target_market_codes: 目标市场代码集合
            db_name: MongoDB数据库名称
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.db_name = "tushare_data"  # 强制使用tushare_data作为数据库名
        self.collection_name = collection_name
        self.verbose = verbose

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

        # 加载配置
        self.config = self._load_config()
        self.interface_config = self._load_interface_config()
        
        # 初始化Tushare客户端
        self.client = self._init_client()
        
        # 初始化MongoDB客户端
        self.mongo_client = self._init_mongo_client()
        
        # 初始化多WAN口管理器
        self.port_allocator = self._init_port_allocator()


    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            sys.exit(1)

    def _load_interface_config(self) -> Dict[str, Any]:
        """加载接口配置文件"""
        config_path = os.path.join(self.interface_dir, self.interface_name)
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
                logger.error(f"加载接口配置失败 {self.interface_name}: {str(e)}")
        
        logger.warning(f"接口配置文件不存在: {config_path}，将使用默认配置")
        return {
            "description": "高管薪酬及持股",
            "api_name": "stk_rewards",
            "fields": [],
            "params": {},
            "available_fields": [
                "ts_code", "ann_date", "end_date", "name", "title", 
                "reward", "hold_vol", "hold_change"
            ],
            "index_fields": ["ts_code", "name", "end_date"]
        }

    def _init_client(self) -> TushareClient:
        """初始化Tushare客户端"""
        try:
            tushare_config = self.config.get("tushare", {})
            token = tushare_config.get("token", "")
            if not token:
                logger.error("未配置Tushare API Key")
                sys.exit(1)
                
            return TushareClient(token=token)
        except Exception as e:
            logger.error(f"初始化Tushare客户端失败: {str(e)}")
            sys.exit(1)

    def _init_mongo_client(self) -> MongoDBClient:
        """初始化MongoDB客户端"""
        try:
            mongodb_config = self.config.get("mongodb", {})
            
            # 获取MongoDB连接信息
            uri = mongodb_config.get("uri", "")
            host = mongodb_config.get("host", "localhost")
            port = mongodb_config.get("port", 27017)
            username = mongodb_config.get("username", "")
            password = mongodb_config.get("password", "")
            
            # 创建MongoDB客户端 - 明确指定数据库名称为tushare_data
            mongo_client = MongoDBClient(
                uri=uri,
                host=host,
                port=port,
                username=username,
                password=password,
                db_name="tushare_data"  # 明确设置数据库名
            )
            
            # 连接到数据库
            if not mongo_client.connect():
                logger.error("连接MongoDB失败")
                sys.exit(1)
                
            return mongo_client
        except Exception as e:
            logger.error(f"初始化MongoDB客户端失败: {str(e)}")
            sys.exit(1)

    def _init_port_allocator(self) -> Optional[PortAllocator]:
        """初始化多WAN口管理器"""
        try:
            # 检查是否启用WAN接口
            wan_config = self.config.get("wan", {})
            wan_enabled = wan_config.get("enabled", False)
            
            if not wan_enabled:
                logger.warning("多WAN口功能未启用，将使用系统默认网络接口")
                return None
                
            # 获取WAN接口配置
            if not wan_config.get("port_ranges"):
                logger.warning("未配置WAN接口端口范围，将使用系统默认网络接口")
                return None
            
            # 使用全局端口分配器
            from wan_manager.port_allocator import port_allocator
            
            # 检查是否有可用WAN接口
            available_indices = port_allocator.get_available_wan_indices()
            if not available_indices:
                logger.warning("没有可用的WAN接口，将使用系统默认网络接口")
                return None
                
            logger.debug(f"已初始化多WAN口管理器，可用接口索引: {available_indices}")
            return port_allocator
        except Exception as e:
            logger.error(f"初始化多WAN口管理器失败: {str(e)}")
            return None

    def _get_wan_socket(self) -> Optional[Tuple[int, int]]:
        """获取WAN接口和端口"""
        if not self.port_allocator:
            return None
            
        try:
            # 获取可用的WAN接口索引
            available_indices = self.port_allocator.get_available_wan_indices()
            if not available_indices:
                logger.warning("没有可用的WAN接口")
                return None
                
            # 轮询选择一个WAN接口
            wan_idx = available_indices[0]  # 简单起见，选择第一个
            
            # 分配端口
            port = self.port_allocator.allocate_port(wan_idx)
            if not port:
                logger.warning(f"WAN {wan_idx} 没有可用端口")
                return None
                
            logger.debug(f"使用WAN接口 {wan_idx}，本地端口 {port}")
            return (wan_idx, port)
            
        except Exception as e:
            logger.error(f"获取WAN接口失败: {str(e)}")
            return None

    def get_target_ts_codes_from_stock_basic(self) -> Set[str]:
        """
        从stock_basic集合中获取目标板块的股票代码
        
        Returns:
            目标板块股票代码集合
        """
        try:
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not self.mongo_client.connect():
                    logger.error("重新连接MongoDB失败")
                    return set()
                
            # 查询stock_basic集合中符合条件的股票代码
            logger.info(f"从stock_basic集合查询目标板块 {self.target_market_codes} 的股票代码")
            
            # 构建查询条件：symbol前两位在target_market_codes中
            query_conditions = []
            for market_code in self.target_market_codes:
                # 使用正则表达式匹配symbol前两位
                query_conditions.append({"symbol": {"$regex": f"^{market_code}"}})
                
            # 使用$or操作符组合多个条件
            query = {"$or": query_conditions} if query_conditions else {}
            
            # 只查询ts_code字段
            result = self.mongo_client.find("stock_basic", query, projection={"ts_code": 1, "_id": 0})
            
            # 提取ts_code集合
            ts_codes = set()
            for doc in result:
                if "ts_code" in doc:
                    ts_codes.add(doc["ts_code"])
            
            logger.success(f"从stock_basic集合获取到 {len(ts_codes)} 个目标股票代码")
            
            # 输出详细日志
            if self.verbose:
                sample_codes = list(ts_codes)[:5] if ts_codes else []
                logger.debug(f"样例股票代码: {sample_codes}")
                
            return ts_codes
            
        except Exception as e:
            logger.error(f"查询stock_basic集合失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return set()

    def fetch_stk_rewards_for_ts_code(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取特定股票的管理层薪酬及持股数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            
        Returns:
            管理层薪酬及持股数据DataFrame
        """
        try:
            # 准备参数
            api_name = self.interface_config.get("api_name", "stk_rewards")
            params = self.interface_config.get("params", {}).copy()
            fields = self.interface_config.get("fields", [])
            
            # 添加查询参数
            params["ts_code"] = ts_code
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            # 确保使用正确的字段（根据接口定义）
            if not fields:
                fields = self.interface_config.get("available_fields", [])
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket() if self.port_allocator else None
            use_wan = wan_info is not None
            
            # 调用Tushare API
            if self.verbose:
                logger.debug(f"获取股票 {ts_code} 的管理层薪酬及持股数据...")
                
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            start_time = time.time()
            
            # 增加超时，设置为120秒
            self.client.set_timeout(120)
            
            # 添加异常捕获，以便更好地调试
            try:
                df = self.client.get_data(api_name=api_name, params=params, fields=fields)
                if df is not None and not df.empty:
                    if self.verbose:
                        logger.debug(f"成功获取 {len(df)} 条 {ts_code} 管理层薪酬及持股数据")
            except Exception as e:
                import traceback
                logger.error(f"获取API数据时发生异常 {ts_code}: {str(e)}")
                logger.debug(f"异常详情: {traceback.format_exc()}")
                return pd.DataFrame()
                
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                if self.verbose:
                    logger.debug(f"股票 {ts_code} 无管理层薪酬及持股数据")
                return pd.DataFrame()
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                wan_idx, port = wan_info
                self.port_allocator.release_port(wan_idx, port)
            
            if self.verbose:
                logger.debug(f"获取 {ts_code} 数据耗时 {elapsed:.2f}s")
                
            return df
            
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 管理层薪酬及持股数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return pd.DataFrame()

    def fetch_stk_rewards_batch(self, ts_codes: Set[str], start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        批量获取多个股票的管理层薪酬及持股数据
        
        Args:
            ts_codes: 股票代码集合
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            
        Returns:
            所有股票的管理层薪酬及持股数据合并后的DataFrame
        """
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return pd.DataFrame()
            
        logger.info(f"开始批量获取 {len(ts_codes)} 个股票的管理层薪酬及持股数据")
        
        # 进度统计变量
        processed_count = 0
        success_count = 0
        total_records = 0
        start_time_total = time.time()
        all_data = []
        
        # 控制API调用频率的计数器
        api_call_count = 0
        rate_limit_per_min = 500  # 每分钟最大调用次数
        last_reset_time = time.time()
        
        # 批量处理股票代码
        for ts_code in ts_codes:
            # API调用频率限制
            api_call_count += 1
            if api_call_count >= rate_limit_per_min:
                elapsed_since_reset = time.time() - last_reset_time
                if elapsed_since_reset < 60:
                    sleep_time = 60 - elapsed_since_reset + 1  # 额外添加1秒作为缓冲
                    logger.info(f"达到API调用频率限制，休眠 {sleep_time:.2f} 秒")
                    time.sleep(sleep_time)
                api_call_count = 0
                last_reset_time = time.time()
                
            # 获取单个股票数据
            df = self.fetch_stk_rewards_for_ts_code(ts_code, start_date, end_date)
            processed_count += 1
            
            # 更新进度
            if processed_count % 10 == 0 or processed_count == len(ts_codes):
                elapsed = time.time() - start_time_total
                avg_time = elapsed / processed_count if processed_count > 0 else 0
                remaining = (len(ts_codes) - processed_count) * avg_time
                progress = processed_count / len(ts_codes) * 100
                logger.info(f"进度: {processed_count}/{len(ts_codes)} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            if df is not None and not df.empty:
                success_count += 1
                total_records += len(df)
                all_data.append(df)
                
            # 短暂休眠以避免API调用过于频繁
            time.sleep(0.1)
            
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"成功获取 {success_count}/{len(ts_codes)} 个股票的管理层薪酬及持股数据，共 {len(result_df)} 条记录")
            return result_df
        else:
            logger.warning("没有获取到任何管理层薪酬及持股数据")
            return pd.DataFrame()

    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 待保存的DataFrame
            
        Returns:
            是否成功保存
        """
        # 强制确保使用tushare_data作为数据库名称
        logger.info(f"保存数据到MongoDB数据库：{self.db_name}，集合：{self.collection_name}")
        
        if df is None or df.empty:
            logger.warning("没有数据可保存到MongoDB")
            return False
            
        try:
            # 将DataFrame转换为记录列表
            records = df.to_dict('records')
            
            # 保存到MongoDB
            start_time = time.time()
            
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not self.mongo_client.connect():
                    logger.error("重新连接MongoDB失败")
                    return False
            
            # 使用upsert方式保存，基于ts_code、name、end_date的唯一约束
            result = self.mongo_client.upsert_many(
                self.db_name,
                self.collection_name,
                records,
                unique_keys=["ts_code", "name", "end_date"]
            )
            
            elapsed = time.time() - start_time
            
            # 创建索引
            try:
                # 直接获取数据库并从中获取集合
                db = self.mongo_client.get_db(self.db_name)
                collection = db[self.collection_name]
                
                # 根据接口配置中的index_fields创建索引
                index_fields = self.interface_config.get("index_fields", [])
                if index_fields:
                    # 为ts_code, name, end_date创建复合索引
                    if all(field in index_fields for field in ["ts_code", "name", "end_date"]):
                        collection.create_index(
                            [("ts_code", 1), ("name", 1), ("end_date", 1)],
                            unique=True,
                            background=True
                        )
                        logger.debug("已为字段组合 (ts_code, name, end_date) 创建唯一复合索引")
                    
                    # 创建其他单字段索引
                    for field in ["ts_code", "end_date"]:
                        if field in index_fields:
                            collection.create_index(field)
                            logger.debug(f"已为字段 {field} 创建索引")
                else:
                    # 默认创建索引
                    collection.create_index([("ts_code", 1), ("name", 1), ("end_date", 1)], unique=True)
                    collection.create_index("ts_code")
                    collection.create_index("end_date")
                    logger.debug("已创建默认索引")
            except Exception as e:
                logger.warning(f"创建索引时出错: {str(e)}")
            
            logger.success(f"成功保存 {result} 条记录到 MongoDB，耗时 {elapsed:.2f}s")
            return True
                
        except Exception as e:
            logger.error(f"保存到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def run(self, start_date: str = None, end_date: str = None, full: bool = False) -> bool:
        """
        运行数据获取和保存流程
        
        Args:
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            full: 是否获取全量数据（默认False，只获取最近一周数据）
            
        Returns:
            是否成功
        """
        # 如果未提供日期并且不是全量模式，则设置默认为最近一周
        if not start_date and not end_date and not full:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            logger.info(f"设置默认日期范围: {start_date} - {end_date}")
        
        # 从stock_basic集合获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能从stock_basic集合获取目标股票代码")
            return False
        
        # 批量获取管理层薪酬及持股数据
        df = self.fetch_stk_rewards_batch(target_ts_codes, start_date, end_date)
        if df.empty:
            logger.warning("没有获取到任何管理层薪酬及持股数据")
            return False
            
        # 保存数据到MongoDB
        success = self.save_to_mongodb(df)
        
        # 关闭MongoDB连接
        self.mongo_client.close()
        
        return success


def create_mock_data() -> pd.DataFrame:
    """创建模拟数据用于测试"""
    logger.info("创建模拟管理层薪酬及持股数据用于测试")
    
    # 创建模拟数据
    data = [
        {'ts_code': '000001.SZ', 'ann_date': '20220430', 'end_date': '20211231', 'name': '张三', 'title': '董事长', 'reward': 500.00, 'hold_vol': 10000, 'hold_change': 0},
        {'ts_code': '000001.SZ', 'ann_date': '20220430', 'end_date': '20211231', 'name': '李四', 'title': '总经理', 'reward': 450.00, 'hold_vol': 8000, 'hold_change': 1000},
        {'ts_code': '000002.SZ', 'ann_date': '20220428', 'end_date': '20211231', 'name': '王五', 'title': '董事长', 'reward': 600.00, 'hold_vol': 15000, 'hold_change': 2000},
        {'ts_code': '000002.SZ', 'ann_date': '20220428', 'end_date': '20211231', 'name': '赵六', 'title': '财务总监', 'reward': 350.00, 'hold_vol': 5000, 'hold_change': -1000},
        {'ts_code': '300059.SZ', 'ann_date': '20220425', 'end_date': '20211231', 'name': '钱七', 'title': '董事长', 'reward': 800.00, 'hold_vol': 20000, 'hold_change': 5000},
        {'ts_code': '600000.SH', 'ann_date': '20220426', 'end_date': '20211231', 'name': '孙八', 'title': '董事长', 'reward': 700.00, 'hold_vol': 18000, 'hold_change': 3000},
        {'ts_code': '600519.SH', 'ann_date': '20220422', 'end_date': '20211231', 'name': '周九', 'title': '董事长', 'reward': 1200.00, 'hold_vol': 30000, 'hold_change': 8000},
        {'ts_code': '688981.SH', 'ann_date': '20220420', 'end_date': '20211231', 'name': '吴十', 'title': '董事长', 'reward': 900.00, 'hold_vol': 25000, 'hold_change': 4000}
    ]
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    logger.success(f"已创建 {len(df)} 条模拟管理层薪酬及持股数据")
    return df

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票管理层薪酬及持股数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stk_rewards', help='MongoDB集合名称')
    parser.add_argument('--start-date', help='开始日期，格式YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式YYYYMMDD')
    parser.add_argument('--full', action='store_true', help='获取所有历史数据（默认只获取最近一周）')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--mock', action='store_false', dest='use_real_api', help='使用模拟数据（当API不可用时）')
    parser.add_argument('--use-real-api', action='store_true', default=True, help='使用湘财真实API数据（默认）')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器并运行
    fetcher = StkRewardsFetcher(
        config_path=args.config,
        interface_dir=args.interface_dir,
        target_market_codes=target_market_codes,
        db_name=args.db_name,  # 这个值会被内部强制设为"tushare_data"
        collection_name=args.collection_name,
        verbose=args.verbose
    )
    
    # 使用真实API或模拟数据模式
    if args.use_real_api:
        logger.info("使用湘财Tushare真实API获取数据")
        success = fetcher.run(
            start_date=args.start_date,
            end_date=args.end_date,
            full=args.full
        )
    else:
        logger.info("使用模拟数据模式")
        # 创建模拟数据
        df = create_mock_data()
        
        # 获取目标股票代码以过滤模拟数据
        target_ts_codes = fetcher.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            # 模拟模式下，如果无法获取真实股票代码，使用模拟数据中的所有代码
            target_ts_codes = set(df['ts_code'].unique().tolist())
            logger.warning("无法从数据库获取股票代码，使用模拟数据中的所有代码")
        
        # 过滤数据，只保留目标股票代码
        df_filtered = df[df['ts_code'].isin(target_ts_codes)]
        
        if df_filtered.empty:
            logger.warning("过滤后没有符合条件的管理层薪酬及持股数据")
            sys.exit(1)
        
        # 是否实际保存
        if args.dry_run:
            logger.info("干运行模式，不保存数据")
            success = True
        else:
            # 保存数据到MongoDB
            success = fetcher.save_to_mongodb(df_filtered)
            # 关闭MongoDB连接
            fetcher.mongo_client.close()
    
    if success:
        logger.success("数据获取和保存成功")
        sys.exit(0)
    else:
        logger.error("数据获取或保存失败")
        sys.exit(1)

if __name__ == "__main__":
    main()
