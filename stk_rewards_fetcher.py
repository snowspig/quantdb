#!/usr/bin/env python
"""
高管薪酬和持股数据获取器 - 获取高管薪酬和持股信息并保存到MongoDB

该脚本用于从湘财Tushare获取高管薪酬和持股信息，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=10194

使用方法：
    python stk_rewards_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stk_rewards_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stk_rewards_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python stk_rewards_fetcher.py --full        # 抓取全量历史数据
"""
import os
import sys
import json
import yaml
import time
import random
import asyncio
import pandas as pd
import concurrent.futures
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
    高管薪酬和持股数据获取器
    
    该类用于从Tushare获取高管薪酬和持股信息并保存到MongoDB数据库，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stk_rewards.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        db_name: str = "tushare_data",
        collection_name: str = "stk_rewards",
        verbose: bool = False,
        use_mock: bool = False,
        full_history: bool = False
    ):
        """
        初始化高管薪酬和持股数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            target_market_codes: 目标市场代码集合
            db_name: MongoDB数据库名称
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
            use_mock: 是否使用模拟数据
            full_history: 是否获取全量历史数据
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.db_name = "tushare_data"  # 强制使用tushare_data作为数据库名
        self.collection_name = collection_name
        self.verbose = verbose
        self.use_mock = use_mock
        self.full_history = full_history

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

        # 加载配置
        self.config = self._load_config()
        self.interface_config = self._load_interface_config()
        
        if not self.use_mock:
            # 初始化Tushare客户端
            self.client = self._init_client()
            
            # 初始化MongoDB客户端
            self.mongo_client = self._init_mongo_client()
            
            # 初始化多WAN口管理器
            self.port_allocator = self._init_port_allocator()


    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            if self.use_mock:
                logger.info("使用模拟数据模式，不加载实际配置文件")
                return {
                    "tushare": {"token": "mock_token"},
                    "mongodb": {"uri": "", "host": "localhost", "port": 27017},
                    "wan": {"enabled": False}
                }
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                return config
        except Exception as e:
            if self.use_mock:
                logger.warning(f"模拟模式下配置文件加载失败，使用默认配置: {str(e)}")
                return {
                    "tushare": {"token": "mock_token"},
                    "mongodb": {"uri": "", "host": "localhost", "port": 27017},
                    "wan": {"enabled": False}
                }
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
            "description": "高管薪酬和持股信息",
            "api_name": "stk_rewards",
            "fields": [],
            "params": {},
            "available_fields": [
                "ts_code", "ann_date", "end_date", "name", 
                "title", "reward", "hold_vol"
            ]
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
        if self.use_mock:
            # 生成模拟数据
            ts_codes = set()
            for market_code in self.target_market_codes:
                for i in range(10):  # 每个市场生成10个模拟代码
                    if market_code in ("00", "30"):
                        ts_codes.add(f"{market_code}{random.randint(1, 9999):04d}.SZ")
                    else:  # 60, 68
                        ts_codes.add(f"{market_code}{random.randint(1, 9999):04d}.SH")
            logger.info(f"已生成 {len(ts_codes)} 个模拟股票代码")
            return ts_codes

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

    def create_mock_data(self, ts_code: str = None) -> pd.DataFrame:
        """
        创建模拟的高管薪酬和持股数据
        
        Args:
            ts_code: 股票代码，如果为None则生成多个股票的数据
            
        Returns:
            模拟数据DataFrame
        """
        logger.info("生成模拟的高管薪酬和持股数据")
        
        # 定义数据结构
        fields = [
            "ts_code", "ann_date", "end_date", "name", 
            "title", "reward", "hold_vol"
        ]
        
        # 生成固定数量的记录
        records = []
        num_records = 100  # 模拟数据条数
        
        # 如果指定了ts_code，则只为该股票生成数据
        ts_codes = [ts_code] if ts_code else [
            f"{mc}{random.randint(1, 9999):04d}.{'SZ' if mc in ('00', '30') else 'SH'}"
            for mc in self.target_market_codes
            for _ in range(5)  # 每个市场代码生成5个股票
        ]
        
        # 定义职位列表
        titles = ["董事长", "总经理", "财务总监", "副总经理", "董事", "独立董事", "监事", "董事会秘书"]
        
        # 定义姓名列表
        names = ["张三", "李四", "王五", "赵六", "钱七", "孙八", "周九", "吴十", 
                "郑一", "王二", "陈三", "林四", "黄五", "刘六"]
        
        # 定义截止日期列表
        end_dates = ["20220331", "20220630", "20220930", "20221231", 
                    "20230331", "20230630", "20230930", "20231231"]
        
        # 随机生成记录
        for _ in range(num_records):
            ts_code = random.choice(ts_codes)
            end_date = random.choice(end_dates)
            
            # 公告日期晚于截止日期
            end_date_obj = datetime.strptime(end_date, "%Y%m%d")
            ann_delay = random.randint(1, 60)  # 1-60天的公告延迟
            ann_date_obj = end_date_obj + timedelta(days=ann_delay)
            ann_date = ann_date_obj.strftime("%Y%m%d")
            
            record = {
                "ts_code": ts_code,
                "ann_date": ann_date,
                "end_date": end_date,
                "name": random.choice(names),
                "title": random.choice(titles),
                "reward": round(random.uniform(10, 500) * 10000, 2),  # 10-500万的薪酬
                "hold_vol": int(random.uniform(0, 1000) * 1000)  # 0-100万股的持股
            }
            records.append(record)
        
        # 创建DataFrame
        df = pd.DataFrame(records)

        # 输出数据样例
        if self.verbose and not df.empty:
            logger.debug(f"模拟数据样例:\n{df.head(3)}")
            
        return df

    def fetch_stk_rewards(self, ts_code: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """
        获取高管薪酬和持股信息
        
        Args:
            ts_code: 股票代码，如果为None则获取所有股票
            end_date: 截止日期，格式：YYYYMMDD
            
        Returns:
            高管薪酬和持股信息DataFrame，如果失败则返回None
        """
        if self.use_mock:
            # 创建模拟数据
            logger.info(f"使用模拟数据模式，生成模拟高管薪酬和持股信息")
            return self.create_mock_data(ts_code)
            
        try:
            # 准备参数
            api_name = self.interface_config.get("api_name", "stk_rewards")
            params = self.interface_config.get("params", {}).copy()  # 创建副本以避免修改原始配置
            fields = self.interface_config.get("fields", [])
            
            # 添加API必需参数
            if ts_code:
                params["ts_code"] = ts_code
                
            if end_date:
                params["end_date"] = end_date
            
            # 确保使用正确的字段（根据接口定义）
            if not fields:
                fields = self.interface_config.get("available_fields", [])
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket() if self.port_allocator else None
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取高管薪酬和持股信息... {f'ts_code={ts_code}' if ts_code else ''}")
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            start_time = time.time()
            
            # 使用客户端获取数据
            logger.debug(f"API名称: {api_name}, 参数: {params}, 字段: {fields if self.verbose else '...'}")
            
            # 增加超时，设置为120秒
            self.client.set_timeout(120)
            logger.info(f"增加API请求超时时间为120秒，提高网络可靠性")
            
            # 添加异常捕获，以便更好地调试
            try:
                df = self.client.get_data(api_name=api_name, params=params, fields=fields)
                if df is not None and not df.empty:
                    logger.success(f"成功获取数据，行数: {len(df)}, 列数: {df.shape[1]}")
                    if self.verbose:
                        logger.debug(f"列名: {list(df.columns)}")
            except Exception as e:
                import traceback
                logger.error(f"获取API数据时发生异常: {str(e)}")
                logger.debug(f"异常详情: {traceback.format_exc()}")
                return None
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                logger.warning(f"API返回数据为空 {f'对于ts_code={ts_code}' if ts_code else ''}")
                return pd.DataFrame(columns=fields if fields else [])
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                wan_idx, port = wan_info
                self.port_allocator.release_port(wan_idx, port)
            
            logger.success(f"成功获取 {len(df)} 条高管薪酬和持股信息，耗时 {elapsed:.2f}s")
            
            # 如果使用详细日志，输出数据示例
            if self.verbose and not df.empty:
                logger.debug(f"数据示例：\n{df.head(3)}")
                
            return df
            
        except Exception as e:
            logger.error(f"获取高管薪酬和持股信息失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return None

    async def fetch_stocks_data_async(self, ts_codes: Set[str], end_dates: List[str] = None) -> pd.DataFrame:
        """
        异步获取多个股票的高管薪酬和持股信息
        
        Args:
            ts_codes: 股票代码集合
            end_dates: 截止日期列表，如果为None则使用最近日期
            
        Returns:
            合并后的数据DataFrame
        """
        if self.use_mock:
            # 在模拟模式下直接返回模拟数据
            return self.create_mock_data()
            
        logger.info(f"开始异步获取 {len(ts_codes)} 支股票的高管薪酬和持股信息")
        
        all_dfs = []
        ts_code_list = sorted(list(ts_codes))  # 排序以确保稳定的处理顺序
        
        # 使用线程池并发请求数据
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            
            # 对每个股票代码提交任务
            for ts_code in ts_code_list:
                if end_dates:
                    # 如果有指定多个日期，则对每个日期获取数据
                    for end_date in end_dates:
                        futures.append(executor.submit(self.fetch_stk_rewards, ts_code, end_date))
                else:
                    # 否则只获取一次数据（无指定日期）
                    futures.append(executor.submit(self.fetch_stk_rewards, ts_code, None))
            
            # 收集结果
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        all_dfs.append(df)
                        if (i + 1) % 10 == 0 or (i + 1) == len(futures):
                            logger.info(f"已完成 {i + 1}/{len(futures)} 个请求")
                except Exception as e:
                    logger.error(f"处理异步请求结果时出错: {str(e)}")
        
        # 合并所有结果
        if not all_dfs:
            logger.warning("未获取到任何数据")
            return pd.DataFrame()
            
        # 合并数据
        combined_df = pd.concat(all_dfs, ignore_index=True)
        logger.success(f"成功获取 {len(combined_df)} 条高管薪酬和持股信息")
        
        return combined_df

    def determine_date_ranges(self) -> List[str]:
        """
        确定需要获取的日期范围
        
        Returns:
            日期列表，格式：YYYYMMDD
        """
        if self.full_history:
            # 全量历史数据模式：返回多个季度末日期
            # 一般从2007年开始有数据
            start_year = 2007
            end_year = datetime.now().year
            
            dates = []
            for year in range(start_year, end_year + 1):
                # 添加四个季度末日期
                dates.extend([f"{year}0331", f"{year}0630", f"{year}0930", f"{year}1231"])
            
            # 过滤出不大于当前日期的日期
            current_date = datetime.now().strftime("%Y%m%d")
            dates = [d for d in dates if d <= current_date]
            
            logger.info(f"全量历史数据模式：将获取 {len(dates)} 个季度末日期的数据，从 {dates[0]} 到 {dates[-1]}")
            return dates
        else:
            # 增量更新模式：仅返回最近一个季度末日期
            now = datetime.now()
            year = now.year
            month = now.month
            
            # 根据当前月份确定最近的季度末月份
            if month <= 3:
                # 去年第四季度末
                date = f"{year-1}1231"
            elif month <= 6:
                # 今年第一季度末
                date = f"{year}0331"
            elif month <= 9:
                # 今年第二季度末
                date = f"{year}0630"
            else:
                # 今年第三季度末
                date = f"{year}0930"
                
            logger.info(f"增量更新模式：将获取 {date} 的高管薪酬和持股数据")
            return [date]

    def filter_data_by_market_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根据目标市场代码过滤数据
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            过滤后的数据DataFrame
        """
        if df is None or df.empty:
            return pd.DataFrame()
            
        logger.info(f"过滤前数据行数: {len(df)}")
        
        # 确保ts_code列存在
        if 'ts_code' not in df.columns:
            logger.error("数据中没有ts_code列，无法按市场代码过滤")
            return df
            
        # 从ts_code提取市场代码（前两位）
        df_with_market = df.copy()
        df_with_market['market_code'] = df_with_market['ts_code'].str[:2]
        
        # 根据目标市场代码过滤
        filtered_df = df_with_market[df_with_market['market_code'].isin(self.target_market_codes)]
        
        # 删除临时列
        if 'market_code' in filtered_df.columns:
            filtered_df = filtered_df.drop(columns=['market_code'])
            
        logger.info(f"过滤后数据行数: {len(filtered_df)}")
        
        if self.verbose:
            # 统计过滤后各市场代码的数据量
            market_counts = {}
            for code in self.target_market_codes:
                count = len(df_with_market[df_with_market['market_code'] == code])
                market_counts[code] = count
                
            logger.debug(f"过滤后各市场代码数据量: {market_counts}")
            
        return filtered_df

    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 待保存的DataFrame
            
        Returns:
            是否成功保存
        """
        if self.use_mock:
            logger.info("模拟模式，不实际保存数据到MongoDB")
            return True
            
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
            
            # 删除集合中的旧数据，仅在全量历史模式下
            if self.full_history:
                if self.verbose:
                    logger.debug(f"全量历史模式：清空集合 {self.db_name}.{self.collection_name} 中的旧数据")
                self.mongo_client.delete_many(self.collection_name, {}, self.db_name)
            
            # 批量插入新数据
            if self.verbose:
                logger.debug(f"向集合 {self.db_name}.{self.collection_name} 插入 {len(records)} 条记录")
                
            # 对于增量更新模式，删除集合中与新数据的ts_code和end_date相同的旧数据
            if not self.full_history and records:
                # 提取新数据中的所有ts_code和end_date组合
                ts_codes = set(record['ts_code'] for record in records if 'ts_code' in record)
                end_dates = set(record['end_date'] for record in records if 'end_date' in record)
                
                if ts_codes and end_dates:
                    # 删除对应的现有数据
                    for end_date in end_dates:
                        for ts_code in ts_codes:
                            query = {'ts_code': ts_code, 'end_date': end_date}
                            self.mongo_client.delete_many(self.collection_name, query, self.db_name)
                            if self.verbose:
                                logger.debug(f"删除旧数据: ts_code={ts_code}, end_date={end_date}")
            
            # 尝试批量插入数据，捕获可能的错误
            try:
                result = self.mongo_client.insert_many(self.collection_name, records, self.db_name)
                inserted_count = len(records) if result else 0
            except Exception as e:
                logger.error(f"批量插入数据失败: {str(e)}，尝试逐条插入...")
                
                # 逐条插入以跳过导致错误的记录
                inserted_count = 0
                for record in records:
                    try:
                        self.mongo_client.insert_one(self.collection_name, record, self.db_name)
                        inserted_count += 1
                    except Exception as insert_err:
                        logger.error(f"插入记录失败: {str(insert_err)}")
                        continue
            
            elapsed = time.time() - start_time
            logger.success(f"成功保存 {inserted_count}/{len(records)} 条记录到MongoDB，耗时: {elapsed:.2f}s")
            
            return inserted_count > 0
            
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
            
    async def run(self):
        """
        运行高管薪酬和持股数据获取流程
        """
        logger.info("开始获取高管薪酬和持股数据")
        
        # 获取目标股票代码（根据市场代码过滤）
        ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not ts_codes:
            logger.error("未获取到目标股票代码，无法继续执行")
            return False
            
        logger.success(f"将处理 {len(ts_codes)} 支股票的高管薪酬和持股数据")
        
        # 确定日期范围
        end_dates = self.determine_date_ranges()
        logger.info(f"将获取以下日期的数据: {end_dates}")
        
        # 异步获取数据
        data_df = await self.fetch_stocks_data_async(ts_codes, end_dates)
        if data_df is None or data_df.empty:
            logger.error("获取数据失败或数据为空")
            return False
            
        # 过滤数据
        filtered_df = self.filter_data_by_market_codes(data_df)
        if filtered_df.empty:
            logger.error("过滤后数据为空")
            return False
            
        # 保存到MongoDB
        success = self.save_to_mongodb(filtered_df)
        if not success:
            logger.error("保存数据到MongoDB失败")
            return False
            
        logger.success("成功完成高管薪酬和持股数据获取和保存流程")
        return True

async def main():
    """主函数"""
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="获取高管薪酬和持股数据")
    parser.add_argument("--verbose", action="store_true", help="输出详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式")
    parser.add_argument("--full", action="store_true", help="获取全量历史数据")
    
    args = parser.parse_args()
    
    # 创建高管薪酬和持股数据获取器
    fetcher = StkRewardsFetcher(
        verbose=args.verbose,
        use_mock=args.mock,
        full_history=args.full
    )
    
    # 运行数据获取流程
    success = await fetcher.run()
    
    # 退出程序
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    # 执行主函数
    asyncio.run(main())
