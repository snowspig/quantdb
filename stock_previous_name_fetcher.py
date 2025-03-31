#!/usr/bin/env python
"""
股票曾用名获取器 - 获取股票曾用名信息并保存到MongoDB

该脚本用于从湘财Tushare获取股票曾用名信息，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=10248

使用方法：
    python stock_previous_name_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stock_previous_name_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stock_previous_name_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
"""
import os
import sys
import json
import yaml
import time
import asyncio
import pandas as pd
from datetime import datetime
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

class StockPreviousNameFetcher:
    """
    股票曾用名获取器
    
    该类用于从Tushare获取股票曾用名信息并保存到MongoDB数据库，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "previous_name.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        db_name: str = "tushare_data",
        collection_name: str = "previous_name",
        verbose: bool = False
    ):
        """
        初始化股票曾用名获取器
        
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
            "description": "股票更名信息",
            "api_name": "previous_name",
            "fields": [],
            "params": {},
            "available_fields": [
                "ts_code", "begindate", "enddate", "ann_dt", "s_info_name", "changereason"
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

    def fetch_previous_name_by_period(self, start_date: str, end_date: str, wan_idx: int = None, port: int = None) -> Optional[pd.DataFrame]:
        """
        按时间段获取股票曾用名信息
        
        Args:
            start_date: 开始日期，格式为 YYYYMMDD
            end_date: 结束日期，格式为 YYYYMMDD
            wan_idx: WAN接口索引，如果为None则获取新的WAN接口
            port: WAN接口端口，如果为None则分配新的端口
            
        Returns:
            股票曾用名信息DataFrame，如果失败则返回None
        """
        try:
            # 准备参数
            api_name = self.interface_config.get("api_name", "previous_name")
            params = self.interface_config.get("params", {}).copy()  # 创建参数的副本，避免修改原始参数
            fields = self.interface_config.get("fields", [])
            
            # 添加日期范围参数
            params.update({
                "start_date": start_date,
                "end_date": end_date
            })
            
            # 确保使用正确的字段（根据接口定义）
            if not fields:
                fields = self.interface_config.get("available_fields", [])
            
            # 确定是否使用现有的WAN接口或获取新的
            use_wan = False
            wan_info = None
            
            if wan_idx is not None and port is not None:
                # 使用传入的WAN接口和端口
                wan_info = (wan_idx, port)
                use_wan = True
            elif self.port_allocator:
                # 获取新的WAN接口和端口
                wan_info = self._get_wan_socket()
                use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取 {start_date} 至 {end_date} 期间的股票曾用名信息...")
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            start_time = time.time()
            
            # 使用客户端获取数据
            logger.debug(f"API名称: {api_name}, 参数: {params}, 字段: {fields if self.verbose else '...'}")
            
            # 增加超时，设置为120秒
            self.client.set_timeout(120)
            
            # 添加异常捕获，以便更好地调试
            try:
                df = self.client.get_data(api_name=api_name, params=params, fields=fields)
                if df is not None and not df.empty:
                    logger.success(f"成功获取 {start_date} 至 {end_date} 期间数据，行数: {len(df)}, 列数: {df.shape[1]}")
                    if self.verbose:
                        logger.debug(f"列名: {list(df.columns)}")
            except Exception as e:
                import traceback
                logger.error(f"获取 {start_date} 至 {end_date} 期间API数据时发生异常: {str(e)}")
                logger.debug(f"异常详情: {traceback.format_exc()}")
                return None
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                logger.warning(f"API返回 {start_date} 至 {end_date} 期间数据为空")
                return pd.DataFrame()
            
            logger.success(f"成功获取 {start_date} 至 {end_date} 期间 {len(df)} 条股票曾用名信息，耗时 {elapsed:.2f}s")
            
            # 如果使用详细日志，输出数据示例
            if self.verbose and not df.empty:
                logger.debug(f"数据示例：\n{df.head(3)}")
                
            return df
            
        except Exception as e:
            logger.error(f"获取 {start_date} 至 {end_date} 期间股票曾用名信息失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return None
            
    def fetch_previous_name(self) -> Optional[pd.DataFrame]:
        """
        获取股票曾用名信息，根据数据量分段获取
        
        Returns:
            股票曾用名信息DataFrame，如果失败则返回None
        """
        try:
            # 定义时间段，每5年为一个区间，从1990年开始
            date_ranges = [
                ("19900101", "19941231"),
                ("19950101", "19991231"),
                ("20000101", "20041231"),
                ("20050101", "20091231"),
                ("20100101", "20141231"),
                ("20150101", "20191231"),
                ("20200101", "20241231"),
                ("20250101", "20291231")  # 预留未来5年的数据区间
            ]
            
            all_data = []
            
            # 如果有多WAN接口支持，创建接口池
            wan_pool = []
            if self.port_allocator:
                # 获取所有可用的WAN接口索引
                indices = self.port_allocator.get_available_wan_indices()
                if indices:
                    for idx in indices:
                        # 为每个WAN接口分配一个端口
                        port = self.port_allocator.allocate_port(idx)
                        if port:
                            wan_pool.append((idx, port))
                            logger.info(f"WAN池添加接口 {idx}，端口 {port}")
            
            # 确定是否可以使用多WAN接口
            use_wan_pool = len(wan_pool) > 0
            
            # 并行或串行获取各时间段的数据
            start_time = time.time()
            logger.info(f"开始分段获取股票曾用名数据，共 {len(date_ranges)} 个时间段")
            
            for i, (start_date, end_date) in enumerate(date_ranges):
                # 如果有WAN池，轮询使用不同的WAN接口
                if use_wan_pool:
                    wan_idx, port = wan_pool[i % len(wan_pool)]
                    logger.info(f"使用WAN池中的接口 {wan_idx}，端口 {port} 获取 {start_date} 至 {end_date} 期间数据")
                    df = self.fetch_previous_name_by_period(start_date, end_date, wan_idx, port)
                else:
                    # 不使用WAN池，每次请求都获取新的WAN接口（如果可用）
                    df = self.fetch_previous_name_by_period(start_date, end_date)
                
                # 添加到结果列表
                if df is not None and not df.empty:
                    all_data.append(df)
                    logger.info(f"成功获取 {start_date} 至 {end_date} 期间数据，共 {len(df)} 条记录")
                
                # 适当休眠，避免请求过于频繁
                time.sleep(1)
            
            # 释放WAN端口池
            if use_wan_pool:
                for wan_idx, port in wan_pool:
                    self.port_allocator.release_port(wan_idx, port)
                    logger.debug(f"释放WAN接口 {wan_idx}，端口 {port}")
            
            # 合并所有数据
            if not all_data:
                logger.error("所有时间段均未获取到数据")
                return None
            
            # 合并所有DataFrame
            df_combined = pd.concat(all_data, ignore_index=True)
            
            # 删除可能的重复记录
            if not df_combined.empty:
                # 确定去重的列，通常是主键字段
                if "ts_code" in df_combined.columns and "begindate" in df_combined.columns:
                    df_combined = df_combined.drop_duplicates(subset=["ts_code", "begindate"])
                    logger.info(f"去重后数据条数: {len(df_combined)}")
            
            elapsed = time.time() - start_time
            logger.success(f"成功获取所有时间段的股票曾用名信息，共 {len(df_combined)} 条记录，总耗时 {elapsed:.2f}s")
            
            return df_combined
            
        except Exception as e:
            logger.error(f"分段获取股票曾用名信息失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
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

    def filter_previous_name_data(self, df: pd.DataFrame, target_ts_codes: Set[str]) -> pd.DataFrame:
        """
        根据目标股票代码集合过滤曾用名数据
        
        Args:
            df: 股票曾用名信息数据
            target_ts_codes: 目标股票代码集合
        
        Returns:
            过滤后的数据
        """
        if df is None or df.empty:
            logger.warning("没有股票曾用名数据可过滤")
            return pd.DataFrame()
        
        logger.info(f"过滤前股票曾用名数量: {len(df)}")
        
        # 确保ts_code列存在
        if 'ts_code' not in df.columns:
            logger.error("数据中没有ts_code列，无法按股票代码过滤")
            return df
        
        # 过滤数据
        df_filtered = df[df['ts_code'].isin(target_ts_codes)].copy()
        
        # 输出过滤统计信息
        logger.info(f"过滤后股票曾用名数量: {len(df_filtered)}")
        
        # 详细统计信息
        if self.verbose:
            # 统计各市场的股票数量
            if not df_filtered.empty and 'ts_code' in df_filtered.columns:
                # 从ts_code提取市场代码
                df_filtered['market_code'] = df_filtered['ts_code'].str[:6].str[:2]
                market_stats = df_filtered['market_code'].value_counts().to_dict()
                
                logger.debug("过滤后各市场代码分布:")
                for code, count in sorted(market_stats.items()):
                    logger.debug(f"  {code}: {count} 记录")
        
        return df_filtered
    
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
            
            # 删除集合中的旧数据（可选）
            if self.verbose:
                logger.debug(f"清空集合 {self.db_name}.{self.collection_name} 中的旧数据")
            
            self.mongo_client.delete_many(self.collection_name, {}, self.db_name)
            
            # 批量插入新数据
            if self.verbose:
                logger.debug(f"向集合 {self.db_name}.{self.collection_name} 插入 {len(records)} 条记录")
                
            result = self.mongo_client.insert_many(self.collection_name, records, self.db_name)
            
            elapsed = time.time() - start_time
            inserted_count = len(result.inserted_ids) if result else 0

            
            if inserted_count > 0:
                # 创建索引 - 修正获取集合的方式
                try:
                    # 直接获取数据库并从中获取集合，避免混淆参数顺序
                    db = self.mongo_client.get_db(self.db_name)
                    collection = db[self.collection_name]
                    
                    # 根据接口配置中的index_fields创建索引
                    index_fields = self.interface_config.get("index_fields", [])
                    if index_fields:
                        for field in index_fields:
                            collection.create_index(field)
                            logger.debug(f"已为字段 {field} 创建索引")
                    else:
                        # 默认为ts_code创建索引
                        collection.create_index("ts_code")
                        logger.debug("已为默认字段ts_code创建索引")
                        
                    # 为update_time创建索引，便于查询最新数据
                    collection.create_index("update_time")
                except Exception as e:
                    logger.warning(f"创建索引时出错: {str(e)}")
                
                logger.success(f"成功将 {inserted_count} 条记录保存到 MongoDB: {self.db_name}.{self.collection_name}，耗时 {elapsed:.2f}s")
                return True
            else:
                logger.error(f"保存到MongoDB失败，未插入任何记录")
                return False
                
        except Exception as e:
            logger.error(f"保存到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
            
    def run(self) -> bool:
        """
        运行数据获取和保存流程
        
        Returns:
            是否成功
        """
        # 从stock_basic集合获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能从stock_basic集合获取目标股票代码")
            return False
            
        logger.info(f"开始分批获取股票曾用名数据，将按5年为单位分段处理，避免超过10000条数据限制")
        
        # 分段获取股票曾用名信息数据
        df = self.fetch_previous_name()
        if df is None or df.empty:
            logger.error("获取股票曾用名信息失败")
            return False
            
        logger.info(f"成功获取到 {len(df)} 条曾用名数据，开始进行过滤处理")
        
        # 过滤数据，只保留目标股票
        filtered_df = self.filter_previous_name_data(df, target_ts_codes)
        if filtered_df.empty:
            logger.warning("过滤后没有符合条件的股票曾用名数据")
            return False
        
        logger.info(f"过滤后保留了 {len(filtered_df)} 条曾用名数据")
            
        # 添加更新时间字段
        filtered_df['update_time'] = datetime.now().isoformat()
        
        # 确保时间字段格式统一
        if 'begindate' in filtered_df.columns:
            logger.info("处理begindate字段的格式")
            # 确保begindate是字符串格式且为8位数字格式 YYYYMMDD
            filtered_df['begindate'] = filtered_df['begindate'].astype(str)
            filtered_df['begindate'] = filtered_df['begindate'].str.replace('-', '')
        
        if 'enddate' in filtered_df.columns:
            logger.info("处理enddate字段的格式")
            # 处理enddate字段，可能包含None值
            filtered_df['enddate'] = filtered_df['enddate'].astype(str)
            filtered_df['enddate'] = filtered_df['enddate'].str.replace('-', '')
            filtered_df['enddate'] = filtered_df['enddate'].replace('None', None)
        
        # 保存数据到MongoDB
        logger.info(f"开始保存 {len(filtered_df)} 条数据到MongoDB")
        success = self.save_to_mongodb(filtered_df)
        
        # 关闭MongoDB连接
        self.mongo_client.close()
        
        if success:
            logger.success(f"成功完成股票曾用名数据的分段获取和保存，共处理 {len(filtered_df)} 条记录")
        else:
            logger.error("保存股票曾用名数据到MongoDB失败")
        
        return success


def create_mock_data() -> pd.DataFrame:
    """创建模拟数据用于测试"""
    logger.info("创建模拟股票曾用名信息数据用于测试")
    
    # 创建模拟数据
    data = [
        {'ts_code': '000001.SZ', 'begindate': '19990101', 'enddate': '20050505', 'ann_dt': '19981231', 's_info_name': '原名称1', 'changereason': 200001000},
        {'ts_code': '000002.SZ', 'begindate': '20000101', 'enddate': '20100505', 'ann_dt': '19991231', 's_info_name': '原名称2', 'changereason': 200001000},
        {'ts_code': '300059.SZ', 'begindate': '20100101', 'enddate': '20180505', 'ann_dt': '20091231', 's_info_name': '原名称3', 'changereason': 200001000},
        {'ts_code': '300750.SZ', 'begindate': '20150101', 'enddate': None, 'ann_dt': '20141231', 's_info_name': '原名称4', 'changereason': 200004000},
        {'ts_code': '600000.SH', 'begindate': '19980101', 'enddate': '20020505', 'ann_dt': '19971231', 's_info_name': '原名称5', 'changereason': 200001000},
        {'ts_code': '600519.SH', 'begindate': '19970101', 'enddate': None, 'ann_dt': '19961231', 's_info_name': '原名称6', 'changereason': 200001000},
        {'ts_code': '688981.SH', 'begindate': '20180101', 'enddate': '20220505', 'ann_dt': '20171231', 's_info_name': '原名称7', 'changereason': 200001000}
    ]
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    logger.success(f"已创建 {len(df)} 条模拟股票曾用名数据")
    return df

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票曾用名信息并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='previous_name', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--mock', action='store_false', dest='use_real_api', help='使用模拟数据（当API不可用时）')
    parser.add_argument('--use-real-api', action='store_true', default=True, help='使用湘财真实API数据（默认）')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器并运行
    fetcher = StockPreviousNameFetcher(
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
        success = fetcher.run()
    else:
        logger.info("使用模拟数据模式")
        # 创建模拟数据
        df = create_mock_data()
        # 获取目标股票代码
        target_ts_codes = fetcher.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            # 模拟模式下，如果无法获取真实股票代码，使用模拟数据中的所有代码
            target_ts_codes = set(df['ts_code'].tolist())
            logger.warning("无法从数据库获取股票代码，使用模拟数据中的所有代码")
        # 过滤数据
        filtered_df = fetcher.filter_previous_name_data(df, target_ts_codes)
        if filtered_df.empty:
            logger.warning("过滤后没有符合条件的股票曾用名数据")
            sys.exit(1)
        # 添加更新时间字段
        filtered_df['update_time'] = datetime.now().isoformat()
        # 是否实际保存
        if args.dry_run:
            logger.info("干运行模式，不保存数据")
            success = True
        else:
            # 保存数据到MongoDB
            success = fetcher.save_to_mongodb(filtered_df)
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