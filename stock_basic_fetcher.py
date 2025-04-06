#!/usr/bin/env python
"""
Stock Basic Fetcher - 获取股票基本信息并保存到MongoDB

该脚本用于从湘财Tushare获取股票基本信息，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=25

使用方法：
    python stock_basic_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stock_basic_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stock_basic_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
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

class StockBasicFetcher:
    """
    股票基本信息获取器

    该类用于从Tushare获取股票基本信息并保存到MongoDB数据库，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stock_basic.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        excluded_stocks: Set[str] = {"600349.SH", "300361.SZ", "300728.SZ"},
        db_name: str = None,
        collection_name: str = "stock_basic",
        verbose: bool = False
    ):
        """
        初始化股票基本信息获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            target_market_codes: 目标市场代码集合
            excluded_stocks: 需要排除的特定股票代码集合
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.excluded_stocks = excluded_stocks
        self.collection_name = collection_name
        self.verbose = verbose

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

        # 加载配置
        self.config = self._load_config()
        self.interface_config = self._load_interface_config()
        
        # 获取token和api_url - 从配置文件读取
        tushare_config = self.config.get("tushare", {})
        self.token = tushare_config.get("token", "")
        self.api_url = tushare_config.get("api_url", "")
        
        # 从配置中读取db_name
        mongodb_config = self.config.get("mongodb", {})
        config_db_name = mongodb_config.get("db_name", "tushare_data")
        # 如果未传入db_name或传入为None，则使用配置文件中的值
        self.db_name = db_name if db_name is not None else config_db_name
        logger.debug(f"使用数据库名称: {self.db_name}")
        
        if not self.token:
            logger.error("未配置Tushare API Key")
            sys.exit(1)
            
        # 记录API配置信息
        mask_token = self.token[:4] + '*' * (len(self.token) - 8) + self.token[-4:] if len(self.token) > 8 else '***'
        logger.debug(f"获取到的API token长度: {len(self.token)}")
        logger.debug(f"获取到的API URL: {self.api_url}")
        
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
            "description": "股票基本信息",
            "api_name": "stock_basic",
            "fields": [],
            "params": {},
            "available_fields": [
                "ts_code", "symbol", "name", "area", "industry", "fullname", 
                "enname", "cnspell", "market", "exchange", "curr_type", 
                "list_status", "list_date", "delist_date", "is_hs"
            ]
        }

    def _init_client(self) -> TushareClient:
        """初始化Tushare客户端"""
        try:
            # 从配置中获取token和api_url
            tushare_config = self.config.get("tushare", {})
            token = tushare_config.get("token", "")
            api_url = tushare_config.get("api_url", "")
            
            if not token:
                logger.error("未配置Tushare API Key")
                sys.exit(1)
                
            # 记录token和api_url信息
            mask_token = token[:4] + '*' * (len(token) - 8) + token[-4:] if len(token) > 8 else '***'
            logger.debug(f"使用token初始化客户端: {mask_token} (长度: {len(token)}), API URL: {api_url}")
                
            # 创建并返回客户端实例，传递api_url
            return TushareClient(token=token, api_url=api_url)
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
            auth_source = mongodb_config.get("auth_source", "admin")
            auth_mechanism = mongodb_config.get("auth_mechanism", "SCRAM-SHA-1")
            
            # 获取MongoDB连接选项
            options = mongodb_config.get("options", {})
            connection_pool_size = options.get("max_pool_size", 100)
            timeout_ms = options.get("connect_timeout_ms", 30000)
            
            # 记录MongoDB连接信息
            logger.debug(f"MongoDB连接信息: {host}:{port}, 认证源: {auth_source}, 认证机制: {auth_mechanism}")
            logger.debug(f"MongoDB连接选项: 连接池大小: {connection_pool_size}, 超时: {timeout_ms}ms")
            logger.debug(f"MongoDB数据库名称: {self.db_name}")
            
            # 创建MongoDB客户端 - 使用从配置中读取的参数
            mongo_client = MongoDBClient(
                uri=uri,
                host=host,
                port=port,
                username=username,
                password=password,
                db_name=self.db_name,  # 使用从配置或初始化参数中获取的db_name
                auth_source=auth_source,
                auth_mechanism=auth_mechanism,
                connection_pool_size=connection_pool_size,
                timeout_ms=timeout_ms
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

    def fetch_stock_basic(self) -> Optional[pd.DataFrame]:
        """
        获取股票基本信息
        
        Returns:
            股票基本信息DataFrame，如果失败则返回None
        """
        try:
            # 准备参数
            api_name = self.interface_config.get("api_name", "stock_basic")
            params = self.interface_config.get("params", {})
            fields = self.interface_config.get("fields", [])
            
            # 确保使用正确的字段（根据接口定义）
            if not fields:
                fields = self.interface_config.get("available_fields", [])
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket() if self.port_allocator else None
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取股票基本信息...")
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
                
                # 在实际应用中创建绑定到特定WAN接口的socket，但这里简化处理
                # 因为目前环境可能无法实际绑定WAN接口，所以仅记录日志
            
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
                logger.error("API返回数据为空")
                return None
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                wan_idx, port = wan_info
                self.port_allocator.release_port(wan_idx, port)
            
            logger.success(f"成功获取 {len(df)} 条股票基本信息，耗时 {elapsed:.2f}s")
            
            # 如果使用详细日志，输出数据示例
            if self.verbose and not df.empty:
                logger.debug(f"数据示例：\n{df.head(3)}")
                
            return df
            
        except Exception as e:
            logger.error(f"获取股票基本信息失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return None

    def filter_stock_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤股票数据，只保留00、30、60和68板块的股票，并排除特定股票
        同时只保留上市日期(list_date)小于等于今天的股票
        
        Args:
            df: 股票基本信息数据
        
        Returns:
            过滤后的数据
        """
        if df is None or df.empty:
            logger.warning("没有股票数据可过滤")
            return pd.DataFrame()
        
        logger.info(f"过滤前股票数量: {len(df)}")
        
        # 确保symbol列存在
        if 'symbol' not in df.columns:
            logger.error("数据中没有symbol列，无法按市场代码过滤")
            
            # 尝试使用ts_code列提取symbol
            if 'ts_code' in df.columns:
                logger.info("尝试从ts_code列提取symbol信息")
                # ts_code格式通常是'000001.SZ'，我们提取前6位
                df['symbol'] = df['ts_code'].str.split('.').str[0]
            else:
                logger.error("数据中既没有symbol也没有ts_code列，无法过滤")
                return df
            
        # 确保symbol列是字符串类型
        df['symbol'] = df['symbol'].astype(str)
        
        # 提取前两位作为市场代码并过滤，并创建显式副本避免SettingWithCopyWarning
        df_filtered = df[df['symbol'].str[:2].isin(self.target_market_codes)].copy()
        
        # 排除特定股票
        if 'ts_code' in df_filtered.columns and self.excluded_stocks:
            before_exclude = len(df_filtered)
            df_filtered = df_filtered[~df_filtered['ts_code'].isin(self.excluded_stocks)]
            excluded_count = before_exclude - len(df_filtered)
            logger.info(f"已排除 {excluded_count} 支特定股票: {', '.join(self.excluded_stocks)}")
        
        # 过滤掉上市日期大于今天的股票（即尚未上市的股票）
        if 'list_date' in df_filtered.columns:
            today_str = datetime.now().strftime('%Y%m%d')
            before_filter_date = len(df_filtered)
            # 过滤非空list_date且小于等于今天的记录
            df_filtered = df_filtered[(df_filtered['list_date'].notna()) & (df_filtered['list_date'] <= today_str)]
            filtered_date_count = before_filter_date - len(df_filtered)
            logger.info(f"已过滤 {filtered_date_count} 支上市日期大于今天的股票")
        else:
            logger.warning("数据中没有list_date列，无法按上市日期过滤")
        
        # 输出过滤统计信息
        logger.info(f"过滤后股票数量: {len(df_filtered)}")
        
        # 详细统计信息
        if self.verbose:
            # 统计各市场代码股票数量
            market_codes = df['symbol'].str[:2].value_counts().to_dict()
            logger.debug("原始数据市场代码分布:")
            for code, count in sorted(market_codes.items()):
                in_target = "✓" if code in self.target_market_codes else "✗"
                logger.debug(f"  {code}: {count} 股票 {in_target}")
            
            # 统计保留的市场代码
            filtered_codes = df_filtered['symbol'].str[:2].value_counts().to_dict()
            logger.debug("保留的市场代码分布:")
            for code, count in sorted(filtered_codes.items()):
                logger.debug(f"  {code}: {count} 股票")
            
            # 检查是否有目标市场代码未出现在数据中
            missing_codes = self.target_market_codes - set(market_codes.keys())
            if missing_codes:
                logger.warning(f"数据中缺少以下目标市场代码: {missing_codes}")
        
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
                        # 检查是否包含ts_code和symbol作为索引字段
                        if "ts_code" in index_fields and "symbol" in index_fields:
                            # 创建唯一复合索引防止重复
                            collection.create_index(
                                [("ts_code", 1), ("symbol", 1)],
                                unique=True,
                                background=True
                            )
                            logger.debug("已为字段组合 (ts_code, symbol) 创建唯一复合索引")
                            
                            # 移除已处理的字段，避免重复创建索引
                            remaining_fields = [f for f in index_fields if f not in ["ts_code", "symbol"]]
                            for field in remaining_fields:
                                collection.create_index(field)
                                logger.debug(f"已为字段 {field} 创建索引")
                        else:
                            # 单独为每个字段创建索引
                            for field in index_fields:
                                collection.create_index(field, unique=(field in ["ts_code", "symbol"]))
                                logger.debug(f"已为字段 {field} 创建{'唯一' if field in ['ts_code', 'symbol'] else ''}索引")
                    else:
                        # 默认为ts_code和symbol创建唯一索引
                        collection.create_index("ts_code", unique=True)
                        collection.create_index("symbol")
                        logger.debug("已为默认字段创建索引，ts_code设置为唯一索引")
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
        # 获取数据
        df = self.fetch_stock_basic()
        if df is None or df.empty:
            logger.error("获取股票基本信息失败")
            return False
            
        # 过滤数据
        filtered_df = self.filter_stock_data(df)
        if filtered_df.empty:
            logger.warning("过滤后没有符合条件的股票数据")
            return False
            
        # Removed update_time field to prevent duplicate data
        
        # 保存数据到MongoDB
        success = self.save_to_mongodb(filtered_df)
        
        # 关闭MongoDB连接
        self.mongo_client.close()
        
        return success


def create_mock_data() -> pd.DataFrame:
    """创建模拟数据用于测试"""
    logger.info("创建模拟股票基本信息数据用于测试")
    
    # 创建模拟数据
    data = [
        {'ts_code': '000001.SZ', 'symbol': '000001', 'name': '平安银行', 'exchange': 'SZSE', 'list_date': '19910403'},
        {'ts_code': '000002.SZ', 'symbol': '000002', 'name': '万科A', 'exchange': 'SZSE', 'list_date': '19910129'},
        {'ts_code': '000063.SZ', 'symbol': '000063', 'name': '中兴通讯', 'exchange': 'SZSE', 'list_date': '19971118'},
        {'ts_code': '000338.SZ', 'symbol': '000338', 'name': '潍柴动力', 'exchange': 'SZSE', 'list_date': '20070430'},
        {'ts_code': '000651.SZ', 'symbol': '000651', 'name': '格力电器', 'exchange': 'SZSE', 'list_date': '19960801'},
        {'ts_code': '000725.SZ', 'symbol': '000725', 'name': '京东方A', 'exchange': 'SZSE', 'list_date': '19970710'},
        {'ts_code': '000858.SZ', 'symbol': '000858', 'name': '五粮液', 'exchange': 'SZSE', 'list_date': '19980827'},
        {'ts_code': '300059.SZ', 'symbol': '300059', 'name': '东方财富', 'exchange': 'SZSE', 'list_date': '20100115'},
        {'ts_code': '300750.SZ', 'symbol': '300750', 'name': '宁德时代', 'exchange': 'SZSE', 'list_date': '20180611'},
        {'ts_code': '600000.SH', 'symbol': '600000', 'name': '浦发银行', 'exchange': 'SSE', 'list_date': '19991110'},
        {'ts_code': '600036.SH', 'symbol': '600036', 'name': '招商银行', 'exchange': 'SSE', 'list_date': '20021109'},
        {'ts_code': '600276.SH', 'symbol': '600276', 'name': '恒瑞医药', 'exchange': 'SSE', 'list_date': '20001018'},
        {'ts_code': '600519.SH', 'symbol': '600519', 'name': '贵州茅台', 'exchange': 'SSE', 'list_date': '20010827'},
        {'ts_code': '600887.SH', 'symbol': '600887', 'name': '伊利股份', 'exchange': 'SSE', 'list_date': '19960403'},
        {'ts_code': '601318.SH', 'symbol': '601318', 'name': '中国平安', 'exchange': 'SSE', 'list_date': '20070301'},
        {'ts_code': '601857.SH', 'symbol': '601857', 'name': '中国石油', 'exchange': 'SSE', 'list_date': '20071105'},
        {'ts_code': '601888.SH', 'symbol': '601888', 'name': '中国中免', 'exchange': 'SSE', 'list_date': '20091026'},
        {'ts_code': '603288.SH', 'symbol': '603288', 'name': '海天味业', 'exchange': 'SSE', 'list_date': '20140211'},
        {'ts_code': '603501.SH', 'symbol': '603501', 'name': '韦尔股份', 'exchange': 'SSE', 'list_date': '20170504'},
        {'ts_code': '688981.SH', 'symbol': '688981', 'name': '中芯国际', 'exchange': 'SSE', 'list_date': '20200716'}
    ]

    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    # 添加其他字段，确保与实际API返回的数据结构一致
    df['delist_date'] = pd.NA
    df['comp_name'] = df['name']
    df['comp_name_en'] = ''
    df['isin_code'] = ''
    df['list_board'] = ''
    df['crncy_code'] = 'CNY'
    df['pinyin'] = ''
    df['list_board_name'] = ''
    df['is_shsc'] = 'N'
    df['comp_code'] = df['symbol']
    
    logger.success(f"已创建 {len(df)} 条模拟股票数据")
    return df

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票基本信息并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--excluded-stocks', default='600349.SH,300361.SZ,300728.SZ', help='需要排除的股票代码，用逗号分隔')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stock_basic', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--mock', action='store_false', dest='use_real_api', help='使用模拟数据（当API不可用时）')
    parser.add_argument('--use-real-api', action='store_true', default=True, help='使用湘财真实API数据（默认）')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 解析需要排除的股票代码
    excluded_stocks = set(args.excluded_stocks.split(',')) if args.excluded_stocks else set()
    
    # 创建获取器并运行
    fetcher = StockBasicFetcher(
        config_path=args.config,
        interface_dir=args.interface_dir,
        target_market_codes=target_market_codes,
        excluded_stocks=excluded_stocks,
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
        # 过滤数据
        filtered_df = fetcher.filter_stock_data(df)
        if filtered_df.empty:
            logger.warning("过滤后没有符合条件的股票数据")
            sys.exit(1)
        # Removed update_time field to prevent duplicate data
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
