#!/usr/bin/env python
"""
Stock Basic Fetcher - 获取股票基本信息并保存到MongoDB

该脚本用于从湘财Tushare获取股票基本信息，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=25

使用方法：
    python stock_basic_fetcher.py             # 简洁日志模式
    python stock_basic_fetcher.py --verbose    # 详细日志模式
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
        db_name: str = "tushare_data",
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
            db_name: MongoDB数据库名称
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.db_name = db_name
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
            tushare_config = self.config.get("tushare", {})
            api_key = tushare_config.get("api_key", "")
            if not api_key:
                logger.error("未配置Tushare API Key")
                sys.exit(1)
                
            return TushareClient(api_key=api_key)
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
            
            # 创建MongoDB客户端
            mongo_client = MongoDBClient(
                uri=uri,
                host=host,
                port=port,
                username=username,
                password=password
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
            df = self.client.get_data(api_name=api_name, params=params, fields=fields)
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
        过滤股票数据，只保留00、30、60和68板块的股票
        
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
        
        # 提取前两位作为市场代码并过滤
        df_filtered = df[df['symbol'].str[:2].isin(self.target_market_codes)]
        
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
                
            self.mongo_client.delete_many(self.db_name, self.collection_name, {})
            
            # 批量插入新数据
            if self.verbose:
                logger.debug(f"向集合 {self.db_name}.{self.collection_name} 插入 {len(records)} 条记录")
                
            result = self.mongo_client.insert_many(self.db_name, self.collection_name, records)
            
            elapsed = time.time() - start_time
            inserted_count = len(result.inserted_ids) if result else 0
            
            if inserted_count > 0:
                # 创建索引
                try:
                    collection = self.mongo_client.get_collection(self.db_name, self.collection_name)
                    if collection:
                        # 根据接口配置中的index_fields创建索引
                        index_fields = self.interface_config.get("index_fields", [])
                        if index_fields:
                            for field in index_fields:
                                collection.create_index(field)
                                logger.debug(f"已为字段 {field} 创建索引")
                        else:
                            # 默认为ts_code和symbol创建索引
                            collection.create_index("ts_code")
                            collection.create_index("symbol")
                            logger.debug("已为默认字段创建索引")
                            
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
            
        # 添加更新时间字段
        filtered_df['update_time'] = datetime.now().isoformat()
        
        # 保存数据到MongoDB
        success = self.save_to_mongodb(filtered_df)
        
        # 关闭MongoDB连接
        self.mongo_client.close()
        
        return success


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票基本信息并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stock_basic', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器并运行
    fetcher = StockBasicFetcher(
        config_path=args.config,
        interface_dir=args.interface_dir,
        target_market_codes=target_market_codes,
        db_name=args.db_name,
        collection_name=args.collection_name,
        verbose=args.verbose
    )
    
    success = fetcher.run()
    
    if success:
        logger.info("股票基本信息获取并保存到MongoDB成功")
        return 0
    else:
        logger.error("获取或保存股票基本信息失败")
        return 1


if __name__ == '__main__':
    sys.exit(main())