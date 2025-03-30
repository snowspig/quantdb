#!/usr/bin/env python
"""
Stock Basic Fetcher - 获取股票基本信息并尝试保存到MongoDB（带CSV/JSON备份功能）

该脚本用于从Tushare获取股票基本信息，首先尝试保存到MongoDB数据库中，
若连接失败则退回到保存为CSV和JSON文件，提供市场代码过滤功能
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

class StockBasicFetcherHybrid:
    """
    股票基本信息获取器（混合存储版）

    该类用于从Tushare获取股票基本信息，首先尝试保存到MongoDB数据库，
    若连接失败则退回到保存为CSV和JSON文件，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stock_basic.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        db_name: str = "doublequant",
        collection_name: str = "stock_basic",
        output_dir: str = "data_output",
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
            output_dir: CSV/JSON文件输出目录（仅当MongoDB连接失败时使用）
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.db_name = db_name
        self.collection_name = collection_name
        self.output_dir = output_dir
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
            token = tushare_config.get("token", "")
            if not token:
                logger.error("未配置Tushare token")
                sys.exit(1)
                
            return TushareClient(token=token)
        except Exception as e:
            logger.error(f"初始化Tushare客户端失败: {str(e)}")
            sys.exit(1)
    
    def _init_mongo_client(self) -> Optional[MongoDBClient]:
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
            
            # 尝试连接到数据库
            if mongo_client.connect():
                logger.success("成功连接到MongoDB")
                return mongo_client
            else:
                logger.warning("无法连接到MongoDB，将使用文件存储作为备用方案")
                return None
                
        except Exception as e:
            logger.warning(f"初始化MongoDB客户端失败: {str(e)}，将使用文件存储作为备用方案")
            return None
            
    def _init_port_allocator(self) -> Optional[PortAllocator]:
        """初始化多WAN口管理器"""
        try:
            wan_config = self.config.get("wan_interfaces", {})
            if not wan_config:
                logger.warning("未配置多WAN口，将使用系统默认网络接口")
                return None
            
            # 获取WAN接口配置
            interfaces = wan_config.get("interfaces", [])
            if not interfaces:
                logger.warning("未配置WAN接口列表，将使用系统默认网络接口")
                return None
            
            # 创建端口分配器
            port_allocator = PortAllocator(interfaces)
            logger.debug(f"已初始化多WAN口管理器，可用接口: {', '.join(interfaces)}")
            return port_allocator
        except Exception as e:
            logger.error(f"初始化多WAN口管理器失败: {str(e)}")
            return None
            
    def _get_source_ip(self) -> Optional[str]:
        """获取源IP地址"""
        if not self.port_allocator:
            return None
            
        try:
            # 轮询获取下一个可用接口的IP
            source_ip = self.port_allocator.get_next_source_ip()
            if source_ip:
                logger.debug(f"使用源IP: {source_ip}")
                return source_ip
        except Exception as e:
            logger.error(f"获取源IP失败: {str(e)}")
            
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
            
            # 获取源IP (多WAN口支持)
            source_ip = self._get_source_ip()
            
            # 调用Tushare API
            logger.info(f"正在从Tushare获取股票基本信息...")
            if source_ip:
                logger.debug(f"使用源IP {source_ip} 请求数据")
                
            start_time = time.time()
            response = self.client.query(api_name=api_name, params=params, fields=fields, source_ip=source_ip)
            elapsed = time.time() - start_time
            
            if response is None or not isinstance(response, dict):
                logger.error("API返回数据格式错误")
                return None
                
            data = response.get("data", {})
            if not data or "items" not in data:
                logger.error("API返回数据为空或格式错误")
                return None
                
            # 将数据转换为DataFrame
            items = data["items"]
            columns = data.get("fields", [])
            df = pd.DataFrame(items, columns=columns)
            
            logger.success(f"成功获取 {len(df)} 条股票基本信息，耗时 {elapsed:.2f}s")
            return df
            
        except Exception as e:
            logger.error(f"获取股票基本信息失败: {str(e)}")
            return None

    def filter_stock_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤股票数据，只保留指定市场代码的股票
        
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
            
        # 如果MongoDB客户端为空，直接返回失败
        if self.mongo_client is None:
            logger.warning("MongoDB客户端未初始化，无法保存数据到MongoDB")
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
                logger.success(f"成功将 {inserted_count} 条记录保存到 MongoDB: {self.db_name}.{self.collection_name}，耗时 {elapsed:.2f}s")
                return True
            else:
                logger.error(f"保存到MongoDB失败，未插入任何记录")
                return False
                
        except Exception as e:
            logger.error(f"保存到MongoDB失败: {str(e)}")
            return False

    def save_to_csv(self, df: pd.DataFrame, suffix: str = None) -> str:
        """
        将数据保存为CSV文件（备用存储方案）
        
        Args:
            df: 待保存的DataFrame
            suffix: 文件名后缀，默认为当前日期
            
        Returns:
            保存的文件路径
        """
        if df is None or df.empty:
            logger.warning("没有数据可保存")
            return ""
            
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 生成文件名
        if suffix is None:
            suffix = datetime.now().strftime("%Y%m%d")
        filename = f"stock_basic_{suffix}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # 保存CSV文件
        try:
            df.to_csv(filepath, index=False)
            logger.success(f"数据已保存至: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"保存CSV文件失败: {str(e)}")
            return ""

    def save_to_json(self, df: pd.DataFrame, suffix: str = None) -> str:
        """
        将数据保存为JSON文件（备用存储方案）
        
        Args:
            df: 待保存的DataFrame
            suffix: 文件名后缀，默认为当前日期
            
        Returns:
            保存的文件路径
        """
        if df is None or df.empty:
            logger.warning("没有数据可保存")
            return ""
            
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 生成文件名
        if suffix is None:
            suffix = datetime.now().strftime("%Y%m%d")
        filename = f"stock_basic_{suffix}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        # 保存JSON文件
        try:
            df.to_json(filepath, orient='records', force_ascii=False, indent=2)
            logger.success(f"数据已保存至: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"保存JSON文件失败: {str(e)}")
            return ""
            
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
        
        mongodb_success = False
        
        # 尝试保存到MongoDB
        if self.mongo_client is not None:
            logger.info("尝试将数据保存到MongoDB...")
            mongodb_success = self.save_to_mongodb(filtered_df)
            
            # 如果成功保存到MongoDB，则关闭连接并返回
            if mongodb_success:
                logger.success("成功将数据保存到MongoDB")
                # 关闭MongoDB连接
                self.mongo_client.close()
                return True
            else:
                logger.warning("保存到MongoDB失败，将使用文件存储作为备用方案")
        else:
            logger.info("MongoDB连接不可用，将使用文件存储作为备用方案")
        
        # MongoDB保存失败或不可用，使用文件存储作为备用
        logger.info("正在将数据保存为文件...")
        suffix = datetime.now().strftime("%Y%m%d")
        csv_path = self.save_to_csv(filtered_df, suffix)
        json_path = self.save_to_json(filtered_df, suffix)
        
        # 清理资源
        if self.mongo_client:
            self.mongo_client.close()
        
        # 只要有一种存储成功，就返回成功
        return mongodb_success or bool(csv_path or json_path)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票基本信息并保存（优先MongoDB，备用CSV/JSON）')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--db-name', default='doublequant', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stock_basic', help='MongoDB集合名称')
    parser.add_argument('--output-dir', default='data_output', help='文件输出目录（MongoDB连接失败时使用）')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器并运行
    fetcher = StockBasicFetcherHybrid(
        config_path=args.config,
        interface_dir=args.interface_dir,
        target_market_codes=target_market_codes,
        db_name=args.db_name,
        collection_name=args.collection_name,
        output_dir=args.output_dir,
        verbose=args.verbose
    )
    
    success = fetcher.run()
    
    if success:
        logger.info("股票基本信息获取并保存成功")
        return 0
    else:
        logger.error("获取或保存股票基本信息失败")
        return 1


if __name__ == '__main__':
    sys.exit(main())