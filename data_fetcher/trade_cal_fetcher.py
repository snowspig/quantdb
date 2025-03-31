#!/usr/bin/env python
"""
Trade Calendar Fetcher - 获取交易日历数据并保存到MongoDB

该脚本用于从湘财Tushare获取交易日历数据，并保存到MongoDB数据库中，默认获取近期数据

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=26

使用方法：
    python trade_cal_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python trade_cal_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python trade_cal_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python trade_cal_fetcher.py --start-date 20200101 --end-date 20221231  # 指定日期范围
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

class TradeCalFetcher:
    """
    交易日历数据获取器
    
    该类用于从Tushare获取交易日历数据并保存到MongoDB数据库
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "trade_cal.json",
        start_date: str = None,  # 默认为None，将在fetch_trade_cal中设置为近期日期
        end_date: str = None,    # 默认为None，将在fetch_trade_cal中设置为当前日期
        exchanges: List[str] = ["SSE", "SZSE"],  # 默认获取沪深交易所
        db_name: str = "tushare_data",
        collection_name: str = "trade_cal",
        verbose: bool = False
    ):
        """
        初始化交易日历获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            start_date: 开始日期，格式YYYYMMDD，默认为None（将自动设置为近期日期）
            end_date: 结束日期，格式YYYYMMDD，默认为None（将自动设置为当前日期）
            exchanges: 交易所列表，默认为["SSE", "SZSE"]
            db_name: MongoDB数据库名称
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.start_date = start_date
        self.end_date = end_date
        self.exchanges = exchanges
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
            "description": "中国A股交易日历",
            "api_name": "trade_cal",
            "fields": [],
            "params": {},
            "index_fields": ["trade_date"],
            "available_fields": ["trade_date", "exchange", "is_open"]
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

    def fetch_trade_cal(self) -> Optional[pd.DataFrame]:
        """
        获取交易日历数据
        
        默认获取近期数据（当前日期前后90天）
        
        Returns:
            交易日历DataFrame，如果失败则返回None
        """
        try:
            # 设置默认日期范围为近期（当前日期前后90天）
            if self.start_date is None:
                # 默认获取当前日期往前90天的数据
                today = datetime.now()
                start_date = (today - timedelta(days=90)).strftime("%Y%m%d")
                self.start_date = start_date
                logger.info(f"未指定开始日期，默认使用当前日期前90天: {start_date}")
                
            if self.end_date is None:
                # 默认获取当前日期往后90天的数据（未来的交易日历安排）
                today = datetime.now()
                end_date = (today + timedelta(days=90)).strftime("%Y%m%d")
                self.end_date = end_date
                logger.info(f"未指定结束日期，默认使用当前日期后90天: {end_date}")
            
            # 准备参数
            api_name = self.interface_config.get("api_name", "trade_cal")
            
            all_data = []
            
            # 为每个交易所获取数据
            for exchange in self.exchanges:
                params = {
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                    "exchange": exchange
                }
                
                fields = self.interface_config.get("fields", [])
                
                # 确保使用正确的字段（根据接口定义）
                if not fields:
                    fields = self.interface_config.get("available_fields", [])
                
                # 创建并使用WAN接口的socket，实现多WAN请求支持
                wan_info = self._get_wan_socket() if self.port_allocator else None
                use_wan = wan_info is not None
                
                # 调用Tushare API
                logger.info(f"正在从湘财Tushare获取{exchange}交易所的交易日历数据...")
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
                        logger.success(f"成功获取{exchange}交易所数据，行数: {len(df)}, 列数: {df.shape[1]}")
                        if self.verbose:
                            logger.debug(f"列名: {list(df.columns)}")
                        all_data.append(df)
                except Exception as e:
                    import traceback
                    logger.error(f"获取{exchange}交易所API数据时发生异常: {str(e)}")
                    logger.debug(f"异常详情: {traceback.format_exc()}")
                    
                elapsed = time.time() - start_time
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                
                logger.success(f"获取{exchange}交易所交易日历数据耗时 {elapsed:.2f}s")
            
            # 合并所有交易所的数据
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                total_records = len(combined_df)
                logger.success(f"成功获取 {total_records} 条交易日历记录")
                
                # 如果使用详细日志，输出数据示例
                if self.verbose and not combined_df.empty:
                    logger.debug(f"数据示例：\n{combined_df.head(3)}")
                    
                return combined_df
            else:
                logger.error("所有交易所API返回数据为空")
                return None
                
        except Exception as e:
            logger.error(f"获取交易日历数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return None

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
                        # 默认为trade_date创建索引
                        collection.create_index("trade_date")
                        logger.debug("已为默认字段trade_date创建索引")
                        
                    # 为exchange创建索引
                    collection.create_index("exchange")
                    logger.debug("已为exchange字段创建索引")
                    
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
        df = self.fetch_trade_cal()
        if df is None or df.empty:
            logger.error("获取交易日历数据失败")
            return False
            
        # 添加更新时间字段
        df['update_time'] = datetime.now().isoformat()
        
        # 保存数据到MongoDB
        success = self.save_to_mongodb(df)
        
        # 关闭MongoDB连接
        self.mongo_client.close()
        
        return success


def create_mock_data() -> pd.DataFrame:
    """创建模拟数据用于测试"""
    logger.info("创建模拟交易日历数据用于测试")
    
    # 创建模拟数据 - 生成6个月的日历数据
    today = datetime.now()
    start_date = (today - timedelta(days=90))
    end_date = (today + timedelta(days=90))
    
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    
    # 为SSE和SZSE生成数据
    data = []
    for exchange in ["SSE", "SZSE"]:
        for date in dates:
            date_str = date.strftime("%Y%m%d")
            # 判断是否是周末
            is_weekend = date.weekday() >= 5
            
            record = {
                'exchange': exchange,
                'trade_date': date_str,
                'is_open': 0 if is_weekend else 1,  # 周末休市
                'pretrade_date': ''  # 可选字段
            }
            data.append(record)
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    logger.success(f"已创建 {len(df)} 条模拟交易日历数据")
    return df

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取交易日历数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--exchange', default='SSE,SZSE', help='交易所代码，用逗号分隔')
    parser.add_argument('--start-date', type=str, help='开始日期，格式YYYYMMDD，默认为当前日期前90天')
    parser.add_argument('--end-date', type=str, help='结束日期，格式YYYYMMDD，默认为当前日期后90天')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='trade_cal', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--mock', action='store_false', dest='use_real_api', help='使用模拟数据（当API不可用时）')
    parser.add_argument('--use-real-api', action='store_true', default=True, help='使用湘财真实API数据（默认）')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
    args = parser.parse_args()
    
    # 解析交易所
    exchanges = args.exchange.split(',')
    
    # 创建获取器并运行
    fetcher = TradeCalFetcher(
        config_path=args.config,
        interface_dir=args.interface_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        exchanges=exchanges,
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
        # 添加更新时间字段
        df['update_time'] = datetime.now().isoformat()
        # 是否实际保存
        if args.dry_run:
            logger.info("干运行模式，不保存数据")
            success = True
        else:
            # 保存数据到MongoDB
            success = fetcher.save_to_mongodb(df)
            # 关闭MongoDB连接
            fetcher.mongo_client.close()
    
    if success:
        logger.success("交易日历数据获取和保存成功")
        sys.exit(0)
    else:
        logger.error("交易日历数据获取或保存失败")
        sys.exit(1)

if __name__ == "__main__":
    main()
