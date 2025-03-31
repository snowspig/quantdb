#!/usr/bin/env python
"""
股票公司信息获取器 - 获取股票公司信息并保存到MongoDB

该脚本用于从湘财Tushare获取股票公司信息，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=112

使用方法：
    python stock_company_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stock_company_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stock_company_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
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

class StockCompanyFetcher:
    """
    股票公司信息获取器
    
    该类用于从Tushare获取股票公司信息并保存到MongoDB数据库，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stock_company.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        db_name: str = "tushare_data",
        collection_name: str = "stock_company",
        verbose: bool = False
    ):
        """
        初始化股票公司信息获取器
        
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
            "description": "股票公司信息",
            "api_name": "stock_company",
            "fields": [],
            "params": {},
            "available_fields": [
                "ts_code", "province", "city", "chairman", "president", 
                "bd_secretary", "reg_capital", "found_date", "chinese_introduction", 
                "comp_type", "website", "email", "office", "ann_date", "country", 
                "business_scope", "company_type", "total_employees", "main_business"
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

    def fetch_stock_company(self) -> Optional[pd.DataFrame]:
        """
        获取股票公司信息
        
        Returns:
            股票公司信息DataFrame，如果失败则返回None
        """
        try:
            # 准备参数
            api_name = self.interface_config.get("api_name", "stock_company")
            params = self.interface_config.get("params", {})
            fields = self.interface_config.get("fields", [])
            
            # 确保使用正确的字段（根据接口定义）
            if not fields:
                fields = self.interface_config.get("available_fields", [])
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket() if self.port_allocator else None
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取股票公司信息...")
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
                logger.error("API返回数据为空")
                return None
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                wan_idx, port = wan_info
                self.port_allocator.release_port(wan_idx, port)
            
            logger.success(f"成功获取 {len(df)} 条股票公司信息，耗时 {elapsed:.2f}s")
            
            # 如果使用详细日志，输出数据示例
            if self.verbose and not df.empty:
                logger.debug(f"数据示例：\n{df.head(3)}")
                
            return df
            
        except Exception as e:
            logger.error(f"获取股票公司信息失败: {str(e)}")
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

    def filter_stock_company_data(self, df: pd.DataFrame, target_ts_codes: Set[str]) -> pd.DataFrame:
        """
        根据目标股票代码集合过滤公司数据
        
        Args:
            df: 股票公司信息数据
            target_ts_codes: 目标股票代码集合
        
        Returns:
            过滤后的数据
        """
        if df is None or df.empty:
            logger.warning("没有股票公司数据可过滤")
            return pd.DataFrame()
        
        logger.info(f"过滤前股票公司数量: {len(df)}")
        
        # 确保ts_code列存在
        if 'ts_code' not in df.columns:
            logger.error("数据中没有ts_code列，无法按股票代码过滤")
            return df
        
        # 过滤数据
        df_filtered = df[df['ts_code'].isin(target_ts_codes)].copy()
        
        # 输出过滤统计信息
        logger.info(f"过滤后股票公司数量: {len(df_filtered)}")
        
        # 详细统计信息
        if self.verbose:
            # 统计各市场的股票数量
            if not df_filtered.empty and 'ts_code' in df_filtered.columns:
                # 从ts_code提取市场代码
                df_filtered['market_code'] = df_filtered['ts_code'].str[:6].str[:2]
                market_stats = df_filtered['market_code'].value_counts().to_dict()
                
                logger.debug("过滤后各市场代码分布:")
                for code, count in sorted(market_stats.items()):
                    logger.debug(f"  {code}: {count} 公司")
        
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
            
        # 获取股票公司信息数据
        df = self.fetch_stock_company()
        if df is None or df.empty:
            logger.error("获取股票公司信息失败")
            return False
            
        # 过滤数据，只保留目标股票
        filtered_df = self.filter_stock_company_data(df, target_ts_codes)
        if filtered_df.empty:
            logger.warning("过滤后没有符合条件的股票公司数据")
            return False
            
        # 添加更新时间字段
        filtered_df['update_time'] = datetime.now().isoformat()
        
        # 保存数据到MongoDB
        success = self.save_to_mongodb(filtered_df)
        
        # 关闭MongoDB连接
        self.mongo_client.close()
        
        return success


def create_mock_data() -> pd.DataFrame:
    """创建模拟数据用于测试"""
    logger.info("创建模拟股票公司信息数据用于测试")
    
    # 创建模拟数据
    data = [
        {'ts_code': '000001.SZ', 'province': '广东', 'city': '深圳', 'chairman': '谢永林', 'reg_capital': '194.05亿', 'found_date': '19870922', 'website': 'www.bank.pingan.com', 'main_business': '货币银行服务'},
        {'ts_code': '000002.SZ', 'province': '广东', 'city': '深圳', 'chairman': '郁亮', 'reg_capital': '115.25亿', 'found_date': '19840530', 'website': 'www.vanke.com', 'main_business': '房地产开发'},
        {'ts_code': '300059.SZ', 'province': '上海', 'city': '上海', 'chairman': '其实', 'reg_capital': '87.13亿', 'found_date': '20050518', 'website': 'www.eastmoney.com', 'main_business': '互联网信息服务'},
        {'ts_code': '300750.SZ', 'province': '福建', 'city': '宁德', 'chairman': '曾毓群', 'reg_capital': '23.29亿', 'found_date': '20110915', 'website': 'www.catl.com', 'main_business': '锂离子电池制造'},
        {'ts_code': '600000.SH', 'province': '上海', 'city': '上海', 'chairman': '郑杨', 'reg_capital': '293.52亿', 'found_date': '19920809', 'website': 'www.spdb.com.cn', 'main_business': '货币银行服务'},
        {'ts_code': '600519.SH', 'province': '贵州', 'city': '贵阳', 'chairman': '丁雄军', 'reg_capital': '125.56亿', 'found_date': '19990901', 'website': 'www.moutaichina.com', 'main_business': '白酒生产'},
        {'ts_code': '688981.SH', 'province': '上海', 'city': '上海', 'chairman': '高永岗', 'reg_capital': '45.28亿', 'found_date': '20151217', 'website': 'www.smics.com', 'main_business': '集成电路芯片制造'}
    ]
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    # 添加其他字段，确保与实际API返回的数据结构一致
    df['president'] = ''
    df['bd_secretary'] = ''
    df['chinese_introduction'] = '这是一个模拟的公司简介'
    df['comp_type'] = '1'
    df['email'] = 'contact@example.com'
    df['office'] = ''
    df['ann_date'] = datetime.now().strftime('%Y%m%d')
    df['country'] = '中国'
    df['business_scope'] = '这是一个模拟的经营范围描述'
    df['company_type'] = '1'
    df['total_employees'] = '1000'
    
    logger.success(f"已创建 {len(df)} 条模拟股票公司数据")
    return df

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票公司信息并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stock_company', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--mock', action='store_false', dest='use_real_api', help='使用模拟数据（当API不可用时）')
    parser.add_argument('--use-real-api', action='store_true', default=True, help='使用湘财真实API数据（默认）')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器并运行
    fetcher = StockCompanyFetcher(
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
        filtered_df = fetcher.filter_stock_company_data(df, target_ts_codes)
        if filtered_df.empty:
            logger.warning("过滤后没有符合条件的股票公司数据")
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