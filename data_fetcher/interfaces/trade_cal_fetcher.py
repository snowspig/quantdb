#!/usr/bin/env python
"""
Trade Calendar Fetcher V2 - 获取交易日历数据并保存到MongoDB

该脚本用于从湘财Tushare获取交易日历数据，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了与stock_basic_fetcher相同的架构和功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=26

使用方法：
    python trade_cal_fetcher.py                   # 使用湘财真实API数据，简洁日志模式，获取近期数据
    python trade_cal_fetcher.py --verbose         # 使用湘财真实API数据，详细日志模式
    python trade_cal_fetcher.py --start-date 20200101 --end-date 20231231  # 指定日期范围
    python trade_cal_fetcher.py --exchange SZSE   # 获取深交所的交易日历
"""
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core.tushare_fetcher import TushareFetcher

class TradeCalFetcher(TushareFetcher):
    """
    交易日历数据获取器V2
    
    该类用于从Tushare获取交易日历数据并保存到MongoDB数据库
    使用TushareFetcher基类提供的通用功能
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "trade_cal.json",
        db_name: str = None,
        collection_name: str = "trade_cal",
        start_date: str = None,
        end_date: str = None,
        exchange: str = "SSE",  # 默认上交所
        verbose: bool = False
    ):
        """
        初始化交易日历数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            start_date: 开始日期（格式：YYYYMMDD，默认为当前日期前一年）
            end_date: 结束日期（格式：YYYYMMDD，默认为当前日期）
            exchange: 交易所代码（SSE：上交所，SZSE：深交所，默认SSE）
            verbose: 是否输出详细日志
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
        
        self.exchange = exchange
        
        # 设置默认日期范围（如果未提供）
        if not start_date or not end_date:
            today = datetime.now()
            if not end_date:
                self.end_date = today.strftime("%Y%m%d")
            else:
                self.end_date = end_date
                
            if not start_date:
                # 默认获取最近一年的数据
                one_year_ago = today - timedelta(days=365)
                self.start_date = one_year_ago.strftime("%Y%m%d")
            else:
                self.start_date = start_date
        else:
            self.start_date = start_date
            self.end_date = end_date
        
        # 日志输出
        logger.info(f"交易所: {self.exchange}, 日期范围: {self.start_date} - {self.end_date}")
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从Tushare获取交易日历数据
        
        Args:
            **kwargs: 可以传入覆盖默认参数的值，比如exchange, start_date, end_date
            
        Returns:
            交易日历数据DataFrame，如果失败则返回None
        """
        try:
            # 准备API参数
            api_name = self.interface_config.get("api_name", "trade_cal")
            
            # 设置API参数：交易所和日期范围，优先使用传入的参数
            exchange = kwargs.get('exchange', self.exchange)
            start_date = kwargs.get('start_date', self.start_date)
            end_date = kwargs.get('end_date', self.end_date)
            
            params = {
                "exchange": exchange,
                "start_date": start_date,
                "end_date": end_date
            }
            
            # 使用接口配置中的available_fields作为请求字段
            fields = self.available_fields
            if not fields:
                logger.warning("接口配置中未定义available_fields，将获取所有字段")
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket()
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取交易日历数据...")
            logger.info(f"数据范围：{start_date} 至 {end_date}，交易所：{exchange}")
            
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            start_time = time.time()
            
            # 增加超时，设置为120秒
            self.client.set_timeout(120)
            logger.debug(f"增加API请求超时时间为120秒，提高网络可靠性")
            
            # 添加异常捕获，以便更好地调试
            try:
                # 如果使用WAN接口，设置本地地址绑定
                if use_wan:
                    wan_idx, port = wan_info
                    self.client.set_local_address('0.0.0.0', port)
                
                df = self.client.get_data(api_name=api_name, params=params, fields=fields)
                
                # 重置客户端设置
                if use_wan:
                    self.client.reset_local_address()
            except Exception as e:
                import traceback
                logger.error(f"获取API数据时发生异常: {str(e)}")
                logger.debug(f"异常详情: {traceback.format_exc()}")
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    
                return None
            
            # 数据验证和处理
            if df is not None and not df.empty:
                logger.success(f"成功获取数据，行数: {len(df)}, 列数: {df.shape[1]}")
                
                # 检查返回的数据包含的字段
                logger.debug(f"API返回的字段: {list(df.columns)}")
                
                # 确保返回字段与接口配置中的字段匹配
                # 但不做字段转换，保留API返回的原始字段名
                missing_fields = [field for field in self.index_fields if field not in df.columns]
                if missing_fields:
                    logger.warning(f"API返回的数据缺少索引字段: {missing_fields}，这可能会影响数据存储")
                    
                    # 如果缺少exchange字段但我们知道交易所，则添加
                    if "exchange" in missing_fields and "exchange" not in df.columns:
                        df["exchange"] = exchange
                        logger.info(f"已添加默认交易所字段: {exchange}")
                
                # 确保日期字段是字符串格式
                if "trade_date" in df.columns and df["trade_date"].dtype != "object":
                    df["trade_date"] = df["trade_date"].astype(str)
                    logger.debug("已将trade_date字段转换为字符串格式")
                
                if self.verbose:
                    logger.debug(f"数据示例：\n{df.head(3)}")
                
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                logger.error("数据为空")
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    
                return None
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                wan_idx, port = wan_info
                self.port_allocator.release_port(wan_idx, port)
            
            logger.success(f"成功获取 {len(df)} 条交易日历数据，耗时 {elapsed:.2f}s")
            return df
            
        except Exception as e:
            logger.error(f"获取交易日历数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 如果有WAN信息，确保释放端口
            try:
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    logger.debug(f"已释放WAN {wan_idx} 的端口 {port}")
            except Exception as release_error:
                logger.warning(f"释放WAN端口时出错: {str(release_error)}")
                
            return None
    
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理获取的交易日历数据
        可以在这里实现对数据的任何处理，如果不需要特殊处理，可以直接返回原始数据
        
        Args:
            df: 原始交易日历数据
            
        Returns:
            处理后的数据
        """
        # 本例中不需要对交易日历数据做特殊处理，返回原始数据即可
        return df
    
    def get_trade_calendar(self, start_date: str = None, end_date: str = None, is_open: int = 1) -> List[str]:
        """
        获取指定日期范围内的交易日列表
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            is_open: 是否开市，1-开市，0-休市
            
        Returns:
            交易日期列表
        """
        # 使用传入的参数或默认参数
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        
        # 首先尝试从MongoDB获取数据
        try:
            # 确保MongoDB连接
            if not self.mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return []
            
            # 构建查询条件
            query = {
                "trade_date": {"$gte": start_date, "$lte": end_date},
                "exchange": self.exchange
            }
            
            if is_open is not None:
                query["is_open"] = is_open
            
            # 查询结果
            result = self.mongodb_handler.find_documents(
                self.collection_name, 
                query,
                {"trade_date": 1, "_id": 0},
                sort=[("trade_date", 1)]
            )
            
            # 提取日期列表
            trade_dates = [doc.get("trade_date") for doc in result if "trade_date" in doc]
            
            if trade_dates:
                logger.info(f"从MongoDB获取到 {len(trade_dates)} 个{'交易' if is_open == 1 else '日历'}日")
                return trade_dates
            else:
                logger.warning(f"MongoDB中未找到符合条件的交易日历数据，将从API获取")
        except Exception as e:
            logger.error(f"查询MongoDB交易日历数据失败: {str(e)}")
            logger.info("将尝试从API获取数据")
        
        # 如果MongoDB中没有数据，尝试从API获取
        try:
            df = self.fetch_data(start_date=start_date, end_date=end_date)
            if df is not None and not df.empty:
                # 处理数据
                if is_open is not None:
                    df = df[df["is_open"] == is_open]
                
                # 保存到MongoDB（这样下次可以直接从MongoDB查询）
                self.save_to_mongodb(df)
                
                # 提取日期列表
                trade_dates = df["trade_date"].tolist() if "trade_date" in df.columns else []
                
                logger.info(f"从API获取到 {len(trade_dates)} 个{'交易' if is_open == 1 else '日历'}日")
                return trade_dates
            else:
                logger.error("从API获取交易日历数据失败")
        except Exception as e:
            logger.error(f"从API获取交易日历数据失败: {str(e)}")
        
        # 如果所有方法都失败，生成日期范围内的所有日期作为备选
        logger.warning("无法获取交易日历数据，将生成日期范围内的所有日期作为备选")
        
        start_date_obj = datetime.strptime(start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(end_date, '%Y%m%d')
        
        all_dates = []
        current_date = start_date_obj
        while current_date <= end_date_obj:
            all_dates.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        
        logger.info(f"生成日期范围内的所有日期，共 {len(all_dates)} 个日期")
        return all_dates
    
    def run(self) -> bool:
        """
        运行数据获取和保存流程
        
        Returns:
            是否成功
        """
        # 使用父类的通用流程
        return super().run()

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取交易日历数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--exchange', default='SSE', help='交易所代码：SSE-上交所, SZSE-深交所')
    parser.add_argument('--start-date', help='开始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式：YYYYMMDD')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='trade_cal', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    
    args = parser.parse_args()
    
    try:
        # 创建获取器并运行
        fetcher = TradeCalFetcher(
            config_path=args.config,
            interface_dir=args.interface_dir,
            exchange=args.exchange,
            start_date=args.start_date,
            end_date=args.end_date,
            db_name=args.db_name,
            collection_name=args.collection_name,
            verbose=args.verbose
        )
        
        success = fetcher.run()
        
        if success:
            logger.success("交易日历数据获取和保存成功")
            return 0
        else:
            logger.error("交易日历数据获取或保存失败")
            return 1
        
    except Exception as e:
        logger.error(f"运行过程中发生异常: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 