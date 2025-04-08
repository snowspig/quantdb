#!/usr/bin/env python
"""
Stock Basic Fetcher V2 - 获取股票基本信息并保存到MongoDB

该脚本用于从湘财Tushare获取股票基本信息，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了与trade_cal_fetcher相同的架构和功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=25

使用方法：
    python stock_basic_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stock_basic_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stock_basic_fetcher.py --market-codes 00,30,60,68  # 指定市场代码
"""

import sys
import time
import pandas as pd
from typing import Set, Optional
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core.tushare_fetcher import TushareFetcher
from datetime import datetime

class StockBasicFetcher(TushareFetcher):
    """
    股票基本信息获取器V2
    
    该类用于从Tushare获取股票基本信息并保存到MongoDB数据库，支持按市场代码过滤
    使用TushareFetcher基类提供的通用功能
    """
    
    def __init__(
        self,
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        excluded_stocks: Set[str] = set(),
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stock_basic.json",
        db_name: str = None,
        collection_name: str = "stock_basic",
        verbose: bool = False
    ):
        """
        初始化股票基本信息获取器
        
        Args:
            target_market_codes: 目标市场代码集合
            excluded_stocks: 需要排除的特定股票代码集合
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
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
        
        self.target_market_codes = target_market_codes
        self.excluded_stocks = excluded_stocks
        
        # 日志输出
        if excluded_stocks:
            logger.info(f"将排除以下股票代码: {', '.join(excluded_stocks)}")
        logger.info(f"目标市场代码: {', '.join(target_market_codes)}")
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从Tushare获取股票基本信息
        
        Returns:
            股票基本信息DataFrame，如果失败则返回None
        """
        try:
            # 准备API参数
            api_name = "stock_basic"
            params = {
                "exchange": "",
                "list_status": "L"  # 上市状态：L上市 D退市 P暂停上市
            }
            
            # 使用接口配置中的available_fields作为请求字段
            fields = self.available_fields
            if not fields:
                logger.warning("接口配置中未定义available_fields，将获取所有字段")
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket()
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.debug(f"正在从湘财Tushare获取股票基本信息...")
            
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
                missing_fields = [field for field in self.index_fields if field not in df.columns]
                if missing_fields:
                    logger.warning(f"API返回的数据缺少索引字段: {missing_fields}，这可能会影响数据存储")
                
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
            
            logger.success(f"成功获取 {len(df)} 条股票基本信息，耗时 {elapsed:.2f}s")
            return df
            
        except Exception as e:
            logger.error(f"获取股票基本信息失败: {str(e)}")
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
        处理获取的股票基本信息数据
        
        主要功能：
        1. 过滤出目标市场代码的股票
        2. 排除特定股票
        3. 过滤上市日期大于今天的股票
        
        Args:
            df: 原始股票基本信息数据
            
        Returns:
            处理后的数据
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
            logger.info(f"已排除 {excluded_count} 支特定股票")
        
        # 过滤掉上市日期大于今天的股票（即尚未上市的股票）
        if 'list_date' in df_filtered.columns:
            today_str = datetime.now().strftime('%Y%m%d')
            before_filter_date = len(df_filtered)
            # 过滤非空list_date且小于等于今天的记录
            df_filtered = df_filtered[(df_filtered['list_date'].notna()) & (df_filtered['list_date'] <= today_str)]
            filtered_date_count = before_filter_date - len(df_filtered)
            logger.debug(f"已过滤 {filtered_date_count} 支上市日期大于今天的股票")
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
    
    def run(self) -> bool:
        """
        运行数据获取和保存流程
        
        Returns:
            是否成功
        """
        # 使用父类的通用流程，会自动调用子类实现的process_data方法
        return super().run()

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票基本信息并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--excluded-stocks', default='', help='需要排除的股票代码，用逗号分隔')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stock_basic', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 解析需要排除的股票代码
    excluded_stocks = set(args.excluded_stocks.split(',')) if args.excluded_stocks else set()
    
    try:
        # 创建获取器并运行
        fetcher = StockBasicFetcher(
            config_path=args.config,
            interface_dir=args.interface_dir,
            target_market_codes=target_market_codes,
            excluded_stocks=excluded_stocks,
            db_name=args.db_name,
            collection_name=args.collection_name,
            verbose=args.verbose
        )
        
        success = fetcher.run()
        
        if success:
            logger.success("股票基本信息数据获取和保存成功")
            return 0
        else:
            logger.error("股票基本信息数据获取或保存失败")
            return 1
            
    except Exception as e:
        logger.error(f"运行过程中发生异常: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 