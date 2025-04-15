#!/usr/bin/env python
"""
Stock Company Fetcher V2 - 获取股票基本信息并保存到MongoDB

该脚本用于从湘财Tushare获取股票基本信息，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了与trade_cal_fetcher相同的架构和功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=25

使用方法：
    python stock_company_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stock_company_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stock_company_fetcher.py --market-codes 00,30,60,68  # 指定市场代码
"""

import sys
import time
import json
import os
import pandas as pd
from typing import Set, Optional, Dict, Any
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core.tushare_fetcher import TushareFetcher
from datetime import datetime

# 共享配置加载函数
def load_shared_config(shared_config_path=None) -> Dict[str, Any]:
    """
    加载共享配置
    
    如果指定了共享配置路径，直接从文件加载
    否则尝试从环境变量获取路径
    
    Args:
        shared_config_path: 共享配置文件路径
        
    Returns:
        Dict[str, Any]: 共享配置字典
    """
    # 首先检查参数
    if shared_config_path:
        config_path = shared_config_path
    # 其次检查环境变量
    elif "QUANTDB_SHARED_CONFIG" in os.environ:
        config_path = os.environ.get("QUANTDB_SHARED_CONFIG")
    else:
        # 如果没有共享配置，返回空字典
        logger.debug("没有找到共享配置路径")
        return {}
    
    try:
        # 检查文件是否存在
        if not os.path.exists(config_path):
            logger.warning(f"共享配置文件不存在：{config_path}")
            return {}
        
        # 加载配置
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        logger.info(f"成功从共享配置中加载设置：{config_path}")
        return config
    except Exception as e:
        logger.error(f"加载共享配置失败：{str(e)}")
        return {}

def get_validation_status(shared_config: Dict[str, Any]) -> Dict[str, bool]:
    """
    从共享配置中获取验证状态
    
    Args:
        shared_config: 共享配置字典
        
    Returns:
        Dict[str, bool]: 验证状态字典
    """
    validation_summary = shared_config.get("validation_summary", {})
    return validation_summary

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
        interface_name: str = "stock_company.json",
        db_name: str = None,
        collection_name: str = "stock_company",
        verbose: bool = False,
        shared_config: Dict[str, Any] = None,
        skip_validation: bool = False
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
            shared_config: 共享配置字典
            skip_validation: 是否跳过验证
        """
        # 使用共享配置中的设置（如果有）
        if shared_config:
            # 可以从共享配置中获取配置文件路径
            config_path = shared_config.get("config_file", config_path)
            # 获取验证状态
            validation_status = get_validation_status(shared_config)
            skip_validation = skip_validation or validation_status.get("all_valid", False)
            
            logger.info(f"使用共享配置：配置文件={config_path}, 跳过验证={skip_validation}")
        
        # 保存skip_validation状态，但不传递给父类
        self.skip_validation = skip_validation
        
        # 检查TushareFetcher是否支持skip_validation参数
        import inspect
        parent_params = inspect.signature(TushareFetcher.__init__).parameters
        parent_args = {}
        
        # 基本参数
        parent_args['config_path'] = config_path
        parent_args['interface_dir'] = interface_dir
        parent_args['interface_name'] = interface_name
        parent_args['db_name'] = db_name
        parent_args['collection_name'] = collection_name
        parent_args['verbose'] = verbose
        
        # 如果父类支持skip_validation，则添加
        if 'skip_validation' in parent_params:
            parent_args['skip_validation'] = skip_validation
            if verbose:
                logger.debug("TushareFetcher支持skip_validation参数")
        else:
            logger.debug("TushareFetcher不支持skip_validation参数，将在子类中处理")
        
        # 调用父类初始化方法
        super().__init__(**parent_args)
        
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
            api_name = "stock_company"
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
        1. 过滤出目标市场代码的股票 (使用ts_code前缀)
        2. 排除特定股票
        
        Args:
            df: 原始股票基本信息数据
            
        Returns:
            处理后的数据
        """
        if df is None or df.empty:
            logger.warning("没有股票数据可过滤")
            return pd.DataFrame()
        
        logger.info(f"过滤前股票数量: {len(df)}")
        
        # 确保ts_code列存在
        if 'ts_code' not in df.columns:
            logger.error("数据中没有ts_code列，无法过滤股票")
            return df
        
        # 确保ts_code列是字符串类型
        df['ts_code'] = df['ts_code'].astype(str)
        
        # 从ts_code提取前缀，格式通常是'000001.SZ'，取前两位作为市场代码
        df['code_prefix'] = df['ts_code'].str.split('.').str[0].str[:2]
        
        # 过滤保留目标市场代码的股票，并创建显式副本避免SettingWithCopyWarning
        df_filtered = df[df['code_prefix'].isin(self.target_market_codes)].copy()
        
        # 排除特定股票
        if self.excluded_stocks:
            before_exclude = len(df_filtered)
            df_filtered = df_filtered[~df_filtered['ts_code'].isin(self.excluded_stocks)]
            excluded_count = before_exclude - len(df_filtered)
            logger.info(f"已排除 {excluded_count} 支特定股票")
        
        # 输出过滤统计信息
        logger.info(f"过滤后股票数量: {len(df_filtered)}")
        
        # 详细统计信息
        if self.verbose:
            # 统计各市场代码股票数量
            market_codes = df['code_prefix'].value_counts().to_dict()
            logger.debug("原始数据市场代码分布:")
            for code, count in sorted(market_codes.items()):
                in_target = "✓" if code in self.target_market_codes else "✗"
                logger.debug(f"  {code}: {count} 股票 {in_target}")
            
            # 统计保留的市场代码
            filtered_codes = df_filtered['code_prefix'].value_counts().to_dict()
            logger.debug("保留的市场代码分布:")
            for code, count in sorted(filtered_codes.items()):
                logger.debug(f"  {code}: {count} 股票")
            
            # 检查是否有目标市场代码未出现在数据中
            missing_codes = self.target_market_codes - set(market_codes.keys())
            if missing_codes:
                logger.warning(f"数据中缺少以下目标市场代码: {missing_codes}")
        
        # 删除临时的code_prefix列
        if 'code_prefix' in df_filtered.columns:
            df_filtered = df_filtered.drop(columns=['code_prefix'])
        
        return df_filtered
    
    def run(self) -> bool:
        """
        运行数据获取和保存流程
        
        Returns:
            是否成功
        """
        # 如果需要跳过验证，检查父类是否已支持
        # 如果父类不支持，则自己处理跳过验证
        if hasattr(self, 'skip_validation') and self.skip_validation:
            import inspect
            parent_run = super().run
            parent_params = inspect.signature(parent_run).parameters
            
            # 如果父类run()不支持skip_validation参数，实现自己的逻辑
            if 'skip_validation' not in parent_params:
                logger.info("父类不支持跳过验证，使用自定义逻辑跳过验证过程")
                
                try:
                    # 获取数据
                    logger.info("开始获取数据...")
                    df = self.fetch_data()
                    if df is None or df.empty:
                        logger.error("获取数据失败或数据为空")
                        return False
                    
                    # 处理数据
                    logger.info("开始处理数据...")
                    processed_df = self.process_data(df)
                    if processed_df is None or processed_df.empty:
                        logger.warning("处理后的数据为空")
                        return False
                    
                    # 存储数据 - 不直接调用save_data，而是尝试使用父类的save_to_mongodb方法
                    # 或者直接调用父类的run方法来处理保存逻辑
                    logger.info("开始保存数据...")
                    
                    # 方法1：尝试使用save_to_mongodb方法（如果存在）
                    if hasattr(self, 'save_to_mongodb'):
                        return self.save_to_mongodb(processed_df)
                    
                    # 方法2：尝试其他可能的方法名
                    for method_name in ['store_data', 'insert_data', 'save']:
                        if hasattr(self, method_name):
                            method = getattr(self, method_name)
                            return method(processed_df)
                    
                    # 方法3：如果没有找到合适的保存方法，则使用父类的run方法
                    logger.info("未找到直接保存方法，回退到父类的run方法...")
                    
                    # 保存处理后的数据供父类使用
                    self._processed_data = processed_df
                    
                    # 创建一个子类，覆盖fetch_data和process_data方法来使用已处理的数据
                    class TempFetcher(TushareFetcher):
                        def __init__(self, parent):
                            self.__dict__ = parent.__dict__
                        
                        def fetch_data(self, **kwargs):
                            # 直接返回原始数据
                            return self._processed_data
                        
                        def process_data(self, df):
                            # 直接返回，因为数据已经处理过
                            return df
                    
                    # 创建临时对象
                    temp_fetcher = TempFetcher(self)
                    
                    # 调用原始的父类run方法
                    from types import MethodType
                    original_run = super(TempFetcher, temp_fetcher).run
                    return original_run()
                    
                except Exception as e:
                    logger.error(f"运行过程中发生异常: {str(e)}")
                    import traceback
                    logger.error(f"详细错误信息: {traceback.format_exc()}")
                    return False
        
        # 否则使用父类的通用流程，会自动调用子类实现的process_data方法
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
    parser.add_argument('--collection-name', default='stock_company', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--shared-config', type=str, default=None, help='共享配置文件路径')
    parser.add_argument('--skip-validation', action='store_true', help='跳过配置验证')
    
    args = parser.parse_args()
    
    # 根据verbose参数设置日志级别
    if not args.verbose:
        # 非详细模式下，设置日志级别为INFO，不显示DEBUG消息
        logger.remove()  # 移除所有处理器
        logger.add(sys.stderr, level="INFO")  # 添加标准错误输出处理器，级别为INFO
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 解析需要排除的股票代码
    excluded_stocks = set(args.excluded_stocks.split(',')) if args.excluded_stocks else set()
    
    try:
        # 加载共享配置（如果有）
        shared_config = load_shared_config(args.shared_config)
        
        # 使用共享配置中的验证状态
        if shared_config:
            validation_status = get_validation_status(shared_config)
            logger.info(f"从共享配置获取验证状态：{validation_status}")
            
            # 如果共享配置中指定了配置文件路径，优先使用
            if "config_file" in shared_config and not args.config:
                args.config = shared_config.get("config_file")
                logger.info(f"从共享配置获取配置文件路径：{args.config}")
        
        # 创建获取器并运行
        fetcher = StockBasicFetcher(
            config_path=args.config,
            interface_dir=args.interface_dir,
            target_market_codes=target_market_codes,
            excluded_stocks=excluded_stocks,
            db_name=args.db_name,
            collection_name=args.collection_name,
            verbose=args.verbose,
            shared_config=shared_config,
            skip_validation=args.skip_validation
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