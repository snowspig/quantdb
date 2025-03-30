#!/usr/bin/env python
"""
Stock Basic to CSV - 获取股票基本信息并保存为CSV

该脚本用于从Tushare获取股票基本信息，并保存为CSV文件，提供市场代码过滤功能
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

class StockBasicFetcher:
    """
    股票基本信息获取器

    该类用于从Tushare获取股票基本信息并保存为CSV文件，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stock_basic.json",
        output_dir: str = "data_output",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        verbose: bool = False
    ):
        """
        初始化股票基本信息获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            output_dir: 输出目录
            target_market_codes: 目标市场代码集合
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.output_dir = output_dir
        self.target_market_codes = target_market_codes
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
            
            # 调用Tushare API
            logger.info(f"正在从Tushare获取股票基本信息...")
            response = self.client.query(api_name=api_name, params=params, fields=fields)
            
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
            
            logger.success(f"成功获取 {len(df)} 条股票基本信息")
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

    def save_to_csv(self, df: pd.DataFrame, suffix: str = None) -> str:
        """
        将数据保存为CSV文件
        
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
        将数据保存为JSON文件
        
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
            
    def run(self) -> Tuple[bool, pd.DataFrame]:
        """
        运行数据获取和保存流程
        
        Returns:
            (成功标志, 过滤后的数据)
        """
        # 获取数据
        df = self.fetch_stock_basic()
        if df is None or df.empty:
            logger.error("获取股票基本信息失败")
            return False, pd.DataFrame()
            
        # 过滤数据
        filtered_df = self.filter_stock_data(df)
        if filtered_df.empty:
            logger.warning("过滤后没有符合条件的股票数据")
            return False, pd.DataFrame()
            
        # 保存数据
        suffix = datetime.now().strftime("%Y%m%d")
        csv_path = self.save_to_csv(filtered_df, suffix)
        json_path = self.save_to_json(filtered_df, suffix)
        
        if csv_path and json_path:
            return True, filtered_df
        else:
            return False, filtered_df


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票基本信息并保存为CSV')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--output-dir', default='data_output', help='输出目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器并运行
    fetcher = StockBasicFetcher(
        config_path=args.config,
        interface_dir=args.interface_dir,
        output_dir=args.output_dir,
        target_market_codes=target_market_codes,
        verbose=args.verbose
    )
    
    success, df = fetcher.run()
    
    if success:
        logger.info(f"成功获取并保存 {len(df)} 条股票基本信息")
        return 0
    else:
        logger.error("获取或保存股票基本信息失败")
        return 1


if __name__ == '__main__':
    sys.exit(main())