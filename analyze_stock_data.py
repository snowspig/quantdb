#!/usr/bin/env python
"""
Stock Data Analyzer - 分析股票基本信息数据

该脚本用于分析导出的股票基本信息CSV数据，验证市场代码过滤是否正确
"""
import os
import sys
import pandas as pd
import argparse
from typing import Dict, List, Set, Tuple
import matplotlib.pyplot as plt
from loguru import logger

# 配置日志
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")


def setup_logging(verbose: bool = False):
    """配置日志级别"""
    # 移除默认处理器
    logger.remove()
    
    # 根据verbose参数设置日志级别
    log_level = "DEBUG" if verbose else "INFO"
    
    # 添加控制台处理器
    logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
    
    if verbose:
        logger.debug("详细日志模式已启用")
    else:
        logger.info("默认日志模式已启用")


def load_csv_data(file_path: str) -> pd.DataFrame:
    """
    加载CSV数据
    
    Args:
        file_path: CSV文件路径
    
    Returns:
        加载的DataFrame
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        return df
    except Exception as e:
        logger.error(f"加载CSV文件失败: {str(e)}")
        sys.exit(1)


def analyze_market_codes(df: pd.DataFrame, target_codes: Set[str] = None) -> Dict[str, int]:
    """
    分析股票市场代码分布
    
    Args:
        df: 股票数据DataFrame
        target_codes: 目标市场代码集合
    
    Returns:
        市场代码统计字典
    """
    if 'symbol' not in df.columns:
        logger.error("数据中没有symbol列，无法分析市场代码")
        return {}
    
    # 统计市场代码
    market_stats = {}
    for stock in df['symbol']:
        # 确保stock是字符串类型
        stock_str = str(stock)
        market_code = stock_str[:2]
        market_stats[market_code] = market_stats.get(market_code, 0) + 1
    
    # 检查是否只包含目标市场代码
    if target_codes:
        non_target_codes = set(market_stats.keys()) - target_codes
        if non_target_codes:
            logger.warning(f"发现非目标市场代码: {non_target_codes}")
        
        missing_codes = target_codes - set(market_stats.keys())
        if missing_codes:
            logger.warning(f"缺少目标市场代码: {missing_codes}")
    
    return market_stats


def analyze_list_status(df: pd.DataFrame) -> Dict[str, int]:
    """
    分析上市状态分布
    
    Args:
        df: 股票数据DataFrame
    
    Returns:
        上市状态统计字典
    """
    status_stats = {}
    
    # 检查是否有退市信息
    if 'name' in df.columns:
        delisted_count = df['name'].str.contains('退市').sum()
        status_stats['退市'] = delisted_count
        status_stats['在市'] = len(df) - delisted_count
    else:
        logger.warning("数据中没有name列，无法分析上市状态")
    
    # 检查是否有list_status列
    if 'list_status' in df.columns:
        for status in df['list_status'].unique():
            status_stats[f'list_status:{status}'] = (df['list_status'] == status).sum()
    
    return status_stats


def analyze_exchange_distribution(df: pd.DataFrame) -> Dict[str, int]:
    """
    分析交易所分布
    
    Args:
        df: 股票数据DataFrame
    
    Returns:
        交易所统计字典
    """
    if 'exchange' not in df.columns:
        logger.warning("数据中没有exchange列，无法分析交易所分布")
        return {}
    
    exchange_stats = df['exchange'].value_counts().to_dict()
    return exchange_stats


def analyze_board_distribution(df: pd.DataFrame) -> Dict[str, int]:
    """
    分析板块分布
    
    Args:
        df: 股票数据DataFrame
    
    Returns:
        板块统计字典
    """
    if 'list_board_name' not in df.columns:
        logger.warning("数据中没有list_board_name列，无法分析板块分布")
        return {}
    
    board_stats = df['list_board_name'].value_counts().to_dict()
    return board_stats


def print_stats(stats: Dict[str, int], title: str):
    """打印统计信息"""
    logger.info(f"\n===== {title} =====")
    for key, value in sorted(stats.items()):
        logger.info(f"{key}: {value}")


def plot_market_distribution(market_stats: Dict[str, int], output_path: str = None):
    """
    绘制市场代码分布饼图
    
    Args:
        market_stats: 市场代码统计字典
        output_path: 输出文件路径
    """
    try:
        plt.figure(figsize=(10, 6))
        plt.pie(market_stats.values(), labels=market_stats.keys(), autopct='%1.1f%%')
        plt.title('股票市场代码分布')
        plt.axis('equal')
        
        if output_path:
            plt.savefig(output_path)
            logger.info(f"市场分布图已保存到: {output_path}")
        else:
            plt.show()
    except Exception as e:
        logger.error(f"绘制市场分布图失败: {str(e)}")


def check_filtered_data(df: pd.DataFrame, target_codes: Set[str]) -> bool:
    """
    检查数据是否正确过滤
    
    Args:
        df: 股票数据DataFrame
        target_codes: 目标市场代码集合
    
    Returns:
        是否正确过滤
    """
    if 'symbol' not in df.columns:
        logger.error("数据中没有symbol列，无法检查过滤")
        return False
    
    # 检查非目标市场代码
    non_target_codes = set()
    for stock in df['symbol']:
        # 确保stock是字符串类型
        stock_str = str(stock)
        market_code = stock_str[:2]
        if market_code not in target_codes:
            non_target_codes.add(market_code)
    
    if non_target_codes:
        logger.error(f"数据过滤不正确，发现非目标市场代码: {non_target_codes}")
        return False
    else:
        logger.success("数据过滤正确，只包含目标市场代码")
        return True


def main():
    """主函数，处理命令行参数并执行分析"""
    parser = argparse.ArgumentParser(description='分析股票基本信息数据')
    parser.add_argument('--file', '-f', default='data_output/stock_basic_20250330.csv', help='CSV文件路径')
    parser.add_argument('--verbose', action='store_true', help='显示详细日志')
    parser.add_argument('--plot', action='store_true', help='绘制分布图')
    parser.add_argument('--output', '-o', help='图表输出路径')
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.verbose)
    
    # 目标市场代码
    target_codes = {'00', '30', '60', '68'}
    
    # 加载数据
    logger.info(f"加载CSV文件: {args.file}")
    df = load_csv_data(args.file)
    logger.info(f"共加载 {len(df)} 条股票记录")
    
    # 分析市场代码
    logger.info("分析市场代码分布...")
    market_stats = analyze_market_codes(df, target_codes)
    print_stats(market_stats, "市场代码分布")
    
    # 检查过滤是否正确
    check_filtered_data(df, target_codes)
    
    # 分析上市状态
    status_stats = analyze_list_status(df)
    print_stats(status_stats, "上市状态分布")
    
    # 分析交易所分布
    exchange_stats = analyze_exchange_distribution(df)
    print_stats(exchange_stats, "交易所分布")
    
    # 分析板块分布
    board_stats = analyze_board_distribution(df)
    print_stats(board_stats, "板块分布")
    
    # 绘制市场代码分布图
    if args.plot:
        output_path = args.output or "market_distribution.png"
        plot_market_distribution(market_stats, output_path)
        
    logger.info("分析完成")


if __name__ == '__main__':
    main()