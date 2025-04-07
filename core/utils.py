#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
工具函数模块
提供系统通用的辅助函数
"""
import os
import re
import json
import time
import logging
import hashlib
import datetime
import calendar
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple, Set

# 设置日志
logger = logging.getLogger("core.utils")
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)

# 日期时间处理函数
def get_current_date(format_str: str = '%Y%m%d') -> str:
    """
    获取当前日期字符串
    
    Args:
        format_str: 日期格式
        
    Returns:
        str: 日期字符串
    """
    return datetime.datetime.now().strftime(format_str)

def get_last_trade_date(current_date: str = None, format_str: str = '%Y%m%d') -> str:
    """
    获取最近的交易日期（非周末）
    
    Args:
        current_date: 当前日期，默认为今天
        format_str: 日期格式
        
    Returns:
        str: 交易日期字符串
    """
    if not current_date:
        current_date = get_current_date(format_str)
        
    date_obj = datetime.datetime.strptime(current_date, format_str)
    
    # 判断是否为周末
    weekday = date_obj.weekday()
    
    # 如果是周六(5)或周日(6)，回退到周五
    if weekday == 5:  # 周六
        date_obj -= datetime.timedelta(days=1)
    elif weekday == 6:  # 周日
        date_obj -= datetime.timedelta(days=2)
        
    return date_obj.strftime(format_str)

def get_date_range(start_date: str, end_date: str, format_str: str = '%Y%m%d') -> List[str]:
    """
    获取日期范围内的所有日期
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        format_str: 日期格式
        
    Returns:
        List[str]: 日期列表
    """
    date_list = []
    start_obj = datetime.datetime.strptime(start_date, format_str)
    end_obj = datetime.datetime.strptime(end_date, format_str)
    
    current = start_obj
    while current <= end_obj:
        date_list.append(current.strftime(format_str))
        current += datetime.timedelta(days=1)
        
    return date_list

def get_workdays(start_date: str, end_date: str, format_str: str = '%Y%m%d') -> List[str]:
    """
    获取日期范围内的工作日（周一至周五）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        format_str: 日期格式
        
    Returns:
        List[str]: 工作日列表
    """
    workdays = []
    
    for date_str in get_date_range(start_date, end_date, format_str):
        date_obj = datetime.datetime.strptime(date_str, format_str)
        if date_obj.weekday() < 5:  # 周一至周五
            workdays.append(date_str)
            
    return workdays

def get_month_start_end(year: int, month: int, format_str: str = '%Y%m%d') -> Tuple[str, str]:
    """
    获取指定年月的第一天和最后一天
    
    Args:
        year: 年份
        month: 月份
        format_str: 日期格式
        
    Returns:
        Tuple[str, str]: (第一天, 最后一天)
    """
    # 获取当月第一天
    first_day = datetime.date(year, month, 1)
    
    # 获取当月最后一天
    _, last_day_num = calendar.monthrange(year, month)
    last_day = datetime.date(year, month, last_day_num)
    
    return first_day.strftime(format_str), last_day.strftime(format_str)

def date_add(date_str: str, days: int = 1, format_str: str = '%Y%m%d') -> str:
    """
    日期加减
    
    Args:
        date_str: 日期字符串
        days: 增加的天数，可为负数
        format_str: 日期格式
        
    Returns:
        str: 结果日期字符串
    """
    date_obj = datetime.datetime.strptime(date_str, format_str)
    result = date_obj + datetime.timedelta(days=days)
    return result.strftime(format_str)

def is_valid_date(date_str: str, format_str: str = '%Y%m%d') -> bool:
    """
    检查日期字符串是否有效
    
    Args:
        date_str: 日期字符串
        format_str: 日期格式
        
    Returns:
        bool: 是否有效
    """
    try:
        datetime.datetime.strptime(date_str, format_str)
        return True
    except ValueError:
        return False

# 股票代码处理函数
def normalize_code(code: str) -> str:
    """
    规范化股票代码
    
    Args:
        code: 原始股票代码
        
    Returns:
        str: 规范化后的股票代码
    """
    if not code:
        return ""
        
    # 去除空格和点
    code = code.strip().replace('.', '')
    
    # 提取纯数字部分
    num_match = re.search(r'\d+', code)
    if not num_match:
        return code
        
    num_code = num_match.group()
    
    # 根据数字判断交易所
    if len(num_code) != 6:
        return code
        
    # 沪市股票
    if num_code.startswith(('6', '9')):
        return f"{num_code}.SH"
        
    # 深市股票
    elif num_code.startswith(('0', '2', '3')):
        return f"{num_code}.SZ"
        
    # 北交所股票
    elif num_code.startswith('4'):
        return f"{num_code}.BJ"
        
    # 未能识别的情况，返回原始代码
    return code

def split_stock_code(ts_code: str) -> Tuple[str, str]:
    """
    拆分 Tushare 格式股票代码
    
    Args:
        ts_code: 带交易所的股票代码，如 000001.SZ
        
    Returns:
        Tuple[str, str]: (股票代码, 交易所)
    """
    parts = ts_code.split('.')
    if len(parts) > 1:
        return parts[0], parts[1]
    return ts_code, ""

def filter_stock_list_by_market(stock_list: List[str], market: str = None) -> List[str]:
    """
    按市场过滤股票列表
    
    Args:
        stock_list: 股票列表
        market: 市场代码 (SH/SZ/BJ)
        
    Returns:
        List[str]: 过滤后的股票列表
    """
    if not market:
        return stock_list
        
    market = market.upper()
    return [code for code in stock_list if code.endswith('.' + market)]

# 数据处理函数
def convert_to_numeric(df: pd.DataFrame, exclude_cols: List[str] = None) -> pd.DataFrame:
    """
    将DataFrame中的字符串列转换为数值类型
    
    Args:
        df: 原始DataFrame
        exclude_cols: 排除的列名列表
        
    Returns:
        pd.DataFrame: 处理后的DataFrame
    """
    exclude_cols = exclude_cols or []
    
    for col in df.columns:
        if col in exclude_cols:
            continue
            
        if pd.api.types.is_string_dtype(df[col]):
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass
                
    return df

def convert_date_columns(df: pd.DataFrame, date_cols: List[str] = None, format_str: str = None) -> pd.DataFrame:
    """
    将DataFrame中的日期列转换为日期类型
    
    Args:
        df: 原始DataFrame
        date_cols: 日期列名列表，默认尝试处理常见日期列
        format_str: 日期格式
        
    Returns:
        pd.DataFrame: 处理后的DataFrame
    """
    # 默认日期列
    default_date_cols = ['trade_date', 'ann_date', 'start_date', 'end_date', 'report_date', 'date']
    date_cols = date_cols or default_date_cols
    
    for col in date_cols:
        if col in df.columns:
            try:
                if format_str:
                    df[col] = pd.to_datetime(df[col], format=format_str)
                else:
                    # 尝试自动识别格式
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
                
    return df

def drop_duplicates_with_priority(df: pd.DataFrame, subset: List[str], keep_fn=None) -> pd.DataFrame:
    """
    根据优先级删除重复行
    
    Args:
        df: 原始DataFrame
        subset: 用于判断重复的列
        keep_fn: 确定保留哪一行的函数，默认保留第一行
        
    Returns:
        pd.DataFrame: 处理后的DataFrame
    """
    if not keep_fn:
        return df.drop_duplicates(subset=subset, keep='first')
        
    # 对于自定义优先级的情况
    result_rows = []
    groups = df.groupby(subset)
    
    for _, group in groups:
        if len(group) > 1:
            # 使用自定义函数选择要保留的行
            selected_row = keep_fn(group)
            if selected_row is not None:
                result_rows.append(selected_row)
        else:
            # 只有一行，直接保留
            result_rows.append(group.iloc[0])
            
    return pd.DataFrame(result_rows)

def fillna_with_method(df: pd.DataFrame, cols: List[str] = None, method: str = 'ffill') -> pd.DataFrame:
    """
    使用指定方法填充缺失值
    
    Args:
        df: 原始DataFrame
        cols: 要填充的列，默认为全部
        method: 填充方法 (ffill/bfill/median/mean/zero)
        
    Returns:
        pd.DataFrame: 处理后的DataFrame
    """
    cols = cols or df.columns
    
    for col in cols:
        if col not in df.columns:
            continue
            
        if method in ['ffill', 'bfill']:
            df[col] = df[col].fillna(method=method)
        elif method == 'median':
            df[col] = df[col].fillna(df[col].median())
        elif method == 'mean':
            df[col] = df[col].fillna(df[col].mean())
        elif method == 'zero':
            df[col] = df[col].fillna(0)
            
    return df

def safe_divide(a, b, default=0):
    """
    安全除法，避免除零错误
    
    Args:
        a: 分子
        b: 分母
        default: 默认值，当分母为0时返回
        
    Returns:
        计算结果或默认值
    """
    try:
        if pd.isna(a) or pd.isna(b) or b == 0:
            return default
        return a / b
    except:
        return default

# 文件操作函数
def ensure_dir(directory: str) -> bool:
    """
    确保目录存在，不存在则创建
    
    Args:
        directory: 目录路径
        
    Returns:
        bool: 是否成功
    """
    try:
        os.makedirs(directory, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"创建目录失败: {directory}, 错误: {str(e)}")
        return False

def get_file_md5(file_path: str) -> str:
    """
    计算文件MD5哈希值
    
    Args:
        file_path: 文件路径
        
    Returns:
        str: MD5哈希值
    """
    try:
        with open(file_path, 'rb') as f:
            md5_hash = hashlib.md5()
            # 分块读取大文件
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        logger.error(f"计算文件MD5失败: {file_path}, 错误: {str(e)}")
        return ""

def list_files(directory: str, pattern: str = None, recursive: bool = False) -> List[str]:
    """
    列出目录中的文件
    
    Args:
        directory: 目录路径
        pattern: 文件名匹配模式
        recursive: 是否递归查找子目录
        
    Returns:
        List[str]: 文件路径列表
    """
    files = []
    
    try:
        path_obj = Path(directory)
        
        if recursive:
            glob_pattern = '**/*' + (pattern or '')
            files = [str(p) for p in path_obj.glob(glob_pattern) if p.is_file()]
        else:
            glob_pattern = '*' + (pattern or '')
            files = [str(p) for p in path_obj.glob(glob_pattern) if p.is_file()]
            
        return files
        
    except Exception as e:
        logger.error(f"列出文件失败: {directory}, 错误: {str(e)}")
        return []

# 配置和性能相关函数
def retry(times: int, exceptions=Exception, delay=0):
    """
    重试装饰器
    
    Args:
        times: 重试次数
        exceptions: 捕获的异常类型
        delay: 重试间隔（秒）
        
    Returns:
        装饰后的函数
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt == times:
                        raise e
                    if delay > 0:
                        time.sleep(delay)
        return wrapper
    return decorator

def timeit(method):
    """
    函数执行时间计时器装饰器
    
    Args:
        method: 被装饰的方法
        
    Returns:
        装饰后的函数
    """
    def timed(*args, **kw):
        start_time = time.time()
        result = method(*args, **kw)
        end_time = time.time()
        
        execution_time = end_time - start_time
        logger.debug(f"{method.__name__} 执行用时: {execution_time:.2f} 秒")
        
        return result
    return timed

def chunks(lst: List, n: int):
    """
    将列表分割成大小为n的块
    
    Args:
        lst: 原始列表
        n: 块大小
        
    Yields:
        列表的一个块
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_cpu_count() -> int:
    """
    获取可用CPU核心数
    
    Returns:
        int: CPU核心数
    """
    import os
    try:
        return os.cpu_count() or 4
    except:
        return 4

# JSON处理函数
def load_json(file_path: str) -> Dict:
    """
    加载JSON文件
    
    Args:
        file_path: 文件路径
        
    Returns:
        Dict: JSON数据
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载JSON文件失败: {file_path}, 错误: {str(e)}")
        return {}

def save_json(data: Dict, file_path: str, indent: int = 4) -> bool:
    """
    保存数据到JSON文件
    
    Args:
        data: 数据
        file_path: 文件路径
        indent: 缩进空格数
        
    Returns:
        bool: 是否成功
    """
    try:
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        return True
    except Exception as e:
        logger.error(f"保存JSON文件失败: {file_path}, 错误: {str(e)}")
        return False

# 字符串处理函数
def remove_spaces(text: str) -> str:
    """
    移除字符串中的所有空白字符
    
    Args:
        text: 原始字符串
        
    Returns:
        str: 处理后的字符串
    """
    if not text:
        return ""
    return ''.join(text.split())

def is_numeric_string(text: str) -> bool:
    """
    检查字符串是否为数值型
    
    Args:
        text: 要检查的字符串
        
    Returns:
        bool: 是否为数值型
    """
    try:
        float(text.strip())
        return True
    except:
        return False