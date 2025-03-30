"""
接口处理器模块，负责处理各个Tushare接口的数据获取和存储
"""
import os
import json
import time
from typing import Dict, Any, List, Optional, Tuple, Set, Union
import pandas as pd
from datetime import datetime, timedelta
import threading
import random
from loguru import logger
from retry import retry

from config import config_manager
from data_fetcher.tushare_client import tushare_client
from storage.mongodb_client import mongodb_client
from wan_manager.port_allocator import port_allocator
from storage.data_models import create_model_from_api


class InterfaceProcessor:
    """
    Tushare接口处理器，负责处理单个接口的数据获取和存储
    """
    
    def __init__(self, interface_name: str):
        """
        初始化接口处理器
        
        Args:
            interface_name: 接口名称
        """
        self.interface_name = interface_name
        self.interface_config = self._load_interface_config()
        self.api_name = self.interface_config.get("api_name", interface_name)
        self.description = self.interface_config.get("description", "")
        self.fields = self.interface_config.get("fields", [])
        self.default_params = self.interface_config.get("params", {})
        self.update_frequency = self.interface_config.get("update_frequency", "daily")
        self.update_priority = self.interface_config.get("update_priority", 5)
        self.index_fields = self.interface_config.get("index_fields", [])
        self.available_fields = self.interface_config.get("available_fields", [])
        
        # 从配置中获取更新设置
        self.batch_size = config_manager.get("update", "batch_size", 500)
        
        logger.debug(f"接口处理器初始化: {self.interface_name} ({self.description}), 优先级: {self.update_priority}")
    
    def _load_interface_config(self) -> Dict[str, Any]:
        """
        加载接口配置
        
        Returns:
            接口配置字典
        """
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'config', 'interfaces', f'{self.interface_name}.json'
        )
        
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config
            except Exception as e:
                logger.error(f"加载接口配置失败 {self.interface_name}: {str(e)}")
        
        logger.warning(f"接口配置文件不存在: {config_path}，将使用默认配置")
        return {
            "description": f"{self.interface_name} 接口",
            "api_name": self.interface_name,
            "fields": [],
            "params": {},
            "update_frequency": "daily",
            "update_priority": 5,
            "index_fields": []
        }
    
    def get_collection_name(self) -> str:
        """
        获取MongoDB集合名称，默认使用接口名称作为集合名
        
        Returns:
            集合名称
        """
        return self.interface_name
    
    def prepare_params(self, base_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        准备请求参数，合并默认参数和自定义参数
        
        Args:
            base_params: 自定义参数
            
        Returns:
            合并后的参数字典
        """
        params = self.default_params.copy()
        if base_params:
            for k, v in base_params.items():
                params[k] = v
        return params
    
    def find_latest_date(self, ts_code: str = None) -> Optional[str]:
        """
        查找最新日期
        
        Args:
            ts_code: 股票代码, 如果为None，则查找所有记录中的最新日期
            
        Returns:
            最新日期字符串，如果没有记录则返回None
        """
        collection_name = self.get_collection_name()
        date_field = self._get_date_field()
        
        if date_field:
            if ts_code:
                return mongodb_client.find_stock_latest_date(collection_name, ts_code, date_field)
            else:
                return mongodb_client.find_latest_date(collection_name, date_field)
        
        return None
    
    def _get_date_field(self) -> Optional[str]:
        """
        获取日期字段名称
        
        Returns:
            日期字段名称
        """
        # 从index_fields中寻找日期字段
        for field in self.index_fields:
            if "date" in field.lower():
                return field
        
        # 常见日期字段名
        common_date_fields = ["trade_date", "end_date", "ann_date", "f_ann_date", "cal_date", "date"]
        for field in common_date_fields:
            if field in self.available_fields or field in self.fields:
                return field
        
        return None
    
    def get_date_range(self, start_date: str = None, end_date: str = None) -> Tuple[str, str]:
        """
        获取日期范围，如果未提供则使用默认值
        
        Args:
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD
            
        Returns:
            (开始日期, 结束日期) 元组
        """
        # 如果未提供结束日期，使用当前日期
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        
        # 如果未提供开始日期，使用最新记录日期或默认为前30天
        if not start_date:
            latest_date = self.find_latest_date()
            if latest_date:
                # 转换为datetime以便进行日期计算
                try:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    # 从最新日期后一天开始获取数据
                    start_date = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                except ValueError:
                    logger.warning(f"日期格式转换失败: {latest_date}，将使用默认日期范围")
                    # 默认为前30天
                    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            else:
                # 没有记录，默认为前30天
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        
        # 确保开始日期不晚于结束日期
        try:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            if start_dt > end_dt:
                logger.warning(f"开始日期 {start_date} 晚于结束日期 {end_date}，将使用结束日期作为开始日期")
                start_date = end_date
        except ValueError:
            logger.error(f"日期格式无效: start_date={start_date}, end_date={end_date}")
        
        return start_date, end_date
    
    @retry(tries=3, delay=2, backoff=2)
    def fetch_data(self, params: Dict[str, Any] = None, fields: List[str] = None, wan_index: int = None) -> pd.DataFrame:
        """
        从Tushare API获取数据
        
        Args:
            params: API参数
            fields: 返回字段列表
            wan_index: 指定WAN索引，用于多WAN负载均衡
            
        Returns:
            数据DataFrame
        """
        # 准备参数
        api_params = self.prepare_params(params)
        
        # 确定要使用的字段
        api_fields = fields or self.fields
        
        # 输出详细的请求信息
        logger.debug(f"API请求: {self.api_name}, 参数: {api_params}, 字段: {api_fields}")
        
        # 获取数据
        try:
            # 如果指定了WAN索引且多WAN已启用
            if wan_index is not None and config_manager.is_wan_enabled():
                # 获取可用WAN索引
                available_wan = port_allocator.get_available_wan_indices()
                
                if available_wan and wan_index in available_wan:
                    # 获取socket
                    port = port_allocator.allocate_port(wan_index)
                    
                    if port:
                        # 创建绑定的socket
                        import socket
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.bind(('0.0.0.0', port))
                        
                        # 设置Tushare客户端使用这个socket
                        logger.debug(f"使用WAN {wan_index} (端口 {port}) 获取数据: {self.api_name}")
                        tushare_client.set_socket(sock)
                        
                        # 获取数据
                        df = tushare_client.get_data(self.api_name, api_params, api_fields)
                        
                        # 释放端口
                        port_allocator.release_port(wan_index, port)
                        return df
            
            # 默认使用无WAN绑定的方式获取数据
            return tushare_client.get_data(self.api_name, api_params, api_fields)
            
        except Exception as e:
            logger.error(f"API请求失败: {self.api_name}, 错误: {str(e)}")
            raise
    
    def save_to_mongodb(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        保存数据到MongoDB
        
        Args:
            df: 要保存的数据
            
        Returns:
            保存结果统计
        """
        if df.empty:
            logger.warning(f"没有数据可保存到MongoDB: {self.interface_name}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
        
        # 获取集合名
        collection_name = self.get_collection_name()
        
        # 保存前处理数据
        df = self._preprocess_data(df)
        
        # 记录数据量
        total_rows = len(df)
        logger.info(f"准备保存 {total_rows} 条数据到 {collection_name}")
        
        # 确保使用适当的索引字段
        index_fields = self.index_fields
        if not index_fields:
            # 尝试构建默认索引字段
            potential_fields = []
            if 'ts_code' in df.columns:
                potential_fields.append('ts_code')
            
            date_field = self._get_date_field()
            if date_field and date_field in df.columns:
                potential_fields.append(date_field)
            
            if potential_fields:
                index_fields = potential_fields
                logger.info(f"为集合 {collection_name} 使用自动生成的索引字段: {index_fields}")
        
        # 保存数据
        try:
            result = mongodb_client.save_dataframe(
                df, 
                collection_name, 
                if_exists='update', 
                index_fields=index_fields,
                chunk_size=self.batch_size
            )
            
            # 记录结果
            if isinstance(result, dict):
                logger.info(f"保存成功: 总计 {result.get('total', 0)} 条记录, "
                          f"新增 {result.get('new', 0)}, "
                          f"更新 {result.get('updated', 0)}, "
                          f"未变 {result.get('unchanged', 0)}")
            else:
                logger.info(f"保存成功: 插入 {result} 条记录")
                result = {"total": total_rows, "new": result, "updated": 0, "unchanged": 0}
            
            return result
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
            raise
    
    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理数据，适用于所有类型的数据
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            处理后的DataFrame
        """
        # 检查并修复ts_code字段中的重复市场后缀问题
        if 'ts_code' in df.columns:
            sample_ts_code = df['ts_code'].iloc[0] if not df.empty else ''
            if '.SZ.SZ' in sample_ts_code or '.SH.SH' in sample_ts_code:
                logger.debug(f"检测到ts_code字段中的重复市场后缀: {sample_ts_code}，将进行修复")
                df['ts_code'] = df['ts_code'].str.replace('.SZ.SZ', '.SZ').str.replace('.SH.SH', '.SH')
        
        # 确保日期字段格式统一
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            if col in df.columns and not pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype(str)
        
        # 确保trade_time字段是字符串格式
        if 'trade_time' in df.columns and not pd.api.types.is_string_dtype(df['trade_time']):
            df['trade_time'] = df['trade_time'].astype(str)
        
        return df
    
    def update_data(
            self, 
            start_date: str = None, 
            end_date: str = None, 
            ts_codes: Union[str, List[str]] = None,
            wan_index: int = None
        ) -> Dict[str, int]:
        """
        更新数据
        
        Args:
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD
            ts_codes: 股票代码或代码列表
            wan_index: 指定WAN索引，用于多WAN负载均衡
            
        Returns:
            更新结果统计
        """
        logger.info(f"开始更新 {self.api_name} 数据")
        total_result = {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
        
        # 处理不同类型的接口
        if self._is_stock_time_series():
            # 股票时间序列数据，需要按股票代码分批处理
            return self._update_stock_time_series(start_date, end_date, ts_codes, wan_index)
        elif self._is_date_based():
            # 基于日期的数据，直接按日期范围获取
            return self._update_date_based_data(start_date, end_date, wan_index)
        else:
            # 基础数据，直接获取全部
            return self._update_basic_data(wan_index)
    
    def _is_stock_time_series(self) -> bool:
        """
        判断是否为股票时间序列数据
        
        Returns:
            是否为股票时间序列数据
        """
        # 判断是否需要ts_code和trade_date参数
        params_keys = set(self.default_params.keys())
        if 'ts_code' in params_keys or 'ts_code' in self.available_fields:
            # 检查是否有日期字段
            date_field = self._get_date_field()
            if date_field:
                return True
        
        return False
    
    def _is_date_based(self) -> bool:
        """
        判断是否为基于日期的数据
        
        Returns:
            是否为基于日期的数据
        """
        # 判断是否需要trade_date参数但不需要ts_code
        params_keys = set(self.default_params.keys())
        if 'trade_date' in params_keys or 'trade_date' in self.available_fields:
            return True
        
        date_field = self._get_date_field()
        return date_field is not None
    
    def _update_stock_time_series(
            self, 
            start_date: str = None, 
            end_date: str = None, 
            ts_codes: Union[str, List[str]] = None,
            wan_index: int = None
        ) -> Dict[str, int]:
        """
        更新股票时间序列数据
        
        Args:
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD
            ts_codes: 股票代码或代码列表
            wan_index: 指定WAN索引，用于多WAN负载均衡
            
        Returns:
            更新结果统计
        """
        # 获取日期范围
        start_date, end_date = self.get_date_range(start_date, end_date)
        
        # 准备股票代码列表
        stock_codes = []
        if ts_codes:
            if isinstance(ts_codes, str):
                stock_codes = [ts_codes]
            else:
                stock_codes = ts_codes
        else:
            # 如果未提供股票代码，尝试获取所有股票基本信息
            try:
                stock_basic = InterfaceProcessor('stock_basic')
                df_stocks = stock_basic.fetch_data()
                if not df_stocks.empty and 'ts_code' in df_stocks.columns:
                    stock_codes = df_stocks['ts_code'].tolist()
                    logger.info(f"获取到 {len(stock_codes)} 只股票的基本信息")
                else:
                    logger.warning("无法获取股票基本信息，将跳过更新")
                    return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
            except Exception as e:
                logger.error(f"获取股票基本信息失败: {str(e)}")
                return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
        
        # 按批次处理股票代码
        total_result = {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
        chunk_size = 50  # 每批处理的股票数量
        
        for i in range(0, len(stock_codes), chunk_size):
            chunk = stock_codes[i:i + chunk_size]
            chunk_str = ','.join(chunk)
            
            logger.info(f"获取 {self.api_name} 数据: 股票代码 {i+1}-{i+len(chunk)}/{len(stock_codes)}, 日期 {start_date}-{end_date}")
            
            try:
                # 构建参数
                params = {
                    'ts_code': chunk_str
                }
                
                # 如果接口支持日期参数，添加日期范围
                if 'start_date' in self.default_params or 'start_date' in self.available_fields:
                    params['start_date'] = start_date
                if 'end_date' in self.default_params or 'end_date' in self.available_fields:
                    params['end_date'] = end_date
                
                # 获取数据
                df = self.fetch_data(params, wan_index=wan_index)
                
                if not df.empty:
                    # 保存数据
                    result = self.save_to_mongodb(df)
                    # 合并结果
                    for key in total_result:
                        if key in result:
                            total_result[key] += result[key]
                else:
                    logger.debug(f"没有新数据: {self.api_name}, 参数: {params}")
            
            except Exception as e:
                logger.error(f"处理批次 {i//chunk_size + 1} 失败: {str(e)}")
                # 继续处理下一批
        
        return total_result
    
    def _update_date_based_data(
            self, 
            start_date: str = None, 
            end_date: str = None,
            wan_index: int = None
        ) -> Dict[str, int]:
        """
        更新基于日期的数据
        
        Args:
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD
            wan_index: 指定WAN索引，用于多WAN负载均衡
            
        Returns:
            更新结果统计
        """
        # 获取日期范围
        start_date, end_date = self.get_date_range(start_date, end_date)
        
        # 计算日期列表
        try:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            # 如果日期范围很大，按月分批处理
            if (end_dt - start_dt).days > 31:
                return self._update_date_based_by_month(start_dt, end_dt, wan_index)
            
            # 构建参数
            params = {}
            
            # 添加日期范围
            if 'start_date' in self.default_params or 'start_date' in self.available_fields:
                params['start_date'] = start_date
            if 'end_date' in self.default_params or 'end_date' in self.available_fields:
                params['end_date'] = end_date
            
            logger.info(f"获取 {self.api_name} 数据: 日期范围 {start_date}-{end_date}")
            
            # 获取数据
            df = self.fetch_data(params, wan_index=wan_index)
            
            if not df.empty:
                # 保存数据
                return self.save_to_mongodb(df)
            else:
                logger.debug(f"没有新数据: {self.api_name}, 日期范围: {start_date}-{end_date}")
                return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
            
        except ValueError:
            logger.error(f"日期格式无效: start_date={start_date}, end_date={end_date}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
    
    def _update_date_based_by_month(
            self, 
            start_dt: datetime, 
            end_dt: datetime,
            wan_index: int = None
        ) -> Dict[str, int]:
        """
        按月分批更新基于日期的数据
        
        Args:
            start_dt: 开始日期
            end_dt: 结束日期
            wan_index: 指定WAN索引，用于多WAN负载均衡
            
        Returns:
            更新结果统计
        """
        total_result = {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
        
        # 按月处理
        current_dt = start_dt
        
        while current_dt <= end_dt:
            # 计算当前月的结束日期
            if current_dt.month == 12:
                next_month = datetime(current_dt.year + 1, 1, 1)
            else:
                next_month = datetime(current_dt.year, current_dt.month + 1, 1)
                
            # 确保不超出总结束日期
            month_end = next_month - timedelta(days=1)
            if month_end > end_dt:
                month_end = end_dt
                
            # 格式化日期
            month_start_str = current_dt.strftime('%Y%m%d')
            month_end_str = month_end.strftime('%Y%m%d')
            
            # 构建参数
            params = {}
            
            # 添加日期范围
            if 'start_date' in self.default_params or 'start_date' in self.available_fields:
                params['start_date'] = month_start_str
            if 'end_date' in self.default_params or 'end_date' in self.available_fields:
                params['end_date'] = month_end_str
            
            logger.info(f"获取 {self.api_name} 数据: 月份 {month_start_str[:6]}, 日期范围 {month_start_str}-{month_end_str}")
            
            try:
                # 获取数据
                df = self.fetch_data(params, wan_index=wan_index)
                
                if not df.empty:
                    # 保存数据
                    result = self.save_to_mongodb(df)
                    # 合并结果
                    for key in total_result:
                        if key in result:
                            total_result[key] += result[key]
                else:
                    logger.debug(f"没有新数据: {self.api_name}, 月份: {month_start_str[:6]}")
            
            except Exception as e:
                logger.error(f"处理月份 {month_start_str[:6]} 失败: {str(e)}")
                # 继续处理下一月
            
            # 移到下一月
            current_dt = next_month
        
        return total_result
    
    def _update_basic_data(self, wan_index: int = None) -> Dict[str, int]:
        """
        更新基础数据
        
        Args:
            wan_index: 指定WAN索引，用于多WAN负载均衡
            
        Returns:
            更新结果统计
        """
        logger.info(f"获取 {self.api_name} 基础数据")
        
        try:
            # 获取数据
            df = self.fetch_data(wan_index=wan_index)
            
            if not df.empty:
                # 保存数据
                return self.save_to_mongodb(df)
            else:
                logger.debug(f"没有数据: {self.api_name}")
                return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}
                
        except Exception as e:
            logger.error(f"获取基础数据失败: {str(e)}")
            return {"total": 0, "new": 0, "updated": 0, "unchanged": 0}


class InterfaceManager:
    """
    接口管理器，负责管理所有接口处理器
    """
    
    def __init__(self):
        """初始化接口管理器"""
        self.interfaces: Dict[str, InterfaceProcessor] = {}
        self.load_interfaces()
    
    def load_interfaces(self):
        """加载所有接口配置"""
        config_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'config', 'interfaces'
        )
        
        if not os.path.exists(config_dir):
            logger.warning(f"接口配置目录不存在: {config_dir}")
            return
            
        for filename in os.listdir(config_dir):
            if filename.endswith('.json'):
                interface_name = filename.replace('.json', '')
                try:
                    processor = InterfaceProcessor(interface_name)
                    self.interfaces[interface_name] = processor
                    logger.debug(f"加载接口: {interface_name}")
                except Exception as e:
                    logger.error(f"加载接口失败 {interface_name}: {str(e)}")
        
        logger.info(f"成功加载 {len(self.interfaces)} 个接口配置")
    
    def get_processor(self, interface_name: str) -> Optional[InterfaceProcessor]:
        """
        获取接口处理器
        
        Args:
            interface_name: 接口名称
            
        Returns:
            接口处理器对象
        """
        if interface_name in self.interfaces:
            return self.interfaces[interface_name]
            
        # 如果不存在，尝试创建
        try:
            processor = InterfaceProcessor(interface_name)
            self.interfaces[interface_name] = processor
            return processor
        except Exception as e:
            logger.error(f"创建接口处理器失败 {interface_name}: {str(e)}")
            return None
    
    def get_all_interfaces(self) -> List[str]:
        """
        获取所有可用接口名称
        
        Returns:
            接口名称列表
        """
        return list(self.interfaces.keys())
    
    def get_interfaces_by_priority(self) -> List[Tuple[str, int]]:
        """
        按照优先级获取接口列表
        
        Returns:
            (接口名称, 优先级)元组列表，按优先级排序
        """
        interfaces = [(name, proc.update_priority) for name, proc in self.interfaces.items()]
        return sorted(interfaces, key=lambda x: x[1])


# 创建全局接口管理器实例
interface_manager = InterfaceManager()