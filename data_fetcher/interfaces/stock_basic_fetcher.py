#!/usr/bin/env python
"""
Stock Basic Fetcher - 获取股票基本信息并保存到MongoDB

该脚本用于从湘财Tushare获取股票基本信息，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=25

使用方法：
    python stock_basic_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stock_basic_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
"""
import os
import sys
import json
import time
import pandas as pd
import pymongo
from datetime import datetime
from typing import Dict, Set, Optional, Any, Tuple
from pathlib import Path
from loguru import logger
import random

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
# 导入平台核心模块
from core import config_manager, mongodb_handler
from core.wan_manager import port_allocator
from core.tushare_client_wan import TushareClientWAN

class StockBasicFetcher:
    """
    股票基本信息获取器

    该类用于从Tushare获取股票基本信息并保存到MongoDB数据库，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stock_basic.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        excluded_stocks: Set[str] = {}, #"600349.SH", "300361.SZ", "300728.SZ"
        db_name: str = None,
        collection_name: str = "stock_basic",
        verbose: bool = False
    ):
        """
        初始化股票基本信息获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            target_market_codes: 目标市场代码集合
            excluded_stocks: 需要排除的特定股票代码集合
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.excluded_stocks = excluded_stocks
        self.collection_name = collection_name
        self.verbose = verbose

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        
        # 加载配置
        self.config = config_manager.get_all_config()
        self.interface_config = self._load_interface_config()
        
        # 获取token和api_url - 从配置文件读取
        tushare_config = self.config.get("tushare", {})
        self.token = tushare_config.get("token", "")
        self.api_url = tushare_config.get("api_url", "")
        
        # 从配置中读取db_name
        mongodb_config = self.config.get("mongodb", {})
        config_db_name = mongodb_config.get("db_name", "tushare_data")
        # 如果未传入db_name或传入为None，则使用配置文件中的值
        self.db_name = db_name if db_name is not None else config_db_name
        logger.debug(f"使用数据库名称: {self.db_name}")
        
        if not self.token:
            logger.error("未配置Tushare API Token")
            sys.exit(1)
            
        # 记录API配置信息
        mask_token = self.token[:4] + '*' * (len(self.token) - 8) + self.token[-4:] if len(self.token) > 8 else '***'
        logger.debug(f"获取到的API token长度: {len(self.token)}")
        logger.debug(f"获取到的API URL: {self.api_url}")
        
        # 初始化Tushare客户端
        self.client = TushareClientWAN(token=self.token, api_url=self.api_url)
        
        # 确保MongoDB连接初始化
        if not mongodb_handler.is_connected():
            logger.info("初始化MongoDB连接...")
            if mongodb_handler.connect():
                logger.info("MongoDB连接成功")
            else:
                logger.error("连接MongoDB失败")
                sys.exit(1)
        else:
            logger.info("MongoDB已连接")
        
        # 验证MongoDB连接和写入权限
        self._verify_mongodb_connection()
        
        # 记录可用字段和索引字段
        self.available_fields = self.interface_config.get("available_fields", [])
        self.index_fields = self.interface_config.get("index_fields", ["ts_code", "symbol"])
        
        # 检查多WAN口支持
        wan_config = config_manager.get_wan_config()
        self.port_allocator = port_allocator
        
        if wan_config.get("enabled", False):
            available_indices = self.port_allocator.get_available_wan_indices()
            wan_count = len(available_indices)
            if wan_count > 0:
                logger.info(f"已启用多WAN接口支持，WAN接口数量: {wan_count}")
            else:
                logger.warning("已启用多WAN接口支持，但未找到可用WAN接口")
        else:
            logger.warning("未启用多WAN接口支持，将使用系统默认网络接口")

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

    def _verify_mongodb_connection(self):
        """验证MongoDB连接和写入权限"""
        try:
            # 尝试ping命令来验证连接和权限
            if mongodb_handler.is_connected():
                db = mongodb_handler.get_database()
                # 使用ping命令验证连接
                db.command('ping')
                logger.debug(f"MongoDB连接和写入权限验证成功")
            else:
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("MongoDB连接失败")
        except Exception as e:
            logger.error(f"MongoDB连接或写入权限验证失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
    
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
            
            # 使用接口配置中的available_fields作为请求字段
            fields = self.interface_config.get("available_fields", [])
            if not fields:
                logger.warning("接口配置中未定义available_fields，将获取所有字段")
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket()
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.debug(f"正在从湘财Tushare获取股票基本信息...")
            logger.debug(f"请求字段：{fields}")
            
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
                missing_fields = [field for field in self.interface_config.get("index_fields", []) if field not in df.columns]
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

    def filter_stock_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤股票数据，只保留00、30、60和68板块的股票，并排除特定股票
        同时只保留上市日期(list_date)小于等于今天的股票
        
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
            logger.info(f"已排除 {excluded_count} 支特定股票: {', '.join(self.excluded_stocks)}")
        
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
    
    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 待保存的DataFrame
            
        Returns:
            是否成功保存
        """
        logger.info(f"保存数据到MongoDB数据库：{self.db_name}，集合：{self.collection_name}")
        
        if df is None or df.empty:
            logger.warning("没有数据可保存到MongoDB")
            return False
            
        try:
            # 将DataFrame转换为记录列表
            records = df.to_dict('records')
            
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not mongodb_handler.connect():
                    logger.error("重新连接MongoDB失败")
                    return False
            
            # 批量保存数据的统计信息
            stats = {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0}
            
            try:
                start_time = time.time()
                
                # 获取ts_code列表，用于查询现有记录
                ts_codes = df['ts_code'].tolist() if 'ts_code' in df.columns else []
                
                # 获取已存在的股票记录
                existing_records = []
                if ts_codes:
                    existing_records = mongodb_handler.find_documents(
                        self.collection_name, 
                        {"ts_code": {"$in": ts_codes}}
                    )
                
                # 建立字典便于查找和比较现有记录
                existing_records_dict = {record["ts_code"]: record for record in existing_records if "ts_code" in record}
                existing_ts_codes = set(existing_records_dict.keys())
                logger.debug(f"找到 {len(existing_ts_codes)} 条已存在的股票记录")
                
                # 批量处理数据
                logger.debug(f"开始批量处理 {len(records)} 条记录...")
                
                # 分离新增数据和更新数据
                new_records = []
                update_operations = []
                
                for record in records:
                    if "ts_code" in record and record["ts_code"] in existing_ts_codes:
                        # 检查记录是否有实际变化（需要更新）
                        existing_record = existing_records_dict[record["ts_code"]]
                        need_update = False
                        
                        # 比较所有字段是否有变化
                        for key, value in record.items():
                            if key not in existing_record or existing_record[key] != value:
                                need_update = True
                                break
                        
                        if need_update:
                            # 构建更新操作
                            update_operations.append(
                                pymongo.UpdateOne(
                                    {"ts_code": record["ts_code"]},
                                    {"$set": record}
                                )
                            )
                        else:
                            # 记录没有变化，统计为跳过
                            stats["skipped"] += 1
                    else:
                        new_records.append(record)
                
                logger.info(f"处理统计: 新记录:{len(new_records)}条, 需更新:{len(update_operations)}条, 无变化跳过:{stats['skipped']}条")
                
                # 批量插入新记录
                if new_records:
                    try:
                        # 使用mongodb_handler的批量插入方法
                        result_ids = mongodb_handler.insert_many_documents(self.collection_name, new_records, ordered=False)
                        stats["inserted"] = len(result_ids)
                        logger.info(f"已批量插入 {stats['inserted']} 条新记录")
                    except pymongo.errors.BulkWriteError as e:
                        # 处理部分插入错误
                        stats["inserted"] = e.details.get('nInserted', 0)
                        failed = len(new_records) - stats["inserted"]
                        stats["failed"] += failed
                        logger.warning(f"批量插入过程中发生 {failed} 条记录写入失败")
                
                # 批量更新记录
                if update_operations:
                    try:
                        # 使用mongodb_handler的批量写入方法
                        result = mongodb_handler.bulk_write(self.collection_name, update_operations, ordered=False)
                        # matched_count表示匹配的记录数，modified_count表示实际修改的记录数
                        matched = result.matched_count
                        modified = result.modified_count
                        stats["updated"] = modified
                        
                        if matched > modified:
                            # 有匹配但未修改的记录，这些记录实际上是完全相同的
                            stats["skipped"] += (matched - modified)
                            logger.debug(f"有 {matched - modified} 条记录匹配但未修改(内容相同)")
                            
                        logger.info(f"已批量更新 {stats['updated']} 条记录")
                    except pymongo.errors.BulkWriteError as e:
                        # 处理部分更新错误
                        stats["updated"] = e.details.get('nModified', 0)
                        failed = len(update_operations) - stats["updated"]
                        stats["failed"] += failed
                        logger.warning(f"批量更新过程中发生 {failed} 条记录更新失败")
                
                # 验证是否有数据被实际写入
                try:
                    count = mongodb_handler.count_documents(self.collection_name, {})
                    logger.debug(f"查询到 {count} 条已存储的记录")
                    

                    
                    # 随机检查一条记录
                    if stats["inserted"] > 0 or stats["updated"] > 0:
                        sample_record = random.choice([r for r in records if "ts_code" in r])
                        query = {"ts_code": sample_record["ts_code"]}
                        found = mongodb_handler.find_document(self.collection_name, query)
                        if found:
                            logger.debug(f"验证写入成功：找到记录 {query}")
                        else:
                            logger.warning(f"验证写入失败：未找到记录 {query}")
                except Exception as e:
                    logger.warning(f"验证写入时出错: {str(e)}")
                                    # 记录操作耗时
                elapsed = time.time() - start_time
                # 总结数据保存结果
                logger.success(f"新插入 {stats['inserted']} 条记录，"
                              f"更新 {stats['updated']} 条记录，"
                              f"跳过 {stats['skipped']} 条记录，"
                              f"失败 {stats['failed']} 条记录，"
                              f"耗时: {elapsed:.2f}秒")
                
                # 如果没有错误发生，就认为操作成功，即使没有实际更新记录
                if stats["failed"] == 0:
                    return True
                # 老的逻辑保留：如果有数据被插入或更新，也返回成功
                elif stats["inserted"] > 0 or stats["updated"] > 0:
                    return True
                else:
                    return False
                
            except Exception as e:
                logger.error(f"保存数据过程中出错: {str(e)}")
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
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
        try:
            # 第一步：检查并确保集合和索引存在
            logger.info("第一步：检查并确保MongoDB集合和索引存在")
            if not self._ensure_collection_and_indexes():
                logger.error("无法确保MongoDB集合和索引，放弃数据获取")
                return False
            
            # 第二步：获取数据
            logger.info("第二步：从湘财Tushare获取股票基本信息")
            df = self.fetch_stock_basic()
            if df is None or df.empty:
                logger.error("获取股票基本信息失败")
                return False
                
            # 第三步：过滤数据
            logger.info("第三步：根据市场代码和排除列表过滤数据")
            filtered_df = self.filter_stock_data(df)
            if filtered_df.empty:
                logger.warning("过滤后没有符合条件的股票数据")
                return False
                
            # 第四步：保存数据到MongoDB
            logger.info("第四步：将股票基本信息保存到MongoDB")
            success = self.save_to_mongodb(filtered_df)
            
            return success
            
        except Exception as e:
            logger.error(f"运行过程中发生异常: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def _ensure_collection_and_indexes(self) -> bool:
        """
        确保MongoDB集合存在并具有正确的索引
        
        Returns:
            bool: 是否成功确保集合和索引
        """
        try:
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.info("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return False
                    
            logger.info(f"检查MongoDB集合 {self.collection_name} 是否存在")
            
            # 使用collection_exists和create_collection方法
            if not mongodb_handler.collection_exists(self.collection_name):
                logger.debug(f"集合 {self.collection_name} 不存在，正在创建...")
                mongodb_handler.create_collection(self.collection_name)
                logger.debug(f"集合 {self.collection_name} 已成功创建")
            else:
                logger.debug(f"集合 {self.collection_name} 已存在")
            
            # 检查并创建索引
            collection = mongodb_handler.get_collection(self.collection_name)
            if collection is None:
                logger.error(f"无法获取集合: {self.collection_name}")
                return False
                
            # 检查并确保索引
            return self._ensure_collection_indexes(collection)
                
        except Exception as e:
            logger.error(f"确保集合和索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _ensure_collection_indexes(self, collection):
        """确保集合有适当的索引"""
        try:
            # 使用接口配置中的index_fields作为索引字段
            index_fields = self.interface_config.get("index_fields", ["ts_code", "symbol"])
            
            # 从配置文件获取索引字段
            if not index_fields:
                # 尝试从接口配置文件重新加载索引字段
                interface_path = os.path.join(self.interface_dir, self.interface_name)
                try:
                    with open(interface_path, 'r', encoding='utf-8') as f:
                        interface_config = json.load(f)
                        index_fields = interface_config.get("index_fields", ["ts_code", "symbol"])
                        logger.info(f"从接口配置文件加载索引字段: {index_fields}")
                except Exception as e:
                    logger.warning(f"从接口配置文件加载索引字段失败: {str(e)}")
                    index_fields = ["ts_code", "symbol"]
                    logger.warning(f"使用默认索引: {index_fields}")
            
            logger.debug(f"为集合 {self.collection_name} 设置索引字段: {index_fields}")
            
            # 检查索引是否已存在
            try:
                # 使用collection.index_information()获取更详细的索引信息
                existing_indexes = collection.index_information()
                logger.debug(f"现有索引信息: {existing_indexes}")
                
                # 确保ts_code和symbol字段有唯一索引
                unique_fields = ["ts_code", "symbol"]
                
                # 针对股票基本信息表的特殊处理 - 创建ts_code和symbol的唯一索引
                for field in unique_fields:
                    if field in index_fields:
                        # 构建索引名称
                        index_name = f"{field}_1"
                        
                        # 检查索引是否已存在
                        if index_name in existing_indexes:
                            # 检查是否是唯一索引
                            if existing_indexes[index_name].get('unique', False):
                                logger.debug(f"字段 {field} 的唯一索引已存在，跳过创建")
                            else:
                                # 索引存在但不是唯一索引，删除并重新创建
                                logger.info(f"字段 {field} 的索引存在但不是唯一索引，重新创建...")
                                try:
                                    collection.drop_index(index_name)
                                    collection.create_index(
                                        [(field, 1)],
                                        unique=True,
                                        background=True,
                                        name=index_name
                                    )
                                    logger.info(f"已为字段 {field} 重新创建唯一索引")
                                except Exception as e:
                                    logger.error(f"重新创建唯一索引时出错: {str(e)}")
                        else:
                            # 索引不存在，创建它
                            logger.info(f"正在为字段 {field} 创建唯一索引...")
                            try:
                                collection.create_index(
                                    [(field, 1)],
                                    unique=True,
                                    background=True,
                                    name=index_name
                                )
                                logger.info(f"已为字段 {field} 创建唯一索引")
                            except Exception as e:
                                logger.error(f"创建唯一索引时出错: {str(e)}")
                
                # 创建其他普通索引
                for field in index_fields:
                    if field not in unique_fields:  # 跳过已创建唯一索引的字段
                        index_name = f"{field}_1"
                        if index_name not in existing_indexes:
                            logger.info(f"正在为字段 {field} 创建普通索引...")
                            try:
                                collection.create_index(
                                    [(field, 1)],
                                    background=True,
                                    name=index_name
                                )
                                logger.info(f"已为字段 {field} 创建普通索引")
                            except Exception as e:
                                logger.warning(f"创建普通索引时出错: {str(e)}")
                        else:
                            logger.debug(f"字段 {field} 的索引已存在，跳过创建")
                
            except Exception as e:
                logger.warning(f"获取索引信息时出错: {str(e)}")
                # 简化处理：尝试直接创建默认索引
                try:
                    # 为ts_code和symbol创建唯一索引
                    for field in ["ts_code", "symbol"]:
                        if field in index_fields:
                            collection.create_index(
                                [(field, 1)],
                                unique=True,
                                background=True
                            )
                            logger.info(f"已为字段 {field} 创建唯一索引")
                except Exception as e:
                    logger.error(f"创建默认索引时出错: {str(e)}")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"确保索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

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


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票基本信息并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--excluded-stocks', default='', help='需要排除的股票代码，用逗号分隔')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stock_basic', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
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
        
        # 使用模拟数据还是真实API数据

            # 正常运行模式
        if args.dry_run:
            logger.info("干运行模式，仅检查集合和索引，获取数据但不保存")
            
            # 检查集合和索引
            if not fetcher._ensure_collection_and_indexes():
                logger.error("检查集合和索引失败")
                sys.exit(1)
                
            # 获取数据
            df = fetcher.fetch_stock_basic()
            if df is not None and not df.empty:
                filtered_df = fetcher.filter_stock_data(df)
                if not filtered_df.empty:
                    logger.success(f"成功获取并过滤 {len(filtered_df)} 条数据，但不保存到数据库")
                    if args.verbose:
                        print("\n数据示例：")
                        print(filtered_df.head(5))
                    success = True
                else:
                    logger.error("过滤后没有符合条件的数据")
                    success = False
            else:
                logger.error("获取数据失败")
                success = False
        else:
            # 正常运行模式
            success = fetcher.run()
        
        if success:
            logger.success("股票基本信息数据" + ("获取成功" if args.dry_run else "获取和保存成功"))
            sys.exit(0)
        else:
            logger.error("股票基本信息数据" + ("获取失败" if args.dry_run else "获取或保存失败"))
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"运行过程中发生异常: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
