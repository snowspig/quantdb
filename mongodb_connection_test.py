#!/usr/bin/env python
"""
MongoDB Connection Test Script - 测试MongoDB连接和认证

这个脚本用于测试MongoDB连接和认证设置，并提供详细的故障排除信息
"""
import os
import sys
import yaml
import time
import argparse
from urllib.parse import quote_plus
from typing import Dict, Any, Optional, Tuple
from loguru import logger

try:
    import pymongo
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
except ImportError:
    logger.error("未安装pymongo模块，请运行: pip install pymongo")
    sys.exit(1)


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


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    从YAML文件加载配置
    
    Args:
        config_path: 配置文件路径,如果为None则使用默认路径
    
    Returns:
        加载的配置字典
    """
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'config', 'config.yaml'
        )
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            logger.info(f"成功从 {config_path} 加载配置")
            return config
    except Exception as e:
        logger.error(f"加载配置失败: {str(e)}")
        raise


def build_connection_uri_standard(config: Dict[str, Any]) -> str:
    """
    构建标准MongoDB连接URI
    
    Args:
        config: MongoDB配置
    
    Returns:
        MongoDB连接URI字符串
    """
    mongodb_config = config.get('mongodb', {})
    
    # 获取基本配置
    host = mongodb_config.get('host', '127.0.0.1')
    port = mongodb_config.get('port', 27017)
    username = mongodb_config.get('username')
    password = mongodb_config.get('password')
    auth_source = mongodb_config.get('auth_source', 'admin')
    auth_mechanism = mongodb_config.get('auth_mechanism', 'SCRAM-SHA-1')
    
    # 构建URI
    if username and password:
        # 转义用户名和密码中的特殊字符
        username = quote_plus(username)
        password = quote_plus(password)
        uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}&authMechanism={auth_mechanism}"
    else:
        uri = f"mongodb://{host}:{port}/"
    
    # 添加其他选项
    options = mongodb_config.get('options', {})
    if options:
        option_parts = []
        for key, value in options.items():
            option_parts.append(f"{key}={value}")
        
        if username and password:  # 已经有了参数
            uri += "&" + "&".join(option_parts)
        else:  # 还没有参数
            uri += "?" + "&".join(option_parts)
    
    return uri


def build_connection_uri_srv(config: Dict[str, Any]) -> str:
    """
    构建MongoDB SRV连接URI
    
    Args:
        config: MongoDB配置
    
    Returns:
        MongoDB SRV连接URI字符串
    """
    mongodb_config = config.get('mongodb', {})
    
    # 获取基本配置
    host = mongodb_config.get('host', 'cluster0.mongodb.net')
    username = mongodb_config.get('username')
    password = mongodb_config.get('password')
    
    # 构建SRV URI
    if username and password:
        # 转义用户名和密码中的特殊字符
        username = quote_plus(username)
        password = quote_plus(password)
        uri = f"mongodb+srv://{username}:{password}@{host}/"
    else:
        uri = f"mongodb+srv://{host}/"
    
    # 添加其他选项
    options_str = ""
    options = mongodb_config.get('options', {})
    auth_source = mongodb_config.get('auth_source')
    auth_mechanism = mongodb_config.get('auth_mechanism')
    
    options_list = []
    
    if auth_source:
        options_list.append(f"authSource={auth_source}")
    
    if auth_mechanism:
        options_list.append(f"authMechanism={auth_mechanism}")
    
    for key, value in options.items():
        options_list.append(f"{key}={value}")
    
    if options_list:
        options_str = "?" + "&".join(options_list)
    
    return uri + options_str


def extract_connection_parts_from_uri(uri: str) -> Dict[str, Any]:
    """
    从URI中提取连接信息
    
    Args:
        uri: MongoDB连接URI
    
    Returns:
        包含连接部分的字典
    """
    result = {
        "scheme": "mongodb",
        "host": None,
        "port": 27017,
        "username": None,
        "password": None,
        "auth_source": None,
        "auth_mechanism": None,
        "options": {}
    }
    
    # 检查URI格式
    if not uri.startswith("mongodb"):
        return result
    
    # 判断是否是SRV格式
    if uri.startswith("mongodb+srv://"):
        result["scheme"] = "mongodb+srv"
        uri = uri.replace("mongodb+srv://", "")
    else:
        uri = uri.replace("mongodb://", "")
    
    # 提取认证信息和主机信息
    if "@" in uri:
        auth_part, uri = uri.split("@", 1)
        if ":" in auth_part:
            result["username"], result["password"] = auth_part.split(":", 1)
    
    # 提取主机和端口
    if "/" in uri:
        host_part, params_part = uri.split("/", 1)
        if ":" in host_part:
            result["host"], port_str = host_part.split(":", 1)
            if port_str.isdigit():
                result["port"] = int(port_str)
        else:
            result["host"] = host_part
    else:
        result["host"] = uri
    
    # 提取参数
    if "?" in uri:
        params_str = uri.split("?", 1)[1]
        params = params_str.split("&")
        
        for param in params:
            if "=" in param:
                key, value = param.split("=", 1)
                if key == "authSource":
                    result["auth_source"] = value
                elif key == "authMechanism":
                    result["auth_mechanism"] = value
                else:
                    result["options"][key] = value
    
    return result


def test_mongodb_connection_with_direct_params(
    host: str, 
    port: int, 
    username: Optional[str] = None, 
    password: Optional[str] = None, 
    auth_source: str = "admin",
    auth_mechanism: str = "SCRAM-SHA-1",
    timeout_ms: int = 5000
) -> Tuple[bool, str]:
    """
    使用直接参数测试MongoDB连接
    
    Args:
        host: MongoDB主机
        port: MongoDB端口
        username: 用户名 (可选)
        password: 密码 (可选)
        auth_source: 认证数据库
        auth_mechanism: 认证机制
        timeout_ms: 超时时间(毫秒)
        
    Returns:
        成功标志和消息元组
    """
    start_time = time.time()
    
    try:
        # 构建客户端对象
        client_kwargs = {
            "host": host,
            "port": port,
            "serverSelectionTimeoutMS": timeout_ms
        }
        
        if username and password:
            client_kwargs.update({
                "username": username,
                "password": password,
                "authSource": auth_source,
                "authMechanism": auth_mechanism
            })
            
        client = MongoClient(**client_kwargs)
        
        # 测试连接
        client.admin.command('ping')
        
        # 获取服务器信息
        server_info = client.server_info()
        version = server_info.get('version', 'Unknown')
        
        # 获取可用数据库列表
        if username and password:
            database_names = client.list_database_names()
            db_count = len(database_names)
            db_list = ", ".join(database_names[:5]) + ("..." if db_count > 5 else "")
        else:
            db_count = 0
            db_list = "N/A (未认证)"
        
        elapsed = time.time() - start_time
        
        return True, f"连接成功! MongoDB版本: {version}, 可用数据库: {db_count} ({db_list}), 耗时: {elapsed:.2f}秒"
        
    except ConnectionFailure as e:
        elapsed = time.time() - start_time
        return False, f"连接失败: 无法连接到服务器 {host}:{port}, 错误: {str(e)}, 耗时: {elapsed:.2f}秒"
        
    except OperationFailure as e:
        elapsed = time.time() - start_time
        if "Authentication failed" in str(e):
            return False, f"认证失败: 用户名或密码错误, 错误: {str(e)}, 耗时: {elapsed:.2f}秒"
        else:
            return False, f"操作失败: {str(e)}, 耗时: {elapsed:.2f}秒"
            
    except ServerSelectionTimeoutError as e:
        elapsed = time.time() - start_time
        return False, f"服务器选择超时: {str(e)}, 耗时: {elapsed:.2f}秒"
        
    except Exception as e:
        elapsed = time.time() - start_time
        return False, f"未知错误: {str(e)}, 耗时: {elapsed:.2f}秒"


def test_mongodb_connection_with_uri(uri: str, timeout_ms: int = 5000) -> Tuple[bool, str]:
    """
    使用URI测试MongoDB连接
    
    Args:
        uri: MongoDB连接URI
        timeout_ms: 超时时间(毫秒)
        
    Returns:
        成功标志和消息元组
    """
    start_time = time.time()
    
    try:
        # 构建客户端对象
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        
        # 测试连接
        client.admin.command('ping')
        
        # 获取服务器信息
        server_info = client.server_info()
        version = server_info.get('version', 'Unknown')
        
        # 尝试列出数据库
        try:
            database_names = client.list_database_names()
            db_count = len(database_names)
            db_list = ", ".join(database_names[:5]) + ("..." if db_count > 5 else "")
        except OperationFailure:
            db_count = 0
            db_list = "N/A (无权限列出数据库)"
        
        elapsed = time.time() - start_time
        
        return True, f"连接成功! MongoDB版本: {version}, 可用数据库: {db_count} ({db_list}), 耗时: {elapsed:.2f}秒"
        
    except ConnectionFailure as e:
        elapsed = time.time() - start_time
        return False, f"连接失败: URI格式可能正确，但无法连接到服务器, 错误: {str(e)}, 耗时: {elapsed:.2f}秒"
        
    except OperationFailure as e:
        elapsed = time.time() - start_time
        if "Authentication failed" in str(e):
            return False, f"认证失败: 用户名或密码错误, 错误: {str(e)}, 耗时: {elapsed:.2f}秒"
        else:
            return False, f"操作失败: {str(e)}, 耗时: {elapsed:.2f}秒"
            
    except ServerSelectionTimeoutError as e:
        elapsed = time.time() - start_time
        return False, f"服务器选择超时: {str(e)}, 耗时: {elapsed:.2f}秒"
        
    except Exception as e:
        elapsed = time.time() - start_time
        return False, f"未知错误: {str(e)}, 耗时: {elapsed:.2f}秒"


def test_database_access(uri: str, database_name: str) -> Tuple[bool, str]:
    """
    测试特定数据库的访问权限
    
    Args:
        uri: MongoDB连接URI
        database_name: 数据库名称
        
    Returns:
        成功标志和消息元组
    """
    start_time = time.time()
    
    try:
        # 构建客户端对象
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # 测试连接
        client.admin.command('ping')
        
        # 获取指定数据库
        db = client[database_name]
        
        # 尝试列出集合
        try:
            collection_names = db.list_collection_names()
            coll_count = len(collection_names)
            coll_list = ", ".join(collection_names[:5]) + ("..." if coll_count > 5 else "")
            
            elapsed = time.time() - start_time
            return True, f"数据库 '{database_name}' 访问成功! 集合数: {coll_count} ({coll_list}), 耗时: {elapsed:.2f}秒"
            
        except OperationFailure as e:
            elapsed = time.time() - start_time
            return False, f"没有权限访问数据库 '{database_name}' 中的集合, 错误: {str(e)}, 耗时: {elapsed:.2f}秒"
        
    except ConnectionFailure as e:
        elapsed = time.time() - start_time
        return False, f"连接失败: URI格式可能正确，但无法连接到服务器, 错误: {str(e)}, 耗时: {elapsed:.2f}秒"
        
    except OperationFailure as e:
        elapsed = time.time() - start_time
        if "Authentication failed" in str(e):
            return False, f"认证失败: 用户名或密码错误, 错误: {str(e)}, 耗时: {elapsed:.2f}秒"
        else:
            return False, f"操作失败: {str(e)}, 耗时: {elapsed:.2f}秒"
            
    except Exception as e:
        elapsed = time.time() - start_time
        return False, f"未知错误: {str(e)}, 耗时: {elapsed:.2f}秒"


def test_connection_from_config(config: Dict[str, Any]) -> None:
    """
    从配置数据测试MongoDB连接
    
    Args:
        config: 配置字典
    """
    mongodb_config = config.get('mongodb', {})
    
    # 1. 使用配置中的URI直接测试
    if 'uri' in mongodb_config:
        uri = mongodb_config['uri']
        logger.info(f"测试配置中的原始URI: {uri}")
        
        # 提取URI部分用于显示
        uri_parts = extract_connection_parts_from_uri(uri)
        logger.info(f"URI解析结果: scheme={uri_parts['scheme']}, host={uri_parts['host']}, port={uri_parts['port']}")
        logger.info(f"认证: username={uri_parts['username']}, authSource={uri_parts['auth_source']}, authMechanism={uri_parts['auth_mechanism']}")
        
        # 测试连接
        success, message = test_mongodb_connection_with_uri(uri)
        
        if success:
            logger.success(message)
        else:
            logger.error(message)
        
        # 如果URI连接失败，尝试修复
        if not success:
            logger.info("尝试构建新的连接URI...")
            
            # 检查uri是否包含数据库名称
            if '/' in uri and not uri.endswith('/'):
                parts = uri.split('/')
                if '?' in parts[-1]:
                    db_name = parts[-1].split('?')[0]
                else:
                    db_name = parts[-1]
                
                if db_name:
                    logger.warning(f"URI中包含数据库名称 '{db_name}'，这可能导致认证问题。尝试移除数据库名称...")
                    uri = uri.replace(f"/{db_name}", "/")
                    logger.info(f"修改后的URI: {uri}")
                    
                    success, message = test_mongodb_connection_with_uri(uri)
                    if success:
                        logger.success(f"使用修改后的URI连接成功: {message}")
                    else:
                        logger.error(f"使用修改后的URI仍然失败: {message}")
            
            # 尝试新格式URI
            host = uri_parts['host'] or mongodb_config.get('host', '127.0.0.1')
            port = uri_parts['port'] or mongodb_config.get('port', 27017)
            username = uri_parts['username'] or mongodb_config.get('username')
            password = uri_parts['password'] or mongodb_config.get('password')
            auth_source = uri_parts['auth_source'] or mongodb_config.get('auth_source', 'admin')
            auth_mechanism = uri_parts['auth_mechanism'] or mongodb_config.get('auth_mechanism', 'SCRAM-SHA-1')
            
            # 构建新的URI
            new_uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}&authMechanism={auth_mechanism}"
            logger.info(f"生成新的URI: {new_uri}")
            
            success, message = test_mongodb_connection_with_uri(new_uri)
            if success:
                logger.success(f"使用新生成的URI连接成功: {message}")
                logger.info("建议更新配置文件中的URI为此格式")
            else:
                logger.error(f"使用新生成的URI仍然失败: {message}")
    
    # 2. 使用分解的参数测试
    if 'host' in mongodb_config or 'username' in mongodb_config:
        host = mongodb_config.get('host', '127.0.0.1')
        port = mongodb_config.get('port', 27017)
        username = mongodb_config.get('username')
        password = mongodb_config.get('password')
        auth_source = mongodb_config.get('auth_source', 'admin')
        auth_mechanism = mongodb_config.get('auth_mechanism', 'SCRAM-SHA-1')
        
        logger.info(f"使用分解参数测试连接: host={host}, port={port}, username={username}, authSource={auth_source}")
        
        success, message = test_mongodb_connection_with_direct_params(
            host=host,
            port=port,
            username=username,
            password=password,
            auth_source=auth_source,
            auth_mechanism=auth_mechanism
        )
        
        if success:
            logger.success(message)
        else:
            logger.error(message)
    
    # 3. 测试数据库访问
    db_name = mongodb_config.get('db')
    if db_name and ('uri' in mongodb_config or 'host' in mongodb_config):
        if 'uri' in mongodb_config:
            uri = mongodb_config['uri']
        else:
            # 构建URI
            host = mongodb_config.get('host', '127.0.0.1')
            port = mongodb_config.get('port', 27017)
            username = mongodb_config.get('username')
            password = mongodb_config.get('password')
            auth_source = mongodb_config.get('auth_source', 'admin')
            auth_mechanism = mongodb_config.get('auth_mechanism', 'SCRAM-SHA-1')
            
            if username and password:
                uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}&authMechanism={auth_mechanism}"
            else:
                uri = f"mongodb://{host}:{port}/"
        
        logger.info(f"测试数据库 '{db_name}' 的访问权限")
        success, message = test_database_access(uri, db_name)
        
        if success:
            logger.success(message)
        else:
            logger.error(message)


def generate_fixed_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    生成修复后的配置
    
    Args:
        config: 原始配置
        
    Returns:
        修复后的配置
    """
    fixed_config = config.copy()
    mongodb_config = config.get('mongodb', {}).copy()
    
    if not mongodb_config:
        logger.warning("配置中没有MongoDB部分，无法生成修复配置")
        return fixed_config
    
    # 获取参数
    host = mongodb_config.get('host', '127.0.0.1')
    port = mongodb_config.get('port', 27017)
    username = mongodb_config.get('username')
    password = mongodb_config.get('password')
    auth_source = mongodb_config.get('auth_source', 'admin')
    auth_mechanism = mongodb_config.get('auth_mechanism', 'SCRAM-SHA-1')
    
    # 如果没有指定host但有URI，从URI中提取
    if 'uri' in mongodb_config and not mongodb_config.get('host'):
        uri_parts = extract_connection_parts_from_uri(mongodb_config['uri'])
        host = uri_parts['host'] or host
        port = uri_parts['port'] or port
        username = uri_parts['username'] or username
        password = uri_parts['password'] or password
        auth_source = uri_parts['auth_source'] or auth_source
        auth_mechanism = uri_parts['auth_mechanism'] or auth_mechanism
    
    # 构建新的URI
    if username and password:
        new_uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}&authMechanism={auth_mechanism}"
    else:
        new_uri = f"mongodb://{host}:{port}/"
    
    # 更新配置
    mongodb_config['uri'] = new_uri
    mongodb_config['host'] = host
    mongodb_config['port'] = port
    if username:
        mongodb_config['username'] = username
    if password:
        mongodb_config['password'] = password
    mongodb_config['auth_source'] = auth_source
    mongodb_config['auth_mechanism'] = auth_mechanism
    
    fixed_config['mongodb'] = mongodb_config
    return fixed_config


def save_fixed_config(config: Dict[str, Any], output_path: str):
    """
    保存修复后的配置
    
    Args:
        config: 修复后的配置
        output_path: 输出文件路径
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        logger.success(f"已保存修复配置到: {output_path}")
    except Exception as e:
        logger.error(f"保存配置失败: {str(e)}")


def generate_connection_code_example():
    """生成连接代码示例"""
    logger.info("\n===== MongoDB连接代码示例 =====")
    
    python_code = """
# 方法1: 使用URI连接字符串(推荐)
from pymongo import MongoClient

# 正确的URI格式示例
uri = "mongodb://username:password@hostname:port/?authSource=admin&authMechanism=SCRAM-SHA-1"
client = MongoClient(uri)

# 访问数据库
db = client['database_name']
collection = db['collection_name']
    
# 方法2: 使用参数连接
client = MongoClient(
    host='hostname',
    port=27017,
    username='username',
    password='password',
    authSource='admin',
    authMechanism='SCRAM-SHA-1'
)

# 注意: 如果密码中包含特殊字符，应使用urllib.parse.quote_plus()进行URL编码
from urllib.parse import quote_plus
password = quote_plus("my@complex:password!")
uri = f"mongodb://username:{password}@hostname:port/?authSource=admin"
"""
    logger.info(f"Python连接代码:\n{python_code}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='测试MongoDB连接和认证')
    parser.add_argument('--verbose', action='store_true', help='显示详细日志')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--generate-fixed', action='store_true', help='生成修复后的配置')
    parser.add_argument('--output-config', help='修复配置输出路径')
    parser.add_argument('--uri', help='直接测试MongoDB连接URI')
    parser.add_argument('--host', help='MongoDB主机')
    parser.add_argument('--port', type=int, default=27017, help='MongoDB端口')
    parser.add_argument('--username', help='用户名')
    parser.add_argument('--password', help='密码')
    parser.add_argument('--auth-source', default='admin', help='认证数据库')
    parser.add_argument('--db', help='要测试的数据库')
    parser.add_argument('--examples', action='store_true', help='显示连接代码示例')
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.verbose)
    
    # 显示代码示例
    if args.examples:
        generate_connection_code_example()
        return
    
    # 测试直接提供的URI
    if args.uri:
        logger.info(f"测试提供的URI: {args.uri}")
        success, message = test_mongodb_connection_with_uri(args.uri)
        if success:
            logger.success(message)
        else:
            logger.error(message)
            
        # 测试指定数据库
        if args.db:
            logger.info(f"测试数据库 '{args.db}' 的访问权限")
            success, message = test_database_access(args.uri, args.db)
            if success:
                logger.success(message)
            else:
                logger.error(message)
                
        return
    
    # 测试直接提供的参数
    if args.host:
        logger.info(f"使用直接参数测试连接: host={args.host}, port={args.port}, username={args.username}")
        success, message = test_mongodb_connection_with_direct_params(
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            auth_source=args.auth_source
        )
        if success:
            logger.success(message)
        else:
            logger.error(message)
            
        # 测试指定数据库
        if args.db:
            # 构建URI
            uri = f"mongodb://{args.username}:{args.password}@{args.host}:{args.port}/?authSource={args.auth_source}"
            if not args.username or not args.password:
                uri = f"mongodb://{args.host}:{args.port}/"
                
            logger.info(f"测试数据库 '{args.db}' 的访问权限")
            success, message = test_database_access(uri, args.db)
            if success:
                logger.success(message)
            else:
                logger.error(message)
                
        return
    
    # 从配置文件测试
    try:
        config = load_config(args.config)
        test_connection_from_config(config)
        
        # 生成修复配置
        if args.generate_fixed:
            fixed_config = generate_fixed_config(config)
            output_path = args.output_config or "fixed_config.yaml"
            save_fixed_config(fixed_config, output_path)
            
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        if args.verbose:
            import traceback
            logger.debug(f"错误详情: {traceback.format_exc()}")
            
    logger.info("MongoDB连接测试完成")


if __name__ == '__main__':
    main()