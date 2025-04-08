# 配置状态管理器使用指南

## 概述

配置状态管理器（Config State Manager）是一个用于解决多个脚本重复验证配置问题的工具。在使用 gorepe.py 执行多个数据获取任务时，每个任务都需要验证 MongoDB 连接、Tushare API 和多 WAN 口配置，这会导致重复验证和效率低下。配置状态管理器通过一次性验证所有配置并缓存结果，解决了这个问题。

## 系统组件

配置状态管理系统由以下组件组成：

1. **core/config_state_manager.py** - 配置状态管理器核心类，负责存储和管理配置验证状态
2. **validate_configurations.py** - 执行实际的配置验证，包括 MongoDB、Tushare API 和 WAN 配置
3. **gorepe.py 集成** - 任务调度器与配置状态管理器的集成，避免重复验证

## 工作原理

1. **状态存储**：配置验证结果存储在内存中，并可以选择保存到配置状态文件（config_state.json）
2. **有效性检查**：验证结果有一定的有效期（默认为 1 小时），超过有效期需要重新验证
3. **一次性验证**：gorepe.py 在启动时检查配置状态，仅在必要时执行验证
4. **强制验证**：可以通过命令行参数强制重新验证所有配置

## 使用方法

### 默认使用

```bash
python gorepe.py
```

这将检查配置状态，如果之前没有验证过或验证已过期，会自动验证配置。

### 强制验证配置

```bash
python gorepe.py --force-config-check
```

使用此选项可以强制重新验证所有配置，忽略缓存状态。

### 单独运行验证工具

```bash
python validate_configurations.py
```

这将执行所有配置验证并输出详细结果，适用于排查配置问题。

## 在自定义脚本中使用

在您的自定义脚本中，可以使用配置状态管理器来避免重复验证：

```python
from core.config_state_manager import config_state_manager

# 检查 MongoDB 是否已验证
mongo_valid, mongo_details = config_state_manager.is_mongodb_validated()
if mongo_valid:
    # 使用已验证的 MongoDB 配置
    mongo_info = mongo_details["connection_info"]
    print(f"使用已验证的 MongoDB 连接: {mongo_info['host']}:{mongo_info['port']}")
else:
    # 如果需要，执行验证
    from validate_configurations import validate_mongodb_connection
    success, info, error = validate_mongodb_connection()
    if success:
        config_state_manager.set_mongodb_validated(True, info)
```

类似地，您可以使用 `is_tushare_validated()` 和 `is_wan_validated()` 检查其他配置的状态。

## 配置文件

配置状态默认存储在 `config/config_state.json` 文件中，格式如下：

```json
{
  "mongodb": {
    "validated": true,
    "timestamp": 1712454379.923,
    "connection_info": {
      "host": "localhost",
      "port": 27017,
      "version": "4.4.6",
      "status": "connected"
    },
    "error": null
  },
  "tushare": {
    "validated": true,
    "timestamp": 1712454380.102,
    "api_info": {
      "token": "abcd***",
      "server": "default",
      "version": "api_v2",
      "status": "connected"
    },
    "error": null
  },
  "wan": {
    "validated": true,
    "timestamp": 1712454381.328,
    "interfaces": [
      {
        "name": "wan1",
        "port_range": [50001, 51000],
        "local_port_tested": 50123,
        "detected_ip": "203.0.113.1",
        "status": "connected"
      },
      {
        "name": "wan2",
        "port_range": [51001, 52000],
        "local_port_tested": 51456,
        "detected_ip": "198.51.100.1",
        "status": "connected"
      }
    ],
    "error": null
  }
}
```

## 配置状态超时

配置状态有效期默认为 1 小时（3600 秒）。您可以通过修改 `core/config_state_manager.py` 中的 `validation_timeout` 属性来调整这个值。

## 故障排除

如果遇到配置问题，请尝试以下步骤：

1. 使用 `--force-config-check` 参数强制重新验证配置
2. 运行 `python validate_configurations.py` 查看详细的验证结果
3. 检查日志文件中的错误信息
4. 删除 `config/config_state.json` 文件，强制重新验证所有配置

## 注意事项

- 配置状态管理器使用单例模式，全局只有一个实例
- 配置验证是并发安全的，可以在多线程或多进程环境中使用
- 验证失败时会记录详细错误信息，可用于故障排除
