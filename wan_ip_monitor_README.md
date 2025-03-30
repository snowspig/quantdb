# WAN IP 监控工具

## 功能概述

该工具用于监控多WAN接口的公网IP地址状态，当检测到所有WAN接口返回相同的公网IP地址时，会触发警报。这通常表明路由器可能发生了故障，导致所有WAN口实际上共用了同一个出口IP。

## 主要文件

- `wan_ip_monitor.py`: 主要监控脚本，实现了多WAN IP检测和警报功能
- `wan_ip_test.py`: 测试工具，用于模拟不同的WAN IP状态场景

## 使用方法

### 启动监控服务

```bash
python wan_ip_monitor.py
```

该命令会启动一个持续运行的监控服务，它会定期检查所有WAN接口的公网IP，并在发现问题时触发警报。

### 测试功能

可以使用测试工具来模拟不同的情况：

```bash
# 正常模式 - 每个WAN有不同的IP
python wan_ip_test.py --mode normal

# 故障模式 - 所有WAN返回相同IP
python wan_ip_test.py --mode same_ip

# 部分故障模式 - 部分WAN返回相同IP
python wan_ip_test.py --mode partial_same
```

## 警报机制

当检测到所有WAN接口返回相同IP时，系统会：

1. 在控制台输出警报信息
2. 将警报记录到 `wan_ip_alert.log` 文件
3. 记录详细检查数据到 `wan_ip_checks.json` 文件

## 配置选项

在 `wan_ip_monitor.py` 文件中可以修改以下配置参数：

- `CHECK_INTERVAL`: 检查间隔时间（秒）
- `ALERT_INTERVAL`: 两次警报之间的最小间隔时间（秒）
- `IP_CHECK_URL`: 用于检测公网IP的服务URL

## 集成方式

该监控工具利用了现有的多WAN管理功能，包括端口分配器和WAN监控组件，可以和数据获取系统无缝集成。当检测到路由器故障时，可以通过增加响应的处理逻辑，暂停或调整数据获取任务。