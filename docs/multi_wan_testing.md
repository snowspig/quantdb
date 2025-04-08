# Multi-WAN Testing Guide

This document explains how to use the built-in tools to test and verify multi-WAN functionality in the QuantDB system.

## Overview

The QuantDB system uses multiple WAN interfaces to load balance API requests, increasing reliability and throughput. To ensure this functionality is working correctly, we've developed several testing tools.

## Configuration

The multi-WAN configuration is stored in `config/config.yaml` under the `wan` section:

```yaml
wan:
  enabled: true
  interfaces:
    - name: "wan1"
      port_range: [50001, 51000]
    - name: "wan2"
      port_range: [51001, 52000]
    - name: "wan3"
      port_range: [52001, 53000]
```

Each interface is defined with a name and a port range used for outgoing connections.

## Testing Tools

### 1. Basic WAN URL Testing

The `test_wan_url.py` script performs basic testing by making multiple requests to a specified URL using different port ranges:

```bash
python test_wan_url.py
```

This script:
- Makes requests from each configured port range
- Records the client IP address seen by the server
- Saves results to `wan_url_test_results.json`

### 2. WAN Detection Testing

For more comprehensive testing, use `test_wan_detection.py`:

```bash
python test_wan_detection.py
```

This script:
- Makes multiple requests using each port range
- Analyzes whether different WAN interfaces are being used
- Verifies correct binding to local ports
- Provides detailed statistics and analysis
- Saves results to `wan_detection_results.json`

### 3. Network Manager Testing

To test the network manager's WAN handling directly:

```bash
python test_network_manager_wan.py
```

This tests the `network_manager.py` module's ability to route requests through different WAN interfaces.

## Interpreting Results

### Multiple WAN Environment

In a true multi-WAN setup, the testing tools will show different IP addresses for different port ranges, indicating that requests are going through different network interfaces.

### Single WAN Environment

If all requests show the same IP address regardless of port range, you are likely in a single WAN environment or your ISP is using NAT that masks the true source IP.

Even in a single WAN environment, the tests confirm that:
1. The port binding is working correctly
2. The multi-WAN code logic is functional
3. Requests can be made through the specified port ranges

## Troubleshooting

If tests fail with connection errors:

1. Check that the port ranges in `config.yaml` are available for use
2. Verify firewall settings aren't blocking outgoing connections
3. Ensure the test URL is accessible through all network paths

For more detailed analysis, check the log files generated during testing.
