# 多机器真实测试

## 注意

所有机器处于同一局域网。

运行调用链如下

```text
Central: real_test/benchmark/lab/run_stress_matrix.sh
├─ auto_disk_cleanup.sh：运行前磁盘检查和清理
├─ labctl.py deploy：同步源码到 Proxy 和 Receiver 主机
├─ labctl.py start
│  └─ SSH 到 Proxy，启动 proxy_controller.py
│     ├─ 启动 site_proxy_sender
│     └─ SSH 到每台 Receiver，启动 receiver_launcher.py
│        └─ 启动 receiver_agent
├─ 等待 Receiver 全部注册到 Proxy
├─ labctl.py run-central
│  └─ central_workload.py 启动 central_sender
│     └─ Central 主动连接 Proxy 并发送文件
├─ verify_remote_receivers.sh：核验各 Receiver 文件 SHA-256
├─ prepare_collect_hardlinks.sh：为结果收集建立硬链接
├─ labctl.py collect：收集并汇总结果
├─ 检查最终结果并清理本轮文件
└─ 退出清理：停止进程并运行 finalize_run_cleanup.sh
```

在中心端强行终止 run_stress_matrix.sh 需要注意其它机器孤儿进程，出现任何问题都先运行 finalize_run_cleanup.sh 。

## 准备


在 Central 仓库根目录复制并编辑主机配置：

```bash
cp real_test/config/hosts.lab.template.json real_test/config/hosts.json
```

填写 Proxy 信息，并在 `receiver_hosts` 中按 Receiver 主机数量增删条目。再编辑 `real_test/config/stress_1g.json` 中的输入文件和接收端数量。

> 当前压力脚本及清理脚本仍含维护者的仓库路径、SSH 和主机设置。给其他环境使用前，还需将这些值改成目标环境；仅修改 `hosts.json` 不够。

## Central 一键启动

```bash
cd ~/share/site
bash real_test/benchmark/lab/run_stress_matrix.sh
```

## 调用链文件用途

| 文件 | 用途 |
| --- | --- |
| `real_test/benchmark/lab/run_stress_matrix.sh` | Central 端主控脚本，按 `stress_1g.json` 执行实验矩阵。 |
| `real_test/benchmark/lab/labctl.py` | 部署源码、启动/查询/停止远端实验、启动 Central 发送端并收集结果。 |
| `real_test/benchmark/lab/proxy_controller.py` | 在 Proxy 上启动代理和 Receiver，等待注册并监控实验状态。 |
| `real_test/benchmark/lab/receiver_launcher.py` | 在 Receiver 主机启动并管理该机的 Receiver 进程。 |
| `real_test/benchmark/lab/central_workload.py` | 在 Central 启动 `central_sender`，连接 Proxy 传输文件。 |
| `real_test/benchmark/lab/verify_remote_receivers.sh` | 远程读取各 Receiver 文件并比较 SHA-256。 |
| `real_test/benchmark/lab/prepare_collect_hardlinks.sh` | 为校验通过的文件建立硬链接，供结果收集使用。 |
| `real_test/benchmark/lab/auto_disk_cleanup.sh` | 检查磁盘空间并按脚本策略清理数据。 |
| `real_test/benchmark/lab/finalize_run_cleanup.sh` | 停止后的收尾清理，会调用大文件清理并清理系统回收站。 |
| `real_test/benchmark/lab/trim_run_large_files.sh` | 删除指定实验目录中的大文件。 |
| `real_test/benchmark/lab/monitor_all_hosts.sh`、`real_test/benchmark/lab/monitor_bottleneck.sh` | 可选监控脚本；前者监控 Proxy 和两台 Receiver，后者监控本机。 |

配置模板：`real_test/config/hosts.lab.template.json` 用于生成主机列表；`real_test/config/experiment.lab.template.json` 是直接使用 `labctl.py` 的小规模示例，矩阵脚本实际读取 `real_test/config/stress_1g.json` 和 `real_test/config/experiment.json`。

实验日志保存在 `test-runs/lab/`。
