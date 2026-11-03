# SiteRepairCast

SiteRepairCast 是一个面向多园区静态文件分发的 Linux/C++17 系统。中心端通过 TCP 向每个园区代理传输一份文件，代理缓存并校验数据，再通过 UDP 组播向园区内多个接收端分发。接收端发生缺块时，代理汇总缺块位图并优先修复公共缺块；Peer 模式下，代理按当前接收状态和固定阈值安排接收端互传，不满足条件时回退到代理修复。传输性能和跨园区流量取决于文件、网络及丢包等实际条件，具体结果以对应测试记录为准。

系统采用固定园区代理和固定接收端集合，关注跨园区重复流量控制、Section 级背压、局部修复、慢节点隔离及异常退出恢复。Peer 修复由代理统一决策和下发任务。

## 架构

```text
                           central_sender
                                 |
                                 | TCP：文件元数据、Section、checkpoint 续传
                                 v
                          site_proxy_sender
                          (园区代理 / 缓存)
                                 |
               +-----------------+------------------+
               |                                    |
  UDP 组播：初始数据、公共缺块修复                TCP：注册、缺块位图、接收端互传任务、
               |                             传输完毕确认、慢节点独立补传
               v                                    v
         receiver_agent a  <---------------->  receiver_agent b
                      UDP 单播：代理调度的接收端互传

代理自身也可经 UDP 组播、单播修复无法互传的零散缺块，并在修复轮次后通过 TCP 为隔离接收端补传。
```

三类进程的职责：

- `central_sender`：计算文件哈希值和传输ID，按 Section 向一个或多个园区代理推送文件，并根据代理 checkpoint 续传；不同代理使用独立线程。
- `site_proxy_sender`：接收并缓存中心数据，收集接收端缺块报告，在启用与不启用接收端互传两种修复模式间切换，负责接收端互传任务调度、代理 UDP 修复和隔离节点 TCP 补传。
- `receiver_agent`：使用 `epoll` 处理组播 UDP、Peer UDP、TCP 控制和 `timerfd`；按 Block 偏移写临时文件，向代理报告接收状态，也可在收到接收端互传修复任务后向其他接收端发送自己已有的 Block。

| 链路 | 协议 | 作用 |
| --- | --- | --- |
| 中心 → 代理 | TCP | 文件元数据、Section 数据、断点续传、结果确认 |
| 代理 → 接收端 | UDP 组播 | 初始数据及多个接收端共有的缺块修复 |
| 代理 → 接收端 | UDP 单播 | Baseline 修复，以及 Peer 模式下无法互传的缺块 |
| 代理 ↔ 接收端 | TCP | 注册、状态上报、缺块位图、Peer 任务、完成确认及 TCP 补传 |
| 接收端 → 接收端 | UDP 单播 | 代理下发任务后的 Peer 修复数据；使用接收端动态绑定的 UDP 端口 |

## 核心设计

### Section 级背压与代理缓存

文件按 **1200 字节有效载荷的 Block** 划分，再按 Section 组织。`central_sender` 与 `site_proxy_sender` 启动时默认每段 **64 Blocks**（可由环境变量或 `--section-blocks` 覆盖）。

中心发送完一个 Section 后等待代理确认；代理校验并写入本地缓存，执行 `fsync`，对非最终 Section 保存 checkpoint，然后向中心确认。中心只重发尚未确认缓存的 Section；代理接收缓存的线程与向接收端分发的工作线程并行，队列按未完成任务字节数进行高/低水位控制（分别为一个最大 Section 字节数的约 **4 倍 / 2 倍**），避免接收和分发速率长期失衡。

每个文件具有独立的 Transfer ID、SHA-256、缓存临时文件和恢复状态；同一园区内多个文件按输入顺序处理，不同园区代理的中心传输可并行进行。中心对代理的 Section 确认表示**代理缓存已提交**，不是所有园区接收端都已经收齐。

### Baseline 与 Peer 两种修复模式

代理通过 `--repair-mode baseline|peer` 选择模式，**默认 `peer`**。`SRCAST_REPAIR_MODE` 环境变量也可指定模式，优先级为 **命令行 > 环境变量 > 默认值**。代理启动日志会输出实际 `repair_mode`。

接收端在初始分发和每轮修复结束后报告当前 Section 的缺块位图。代理按 Block 汇总缺失目标：

1. **公共缺块**：至少两个接收端缺少同一 Block 时，代理组播修复一次。这部分在两种模式下均由代理完成。
2. **零散缺块（Baseline）**：只有一个接收端缺少该 Block 时，代理使用缓存进行 UDP 单播。
3. **零散缺块（Peer）**：代理寻找当前持有该 Block 且符合状态条件的其他接收端，通过 TCP 下发 `PeerRepairTask`，由源接收端读取本地 Block 并向目标接收端发送 UDP 数据。
4. **回退**：找不到合适源节点、任务规模过小或任务下发失败时，当前轮由代理 UDP 单播。Peer 数据发出后仍需依赖目标接收端下一轮缺块位图确认是否补齐，并不是“下发任务即修复成功”。
5. **TCP 补传**：达到配置轮数后仍未完成的接收端可从主修复组隔离，由代理在缓存可用后执行 TCP 补传。

当前 Peer 调度策略具有明确限制：同一源接收端每轮最多承担 **2 个目标**；源到目标累计不足 **16 个 Block** 时回退代理单播；一条 `PeerRepairTask` 最多携带 **64 个 Block**。这些是当前实现中的调度阈值，不代表动态负载均衡或带宽最优调度。

`SRCAST_BACKFILL_ROUNDS=N` 控制允许多少轮 UDP 修复后切换 TCP 补传；未设置时跟随 `--max-repair-rounds`，设为 `0` 表示初次缺块上报后即可切换。

### 接收端数据写入和恢复状态

UDP 数据（包括初始组播及接收端互传）通过 CRC、Transfer ID、Section、Block、偏移和长度校验后，按 Block 偏移写入 `.part-<transfer_id>` 临时文件。收到 Block 时先执行写入，再更新**内存中的** `received[]` 和 `received_count`；这一步**不会对每个 Block 执行 `fsync`**。

当前普通 UDP/Peer 数据路径的 Section 完成顺序为：

```text
Block 到达：校验 -> pwrite -> 更新内存接收位图
Section 收齐：fdatasync(临时文件) -> 保存 .state.tmp 并 rename 为 .state
             -> 向代理报告 Section Complete
最终文件收齐：fsync(临时文件) -> SHA-256 -> rename 为正式文件
```

TCP Backfill 路径不同：收到待补传 Block 后，会对临时文件执行 `fsync` 并保存接收状态。临时状态文件采用 `flush`、关闭文件及 `rename`，**没有显式对状态文件及父目录执行 `fsync`**；所以本项目的恢复能力主要面向进程异常退出（例如 `kill -9`），不承诺断电后的每次状态提交都永久保留。重启时检查 Transfer ID、文件大小、摘要、Section 参数及位图计数后恢复；整个文件须通过 SHA-256 校验才会改名为正式文件。

### 事件循环与故障隔离

接收端的 `epoll` 循环管理组播 UDP、Peer UDP、TCP 控制连接及 `timerfd`。组播和 Peer UDP 的接收处理每次最多处理 **256 包 / 2 ms**，避免持续 UDP 输入长期独占事件循环；`SECTION_END` 后等待约 **200 ms** 的 UDP 安静期，最长等待 **1 s** 再生成本轮缺块报告。当前 Peer 发送任务在接收端控制消息处理过程中**同步执行**，未单独实现 Peer 专用发送线程池。

中心为不同园区代理使用独立线程；代理最多使用 **4 个工作线程**并发处理被隔离接收端的 TCP 补传。缓存有目录级容量上限，但不实现自动淘汰策略。

## 目录结构

```text
.
├── CMakeLists.txt
├── include/                  协议、控制帧 I/O、文件 I/O、存储辅助和线程池
├── src/
│   ├── central_sender.cpp    中心发送端
│   ├── sender.cpp            园区代理（含 Peer 调度逻辑）
│   ├── receiver.cpp          接收端（含 Peer UDP 数据发送/接收）
│   └── checksum.cpp          摘要实现
├── scripts/                  启动脚本及冒烟、故障注入测试
├── benchmark/                单机吞吐与多主机 Baseline/Peer 对照工具
└── docs/                     四台物理主机测试说明
```

## 环境与构建

依赖 Linux、CMake ≥ 3.16、C++17、OpenSSL Crypto、POSIX Threads 和 IPv4 multicast。Ubuntu/Debian 可安装：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libssl-dev python3
```

**常规运行及四机脚本统一使用 `build/`**，与 `scripts/run_*.sh` 的默认路径一致：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4
```

生成 `build/central_sender`、`build/site_proxy_sender` 和 `build/receiver_agent`。需要 Debug 时可以另建 `build-debug/`；使用启动脚本时相应添加 `--build-dir ./build-debug`。

进行吞吐测试前，可根据网卡和系统配置调整 UDP 接收缓冲上限（此命令立即修改当前内核参数）：

```bash
sudo sysctl -w net.core.rmem_max=33554432
```

## 四台物理主机快速启动

四台机器须使用同一版本源码，且 Proxy 与 Receiver 所在局域网络需要支持组播。示例：A=Central `192.168.1.10`、B=Proxy `192.168.1.20`、C/D=Receiver `192.168.1.31` / `192.168.1.32`。在四台机器分别完成上面的 Release 构建后，按 **B → C → D → A** 启动：

```bash
# Machine B：Proxy
./scripts/run_proxy.sh --interface-ip 192.168.1.20 --receivers 2 --repair-mode peer

# Machine C：Receiver 1
./scripts/run_receiver.sh --interface-ip 192.168.1.31 \
  --proxy-ip 192.168.1.20 --output-dir ./received

# Machine D：Receiver 2
./scripts/run_receiver.sh --interface-ip 192.168.1.32 \
  --proxy-ip 192.168.1.20 --output-dir ./received

# Machine A：Central（最后启动，需提前准备 input.bin）
./scripts/run_central.sh --proxy-ip 192.168.1.20 --file ./input.bin
```

真实机器需填写各自 IPv4。Receiver 会自动绑定动态 Peer UDP 端口，代理通过接收端 TCP 来源 IP 和注册的 Peer 端口识别进程；**同一主机可以运行多个 Receiver**。Peer 模式需要确保接收端之间的动态 UDP 端口可达。完整端口、防火墙及 SHA-256 检查见 [四台物理主机测试指南](docs/FOUR_HOST_TEST.md)。

## 本机运行示例

### 1. 启动园区代理

```bash
mkdir -p proxy-cache received-1 received-2
./build/site_proxy_sender \
  --multicast-ip 239.255.42.99 --udp-port 5000 \
  --interface-ip 127.0.0.1 --control-port 6000 \
  --receiver-count 2 --pace-us 200 --gap-ms 0 \
  --max-repair-rounds 3 --section-blocks 4096 \
  --repair-mode peer --central-listen 7000 --cache-dir proxy-cache
```

`--central-listen 7000` 表示代理进入中心模式，在 TCP `7000` 端口等待中心连接。不加这个参数时，代理可按普通发送模式从本地文件向接收端分发。

### 2. 启动两个接收端

```bash
./build/receiver_agent --multicast-ip 239.255.42.99 --udp-port 5000 \
  --interface-ip 127.0.0.2 --proxy-ip 127.0.0.1 \
  --control-port 6000 --output-dir received-1

./build/receiver_agent --multicast-ip 239.255.42.99 --udp-port 5000 \
  --interface-ip 127.0.0.3 --proxy-ip 127.0.0.1 \
  --control-port 6000 --output-dir received-2
```

### 3. 启动中心

```bash
dd if=/dev/urandom of=input.bin bs=1024 count=256 status=none
./build/central_sender --proxy 127.0.0.1 7000 \
  --pace-us 0 --section-blocks 4096 --file input.bin
```

`--file` 和 `--proxy` 都允许重复。多园区时可使用：

```bash
./build/central_sender --pace-us 0 --section-blocks 4096 \
  --proxy 127.0.0.1 7000 --proxy 127.0.0.1 7001 --file input.bin
```

每个 `--proxy <ip> <port>` 代表一个园区代理。每个代理独立进行组播、修复和本地缓存。

## 一次文件传输的主要流程

1. 代理监听控制端口，接收端注册，提供自己的 Peer UDP 端口；代理等待预期数量的接收端。
2. 中心计算文件 SHA-256、大小、Block 数、Transfer ID，发送 `CENTRAL_FILE_META`。
3. 代理检查本地缓存、`.part` 与 checkpoint，回应 `CENTRAL_RESUME`。
4. 代理向接收端下发 `FILE_META`，接收端创建/恢复临时文件并回复 `META_READY`。
5. 中心分 Section 发送 `CENTRAL_DATA` / `CENTRAL_FILE_END`；代理检验并提交缓存 Section，确认后推进下一 Section。
6. 代理将该 Section 数据组播到接收端；接收端校验并写入临时文件。
7. 结束通知后接收端等待 UDP 安静期，报告 Section 完成或缺块位图。
8. 代理聚合缺块：公共缺块组播；在 `peer` 模式下调度接收端互传零散缺块，不满足条件时代理单播；在 `baseline` 模式下零散缺块直接由代理单播。
9. 下一轮根据目标接收端的新位图判断缺块是否已修复。达到配置轮次的慢接收端进入后续 TCP 补传。
10. 每个 Section 接收完整后同步数据和状态；整文件完成后执行 SHA-256 校验、正式文件改名和完成确认。
11. 代理处理隔离接收端的补传，向中心报告结果；多文件模式继续下一个文件。

## 测试与结果

### 自动化测试入口

```bash
bash scripts/all_smoke_test.sh
```

也可使用 CMake 目标：

```bash
cmake -S . -B test-runs/builds/cmake-test -DCMAKE_BUILD_TYPE=Debug
cmake --build test-runs/builds/cmake-test --target test
cmake --build test-runs/builds/cmake-test --target cleantest
```

当前 `all_smoke_test.sh` 调用的用例包括命令行/角色、局部修复、中心缓存、文件名、Section 流程、中心续传、TCP 补传、故障恢复、benchmark smoke 和多代理场景。

###单机吞吐测试

在 **2 核、4 GiB Linux 单机回环环境** 使用 Release 构建，测试 **256 MiB** 文件、4096 Blocks/Section、默认不额外 sleep 限速。每组 5 轮、结果取端到端吞吐中位数：

| Receiver 数量 | 端到端吞吐中位数 |
| ---: | ---: |
| 1 | 15.293 MiB/s |
| 2 | 14.416 MiB/s |
| 4 | 13.283 MiB/s |
| 8 | 10.791 MiB/s |

历史记录显示这 20 轮通过 SHA-256 校验。调整 UDP 接受队列大小后，测试期间 `UdpRcvbufErrors` 不再增长；8 Receiver 吞吐由约 0.49 MiB/s 提升至 10.79 MiB/s。。

```bash
python3 benchmark/throughput.py
```

### 性能测试与结果分析

测试在 1 个 Central 主机、1 个 Proxy 主机和 2 台 Receiver 主机的环境中进行；各场景运行不同数量的 Receiver 进程，评估 SiteRepairCast 在不同模拟丢包比例和文件规模下的传输耗时、代理 CPU 占用及内存使用情况。

实验对比两种修复模式：

Baseline：缺块由 Proxy 通过 UDP 组播或单播修复。

Peer：公共缺块仍由 Proxy 组播修复，其他缺块优先由 Proxy 调度接收端之间进行 UDP 互传，不满足互传条件时回退至 Proxy UDP 单播。

以下耗时、CPU 和 RSS 数据均为成功运行样本的中位数。CPU 表示 Proxy 进程平均 CPU 占用率，RSS 表示 Proxy 进程峰值常驻内存。表中的模拟丢包率为代码控制丢包的几率。

1.不同丢包比例下的性能对比（128 MiB）

使用 128 MiB 文件和 100 个 Receiver 进程，设置 0%～1% 的初始 Block 丢弃比例，比较两种修复模式的传输表现。

| 模拟丢包率 | Baseline 次数 | Baseline 耗时 | Baseline CPU | Baseline RSS | Peer 次数 | Peer 耗时 | Peer CPU | Peer RSS | Peer 调度 Block |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0% | 11 | 28.503 s | 6.76% | 5.609 MiB | 6 | 29.302 s | 6.66% | 5.543 MiB | 0 |
| 0.1% | 5 | 37.090 s | 5.11% | 5.664 MiB | 5 | 37.015 s | 5.17% | 5.918 MiB | 0 |
| 0.3% | 5 | 40.952 s | 5.08% | 5.617 MiB | 5 | 40.477 s | 5.22% | 5.926 MiB | 0 |
| 0.5% | 5 | 43.870 s | 5.08% | 5.793 MiB | 5 | 36.397 s | 5.30% | 5.973 MiB | 55,272 |
| 1.0% | 5 | 53.049 s | 4.94% | 5.793 MiB | 6 | 35.463 s | 5.29% | 6.043 MiB | 110,544 |

从测试结果可以看出，丢包比例较低时，两种模式的传输耗时基本接近。0.1% 和 0.3% 场景下，Peer 调度 Block 数为 0，修复主要由 Proxy 组播完成。

当模拟丢包比例增加到 0.5% 时，Peer 模式的耗时中位数从 Baseline 的 43.870 秒降至 36.397 秒，降低约 17.0%。在 1.0% 场景下，耗时从 53.049 秒降至 35.463 秒，降低约 33.2%。

与此同时，Peer 模式下代理 CPU 占用及峰值 RSS 与 Baseline 整体接近，未出现明显的代理内存开销增长。

当缺块足以形成较大的互传任务时，接收端辅助修复能够减少代理直接参与修复的数据量，并缩短成功运行的传输耗时；在低丢包比例下，两种模式表现接近。

2.大文件传输性能对比（1 GiB）

将文件规模增加至 1 GiB，保持 100 个 Receiver 进程，并使用 1.0% 初始 Block 丢弃比例，进一步观察大文件场景下两种修复模式的性能。


2.1 运行详细数据

| 模式 | 耗时 (s) | 代理 CPU | 代理峰值 RSS (MiB) | Peer 调度 Block |
|---|---:|---:|---:|---:|
| Baseline | 445.098 | 4.92% | 7.793 | 0 |
| Baseline | 444.694 | 4.98% | 8.637 | 0 |
| Baseline | 446.083 | 5.03% | 6.645 | 0 |
| Baseline | 445.480 | 4.90% | 8.672 | 0 |
| Baseline | 445.855 | 4.98% | 8.672 | 0 |
| Peer | 316.717 | 5.33% | 6.961 | 894,676 |
| Peer | 312.236 | 5.51% | 6.941 | 894,700 |
| Peer | 313.116 | 5.81% | 6.957 | 894,700 |
| Peer | 314.399 | 5.62% | 8.664 | 894,700 |
| Peer | 311.622 | 5.44% | 8.617 | 894,399 |

在这组样本中，Peer 模式耗时中位数为 313.116 秒，相比 Baseline 的 445.480 秒降低约 29.7%。

表中 Peer 耗时中位数为 313.116 秒，调度 Block 数中位数为 894,700。Proxy 峰值 RSS 的中位数在这组 Peer 样本中为 6.961 MiB，在 Baseline 样本中为 8.637 MiB。

3.接收端数量扩展测试（128 MiB）

使用 128 MiB 文件，在不丢失的情况下，将 Receiver 进程数量从 100 增至 500，观察代理向更多接收端分发文件时的性能变化。

该组实验采用 Baseline 模式，主要评估接收端规模增加对传输耗时、代理 CPU 和峰值 RSS 的影响。

| Receiver 数量 | 模式 | 次数 | 耗时中位数 | 代理 CPU 中位数 | 代理峰值 RSS 中位数 |
|---:|---|---:|---:|---:|---:|
| 100 | Baseline | 11 | 28.503 s | 6.76% | 5.609 MiB |
| 200 | Baseline | 6 | 34.039 s | 7.20% | 5.686 MiB |
| 300 | Baseline | 6 | 47.543 s | 5.87% | 5.734 MiB |
| 400 | Baseline | 6 | 65.275 s | 7.75% | 6.152 MiB |
| 500 | Baseline | 6 | 83.568 s | 7.83% | 6.236 MiB |

随着 Receiver 数量从 100 增加至 500，运行耗时中位数由 28.503 秒增至 83.568 秒，约为原来的 2.93 倍。

代理峰值 RSS 中位数从 5.609 MiB 增加至 6.236 MiB，增幅约 11.2%。CPU 占用率存在一定波动，但没有随着 Receiver 数量增加而相同比例线性增长。

4. 实验结论

综合测试结果，接收端辅助修复的主要收益体现在存在一定缺块量时减少 Proxy 直接承担的 UDP 修复任务，将修复下放到接收端，通过先组播再接收端互传的方式避免接收端均衡大量增加。

### 分布式校准工具

`benchmark/distributed_compare.py` 使用 Central 机器上的控制器，通过 SSH 启动 Proxy 与两个 Receiver Host 上的多个进程，输出 `raw.csv`、`summary.csv` 和日志。正式对照前，四台主机应使用相同版本构建，并将配置里的 `host`、`repo`、`interface_ip`、`ssh_options` 等替换为实际环境。

```bash
# 四台机器分别执行
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4

# 在 Central 机器创建私有配置副本（配置文件中的主机信息需自行检查和编辑）
mkdir -p test-runs
cp benchmark/distributed_config.json test-runs/distributed_config.local.json
python3 - <<'PY'
import json
from pathlib import Path
p = Path('test-runs/distributed_config.local.json')
cfg = json.loads(p.read_text(encoding='utf-8'))
cfg['build_dir'] = 'build'
cfg['drop_blocks_per_receiver'] = 128
p.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
PY

# 检查/编辑 test-runs/distributed_config.local.json 中的主机和 SSH 配置后，在 Central 运行
python3 benchmark/distributed_compare.py \
  --config test-runs/distributed_config.local.json \
  --receivers 20 --modes baseline peer --repetitions 1 \
  --file-size 134217728
python3 benchmark/plot_distributed.py test-runs/artifacts/distributed/summary.csv
```

绘图脚本需要额外安装 Python 的 `matplotlib`；只运行 `distributed_compare.py` 不依赖该绘图库。

**配置注意事项：** 当前仓库中 `benchmark/distributed_config.json` 的 `build_dir` 是 `build`，因此构建目录需要一致。其 `drop_blocks_per_receiver=1024` **不适合** `--receivers 20`、`section_blocks=4096` 的默认校准：`distributed_compare.py` 要求 `receiver_count × drop_blocks_per_receiver <= section_blocks - 17`，即 `20×1024 > 4079` 不成立；上面私有副本改为 `128` 后才满足这个约束。此改动只用于使该默认校准场景符合脚本输入条件，**不是对上传的 136 条实验记录进行重新测试**。

大于 **20 Receiver / 128 MiB / 1 轮** 的任务，需要在确认实验资源后显式传 `--allow-large-run`。控制器会清理它自己生成的运行目录；不要把 `work_root` 指向含个人数据的路径。真实部署参数和实验环境信息不要直接提交到公共仓库。

## 当前边界

- 仅支持 Linux、IPv4 和可信实验网络；控制面没有 TLS、用户认证与授权。
- 每个代理负责一个园区及启动时固定的接收端集合；没有动态发现、成员自动增减或全局多代理协同调度。
- 已实现 **代理集中调度的接收端互传**；未实现去中心化 P2P、跨园区 Peer 传输、FEC、QUIC、目录同步或 Web 管理界面。
- Section 按顺序推进，没有多 Section 滑动窗口；Peer 源节点选择基于当前状态和固定阈值，没有实时带宽探测或自适应负载分配。
- 缓存配额为目录级硬上限，没有自动淘汰策略；接收端 Peer 发送任务目前在控制消息处理链路中同步执行。
- 状态文件写入采用临时文件加 `rename`，但尚未实现状态文件和目录的完整掉电持久化保证。