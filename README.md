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
├── scripts/                  启动脚本、测试兼容入口和正确性测试总入口
├── real_test/                多机器真实部署说明、配置模板和正确性测试脚本
├── benchmark/                多机 Baseline/Peer 性能实验控制器
└── real_test/README.md                   多机器测试文档导航
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

## 多机器部署与真实测试

最小部署包含 1 台 Central、1 台 Proxy 和至少 1 台 Receiver；可按需增加 Receiver 主机。网络要求、角色配置、逐机启动、SHA-256 验证、正确性 `.sh` 用例及多机性能实验见 [多机器真实测试指南](real_test/README.md)。

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

当前 `all_smoke_test.sh` 调用的用例包括命令行/角色、局部修复、中心缓存、文件名、Section 流程、中心续传、TCP 补传、Peer 修复屏障、故障恢复和多代理场景。Benchmark smoke 在本机回环下检查分发、修复、补传和 SHA-256 正确性，不输出性能结论。测试脚本源码位于 `scripts/`，多机器性能实验脚本位于 `benchmark/lab/`。

## 多机环境与基准配置

多机器的网络、构建、端口、防火墙与手动启动步骤见 [real_test/README.md](real_test/README.md)。跨机器 Baseline/Peer 校准的主机配置、SSH 关系和运行命令见 [benchmark/README.md](benchmark/README.md)。

基准控制器默认最多运行 20 个 Receiver、128 MiB 文件和 1 轮；扩大规模或增加轮数前，需显式传入 `--allow-large-run`。

## 当前边界

- 仅支持 Linux、IPv4 和可信实验网络；控制面没有 TLS、用户认证与授权。
- 每个代理负责一个园区及启动时固定的接收端集合；没有动态发现、成员自动增减或全局多代理协同调度。
- 已实现 **代理集中调度的接收端互传**；未实现去中心化 P2P、跨园区 Peer 传输、FEC、QUIC、目录同步或 Web 管理界面。
- Section 按顺序推进，没有多 Section 滑动窗口；Peer 源节点选择基于当前状态和固定阈值，没有实时带宽探测或自适应负载分配。
- 缓存配额为目录级硬上限，没有自动淘汰策略；接收端 Peer 发送任务目前在控制消息处理链路中同步执行。
- 状态文件写入采用临时文件加 `rename`，但尚未实现状态文件和目录的完整掉电持久化保证。
