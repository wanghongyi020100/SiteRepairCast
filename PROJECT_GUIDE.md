# SiteRepairCast Local Project Guide

生成日期：2026-09-11

基准提交：5f87651c660720022e03e6df278ee1d2009185ea

文档目的：本文件从本地仓库源码、脚本和构建配置整理项目结构、文件用途、运行方式和已知边界，便于以后直接检查本地项目，不依赖 GitHub 或外部资料。

## 项目目录树

使用 `tree -a -I '.git|build*|test-runs|__pycache__|*.pyc'` 生成；构建目录和 test-runs 未列入。

```text
.
├── .gitattributes
├── .gitignore
├── CMakeLists.txt
├── README.md
├── config
│   └── four-host
│       ├── central.env.example
│       ├── proxy.env.example
│       ├── receiver1.env.example
│       └── receiver2.env.example
├── docs
│   └── FOUR_HOST_TEST.md
├── benchmark
│   ├── README.md
│   ├── distributed_compare.py
│   ├── distributed_config.json
│   ├── plot_distributed.py
│   ├── process_cpu.py
│   ├── export_project_code.sh
│   ├── profiles
│   │   ├── smoke.json
│   │   └── standard.json
│   ├── run.py
│   └── throughput.py
├── include
│   ├── checksum.hpp
│   ├── cli_utils.hpp
│   ├── control_io.hpp
│   ├── file_descriptor.hpp
│   ├── file_io.hpp
│   ├── protocol.hpp
│   ├── storage_utils.hpp
│   ├── system_utils.hpp
│   ├── thread_pool.hpp
│   └── transfer_options.hpp
├── scripts
│   ├── all_smoke_test.sh
│   ├── benchmark_report.sh
│   ├── benchmark_smoke_test.sh
│   ├── central_cache_smoke_test.sh
│   ├── central_resume_smoke_test.sh
│   ├── fault_recovery_smoke_test.sh
│   ├── local_repair_smoke_test.sh
│   ├── multi_proxy_smoke_test.sh
│   ├── role_cli_smoke_test.sh
│   ├── run_central.sh
│   ├── run_proxy.sh
│   ├── run_receiver.sh
│   ├── section_flow_smoke_test.sh
│   └── tcp_backfill_smoke_test.sh
└── src
    ├── central_sender.cpp
    ├── checksum.cpp
    ├── receiver.cpp
    └── sender.cpp
```

## 文件索引

| 路径 | 类型 | 主要用途 | 关键入口/内容 |
| --- | --- | --- | --- |
| `.gitattributes` | Git 配置 | 记录仓库属性规则。 | 当前用于版本控制属性管理。 |
| `.gitignore` | Git 配置 | 排除构建目录、产物和本地运行文件。 | 覆盖 build、artifacts、缓存、接收目录等生成物。 |
| `CMakeLists.txt` | 构建配置 | 定义 C++17 项目、公共库和三个可执行程序。 | 目标包括 central_sender、site_proxy_sender、receiver_agent、test、cleantest。 |
| `README.md` | 项目说明 | 说明架构、运行顺序、测试和性能背景。 | 包含中心、代理、接收端的数据流和示例命令。 |
| `config/four-host/*.env.example` | Shell 配置示例 | 提供四台物理主机的地址、端口和目录模板。 | 分别对应 Central、Proxy 和两个 Receiver。 |
| `docs/FOUR_HOST_TEST.md` | 部署文档 | 四台 Linux 主机的构建、网络检查、启动和摘要验证手册。 | 说明真实 LAN IP、端口、防火墙和常见错误。 |
| `benchmark/README.md` | Benchmark 文档 | 说明 benchmark 脚本的输入、输出和场景。 | 配合 profiles 和 run.py 生成测试数据。 |
| `benchmark/profiles/smoke.json` | Benchmark 配置 | 小规模 benchmark smoke profile。 | 定义文件大小、接收端数量、轮次和 section_blocks。 |
| `benchmark/profiles/standard.json` | Benchmark 配置 | 标准 benchmark profile。 | 定义更完整的吞吐场景参数。 |
| `benchmark/run.py` | Python 脚本 | 自动构建、启动代理/接收端/中心并汇总 benchmark。 | BenchmarkRunner、ScenarioResult、main。 |
| `benchmark/distributed_compare.py` | Python 脚本 | 通过 SSH 编排四台主机上的 Baseline/Peer 对比。 | 使用三个角色的 named CLI，并在每轮后清理远端传输文件。 |
| `benchmark/throughput.py` | Python 脚本 | 解析 benchmark CSV 并输出吞吐摘要。 | median、load_rows、main。 |
| `include/checksum.hpp` | C++ 头文件 | 声明 SHA-256、CRC32 和摘要十六进制工具。 | srcast::sha256_file、srcast::sha256_fd、srcast::crc32、srcast::hex_digest。 |
| `include/cli_utils.hpp` | C++ 头文件 | 共用命令行数值、端口、IPv4 和组播地址验证。 | parse_port、parse_nonnegative_int、parse_positive_u32、parse_multicast_ipv4。 |
| `include/control_io.hpp` | C++ 头文件 | TCP 控制帧收发工具。 | write_all、read_all、send_control_frame、receive_control_frame。 |
| `include/file_descriptor.hpp` | C++ 头文件 | RAII 文件描述符封装。 | FileDescriptor 禁止拷贝、支持移动并在析构时 close。 |
| `include/file_io.hpp` | C++ 头文件 | 块读取和定位写入工具。 | read_block、read_bytes_at、write_all_at。 |
| `include/protocol.hpp` | C++ 头文件 | 定义 UDP 包、TCP 控制消息、编解码和 section 工具。 | PacketWriter、PacketReader、ControlType、PacketType、encode/decode 系列、section_*。 |
| `include/storage_utils.hpp` | C++ 头文件 | 检查磁盘空间和可配置目录容量上限。 | require_disk_space、storage_limit、directory_usage、require_storage_limit。 |
| `include/system_utils.hpp` | C++ 头文件 | 统一把 errno 转为 runtime_error。 | system_error。 |
| `include/thread_pool.hpp` | C++ 头文件 | 简单线程池，用于并发处理补传任务。 | ThreadPool::enqueue、worker_loop。 |
| `include/transfer_options.hpp` | C++ 头文件 | 从环境变量读取 section 大小。 | section_blocks_from_env。 |
| `scripts/all_smoke_test.sh` | Shell 测试 | 顺序运行所有 smoke、benchmark smoke 和 multi-proxy 测试。 | 最终输出 all SiteRepairCast smoke tests passed。 |
| `scripts/benchmark_report.sh` | Shell 测试/报告 | 构建 Release 场景并生成 benchmark CSV/Markdown 报告。 | run_scenario、start_proxy、start_receiver、wait_for_registration_count。 |
| `scripts/benchmark_smoke_test.sh` | Shell 测试 | 验证 benchmark profile 路径能构建和运行。 | 调用 benchmark/run.py 的 smoke profile。 |
| `scripts/central_cache_smoke_test.sh` | Shell 测试 | 验证中心到代理缓存、接收端输出和缓存复用。 | 等待 registered receiver 日志并校验 SHA-256。 |
| `scripts/central_resume_smoke_test.sh` | Shell 测试 | 验证中心断线后按 section checkpoint 续传。 | 使用 SRCAST_STOP_AFTER_SECTIONS 注入中心提前退出。 |
| `scripts/fault_recovery_smoke_test.sh` | Shell 测试 | 验证乱序、重复和损坏 DATA 的恢复行为。 | 使用 SRCAST_REVERSE_INITIAL_DATA、SRCAST_DUPLICATE_DATA_BLOCKS、SRCAST_CORRUPT_DATA_BLOCKS。 |
| `scripts/local_repair_smoke_test.sh` | Shell 测试 | 验证代理本地文件分发和 UDP 修复。 | 使用 SRCAST_DROP_INITIAL_BLOCKS 注入接收端首轮缺块。 |
| `scripts/multi_proxy_smoke_test.sh` | Shell 测试 | 验证 central_sender 的多代理 flag 模式。 | 使用多个 --proxy 和 --pace-us 启动中心。 |
| `scripts/role_cli_smoke_test.sh` | Shell 测试 | 验证三个角色的 named CLI、帮助、错误诊断和端到端 SHA-256。 | 使用两 Receiver 的小型 loopback 传输。 |
| `scripts/run_central.sh`、`run_proxy.sh`、`run_receiver.sh` | Shell 启动器 | 从任意当前目录启动对应角色，支持可信 `.env` 配置。 | 配置优先级为默认值、config、CLI。 |
| `scripts/section_flow_smoke_test.sh` | Shell 测试 | 验证 section 级流程和可配置 section 大小。 | 结合 SRCAST_SECTION_BLOCKS 检查分段分发。 |
| `scripts/tcp_backfill_smoke_test.sh` | Shell 测试 | 验证慢接收端隔离后的 TCP backfill。 | 使用 SRCAST_BACKFILL_ROUNDS 和大量初始丢块。 |
| `src/central_sender.cpp` | C++ 源码 | 中心发送端，连接一个或多个代理并按 section 推送文件。 | ProxyEndpoint、ProxySession、parse_options、send_file、run_proxy_session、main。 |
| `src/checksum.cpp` | C++ 源码 | 实现 SHA-256 和 CRC32。 | sha256_file、sha256_fd、sha256_bytes、crc32。 |
| `src/receiver.cpp` | C++ 源码 | 接收端，处理 UDP 数据、TCP 控制、状态恢复和最终提交。 | ControlStreamReader、ReceiverOptions、TransferState、initialize_transfer、finish_drain_and_report、finalize_transfer、main。 |
| `src/sender.cpp` | C++ 源码 | 园区代理，接收中心或本地文件，缓存后分发给接收端并执行修复。 | SenderOptions、ReceiverConnection、CachedCentralFile、parse_options、install_receiver_connection、distribute_section、receive_central_file、main。 |
| `PROJECT_GUIDE.md` | 本地文档 | 汇总本地项目结构、文件用途、协议概览、运行命令和已知边界。 | 本文件由本次任务生成，供后续离线检查仓库。 |

## 三个进程角色及数据流

central_sender 负责计算文件大小、总 block 数、SHA-256 和 transfer_id，通过 TCP 连接每个代理；发送 CENTRAL_FILE_META 后等待代理返回 CENTRAL_RESUME，再从指定 section 继续发送 CENTRAL_DATA，并在每个 section 后发送 CENTRAL_FILE_END 等待 CENTRAL_STATUS。

site_proxy_sender 是园区代理。普通模式下它从本地文件读取并发送，中心模式下它监听 --central-listen 端口接收中心数据，先写入缓存和 checkpoint，再向固定数量接收端发送 FILE_META、UDP DATA/END，并根据接收端状态执行 UDP 修复或 TCP backfill。

receiver_agent 连接代理 TCP 控制端，加入 multicast，并在真实 `interface_ip` 上为 peer repair 绑定内核分配的随机 UDP 端口。它把 DATA 按 block 偏移写入 `<原始文件名>.part-<transfer_id>`，按 section 维护位图和状态，校验整文件 SHA-256 后把临时文件原子改名为源文件 basename，例如 `received/t128.txt`。

数据流为：中心到代理使用 TCP；代理到接收端初始数据和 UDP 修复使用组播或单播 UDP；代理与接收端的注册、状态、完成确认和 TCP backfill 使用 TCP 控制帧。

## 协议与控制消息概览

UDP 数据包使用 kMagic、kVersion 和 PacketType，类型包括 Meta、Data、End。Data 带 transfer_id、section_id、block_id、offset、payload_size 和 crc32，接收端用 transfer_id 过滤旧包并校验边界与 CRC。

TCP 控制帧使用 kControlMagic、kVersion、ControlType 和长度前缀，最大帧大小为 kMaxControlFrameSize。控制类型包括 Register、FileMeta、MetaReady、SectionStatus、RepairBegin、ReceiverComplete、CompleteAck、TransferResult、SectionEnd、ReceiverReady、SessionEnd、CentralFileMeta、CentralData、CentralFileEnd、CentralStatus、CentralSessionEnd、CentralResume、BackfillBegin、BackfillData、BackfillEnd。

receiver ID 由代理侧的 receiver_endpoint_key 生成，使用接收端 TCP peer IP 和注册消息携带的 UDP port 组成 ip:port；源码没有单独的命令行 receiver identity 参数。

section 由 named CLI `--section-blocks`、`SRCAST_SECTION_BLOCKS` 或默认 kDefaultSectionBlockCount 控制，优先级依次降低。中心和代理使用 section_id、section_first_block、section_block_count 和 section_count_for_blocks 分段，中心每完成一个 section 等待代理缓存确认，接收端每轮 section drain 后报告状态。

ACK/NACK 语义通过状态消息实现：接收端用 SectionStatusMessage 上报 Complete、Missing 或 Failed 以及缺块位图；代理用 CompleteAck 确认 ReceiverComplete，用 TransferResult 通知失败或完成，用 CentralStatus 向中心确认 section 缓存结果。源码没有名为 NACK 的独立 ControlType。

TCP backfill 由代理对隔离或慢节点发送 BackfillBegin、BackfillData、BackfillEnd，接收端通过同一控制连接接收缺失 block 并最终走 SHA-256 校验和 ReceiverComplete/CompleteAck。

## 核心结构与函数职责

公共层：PacketWriter/PacketReader 负责网络序列化；FileDescriptor 管理 fd 生命周期；ThreadPool 承载并发补传；checksum 工具负责 SHA-256 和 CRC32；storage 工具在写入前检查磁盘空间和可选容量上限。

中心端：CentralOptions 保存代理列表、文件列表、pace_us 和 section_blocks；parse_options 同时支持旧位置参数和 --proxy/--pace-us flag 形式；send_file 计算元数据、根据 CENTRAL_RESUME 续传并逐 section 等待代理确认；run_proxy_session 为每个代理串行发送文件，main 为多个代理创建线程。

代理端：SenderOptions 保存组播、控制端口、接收端数量、修复轮数、缓存目录和中心监听端口；ReceiverConnection 跟踪每个接收端的控制 fd、UDP 目标、恢复状态和缺块状态；CachedCentralFile 描述已缓存中心文件；install_receiver_connection 安装或接管接收端连接；distribute_section 执行初始 UDP 分发、缺块聚合、UDP 修复和隔离；receive_central_file 写入缓存、提交 checkpoint 并向中心汇报。

接收端：ReceiverOptions 保存组播地址、输出目录、代理控制面和接口地址；TransferState 保存当前文件、位图、临时路径和状态路径；make_registration 汇报 Idle/Receiving/Completed；initialize_transfer 创建或恢复 .part 和 .state；finish_drain_and_report 统计缺块并发送 SectionStatus；finalize_transfer 校验摘要并提交最终文件。

## 命令行、构建和测试

构建：

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j "$(nproc)"
```

代理普通模式：

```bash
./build-debug/site_proxy_sender --multicast-ip 239.255.42.99 --udp-port 5000 \
  --interface-ip 127.0.0.1 --control-port 6000 --receiver-count 2 \
  --pace-us 200 --gap-ms 0 --max-repair-rounds 3 --section-blocks 4096 \
  --repair-mode peer --file input.bin
```

代理中心模式：

```bash
./build-debug/site_proxy_sender --multicast-ip 239.255.42.99 --udp-port 5000 \
  --interface-ip 127.0.0.1 --control-port 6000 --receiver-count 2 \
  --pace-us 200 --gap-ms 0 --max-repair-rounds 3 --section-blocks 4096 \
  --repair-mode peer --central-listen 7000 --cache-dir proxy-cache
```

接收端：

```bash
./build-debug/receiver_agent --multicast-ip 239.255.42.99 --udp-port 5000 \
  --output-dir received-1 --proxy-ip 127.0.0.1 --control-port 6000 \
  --interface-ip 127.0.0.2
```

中心端：

```bash
./build-debug/central_sender --proxy 127.0.0.1 7000 --pace-us 0 \
  --section-blocks 4096 --file input.bin
./build-debug/central_sender --proxy 127.0.0.1 7000 --proxy 127.0.0.1 7001 \
  --pace-us 0 --section-blocks 4096 --file input.bin
```

三个程序都提供 `--help`，仓库内部脚本和 benchmark 使用 named CLI；程序仍兼容原有位置参数。四台物理主机的快速入口见 `scripts/run_*.sh` 和 `docs/FOUR_HOST_TEST.md`。

测试：

```bash
bash scripts/all_smoke_test.sh
cmake --build build-debug --target test
cmake --build build-debug --target cleantest
```

## 环境变量

以下清单由 `rg -n "getenv|SRCAST_"` 核对。

| 变量 | 使用位置 | 用途 |
| --- | --- | --- |
| SRCAST_SECTION_BLOCKS | include/transfer_options.hpp、脚本和 benchmark | 覆盖默认每个 section 的 block 数；空值使用默认值，非法值会报错。 |
| SRCAST_REPAIR_MODE | src/sender.cpp、脚本和 benchmark | 选择 baseline 或 peer；`--repair-mode` 存在时由 CLI 覆盖。 |
| SRCAST_STOP_AFTER_SECTIONS | src/central_sender.cpp、central_resume_smoke_test.sh、benchmark/run.py | 测试 hook：中心确认指定数量 section 后主动断开，用于验证续传。 |
| SRCAST_BACKFILL_ROUNDS | src/sender.cpp、tcp_backfill_smoke_test.sh、benchmark | 控制 UDP 修复多少轮后切到 TCP backfill；未设置时使用 max_repair_rounds。 |
| SRCAST_DUPLICATE_DATA_BLOCKS | src/sender.cpp、fault_recovery_smoke_test.sh、benchmark_report.sh | 测试 hook：代理对指定 block 发送重复 DATA。 |
| SRCAST_CORRUPT_DATA_BLOCKS | src/sender.cpp、fault_recovery_smoke_test.sh、benchmark_report.sh | 测试 hook：代理对指定 block 发送损坏 DATA。 |
| SRCAST_REVERSE_INITIAL_DATA | src/sender.cpp、fault_recovery_smoke_test.sh | 测试 hook：反向发送初始 DATA，用于乱序验证。 |
| SRCAST_CACHE_LIMIT_BYTES | src/sender.cpp、include/storage_utils.hpp | 限制代理缓存目录可使用容量；0 或空表示不限制。 |
| SRCAST_DROP_INITIAL_BLOCKS | src/receiver.cpp、多个 smoke/benchmark 脚本 | 测试 hook：接收端首次收到指定 block 时丢弃一次。 |
| SRCAST_OUTPUT_LIMIT_BYTES | src/receiver.cpp、include/storage_utils.hpp | 限制接收端输出目录可使用容量；0 或空表示不限制。 |

## 运行时目录和状态文件

build-debug 等 build* 目录是普通 CMake 生成物，不是源文件。测试和 benchmark 的构建、临时文件及结果统一保存在 test-runs/，也是生成物。

代理缓存目录由 site_proxy_sender 参数指定；中心模式会保存最终缓存文件、同名 .part 临时文件和 .state checkpoint。.state 先写 .tmp 再 rename，用于只承认已提交 section。

接收端输出目录由 receiver_agent 参数指定；最终文件名使用 Central 源文件 basename，例如 `received/t128.txt`。接收过程中使用 `received/t128.txt.part-<transfer_id>`，恢复状态使用 `received/t128.txt.part-<transfer_id>.state`，状态提交时会先写 `.tmp` 再 rename。这些都是运行生成物，不属于源码清单。

README 示例中的 proxy-cache、received-1、received-2，以及脚本创建的临时 work 目录都是运行目录；.gitignore 会排除常见构建和产物路径。

## 已知边界

三个进程仍是独立 Linux 可执行程序，面向可信 LAN/VLAN，不提供 TLS、身份认证、服务发现或自动防火墙配置。物理四机环境是否允许 multicast 取决于实际交换网络，需按 `docs/FOUR_HOST_TEST.md` 在目标环境验证；本地 loopback smoke 不能替代真实网络验收。
