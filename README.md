# SiteRepairCast

SiteRepairCast 是一个面向多园区静态文件分发的 Linux 系统。对于每个待分发文件，中心只向每个园区代理发送一份副本，代理缓存已校验数据，再通过 UDP 组播分发给园区内的接收端。接收端发生局部丢包时，由代理使用本地缓存修复，不让同一批修复数据重新穿过跨园区链路。

借鉴边缘缓存的思路，但采用中心主动推送、固定园区代理和固定接收端集合，重点解决多接收端分发、局部修复、慢节点隔离和异常恢复。

## 架构

```text
central_sender
      |
      | TCP，跨园区可靠传输
      v
site_proxy_sender
      |
      |-- UDP 组播或单播数据 --> receiver_agent 1..N
      `-- TCP 控制与补传 -----> receiver_agent 1..N
```

三个层次的职责：

- `central_sender`：计算文件摘要和 Transfer ID，按 Section 向一个或多个代理发送文件，并根据代理 checkpoint 续传。
- `site_proxy_sender`：接收并缓存中心数据，聚合接收端缺块位图，选择组播、单播或 TCP 补传。
- `receiver_agent`：使用 `epoll` 同时处理 UDP、TCP 和定时器事件，按 Block 偏移落盘并持久化接收状态。

通道划分：

| 链路 | 协议 | 内容 |
| --- | --- | --- |
| 中心到代理 | TCP | 元数据、Section 数据、checkpoint 和结果确认 |
| 代理到接收端 | UDP 组播或单播 | 初始数据和修复数据 |
| 代理与接收端 | TCP | 注册、状态、缺块位图、完成确认和 TCP 补传 |

## 核心设计

### Section 级背压

文件先按 1200 字节的 Block 划分，再按 Section 组织。中心发送完一个 Section 后等待代理确认；代理只有在数据校验、落盘、`fsync` 和 checkpoint 提交完成后才回复。中心不会一直领先于代理，异常恢复也只从已提交 Section 继续。

### 局部修复

接收端在每轮结束后上报缺块位图。代理按同一 Block 的缺失接收端数量选择策略：

- 多个接收端缺少同一 Block：组播一次；
- 只有一个接收端缺块：向该接收端 UDP 单播；
- 某个接收端缺块过多：从主修复轮隔离，文件缓存完成后通过 TCP 补传。

### 崩溃一致性

代理和接收端都先写临时文件，再保存状态。关键顺序是：

```text
pwrite -> fsync 数据 -> 写临时状态 -> rename 状态 -> 对外确认
```

整文件 SHA-256 校验通过后才把 `.part` 原子改名为正式文件。进程在中间被杀死时，正式文件保持不变，重启后从持久化状态继续。

### 事件饥饿防护

接收端的一个 `epoll` 循环同时管理 UDP 套接字、TCP 控制连接和 `timerfd`。每次 UDP 就绪只处理最多 256 个包，并限制在 2 ms 内，避免持续到达的数据包长期占用事件循环，使控制消息和定时器得不到处理。

### 故障隔离

中心为每个园区代理创建独立线程，一个代理断线不会阻塞其他代理。代理使用最多 4 个工作线程并发处理被隔离接收端的 TCP 补传，慢节点不会无限拖住主组。

## 目录结构

```text
.
|-- CMakeLists.txt
|-- include/                    协议、I/O、校验、存储和线程池
|-- src/
|   |-- central_sender.cpp      中心发送端
|   |-- sender.cpp              园区代理
|   |-- receiver.cpp            接收端
|   `-- checksum.cpp            摘要实现
`-- scripts/                    冒烟、故障注入和 benchmark
```

## 环境与构建

项目依赖 Linux、CMake 3.16、C++17、OpenSSL Crypto、POSIX Threads 和 IPv4 multicast。Ubuntu 或 Debian 可安装：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libssl-dev python3
```

构建：

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j 4
```

生成三个程序：

```text
build-debug/central_sender
build-debug/site_proxy_sender
build-debug/receiver_agent
```

## 运行顺序

中心模式按代理、接收端、中心的顺序启动。

中心支持一次传入多个文件。同一个园区内按命令行顺序串行处理文件；多个园区代理之间由中心使用独立线程并行传输。每个文件都有独立的 Transfer ID、SHA-256、临时文件和 checkpoint。

### 1. 启动园区代理

```bash
mkdir -p proxy-cache received-1 received-2
./build-debug/site_proxy_sender 239.255.42.99 5000 127.0.0.1 6000 2 200 0 3 --central-listen 7000 proxy-cache
```

参数依次为组播地址、UDP 端口、发送网卡地址、控制端口、接收端数量、包间隔微秒、多文件间隔毫秒、最大修复轮数，以及中心监听端口和缓存目录。

`--central-listen 7000` 是一个开关加端口，表示代理进入中心模式并在 TCP `7000` 端口等待中心连接。不写会退回普通发送模式：代理拿本地文件直接往接收端发。

### 2. 启动接收端

```bash
./build-debug/receiver_agent 239.255.42.99 5000 received-1 127.0.0.1 6000 1 127.0.0.1
./build-debug/receiver_agent 239.255.42.99 5000 received-2 127.0.0.1 6000 2 127.0.0.1
```

参数依次为组播地址、UDP 端口、输出目录、代理控制面 IP、代理控制端口、接收端 ID、接收网卡地址。接收端 ID 需要和代理期望的接收端数量对应，并且每个接收端保持唯一。

### 3. 启动中心

```bash
dd if=/dev/urandom of=input.bin bs=1024 count=256 status=none
./build-debug/central_sender 127.0.0.1 7000 0 input.bin
```

普通中心模式的参数依次为代理 IP、代理中心监听端口、发送间隔微秒、待分发文件。这里 `7000` 要和代理启动时的 `--central-listen 7000` 对上。

多园区时重复使用 `--proxy`：

```bash
./build-debug/central_sender --pace-us 0 --proxy 127.0.0.1 7000 --proxy 127.0.0.1 7001 input.bin
```

`--pace-us 0` 表示中心向代理发送数据时不额外 sleep 限速；每个 `--proxy <ip> <port>` 表示一个园区代理。多个代理会并行接收中心数据，每个园区内部仍由自己的代理负责组播、修复和缓存。

## 完整传输流程

1. 先启动代理再启动接收端连接代理并发送 `REGISTER`，代理确认接收端数量和 ID。
2. 启动中心连接代理，计算文件大小、Block 数量、SHA-256 和 Transfer ID。
3. 中心发送 `CENTRAL_FILE_META`。
4. 代理检查正式缓存、`.part` 和 checkpoint，返回 `CENTRAL_RESUME`。
5. 代理向接收端发送 `FILE_META`；接收端建立或恢复临时文件和接收状态，回复 `META_READY`。
6. 中心从恢复位置开始发送 `CENTRAL_DATA`，完成一个 Section 后发送 `CENTRAL_FILE_END`。
7. 代理检查传输 ID、Section、Block、偏移、长度和 CRC，写入缓存临时文件。
8. 代理确认 Section 完整后落盘并更新 checkpoint，再把该 Section 分发到园区内。
9. 接收端按 Block 偏移写入文件；重复 Block 幂等忽略，损坏或越界数据拒绝。
10. 接收端收到结束通知后等待 UDP 安静期，再通过 TCP 上报完整状态或缺块位图。
11. 代理聚合位图并进行有限轮组播或单播修复，缺块过多的接收端转入 TCP 补传。
12. 接收端收齐全部 Block 后执行 SHA-256 校验，原子提交最终文件并发送完成消息。
13. 代理在整文件缓存完成后处理隔离接收端，并向中心返回最终缓存状态。
14. 中心完成当前文件；传入多个文件时，继续处理下一个文件；多代理模式下，各代理会话独立结束。

## 测试

运行全部冒烟、故障注入、多代理测试和 benchmark：

```bash
bash scripts/all_smoke_test.sh
```

可以通过 CMake 目标执行：

```bash
cmake -S . -B build-test -DCMAKE_BUILD_TYPE=Debug
cmake --build build-test --target test
cmake --build build-test --target cleantest
```

`cleantest` 会删除各测试构建目录和 `artifacts/benchmark`，再运行完整测试。测试覆盖：

- 初始丢包与缺块位图；
- 组播修复、UDP 单播修复和 TCP 补传；
- Section 背压和可配置 Section 大小；
- 中心断线续传、代理 `kill -9` 恢复和接收端状态恢复；
- 乱序、重复、CRC 损坏和非法控制帧；
- 一个中心并发连接两个园区代理；
- 正常、混合丢包和慢接收端三类 benchmark 场景。

## 当前边界

- 仅支持 Linux 和可信实验网络；控制面没有 TLS、认证和授权。
- 每个代理对应一个园区和启动时固定的接收端集合。
- Section 顺序推进，尚未实现多 Section 滑动窗口。
- 缓存配额是目录级硬上限，没有淘汰策略。
- 没有 FEC、QUIC、P2P、动态成员、目录同步和 Web 管理界面。
- benchmark 为单机回环结果，真实部署仍需独立主机和网络故障注入验证。
