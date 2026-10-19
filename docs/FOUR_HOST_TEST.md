# SiteRepairCast 四台物理主机测试

本文说明如何在同一 LAN/VLAN 中用四台 Linux 主机运行现有的 Central、Proxy 和两个 Receiver。程序不会自动修改网卡、路由或防火墙。

## 1. 拓扑与示例地址

```text
Machine A  Central   192.168.1.10
        | TCP -> 192.168.1.20:7000
Machine B  Proxy     192.168.1.20
        | TCP control :6000
        | UDP multicast -> 239.255.42.99:5000
        +----------------------+----------------------+
Machine C  Receiver 1  192.168.1.31   Machine D  Receiver 2  192.168.1.32
```

`192.168.1.x` 是主机地址，`239.255.42.99` 是组播组地址，两者不能互换。Central 是 TCP 客户端，不加入组播，也不需要 `--central-ip`。四台机器应处在能够直接传递组播的同一 LAN/VLAN；程序保持 LAN 范围的 multicast TTL 1。

## 2. 在四台机器上编译

四台机器使用同一版本的仓库，并分别执行：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libssl-dev
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

构建会生成三个独立程序：`build/central_sender`、`build/site_proxy_sender` 和 `build/receiver_agent`。

## 3. 确认真实 LAN 地址

在 Proxy 和每台 Receiver 上检查本机地址与路由：

```bash
ip -br addr
ip route
```

将配置中的 `INTERFACE_IP` 改为该机器真实 LAN IPv4。不要在物理主机测试中照搬 smoke test 使用的 `127.0.0.1`、`127.0.0.2` 或 `127.0.0.3`。

先测试普通 IP 连通性，例如从其他机器访问 Proxy：

```bash
ping 192.168.1.20
```

## 4. 准备本地配置

仓库不包含四台物理主机的测试配置样例。可以直接使用第 6 节的命令行参数；如需 `.env` 文件，请在本机创建并确保不会提交到仓库。

## 5. 端口与防火墙检查

默认端口如下：

| 用途 | 端口 |
| --- | --- |
| Proxy -> Receiver multicast DATA | UDP 5000 |
| Receiver <-> Proxy control | TCP 6000 |
| Central -> Proxy | TCP 7000 |

Proxy 至少需要允许 TCP 6000、TCP 7000 和 UDP multicast 出站。Receiver 至少需要允许 UDP 5000、到 Proxy:6000 的 TCP 出站，以及 peer repair 使用的动态 UDP 端口。peer UDP 端口由内核随机分配，无需命令行配置。

项目不会执行 `ufw disable`、`iptables -F` 或其他防火墙修改。请按所在网络的安全策略放行所需流量。

Proxy 启动后，可从 Central 检查中心端口，从 Receiver 检查控制端口：

```bash
# Machine A
nc -vz 192.168.1.20 7000

# Machine C / D
nc -vz 192.168.1.20 6000
```

`nc` 只检查 TCP 连通性，不能用它判断 UDP multicast 是否可靠。

## 6. 按顺序启动

推荐顺序是 Proxy、Receiver 1、Receiver 2、Central。Proxy 会先监听控制端口并等待固定数量的 Receiver 注册，然后处理 Central 的文件。

Machine B（Proxy）：

```bash
./scripts/run_proxy.sh --interface-ip 192.168.1.20 --receivers 2
```

Machine C（Receiver 1）：

```bash
./scripts/run_receiver.sh --interface-ip 192.168.1.31 \
  --proxy-ip 192.168.1.20 --output-dir ./received
```

Machine D（Receiver 2）：

```bash
./scripts/run_receiver.sh --interface-ip 192.168.1.32 \
  --proxy-ip 192.168.1.20 --output-dir ./received
```

确认 Proxy 日志已经出现两个 `registered receiver=` 后，在 Machine A 启动 Central：

```bash
./scripts/run_central.sh --proxy-ip 192.168.1.20 --file ./input.bin
```

## 7. 验证输出 SHA-256

不要只根据退出码判断成功。分别执行：

```bash
# Machine A
sha256sum input.bin

# Machine B
sha256sum proxy-cache/central-transfer-*.bin

# Machine C / D
sha256sum received/input.bin
```

源文件、Proxy 缓存和两个 Receiver 最终文件的四个摘要必须完全一致。Receiver 最终文件使用 Central 源文件 basename；例如 Central 发送 `/home/a/t128.txt`，Receiver 应得到 `received/t128.txt`，传输中的临时文件和状态文件分别是 `received/t128.txt.part-<transfer_id>` 和 `received/t128.txt.part-<transfer_id>.state`。完成确认后，可以删除本次测试的 `input.bin`、`proxy-cache/central-transfer-*` 和 `received/input.bin`；`.part-*`/`.state` 是未完成传输的恢复数据，确认不再恢复该测试时也可一起清理。

## 8. 常见错误

- `--interface-ip is required`：物理主机模式必须显式传入本机 LAN IPv4，或在配置文件中设置 `INTERFACE_IP`。
- `invalid --multicast-ip: ... is not multicast`：`--multicast-ip` 必须是组播地址，例如 `239.255.42.99`，不能填 Proxy 的 `192.168.x.x` 地址。
- `connect proxy control` 或 `connect proxy central port`：检查 Proxy 是否已启动、IP/端口是否一致，并用 `ping`/`nc -vz` 检查连通性。
- Proxy 一直等待 Receiver：检查 `RECEIVER_COUNT`、Receiver 的 `PROXY_IP`/`CONTROL_PORT` 以及 Proxy 日志中的注册记录。
- TCP 正常但 UDP 数据缺失：确认所有机器位于支持组播的同一 LAN/VLAN，检查交换机 IGMP snooping、主机防火墙和所选 `INTERFACE_IP`。
- 输出目录或缓存目录报错：确认路径可写、磁盘空间充足，且路径不是普通文件。
- SHA-256 不一致：保留四端日志与 `.part/.state` 供排查，不要把未校验的 `.part` 当作最终文件。

三个二进制的完整参数及默认值可分别查看：

```bash
./build/central_sender --help
./build/site_proxy_sender --help
./build/receiver_agent --help
```
