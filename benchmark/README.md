# SiteRepairCast benchmark / validation

这个目录把简历中的“支持分发、丢包修复、慢节点隔离、断点续传和崩溃恢复”变成可重复运行、保留原始证据的实验。运行器调用现有三个程序及其测试环境变量，不修改生产协议和传输路径。

唯一配套 C++ 改动记录在 `src/receiver.cpp` 的 `should_drop_initial_block_for_test`：删除对 `state.last_end_round` 的提前返回，并同步修改注释。原钩子在 Section 0 结束后会让后续配置 Block 永远不再丢弃，导致“整文件 3%”实际只执行了一小部分；现在由已有的 `intentionally_dropped_blocks.insert` 保证每个配置 Block 只在首次到达时丢一次，修复重传正常接收。未设置 `SRCAST_DROP_INITIAL_BLOCKS` 时路径立即返回，生产运行行为不变。

## 快速运行

依赖与主项目一致：Linux、CMake 3.16+、C++17 编译器、OpenSSL 开发包、Python 3.8+。Python 脚本只使用标准库，不需要 `pip install`；IPv4 loopback 必须支持 multicast。

从仓库根目录运行：

```bash
python3 benchmark/run.py --profile benchmark/profiles/smoke.json
```

`smoke.json` 使用 512 KiB 文件、每场景 1 轮，只检查整条采集链路。它会运行：

- 2 个接收端的无丢包与 3% 确定性相关有效丢包；
- 4 个接收端、其中 2 个落后接收端的隔离开/关 A/B；
- 中心在确认 2 个 Section 后断线并续传；
- 代理在提交第 2 个 Section 后被 `SIGKILL`，随后代理与接收端重启恢复。

正式收集多轮数据：

```bash
python3 benchmark/run.py --profile benchmark/profiles/standard.json
```

`standard.json` 明确记录了 8 MiB、8 接收端、5 次重复和 1%/3%/5% 丢包等条件。它不是 1 GB 实验，报告也不会把结果外推为 1 GB。建议复制配置、调整 `file_size_bytes` 和 `repetitions`，并把修改后的 JSON 与结果一起保存。

常用选项：

```bash
# 只跑一个场景并指定新的输出目录
python3 benchmark/run.py \
  --profile benchmark/profiles/standard.json \
  --scenario central-resume-8 \
  --output artifacts/validation/resume-8m-5runs

# 已有 Release 构建时复用它
python3 benchmark/run.py \
  --profile benchmark/profiles/smoke.json \
  --build-dir build-benchmark-validation \
  --no-build
```

输出目录必须是一个尚不存在的新目录，运行器不会覆盖旧结果。默认写到 `artifacts/validation/<时间>-<profile>`，该位置已由项目 `.gitignore` 忽略。为避免固定回环组播互相干扰，同一仓库一次只允许运行一个 benchmark。

## 输出与可审计性

每次运行产生：

```text
artifacts/validation/<run>/
├── manifest.json          # 完整 profile、机器、编译器、commit 和执行命令
├── build.log              # Release 配置与构建日志
├── raw/
│   ├── metrics.csv        # 每一轮一行的原始指标
│   ├── metrics.jsonl      # 含完整字段的逐轮记录
│   └── <scenario>-rNN/
│       ├── central*.log
│       ├── proxy*.log
│       ├── receiver-*.log
│       └── run.json       # 精确 argv、测试 hook、结果或错误
├── summary.csv            # 按场景聚合的成功率、median、P95、P99
├── summary.json
└── report.md              # 可读报告、慢节点 A/B 与口径限制
```

失败时原始日志仍会保留。默认在第一轮失败后停止；加 `--keep-going` 才会继续其余场景。最终输出文件以 SHA-256 与源文件逐一比较，任何缺失、重复输出或摘要不一致都会把该轮标成失败。

## 指标定义

- `duration_ms`：启动中心发送端前到该轮所有进程退出的墙钟时间，不含构建与接收端注册。
- `receiver_completion_ms`：从启动中心到接收端最终文件原子 `rename` 可见的时间，轮询分辨率约 10 ms。
- `normal_completion_ms`：慢节点实验中所有非落后接收端完成时间的最大值，用于同一 profile 的隔离开/关对照。
- `repair_*_packets`、`tcp_backfill_blocks`：从代理原始日志汇总的实际执行计数。
- `resume_section`、`recovered_blocks_per_receiver`：从中心/接收端恢复日志解析的 checkpoint 位置。
- `recovery_ms`：第二次中心启动或崩溃后重启完成，到全部恢复完成的实测时间。
- `crash_to_recovered_ms`：从向代理发送 `SIGKILL` 到全部接收端恢复完成，包含脚本中的 200 ms 重启间隔、进程重启和重新注册。
- `success_rate_percent`、`sha256_success_rate_percent`：实际成功轮数除以配置轮数。
- P95/P99：对多轮墙钟数据使用 nearest-rank；smoke 只有一轮，因此只表示采集链路正常，不是有统计意义的尾延迟。

`cross_site_payload_bytes_protocol_derived` 是正常完成或 checkpoint 主动断线场景中按协议已知的中心文件 payload；`SIGKILL` 场景可能存在正在发送但尚未确认的 Section，因此该字段留空，不能假装精确。`direct_tcp_payload_bytes_model` 是“向每个接收端各发一份”的比较模型。它们都不是抓包得到的 wire bytes，不包含 TCP/UDP/IP 头和控制帧，报告会始终明确标为 derived/model，不能描述成“实测带宽节省”。

## 场景边界

分发丢包场景通过 `SRCAST_DROP_INITIAL_BLOCKS` 确定性地让每个 receiver 丢弃指定 DATA Block 的首次到达，并在修复重传时正常接收。配置百分比以整文件 Block 数为分母，运行器还会强制核对配置数与 receiver 日志中的实际丢弃数。它是可重复的应用层测试钩子，不是随机网络丢包模拟器，不能写成“实测链路丢包率”。单机上的多个 receiver 使用同一个 IP 和 UDP 端口，针对单个 receiver 的 UDP 单播无法可靠归属，因此所有 receiver 丢同一组 Block，保证修复走组播。

慢节点 A/B 则使用 `SRCAST_DROP_INITIAL_BLOCKS` 让两个接收端在第一个 Section 丢相同的一组 Block：

- `slow_shared` 把隔离阈值设为最大值，落后节点留在主修复组；
- `slow_isolated` 使用 profile 中的阈值，要求日志确认隔离并执行 TCP backfill；
- 两边输入、Section、pace、接收端数和缺块集合相同，比较正常接收端完成时间。

这能验证“落后节点是否阻塞主组”的实现路径，但它模拟的是确定性缺块，不是独立主机上的真实限速。要写“1 个限速慢节点”或真实网络 P99，必须在多机环境使用 `tc/netem` 或硬件限速后重新运行，不能使用本机 smoke 数字代替。

中心续传使用 `SRCAST_STOP_AFTER_SECTIONS`；崩溃恢复在日志确认 Section checkpoint 后向代理发送 `SIGKILL`，随后复用 cache 和 receiver 状态目录重启。报告中的“已保留 payload”和“已确认 payload 重传 0 字节”由恢复 Section 边界推导，真实耗时、恢复位置和最终 SHA-256 则来自本次执行。

## 简历使用原则

只引用与你保存的 `manifest.json`、raw logs 和 summary 对应的真实数据。推荐至少 5 轮，在机器空闲且环境固定时运行，并写清文件大小、接收端数、丢包方式和单机/多机条件。不要只选最好的一轮，不要把 payload 模型写成抓包流量，也不要把 512 KiB smoke 外推成 1 GB 结果。
