# SiteRepairCast Benchmark

## 怎么测试

在项目根目录执行正式测试：

```bash
python3 benchmark/run.py --profile benchmark/profiles/standard.json
```

`standard.json` 使用 8 MiB 文件、8 个 Receiver，每个场景重复 5 次。脚本会自动编译
Release 版的 Central、Proxy 和 Receiver，启动各场景所需的进程，并对每个输出文件做 SHA-256 校验。

只想检查测试脚本能否正常运行，可以使用 512 KiB、单轮的 smoke 配置：

```bash
python3 benchmark/run.py --profile benchmark/profiles/smoke.json
```

结果保存在 `artifacts/validation/<时间>-<profile>/`：

- `report.md` 保存汇总结果
- `summary.csv` 和 `summary.json` 保存各场景统计值
- `raw/metrics.csv` 和 `raw/metrics.jsonl` 保存每一轮的原始指标
- `raw/<scenario>-rNN/` 保存各进程日志、启动参数和测试钩子

## 测试了什么

| 场景 | 验证内容 |
| --- | --- |
| `distribution-clean-8` | 8 个 Receiver 无丢包完成分发 |
| `distribution-loss-1pct-8` | 每个 Receiver 丢弃 1% 的 DATA 首次到达，再通过组播修复 |
| `distribution-loss-3pct-8` | 每个 Receiver 丢弃 3% 的 DATA 首次到达，再通过组播修复 |
| `distribution-loss-5pct-8` | 每个 Receiver 丢弃 5% 的 DATA 首次到达，再通过组播修复 |
| `slow-shared-repair-8` | 2 个落后 Receiver 留在主修复组，继续参与 UDP 修复 |
| `slow-isolated-8` | 隔离相同的 2 个落后 Receiver，通过 TCP backfill 补齐缺块 |
| `central-resume-8` | Central 确认 4 个 Section 后断开，重连 Proxy 并从 checkpoint 继续 |
| `proxy-crash-recovery-8` | Proxy 提交第 4 个 Section 后被 `SIGKILL`，重启 Proxy 和 Receiver 后恢复 |

测试记录完成时间、修复包数、TCP backfill 块数、恢复 Section 和恢复耗时，并校验所有
Receiver 的最终文件是否与输入文件一致。

## 测试结果

测试环境：2 个逻辑 CPU、3.77 GiB 内存、Linux、g++ 13.3.0、Python 3.12.3。
以下是 `standard.json` 在同机 loopback 环境下的 5 轮结果。

| 场景 | 通过 | 完成时间中位数 | P95 | 恢复耗时中位数 | SHA-256 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `distribution-clean-8` | 5/5 | 17.524 s | 20.568 s | - | 100% |
| `distribution-loss-1pct-8` | 5/5 | 21.406 s | 23.890 s | - | 100% |
| `distribution-loss-3pct-8` | 5/5 | 21.602 s | 23.494 s | - | 100% |
| `distribution-loss-5pct-8` | 5/5 | 23.663 s | 27.198 s | - | 100% |
| `slow-shared-repair-8` | 5/5 | 17.187 s | 19.004 s | - | 100% |
| `slow-isolated-8` | 5/5 | 16.926 s | 17.535 s | - | 100% |
| `central-resume-8` | 5/5 | 16.831 s | 17.002 s | 11.867 s | 100% |
| `proxy-crash-recovery-8` | 5/5 | 16.794 s | 16.908 s | 11.659 s | 100% |

8 个场景共运行 40 轮，全部通过，所有 Receiver 的最终文件 SHA-256 校验成功。

- 1% / 3% / 5% 场景中，每个 Receiver 分别实际丢弃 70 / 210 / 350 个 Block，修复后均完整输出。
- 慢节点隔离后，正常 Receiver 完成时间中位数从 17.133 s 降至 16.745 s，下降 2.268%；
  被隔离的 2 个 Receiver 每轮共通过 TCP backfill 恢复 256 个 Block。
- Central 重连后从 Section 4/14 继续，恢复耗时中位数为 11.867 s。
- Proxy 崩溃重启后从 Section 4/14 恢复，恢复耗时中位数为 11.659 s。

丢包由应用层测试钩子确定性注入，慢节点也由缺块模拟。这些数据验证了协议路径，不等同于多机真实网络测试。
