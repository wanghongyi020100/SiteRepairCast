# SiteRepairCast 工程结构审查与整理报告

审查日期：2026-10-20

## 1. 仓库状态

| 项目 | 状态 |
| --- | --- |
| 仓库路径 | `Z:\site`（Git 返回的共享挂载路径为 `//192.168.157.128/Ubuntu_22.04/site`） |
| 分支 / HEAD | `master` / `8820b6d`（`Remove four-host test configs`） |
| 修改前工作区 | 已跟踪文件干净；有 4 个未跟踪文件，均予以保留 |
| 修改后工作区 | 5 个已跟踪文件修改；4 个原有未跟踪资产保留；新增本报告 |
| 执行环境 | Windows PowerShell。当前没有可启动的 WSL Linux 发行版；本机有 Windows 版 CMake/G++，不能据此验证 Linux/POSIX 构建 |

未跟踪资产及用途判断：

| 路径 | 内容用途判断 | 处理 |
| --- | --- | --- |
| `scripts/peer_repair_barrier_smoke_test.sh` | Proxy 调度 Receiver 互传、完成屏障、超时和单播回退的端到端冒烟测试 | 保留，并加入全量冒烟入口 |
| `scripts/rtt_control_protocol_test.py` | RTT 控制协议的伪接收端/协议检查辅助程序 | 保留；由 RTT 冒烟脚本调用 |
| `scripts/rtt_drain_smoke_test.sh` | RTT 自适应静默期和旧控制帧处理的冒烟测试 | 保留，并加入全量冒烟入口 |
| `benchmark/profiles/rtt_drain_scale.json` | 100 至 500 个 Receiver 的固定等待与 RTT 等待对比场景草案 | 保留；当前 `benchmark/run.py` 未读取 `drain_mode` 字段，不能视为已接入或已执行的基准配置 |

`config/four-host/`、`PROJECT_GUIDE.md`、源码导出文件、`build/` 和 `test-runs/` 当前由忽略规则排除，但它们存在于工作区。它们可能包含本地配置、构建缓存、实验记录和恢复/测试产物，本轮均未修改或清理。现有 `build/CMakeCache.txt` 指向 `/home/s/share/site`，不是当前 Windows 路径下可复用的构建验收证据。

## 2. 原始结构评价

当前结构紧凑，目录职责容易辨认：`src/` 按 Central、Proxy、Receiver 三个进程角色分开；`include/` 集中放置协议、文件描述符、文件 I/O、校验和线程池等共享头文件；`scripts/` 放启动及冒烟脚本；`benchmark/` 包含本机/多机基准和实验室运维工具；`docs/` 保存多机运行说明。CMake 直接从仓库根目录构建三个可执行程序和公共校验库。

本轮没有发现能证明“仅为归类而移动”具有收益的证据。三个主要 `.cpp` 仍然较大，但其职责与进程边界一致；目前没有清晰、低风险的拆分边界，强行增加多层目录会带来更多 include、构建和说明同步成本。`include/` 同时含公共协议头和内部小工具头，尚无安装/导出头文件规则；在没有库消费者或安装接口前，不单独建立 public/internal 层级。

确认的问题主要是入口与说明不同步：两个现存的新冒烟脚本没有被 `all_smoke_test.sh` 调用；README 未准确说明代理调度的 Receiver 互传边界，且把“P2P”整体列为未实现，容易与已实现的数据面互传混淆；`.gitignore` 有重复规则，未忽略根目录 `central_sender` 可执行文件。

## 3. 文件路径审查

| 原始路径 | 新路径 / 保留 | 原因 | 影响 |
| --- | --- | --- | --- |
| `src/central_sender.cpp` | 保留 | Central 角色入口和实现集中，CMake 引用明确 | 无路径改动 |
| `src/sender.cpp` | 保留 | Proxy 角色入口和实现集中；当前没有足够证据安全拆分 | 无路径改动；仅注释澄清调度约束 |
| `src/receiver.cpp` | 保留 | Receiver 事件循环、状态及文件提交属于同一进程实现 | 无路径改动；仅注释澄清状态持久化时机并移除陈旧注释代码 |
| `include/*.hpp` | 保留 | 共享协议/I/O/系统工具和 RAII 头文件集中，引用稳定 | 无路径改动 |
| `scripts/*_smoke_test.sh` | 保留 | 测试脚本已通过脚本自身位置计算仓库根目录；缺失的两个全量入口引用已补齐 | 全量测试会额外执行互传屏障和 RTT 两个冒烟脚本 |
| `scripts/run_*.sh` | 保留 | 启动脚本以 `BASH_SOURCE` 定位仓库，二进制目录可通过参数调整 | 相对文件、缓存及输出路径仍按调用进程工作目录解释 |
| `benchmark/`、`benchmark/lab/` | 保留 | 基准代码与多机实验运维工具有独立用途，不与产品源码混放 | 无路径改动 |
| `benchmark/profiles/rtt_drain_scale.json`（未跟踪） | 保留在原位置 | 内容是 RTT 扩展对照场景草案；Runner 当前不消费 `drain_mode` | 未宣称已接入；需后续明确 Runner 支持或将其作为独立实验配置管理 |
| `config/four-host/`（被忽略的本地目录） | 保留 | 目录含本地 `.env.example` 配置资产；仓库规则明确排除其提交 | 未读取或覆盖配置内容 |
| `build/`、`test-runs/`（被忽略目录） | 保留 | 含既有二进制、构建缓存、实验历史及测试结果；清理可能损失用户资产 | 未运行会覆盖它们的构建或清理目标 |
| `site_code.txt`、`PROJECT_GUIDE.md` 等本地忽略文件 | 保留 | 属于本地检查/导出资产，不参与正式构建；用途可辨认 | 未删除 |
| `SITE_REPAIR_CAST_STRUCTURE_REVIEW.md` | 新增 | 按本任务要求交付审查、变更、验证和遗留问题记录 | 新增文档，不影响程序行为 |

路径行为静态审查：Central 输入路径来自 CLI；Proxy 缓存目录和 Receiver 输出目录可配置，启动脚本提供仓库内默认目录；Proxy checkpoint 与缓存文件相邻，Receiver 临时文件及状态文件位于输出目录并带 Transfer ID。相对路径仍依赖进程工作目录，这是常规 CLI 路径语义，但多机部署应显式指定长期稳定的缓存/输出目录，避免服务从不同工作目录启动后找不到既有恢复状态。CMake 使用 `${CMAKE_SOURCE_DIR}` 和 `${CMAKE_BINARY_DIR}` 组成脚本及测试目录路径，没有发现硬编码 Windows 路径。

## 4. 实施变更

### 源码与注释

- `src/sender.cpp`：说明 Proxy 汇总缺块后按组播、Receiver 互传、Proxy 单播推进；明确 TCP 任务下发成功不等于数据到达，最终以目标下一轮位图确认。
- `src/receiver.cpp`：修正易误读的落盘顺序说明，明确 Block 写入后先更新内存位图，完整 Section 才执行 `fdatasync` 并保存状态；删除被注释掉的旧 `fsync`/逐 Block 状态保存代码。
- 以上均为注释整理，不改变协议、调度、线程或持久化行为。

### 脚本与配置

- `scripts/all_smoke_test.sh`：把已有的 `peer_repair_barrier_smoke_test.sh` 与 `rtt_drain_smoke_test.sh` 加入统一冒烟列表。
- `.gitignore`：忽略根目录生成的 `central_sender`；删除重复的 `hosts.json` 和 `*.full` 规则。忽略配置目录及本地测试资产的既有规则保持不变。
- 没有移动文件，也没有修改 CMake 构建目标。全量脚本中的 10 个冒烟脚本路径静态检查均能在 `scripts/` 下找到。

### 文档

- `README.md`：补充代理控制的 Receiver 互传流程、任务屏障和失败回退；把物理主机快速启动标题改为中文；补上新增测试覆盖；澄清 `cleantest` 实际删除的是构建目录下的 `test-runs/`；将能力边界从笼统“没有 P2P”改为“没有自治 P2P”，避免否认已有的 Proxy 调度互传。
- 新增本报告；其他文档未发现需要为本轮变更同步改路径的引用。

除全量脚本会多执行两个既有冒烟用例外，以上修改不改变运行时业务行为。没有进行目录迁移、协议调整或新增功能实现。

## 5. 中文优化情况

README 主体已经以中文介绍架构、数据流、Section 背压、修复、恢复、构建和部署；本轮不做机械重写，而是修正关键术语和已实现能力边界，使“Proxy 控制的 Receiver 互传”与“自治 P2P”区分清楚。对 Proxy 的任务调度确认条件和 Receiver 的 Section 状态持久化约束补充了中文原因说明。清理了 Receiver 热路径中误导性的旧注释代码。

`docs/FOUR_HOST_TEST.md` 和 `benchmark/README.md` 已以中文为主，代码、协议字段、CLI 选项和机器读取字段保留原名。现有 benchmark 数字属于仓库已有记录，本轮没有重跑，也没有将其标记为本轮验证结果。

## 6. 测试与验证

| 检查 | 结果 |
| --- | --- |
| `git diff --check` | 通过；没有空白错误 |
| 全量冒烟清单路径核对 | 10 个条目均能在 `scripts/` 找到 |
| README 相对 Markdown 链接核对 | `docs/FOUR_HOST_TEST.md` 存在 |
| Linux CMake 构建 | `NOT_RUN`：本轮环境没有可启动的 WSL Linux 发行版；Windows CMake/G++ 不能证明 POSIX/Linux 构建成功 |
| `scripts/all_smoke_test.sh` / 基础端到端分发 | `NOT_RUN`：需要 Linux/POSIX 网络与进程语义；本轮只修改文档、注释、忽略规则及测试清单入口 |

本轮没有重用或引用现有 `build/` 二进制/`test-runs/` 日志作为验证证据。仓库内历史 benchmark 报告和运行记录不等于当前工作树验证。

## 7. 遗留问题

### P0

- 本轮静态审查没有确认新的 P0 问题；这不代表通过了运行时正确性或安全性审计。

### P1

- 控制面目前没有 TLS、认证和授权；部署边界仍应限制在可信网络，README 已将其列为限制。
- 缓存、输出和恢复状态路径支持相对路径，服务启动工作目录改变时可能导致恢复数据定位变化。运维配置应显式使用稳定目录；未来可考虑启动时打印规范化路径或要求配置绝对路径。
- Windows 工作区无法完成 Linux 构建和网络冒烟，修改后的当前树仍需在 Linux 环境验证。

### P2

- `benchmark/profiles/rtt_drain_scale.json` 是未跟踪草案，Runner 当前不处理 `drain_mode`；应决定将其接入 Runner、改为单独驱动脚本，或作为暂存配置保留。
- 三个角色实现文件较大。当前职责清楚但文件内聚度较高，后续应围绕可复用组件/测试边界逐步拆分，不建议为目录形式而迁移。
- `cleantest` 会递归删除 `${CMAKE_BINARY_DIR}/test-runs`。README 已提示检查目录内容；长期可考虑增加清理范围保护或采用唯一临时目录。

## 8. 最终工程评价

作为 C++ Linux 后端求职项目，仓库有清晰的 Central/Proxy/Receiver 进程角色、共享协议和文件工具、可重复的本机故障注入脚本，以及独立的多机实验工具。README 已能说明可靠传输、断点恢复、校验和代理调度修复的主要路径；本轮修正了 Peer 修复能力边界和冒烟入口遗漏。

项目规模保持紧凑，构建路径引用可跨工作目录使用；运行路径有 CLI 配置能力，但相对路径仍受工作目录影响。主要维护短板是角色实现文件较大、多机安全控制尚未实现、当前工作树没有本轮 Linux 验收证据。目录未因本次整理发生变化，业务语义未修改；验证结论限于差异格式与静态路径清点。
