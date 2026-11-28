# Turing Agent Runtime

[![verify](https://github.com/XiaoBinGan/Turing_AgentRuntime/actions/workflows/verify.yml/badge.svg)](https://github.com/XiaoBinGan/Turing_AgentRuntime/actions/workflows/verify.yml)
[![python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![docs](https://img.shields.io/badge/website-live-brightgreen)](https://xiaobingan.github.io/Turing_AgentRuntime/)

**Turing Agent Runtime** — a production-shaped runtime for LLM agents. It provides the **shape** — control
loop, state ownership, tool dispatch, governance, observability, persistence —
and leaves the **content** — prompts, business vocabulary, domain rules — to the
application layer.

> 中文说明见下方，或直接跳转 [中文](#中文说明)。

---

## Why this exists

Most agent frameworks start from prompts and grow into a bundle of helpers. That
works for demos and breaks the moment two teams need to share one runtime with
different prompts, different tools, and different compliance rules.

This project starts from the opposite end. It began as the reference
implementation for a full-stack architect's notes on enterprise agent systems —
a 18-chapter curriculum covering agent fundamentals, planning, tool calling, MCP,
memory, RAG, context management, sandboxing, multi-agent, evaluation,
performance, enterprise architecture, system design, and runtime internals.

Every chapter asks "how would you actually build this". This repository is the
answer that survived contact with a test suite: **162 modules, 76k lines, 2349
tests, 11 end-to-end scenarios**, all green and stable across repeated runs.

The central design bet: **the runtime owns the shape, the application owns the
content**. Concretely —

- A frozen contract layer (`contracts.py`) that defines 15 protocols and every
  state ownership rule. Nothing else may invent a shared type.
- A bounded control loop with six termination conditions and three levels of
  deadlock detection.
- A patch-based context protocol where every write declares an owner, and an
  ownership table decides whether that write is legal.
- Optional, pluggable everything: vector backends, memory providers, sandboxes,
  storage, LLM providers, MCP transports, tracers.

If you want a library that helps you prompt an LLM, this is not it. If you want
the skeleton that a team can standardize on for years, read on.

---

## Architecture

```
                     ┌──────────────────────────────────────────┐
                     │  Governance  (fail-closed)               │
                     │  authn · RBAC · input/output guard       │
                     │  quota · rate limit · approvals          │
                     └──────────────────────────────────────────┘
                                        │
  request ──► Gateway ──► Loop ──► Dispatcher ──► Tool
                 │          │           │
                 │          │           └── Secret injection, timeout,
                 │          │               idempotency cache, per-turn limit
                 │          │
                 │          ├── Context  (patch log = trajectory = WAL)
                 │          │      └── per-field OWNER_TABLE
                 │          │
                 │          └── ExecCtx  (principal, budget, deadline,
                 │                       trace, approvals, depth, cancel)
                 │
        ┌────────┴────────┬──────────────┬──────────────┐
        │                 │              │              │
   Tool plane      Memory plane   Knowledge plane  Workflow plane
   (MCP, sandbox)  (extract,      (RAG, vector,    (DAG, sub-agent,
                    recall)        FTS, splitter)   saga, envelope bus)
        │                 │              │              │
        └────────┬────────┴──────────────┴──────────────┘
                 │
        ┌────────┴─────────┐
        │  Quality (fail-open)      Observability
        │  eval · critique          metrics · traces · audit
        └───────────────────────────┘
```

One mainline, four resource planes, two guarantee planes. Governance fails
closed (no decision means no); quality fails open (a broken evaluator must not
stop a turn).

![Architecture overview](docs/design/images/architecture.png)

![Request sequence](docs/design/images/request-sequence.png)

### The seven decisions that matter

1. **Patch protocol context.** State is not a dict you mutate. Every write is
   `apply(Patch(op, path, by, value))`, and `OWNER_TABLE` decides whether that
   `by` may touch that `path`. The patch log is simultaneously the trajectory,
   the replay source, and the write-ahead log.

2. **ExecCtx as aorta.** Principal, budget, deadline, trace id, idempotency key,
   approvals, depth, and cancellation all travel in one object. Every plane
   receives the same facts without reaching for globals.

3. **Bounded loop.** Six termination conditions (done, budget, deadline,
   deadlock, unrecoverable, needs-confirmation) and three deadlock detectors
   (iteration count, semantic fingerprint, arbitration). A loop that cannot stop
   is not a runtime, it is an incident.

4. **Three-state split by lifecycle.** Context lives one turn. Session lives one
   conversation. Memory lives a retention matrix. Knowledge lives versions.
   Mixing them is the most common architectural mistake in agent systems.

5. **SPI everywhere, dependencies nowhere.** The core runtime imports nothing
   third-party. Every external system is behind a protocol with a stdlib default
   and an optional mature implementation.

6. **Single-request amplification is a threat model.** Node counts, budgets, page
   sizes, metric label cardinality, workflow capacity — all bounded, because one
   request should never be able to consume unbounded resources.

7. **Observability is a plane, not a logger.** Metrics, traces, and audit are
   separate concerns with separate failure modes. Audit never records raw goals.
   Metric labels are normalized so a client cannot explode cardinality.

![Failure posture](docs/design/images/failure-posture.png)

Deeper design documents live in [`docs/`](docs/):

| Document | Contents |
|---|---|
| [QUICKSTART.md](docs/design/QUICKSTART.md) | Runnable examples, concept map, onboarding friction |
| [STATUS.md](docs/design/STATUS.md) | Module map with dependency graphs |
| [SPI.md](docs/design/SPI.md) | Every extension point and its contract |
| [TOOLS.md](docs/design/TOOLS.md) | Tool manifest, risk tiers, approval flow |
| [DEPLOY.md](docs/design/DEPLOY.md) | 20+ environment variables, failure modes |
| [ROBUSTNESS.md](docs/design/ROBUSTNESS.md) | Destructive test findings and fixes |
| [RAG-VERIFICATION.md](docs/design/RAG-VERIFICATION.md) | Real embedding and reranker backends, and why two-stage retrieval matters |
| [DESIGN-C.md](docs/design/DESIGN-C.md) | Parallel tools, sub-agent progress, workflow engine |
| [DEVLOG.md](docs/design/DEVLOG.md) | Round-by-round engineering log |

---

## Quick start

Zero third-party dependencies for the core:

> **Not on PyPI yet.** The name `agent-runtime` on PyPI belongs to an unrelated
> AutoGen placeholder package (v0.0.1) that pulls in four `autogen-*`
> distributions and cannot be imported. Do not `pip install agent-runtime`.
> The distribution name will be `turing-agent-runtime` when it is published.

Install from the repository:

```bash
pip install "git+https://github.com/XiaoBinGan/Turing_AgentRuntime.git"          # core only
pip install "git+https://github.com/XiaoBinGan/Turing_AgentRuntime.git#egg=turing-agent-runtime[server]"
```

Or work from a clone, which also gives you the examples, the demo and the tests:

```bash
git clone https://github.com/XiaoBinGan/Turing_AgentRuntime.git
cd Turing_AgentRuntime
python3 -m venv .venv && .venv/bin/pip install -r requirements-core.txt

.venv/bin/python examples/quickstart/01_minimal.py    # 40 lines, no API key needed
.venv/bin/python examples/quickstart/03_approval.py   # a T2 tool, approval, three outcomes
.venv/bin/python demo/run.py                          # 11 end-to-end scenarios
```

Start with `01_minimal.py`: a custom tool, a fake LLM, one loop, printed trace.
No network, no key. Then read [QUICKSTART.md](docs/design/QUICKSTART.md) for the
concept map.

`requirements-core.txt` is empty by design. The core runtime runs on the standard
library alone. `requirements.txt` adds the optional integrations (FastAPI,
tiktoken, prometheus_client, jsonschema, redis, qdrant, pymilvus, OpenTelemetry,
the MCP SDK).

Run the full verification:

```bash
./verify.sh                    # 8 gates: compile, tests, contracts, docs-facts, config-defaults, open-items, scenarios, HTTP
VERIFY_CLUSTER=1 VERIFY_LANGFUSE=1 ./verify.sh    # 8 gates + 2 optional (cluster, langfuse), needs live services
```

Start the HTTP service:

```bash
AGENT_RUNTIME_ALLOW_ANONYMOUS=1 .venv/bin/python serve.py
curl -X POST localhost:8000/v1/turns \
  -d '{"goal":"api p99 is 3s, find the root cause",
       "principal":{"user_id":"u","tenant":"acme","roles":["sre"],
                    "scopes":["runtime:run","tool.read"]}}'
```

---

## What is verified, and what is not

Verified by the suite:

- 2349 unit and integration tests, stable across repeated full runs
- 11 end-to-end scenarios, 80 assertions
- Concurrency: 100 threads against one gateway, 8 threads x 5 turns, 60 mixed
  workflow requests, zero exceptions
- Memory bounded: 900 turns grow 3KB with a capped audit log
- Rate limiting exact under concurrency (40 concurrent, limit 10, exactly 10 pass)
- Real model convergence: 4-6 steps to DONE on a root-cause task
- Real Milvus cluster, real Langfuse ingestion, real OTLP delivery
- Graceful shutdown ordering, checkpoint crash recovery, workflow persistence

Operational surface, all exercised with reproducible scripts:

| Capability | Evidence | Script |
|---|---|---|
| Multi-replica | 2 replicas on shared Redis; quota exact across both (limit=4, 4 pass, 5th refused on either replica) | `scripts/verify_multi_replica.sh` |
| Capacity baseline | Single replica 145 RPS / p99 14ms at concurrency 1; 192 RPS with 2 replicas | `scripts/loadtest.py` |
| Backup and restore | sqlite via backup API, file store, corruption rejected | `scripts/verify_backup_restore.sh` |
| Schema migration | Versioned envelope migrations, applied stepwise, refuses unknown and future versions | `tests/test_store_migration.py` |
| SLO and alerting | Six SLIs with targets derived from measured baseline; 9 rules validated by promtool | `docs/design/SLO.md`, `deploy/prometheus/` |
| On-call runbook | Per-alert: how to confirm, what to do first, what not to do | `docs/design/RUNBOOK.md` |
| Chaos drill | KV down, store read-only, upstream down, client abort: all fail in the expected posture | `scripts/chaos_drill.sh` |

Also measured:

| Capability | Result | Script |
|---|---|---|
| Soak, **150 minutes** | 1,036,418 requests, 99.9998% success, p99 101.8ms, threads constant at 10, file handles flat, steady-state RSS 101MB -> 97MB (no leak), peak growing 268 -> 487MB from longer GC cycles | `scripts/soak_test.py` |
| Real-LLM latency | Two upstreams measured: deepseek-flash p50 2672ms, and a local Strata engine on 2xT4 at p50 4266ms. A 4-6 step turn is 11-26s end to end. Also ran a full turn against the local engine to confirm the text-rendered tool descriptions produce valid `tool_calls` | `scripts/real_llm_capacity.py` |
| Multi-worker | Requests do distribute across 4 workers, but throughput does not rise — the load generator saturates first | `AGENT_RUNTIME_WORKERS` |
| Cross-replica sessions | Works via `AGENT_RUNTIME_STORE=redis`: turn_index stays continuous across replicas | `_make_store()` |
| sqlite write contention | Multi-process writes to one file scale to 3.07x with zero failures; the case for Redis is fault domain, not throughput | `scripts/bench_sqlite.py` |

What is still open:

- **Network RTT caps throughput, and that is now measured.** From the same host,
  8 workers reach 6,911-8,068 RPS. From another machine over a 28ms link, the
  same setup tops out at 3,260 RPS with server CPU at only 271% — the requests
  are in flight, not being processed. Capacity planning has to start from where
  the clients are.
- **Soak reached 2.5 hours.** Truly multi-day behaviour under real traffic is
  still unmeasured.

Details, including a wrong conclusion I had to retract, are in
`docs/design/CAPACITY.md`.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The short version: read `contracts.py`
first, never modify it without a migration note, keep business vocabulary out of
the runtime, and add a test that would have caught your bug.

## License

[Apache-2.0](LICENSE). Patent grant included, because infrastructure should be
safe to adopt.

---

# 中文说明

## 这个项目是什么

**Turing Agent Runtime** —— 一个**生产形态的 LLM Agent 运行时**。它提供**形状** —— 控制循环、状态归属、
工具分发、治理、可观测性、持久化；把**内容** —— 提示词、业务词汇、领域规则 ——
留给应用层。

## 由来

大多数 Agent 框架从提示词出发，长成一堆辅助函数。做演示可以，但两个团队要用同一套
运行时、各自的提示词与工具、各自的合规要求时，就散了。

这个项目从反方向开始。它源自一份全栈架构师笔记里的企业级 Agent 课程 ——
18 章，覆盖 Agent 基础、规划、工具调用、MCP、记忆、RAG、上下文管理、沙箱、
多 Agent、评测、性能、企业级架构、系统设计、运行时源码。

每一章都在问「这东西真建起来长什么样」。这个仓库就是那份答案，而且它经过了
测试套件的检验：**158 个模块、7.6 万行、2349 个用例、11 个端到端场景**，
全绿，且重复跑稳定。

核心设计赌注：**运行时拥有形状，应用拥有内容**。具体地：

- 一层冻结的契约（`contracts.py`），定义 15与全部状态归属规则。
  其他任何地方都不得自造共享类型。
- 一个有界的控制循环，六个终止条件，三层死锁检测。
- 一套补丁式上下文协议：每次写入都要声明 owner，由归属表裁决是否合法。
- 一切都可选、可插拔：向量后端、记忆提供者、沙箱、存储、LLM 提供者、
  MCP 传输、追踪器。

如果你要的是「帮我调提示词的库」，这不是。如果你要的是一个团队能用很多年的骨架，
继续读。

## 架构

一条主线、四个资源面、两个保障面。治理面 fail-closed（判不出来就是拒绝），
质量面 fail-open（评测器坏了不能拦住回合）。

![架构全景](docs/design/images/architecture.png)

![请求时序](docs/design/images/request-sequence.png)

### 七个关键决定

1. **补丁协议上下文**。状态不是一个随手改的 dict。每次写入都是
   `apply(Patch(op, path, by, value))`，由 `OWNER_TABLE` 裁决这个 `by`
   能不能碰这个 `path`。补丁日志同时是轨迹、回放源、预写日志。

2. **ExecCtx 是大动脉**。principal、预算、截止时间、trace id、幂等键、审批、
   深度、取消信号都在一个对象里流转。每个面拿到同样的事实，不用摸全局。

3. **有界循环**。六个终止条件（完成、预算、超时、死锁、不可恢复、等待确认）
   与三层死锁检测（迭代计数、语义指纹、仲裁）。停不下来的循环不是运行时，
   是事故。

4. **按生命周期三态分离**。Context 活一个回合，Session 活一次对话，
   Memory 活一张保留矩阵，Knowledge 活版本。混在一起是 Agent 系统里最常见的
   架构错误。

5. **处处 SPI，处处零依赖**。核心运行时不 import 任何第三方。每个外部系统都在
   一个协议后面，默认实现用标准库，成熟实现可选。

6. **单请求放大是威胁模型**。节点数、预算、分页、指标基数、workflow 容量 ——
   全部有界，因为一个请求不该能吃掉无限资源。

7. **可观测性是一个面，不是一行日志**。指标、追踪、审计三件事各有各的失败模式。
   审计不记原始 goal。指标 label 做归一化，客户端炸不了基数。

详细设计文档在 [`docs/`](docs/)，索引见上方英文部分的表格。

## 快速开始

核心零第三方依赖：

> **尚未发布到 PyPI。** PyPI 上的 `agent-runtime` 属于一个无关的 AutoGen 占位包
> （v0.0.1，会拉进四个 `autogen-*` 包，且根本无法 import）。
> **不要执行 `pip install agent-runtime`。** 发布后本项目的分发名是 `turing-agent-runtime`。

从仓库安装：

```bash
pip install "git+https://github.com/XiaoBinGan/Turing_AgentRuntime.git"          # 仅核心
pip install "git+https://github.com/XiaoBinGan/Turing_AgentRuntime.git#egg=turing-agent-runtime[server]"
```

或者 clone 下来（同时得到示例、demo 与测试套件）：

```bash
git clone https://github.com/XiaoBinGan/Turing_AgentRuntime.git
cd Turing_AgentRuntime
python3 -m venv .venv && .venv/bin/pip install -r requirements-core.txt

.venv/bin/python examples/quickstart/01_minimal.py    # 40 行，不需要 API key
.venv/bin/python examples/quickstart/03_approval.py   # T2 工具 → 审批 → 三种结果
.venv/bin/python demo/run.py                          # 11 个端到端场景
```

**从 `01_minimal.py` 开始**：一个自定义工具、一个假 LLM、跑一次循环、打印轨迹。
不联网、不需要密钥。然后看 [QUICKSTART.md](docs/design/QUICKSTART.md) 的概念速查表。

`requirements-core.txt` 是空的，这是设计。核心运行时只靠标准库。
`requirements.txt` 装的是可选集成（FastAPI、tiktoken、prometheus_client、
jsonschema、redis、qdrant、pymilvus、OpenTelemetry、MCP SDK）。

完整验收：

```bash
./verify.sh                    # 8 道门禁：编译、测试、契约、文档事实、配置默认、开放项时效、场景、HTTP
VERIFY_CLUSTER=1 VERIFY_LANGFUSE=1 ./verify.sh    # 9 道，需要真服务
```

## 已验证与未验证

**已验证**：2349 个用例重复跑稳定；11 个端到端场景 80 条断言；100 线程并发
零异常；900 回合内存增长 3KB；并发下限流精确（40 并发 limit 10 恰好 10 通过）；
真模型 4-6 步收敛；真 Milvus 集群、真 Langfuse 投递、真 OTLP；优雅关闭顺序、
检查点崩溃恢复、workflow 持久化。

**运维面，全部用可复现脚本验证过**：

| 能力 | 证据 | 脚本 |
|---|---|---|
| 多副本 | 2 副本共用 Redis；配额跨副本精确（limit=4，前 4 次通过，第 5 次在**任一**副本都被拒） | `scripts/verify_multi_replica.sh` |
| 容量基线 | 单副本并发 1 时 145 RPS / p99 14ms；2 副本合计 192 RPS | `scripts/loadtest.py` |
| 备份恢复 | sqlite 用官方 backup API，file 目录级，坏备份被拒 | `scripts/verify_backup_restore.sh` |
| schema 迁移 | 版本化信封迁移，逐级应用，拒绝未知与未来版本 | `tests/test_store_migration.py` |
| SLO 与告警 | 6 个 SLI，目标值取自实测基线；9 条规则经 promtool 校验 | `docs/design/SLO.md`、`deploy/prometheus/` |
| 值班手册 | 每条告警：怎么确认、先做什么、**不要做什么** | `docs/design/RUNBOOK.md` |
| 故障演练 | KV 挂、store 只读、上游不可达、客户端中断：全部按预期姿态失败 | `scripts/chaos_drill.sh` |

**也已实测**：

| 能力 | 结果 | 脚本 |
|---|---|---|
| 长稳 **150 分钟** | 103.6 万请求、成功率 99.9998%、p99 101.8ms、线程恒定 10、句柄平坦、稳态内存 101→97MB（无泄漏）、峰值 268→487MB 是 GC 周期变长 | `scripts/soak_test.py` |
| 真实 LLM 延迟 | 两组上游：deepseek-flash p50 2672ms；本地 Strata（2×T4）p50 4266ms。4-6 步回合端到端 11-26 秒。另跑了完整回合验证「文本渲染的工具描述能被模型正确转成 tool_calls」 | `scripts/real_llm_capacity.py` |
| 多 worker | 本机 10 核：**4 worker 2.97×**（2,247 → 6,681 RPS，CPU 109% → 407%）；64 核池：**8 worker 9.0×，平台期 21,482 RPS（65.3× @ 56 worker）**；**p99 随 worker 数单调下降**（c=64：1w 504ms → 16w 42ms）。早先"没有提升"的结论是旧压测客户端造成的（它自身只有 ~110 RPS 上限） | `scripts/bench_multiworker.sh` |
| 会话跨副本 | 已支持：`AGENT_RUNTIME_STORE=redis` 下 `turn_index` 跨副本连续 | `_make_store()` |
| sqlite 写入争用 | 多进程写同一文件 3.07×、零失败；选 Redis 的理由是故障域不是吞吐 | `scripts/bench_sqlite.py` |

**一条实测结论（不是待办）**：网络 RTT 会封顶吞吐。同机 8 worker 到
6,911-8,068 RPS；换一台机器经 28ms 链路打过来，同样配置只有 3,260 RPS，且服务端
CPU 仅 271% —— 请求在路上，不在处理。容量规划要先问"客户端在哪"。

**仍然开放的**（都是"还没测到这里"，不是"测了不通过"）：

- **长稳 > 12 小时**：已跑满 150 分钟 / 103.6 万请求（无泄漏），12 小时长稳正在
  243 上跑；更久的缓慢漂移仍未测。 <!-- checked: 2026-11-28 -->
- **请求级 cache 字段**：四组上游的 `usage.prompt_tokens_details` 都是 null，
  单请求能省多少只能估；累计值有 `/metrics` 兜底（省 87.7%）。 <!-- checked: 2026-11-28 -->

细节（含一个我不得不撤回的错误结论）见 `docs/design/CAPACITY.md`。

## 参与贡献

见 [CONTRIBUTING.md](CONTRIBUTING.md)。短版本：先读 `contracts.py`，不带迁移说明
不要改它，业务词汇不进运行时，修 bug 要带上能抓住那个 bug 的测试。

## 许可证

[Apache-2.0](LICENSE)。含专利授权 —— 基础设施应该让人敢于采用。
