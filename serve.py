"""serve.py — HTTP 服务层：把「库」变成「进程」（W31）。

================================================================================
它在主线里的位置
================================================================================

    client ──HTTP──▶ serve.py ──▶ Gateway.handle / handle_stream ──▶ Loop ⇄ Context
                       │
                       ├─ POST /v1/turns          同步跑一轮 → TurnResult JSON
                       ├─ POST /v1/turns/stream   SSE：逐条 event/data
                       ├─ GET  /metrics           Prometheus 文本
                       ├─ GET  /healthz           存活探针（进程活着就 200）
                       ├─ GET  /readyz            就绪探针（依赖自检，坏一个就 503）
                       ├─ GET  /v1/info           运行时自述（版本/依赖/后端种类）
                       ├─ POST /v1/workflows      提交静态 DAG（异步 → 202 + workflow_id）
                       ├─ GET  /v1/workflows/{id}  查进度（层 / 节点 / 步骤 / 错误）
                       └─ GET  /v1/workflows      分页列表（limit / offset）

在此之前，Gateway / Metrics / ShutdownCoordinator 各自都是**库**：
`handle()` 只能被 Python 代码调、`Metrics.render()` 产出文本却没有端点、
`ShutdownCoordinator` 写好了但没人收到 SIGTERM。本模块只做一件事 —— 把它们
**串成一个可被外部访问、可被优雅收口的进程**，不复制其中任何一条业务判断。

================================================================================
三条硬纪律（下面每条设计取舍都归到这三条上）
================================================================================

1. **HTTP 层不重新鉴权、不重新护栏、不重新审计。**
   这些是 Gateway 的职责（`core/gateway.py` 的 ①~⑪）。服务层如果「顺手再判一次」，
   就会出现两套口径：一处放行、一处拒绝，而审计只知道其中一套。所以这里**只翻译**：
   请求体 → Gateway 的 request dict，TurnResult → HTTP 状态码 + JSON。唯一新增的
   判断是「进程是否正在关停」（那是进程生命周期，Gateway 不知道），以及请求体形状
   （pydantic 做的字段级校验，比 Gateway 的类型检查更早、更具体）。

2. **审计与日志里不出现 goal 原文。**
   Gateway 已经只记 `goal_len`（见 `_audit`）。服务层**绝不能**把请求体整条塞进
   日志"方便排查" —— 那一行日志会把 Gateway 刻意不记的东西重新写下来。所以本模块
   的日志字段只有：路径、状态码、tenant、错误码、耗时。

3. **观测面 fail-open，但探针 fail-closed。**
   `/metrics` 渲染失败 → 500 + 原因（不是空 body：空 exposition 会让抓取端以为
   "指标掉了"，而真实情况是"渲染坏了"）；日志 sink 抛异常 → 吞掉。
   反过来 `/readyz`：依赖探不通就是 503，**不许假装 ready** —— 一个"永远 ready"的
   探针等于没有探针，负载均衡会把流量打到一个连不上 store 的进程上。

================================================================================
状态码映射为什么这么定
================================================================================

    ok=True                                    → 200
    bad_request（形状/参数错）                  → 400
    unauthorized（principal 缺字段/坏形状）      → 401
    forbidden（RBAC / scope 不足）              → 403
    input_blocked（输入护栏拒绝）                → 422
    rate_limited（租户配额）                     → 429（带 Retry-After）
    shutting_down（进程正在排空）                → 503
    resume_failed（ctx_id 载入失败）             → 500
    internal_error / loop_error / 未知码         → 500

- `input_blocked` 用 **422** 而不是 400：请求在**语法**上完全合法（goal 是字符串、
  长度合法），被拒是因为**语义**不可接受。用 400 会让客户端以为「改一下格式再试」，
  而正确动作是「换个目标」。同理 JSON Schema 校验失败（pydantic）也是 422 —— 两者
  共享"内容不可处理"这一语义，客户端只需看 body 里的 error 区分。
- `rate_limited` 用 **429** 并带 `Retry-After`：429 是**唯一**告诉客户端"现在别来、
  过 N 秒再来"的状态码。用 403 会被客户端当成永久拒绝而放弃重试；用 503 会让
  负载均衡把整个实例摘掉（配额超限是租户问题，不是实例问题）。
- `shutting_down` 用 **503**：这是"暂时不可用、可重试"，且会触发 LB 摘流。
  **不能**用 500（那是"我坏了"）也不能用 503 之外的 4xx（那会教客户端不要重试）。
- `resume_failed` / 内部异常用 **500**：请求本身没问题，是服务端的状态/代码问题。
  未知错误码**默认 500**（fail-closed 的状态码版本：不认识 = 不是客户端能修的）。
- `ok=True` 但 `phase=WAITING_USER` 仍然是 **200**：挂起是一个**成功的**回合结果
  （检查点已落盘，客户端该拿 ctx_id 去续跑），不是错误。

================================================================================
鉴权层（auth.py）：它怎么和上面这张表共存
================================================================================

`create_app(..., authenticator=...)` 会在最外层装 `auth.install_auth` 的中间件。
它只判「你是谁」，**不判** scope/RBAC（那是 Gateway 的事，见硬纪律 1）：

    缺凭据（没 Authorization / 没 X-API-Key）  → 401 + WWW-Authenticate
    凭据不认（key 不在表里 / JWT 过期 / 算法不对）→ 403
    服务端没配任何凭据方式                       → 500（部署错误，不是客户端的错）

为什么 401/403 这样分：401 = 「你没给」，客户端该去取凭据再**原样重试**；403 =
「你给了但不被接受」，重试同一个请求没有意义。混成一个码，客户端只能统一
"登录一次再重试"，在凭据确实无效时就变成登录风暴。

请求体里的 `principal` 在启用鉴权后**降级为一致性声明**：与凭据身份冲突
（user_id / tenant 不同，或声明了凭据没有的 roles/scopes）→ 403
`principal_conflict`。执行时一律以**凭据身份**为准，绝不在两者之间"挑一个"。

默认豁免只有 `/healthz`；`/readyz`（依赖拓扑）与 `/metrics`（业务量）默认要鉴权。

================================================================================
启动期 fail-closed 与 HTTP 层加固（本轮新增）
================================================================================

**默认拒绝**：`main()` 拿不到任何可用凭据（API key 表 / JWT secret）时**拒绝启动**
（`StartupRefused`），不再"打一行 WARN 然后裸奔"。理由很直接：不装鉴权时 principal
由**请求体**给出 —— 任何客户端 `POST /v1/turns` 带上
`{"principal": {"tenant": "acme", "scopes": ["runtime:run"], "roles": ["admin"]}}`
就能自称任何身份、任何租户、任何权限。这样的进程起在公网上，它的每一次成功响应
都是一次越权，而"启动失败"发生在任何人连上之前 —— 最便宜的失败点是启动。

三条出口（`resolve_auth_mode` 的判定顺序见它的 docstring）：

    AGENT_RUNTIME_API_KEYS / AGENT_RUNTIME_JWT_SECRET         → enforced（强制鉴权）
    AGENT_RUNTIME_ALLOW_ANONYMOUS=1 或 AGENT_RUNTIME_AUTH=off → disabled（显式放行）
    什么都没配                                                → StartupRefused

`create_app(authenticator=None)` 的**旧行为保留**（`auth_required` 默认 False）：
既有 55 个 serve 用例注入的假件靠它。只有生产入口 `main()` 传 `auth_required=True`。
`/v1/info` 的 `auth.mode` 让运维一眼看出当前是 `enforced` 还是 `disabled`。

三道资源边界（都能用环境变量关掉；关掉是 0，不是"配错了"）：

    AGENT_RUNTIME_MAX_BODY_BYTES     请求体上限，默认 1 MiB；超限 → 413
                                     **按 Content-Length 先拒**，不把超限的 body 读进内存
    AGENT_RUNTIME_HTTP_RATE          每 IP 每分钟请求数，默认 0（关闭）；超限 → 429
                                     只作用于 /v1/*：把 /healthz、/metrics 也限流，
                                     等于让"监控探不到"变成"监控自己拖死实例"
    AGENT_RUNTIME_REQUEST_TIMEOUT_S  请求超时，默认 0（关闭）；超时 → 504

**超时的真实边界（不要自己骗自己）**：`Gateway.handle` 跑在 `asyncio.to_thread` 的
线程里。超时只能"停止等待"——取消协程、回 504，**不能杀线程**。CPython 没有安全的
强制终止线程手段（`PyThreadState_SetAsyncExc` 只在字节码边界插异常，线程可能停在
持锁 / 半写文件的状态）。所以：

    - 504 之后那一回合**仍在后台跑完**（继续烧 token、调工具、写 Context）。
      日志里有一条 `http.timeout_detached` 如实记下这件事，不假装它没发生；
    - 长期超时会占满线程池 worker（线程泄漏的另一种形态），进程"看起来能收请求、
      实际什么都不动"。这是超时必须配得很宽（≥ 最慢回合的 p99）的原因；
    - 要真正中止执行，正确的位置是 Gateway 的预算/deadline（`ExecCtx.deadline`），
      不是 HTTP 层的定时器。本模块只负责"别让客户端无限等"。

装配顺序（从外到内）：限流 → 超时 → 体量 → 鉴权 → 路由。限流在最外：它要挡住
**连凭据都没通过**的洪水，否则一次坏凭据的爆破会绕过所有配额直接打到鉴权器上。

================================================================================
怎么跑
================================================================================

    # 起服务（默认 127.0.0.1:8000，默认 store/kv/vector 全是进程内实现）
    .venv/bin/python serve.py

    # 换成持久化 + 分布式 KV（探针会如实报告它们是否真的连上）
    AGENT_RUNTIME_STORE=sqlite AGENT_RUNTIME_KV=redis .venv/bin/python serve.py

    # 同步回合
    curl -s -X POST localhost:8000/v1/turns -H 'content-type: application/json' \\
         -d '{"goal":"排查 api 超时","principal":{"user_id":"u1","tenant":"acme",\\
              "scopes":["runtime:run"]}}'

    # 启用鉴权后（AGENT_RUNTIME_API_KEYS / AGENT_RUNTIME_JWT_SECRET），凭据走请求头：
    #   -H 'X-API-Key: <key>'  或  -H 'Authorization: Bearer <jwt>'
    # 请求体里的 principal 只做一致性声明，与凭据身份冲突就 403。

    # 流式回合（-N 关掉 curl 缓冲，否则看不到逐条）
    curl -sN -X POST localhost:8000/v1/turns/stream -H 'content-type: application/json' \\
         -d '{"goal":"x","principal":{"user_id":"u1","tenant":"acme","scopes":["runtime:run"]}}'

    # 探针 / 指标
    curl -s localhost:8000/healthz ; curl -s localhost:8000/readyz
    curl -s localhost:8000/metrics | head

    # 优雅关闭：drain → flush → 退出（日志里能看到 server.draining / metrics.flushed）
    kill -TERM $(pgrep -f 'python serve.py')

================================================================================
依赖与边界
================================================================================
- 只读 `contracts.py` / `core/*` / `plane/*` / `builder.py` / `adapters/*`：
  一条都不改。实现与装配全靠公开接口 + `getattr` 容错。
- Gateway 是**同步**的（LLM/工具调用是阻塞 IO），所以所有调用都过
  `asyncio.to_thread`：直接 `await` 会把整个事件循环钉住，一个慢回合能让
  `/healthz` 也超时 —— 探针跟着业务一起死是最坏的故障形态。
- 依赖注入全部可选：`create_app(gateway=..., metrics=..., logger=..., shutdown=...,
  deps=...)` 每个都能换假件，测试不需要真端口、真 Redis、真 LLM。
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import fnmatch
import functools
import importlib.util
import json
import math
import os
import platform
import re
import secrets
import signal
import sys
import threading
import time
import types
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Mapping, Sequence

from fastapi import Depends, FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from auth import (
    DEFAULT_EXEMPT_PATHS,
    Authenticator,
    current_principal,
    install_auth,
    parse_api_keys_spec,
    parse_bool,
)
from contracts import (
    DAGNode,
    DAGSpec,
    ExecCtx,
    Principal,
    SubagentResult,
    SubagentSpec,
)
from core.gateway import MAX_GOAL_CHARS, Gateway
from core.observability import (
    SHUTTING_DOWN,
    JsonLogger,
    Metrics,
    ShutdownCoordinator,
    instrument_gateway,
)
# Workflow Plane 的静态 DAG 只有拓扑与调度，没有 HTTP 入口 —— 那一层在这里。
# 只 import 纯拓扑/调度件（`plane/dag.py` 只依赖 contracts.py），不碰 `plane/workflow.py`：
# 后者要 dispatcher/builder/llm_factory 三件套，那是 build_gateway_from_env 的内部装配。
from plane.dag import DAG, DAGRunner, DAGValidationError, SKIP_TERM

__all__ = [
    "create_app",
    "main",
    "build_gateway_from_env",
    "build_authenticator_from_env",
    "StartupRefused",
    "STARTUP_REFUSED_MESSAGE",
    "require_auth_config",
    "resolve_auth_mode",
    "install_hardening",
    "BodyLimitMiddleware",
    "RateLimitMiddleware",
    "TimeoutMiddleware",
    "DEFAULT_MAX_BODY_BYTES",
    "DEFAULT_RATE_PER_MINUTE",
    "DEFAULT_REQUEST_TIMEOUT_S",
    "SignalDriver",
    "DepSpec",
    "TurnRequest",
    "PrincipalBody",
    "WorkflowRequest",
    "DAGNodeBody",
    "WorkflowNodeState",
    "WorkflowState",
    "WorkflowRegistry",
    "WorkflowSubmitError",
    "validate_workflow_spec",
    "status_for_result",
    "error_code",
    "result_payload",
    "sse_frame",
    "STATUS_BY_CODE",
    "VERSION",
    "PROMETHEUS_CONTENT_TYPE",
]

#: 运行时自述里的版本号。不读 git、不 import 打包元数据：一个探针不该为此碰文件系统。
VERSION = "0.8.0"

#: 服务名（进日志的 `service` 字段与 /v1/info）
SERVICE_NAME = "agent-runtime"

#: Prometheus 抓取端认的 Content-Type。`version=` 参数不是装饰：抓取端按它选解析器。
PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4"

#: 依赖探针的超时（秒）。**必须有上限**：一个挂住的 Redis 会让 /readyz 一起挂住，
#: 于是编排器只能等它的 kubelet 超时 —— 而那时我们连"是哪个依赖坏了"都报不出来。
READY_TIMEOUT_S = 2.0

#: 存活探针的语义是"进程还在"，不需要任何依赖信息
HEALTH_PATH = "/healthz"

#: SSE 响应头：禁缓存 + 不缓冲（否则代理会把事件攒成一坨再吐）
SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


# ══════════════════════════════════════════════════════════════
# 一、状态码映射（TurnResult.error 的第一段 → HTTP 状态）
# ══════════════════════════════════════════════════════════════
#: 错误码 → 状态码。键是 `error` 冒号前的**第一段**（`bad_request: goal 必填` → `bad_request`）。
#: 映射理由见模块 docstring「状态码映射为什么这么定」。
STATUS_BY_CODE: dict[str, int] = {
    "": 200,
    "bad_request": 400,
    "unauthorized": 401,
    "forbidden": 403,
    "input_blocked": 422,
    "rate_limited": 429,
    "shutting_down": 503,
    "resume_failed": 500,
    # **409 而不是 500**：ctx_id 指向一个挂起等确认的任务，但请求没给 decision。
    # 这是"资源当前状态与请求冲突"——客户端换带 decision 的请求就能成功，
    # 所以它既不是服务端故障（500），也不是永久拒绝（403）。
    # 早先 `decision` 在 Gateway 里硬编码 True，这个码根本不会出现；
    # 现在"没表态"被显式拒绝，就必须有一个能表达"补上 decision 再来"的状态码。
    "resume_needs_decision": 409,
    "internal_error": 500,
    "loop_error": 500,
}

#: 不认识的错误码 → 500。**不降级成 4xx**：那会教客户端"这是你的问题"，
#: 而一个陌生错误码恰恰说明服务端出现了新故障（例如某个依赖返回了新的失败）。
DEFAULT_ERROR_STATUS = 500


def error_code(error: Any) -> str:
    """`error` 串 → 错误码（冒号前第一段）。

    与 `core/observability._code_only` 同一个口径：详细 reason 可能夹带用户输入片段，
    而错误码是要进指标 label / 日志字段的（长期保存），不能带自由文本。
    """
    text = str(error or "").strip()
    if not text:
        return ""
    return text.split(":", 1)[0].strip()


def status_for_result(result: Any) -> int:
    """TurnResult → HTTP 状态码。`ok=True` 永远 200，其余按错误码查表。

    空错误码 + `ok=False` 是**异常**（Gateway 的出口永远带 error），必须落到 500：
    若让它落进 `STATUS_BY_CODE[""] = 200`，一个失败回合会以 200 返回，
    客户端的重试/告警全部失效 —— 静默失败比 500 危险得多。
    """
    # **依赖故障必须对监控可见**：`ok=True` 只表示"请求被正确处理了"，
    # 但 Loop 因 LLM/工具抛异常而 `UNRECOVERABLE` 时，任务其实没完成。
    # 早期这里一律 200 → 模型服务挂了，HTTP 层全是 200，监控与告警完全看不到
    # （"静默失败比 500 危险"）。落 502：网关/上游不可用，客户端应重试。
    if str(getattr(result, "term_reason", "") or "") == "UNRECOVERABLE":
        return 502
    if bool(getattr(result, "ok", False)):
        return 200
    code = error_code(getattr(result, "error", ""))
    if not code:
        return DEFAULT_ERROR_STATUS
    return STATUS_BY_CODE.get(code, DEFAULT_ERROR_STATUS)


def _retry_after(result: Any) -> int | None:
    """从 `vertex.quota.retry_after_s` 取建议重试秒数（只对 429 有意义）。"""
    vertex = getattr(result, "vertex", None)
    if not isinstance(vertex, Mapping):
        return None
    quota = vertex.get("quota")
    if not isinstance(quota, Mapping):
        return None
    raw = quota.get("retry_after_s")
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        return None
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        return None
    return max(1, int(math.ceil(seconds)))


#: TurnResult 的字段顺序（与 `core/gateway.py` 的 `to_dict` 对齐）。
#: 服务层**不新增字段**：客户端只该学一套 schema，多一套就是将来的一次迁移。
_RESULT_FIELDS = ("ok", "text", "phase", "term_reason", "task_id", "steps",
                  "partial", "error", "audit_id", "latency_ms", "vertex")


def result_payload(result: Any) -> dict:
    """TurnResult → 可 JSON 化的 dict。优先用对象自己的 `to_dict()`。

    为什么要兜底：`gateway` 是可注入的，注入的对象不一定是 `TurnResult`
    （测试里的假件、将来换成别的实现）。出口不能因为"少一个方法"就 500。
    """
    dumper = getattr(result, "to_dict", None)
    if callable(dumper):
        try:
            data = dumper()
            if isinstance(data, Mapping):
                return dict(data)
        except Exception:               # noqa: BLE001 - 坏对象也要能出去
            pass
    return {name: getattr(result, name, None) for name in _RESULT_FIELDS}


def _shutting_down_payload() -> dict:
    """关停期间拒绝请求时的 body。**形状与 TurnResult 一致** —— 客户端只写一套解析。"""
    return {"ok": False, "text": "", "phase": "", "term_reason": "", "task_id": "",
            "steps": 0, "partial": False, "error": SHUTTING_DOWN, "audit_id": "",
            "latency_ms": 0.0, "vertex": {}}


# ══════════════════════════════════════════════════════════════
# 二、请求体（pydantic）：形状校验在这里，语义判定在 Gateway
# ══════════════════════════════════════════════════════════════
class PrincipalBody(BaseModel):
    """`principal` 的线上形状。

    启用鉴权时 `user_id` / `tenant` **可省略**：身份由凭据给出，请求体只描述
    "要做什么"，不描述"我是谁"。不填就是"未声明"，由凭据身份覆盖；
    填了但不一致 → 403 `principal_conflict`（防"带自己的 token 却声称是别人"）。

    未启用鉴权时它们**必须填**：Gateway 靠 tenant 分区（配额窗口、跨租户续跑判定），
    缺了会被以 `unauthorized` 拒绝。这个判定在端点里按当前 auth 模式做，
    而不是在 pydantic 层 —— 同一个请求体，两种模式下必填性不同。

    `roles`/`scopes` 默认空列表：空 scope 会被 Gateway 判 `forbidden`（fail-closed），
    所以"默认值"不等于"默认放行"。这里刻意**不**给任何默认权限。
    """

    model_config = ConfigDict(extra="ignore")

    # `user_id` / `tenant` 在**启用鉴权时可以不填**：身份已由凭据给出
    # （`auth.install_auth` 把 Principal 注入 request.state）。
    # 早期两者都是必填，于是"配了 API Key 还得在 body 里再抄一遍自己是谁" ——
    # 不但多此一举，还容易抄错（抄错会被判 principal_conflict 403）。
    # 未启用鉴权时它们仍然必须给（否则 Gateway 无法分区）。
    # 这个"必填性"由 `require_identity` 在端点里按 auth 模式判定，见 `_principal_body_required`。
    user_id: str = Field("", description="调用者标识（启用鉴权时可省略，取凭据身份）")
    tenant: str = Field("", description="租户：配额与隔离的分区键（启用鉴权时可省略）")
    roles: list[str] = Field(default_factory=list, description="角色（RBAC 输入）")
    scopes: list[str] = Field(default_factory=list,
                              description="权限串，进运行时至少需要 runtime:run")

    def to_gateway(self) -> dict:
        """转成 Gateway `_resolve_principal` 吃的 dict（只给契约字段）。"""
        return {"user_id": self.user_id, "tenant": self.tenant,
                "roles": list(self.roles), "scopes": list(self.scopes)}


#: 单请求可申请的预算上限。**必须有** —— 否则一个请求可以拿 10^12 token 的额度，
#: 一次就把自己的配额、成本闸、步数上限全部架空。
#:
#: 依据：真实排查任务实测 4 万 token / 6 步；200 万 token / 40 步给到 50 倍余量。
#: 要更大额度应该是**租户配置**，不是请求参数 —— 请求来自调用方，配置来自运维。
MAX_TOKEN_TOTAL = 2_000_000
MAX_COST_USD = 50.0
MAX_TOOL_CALLS = 200
MAX_STEPS = 40


class BudgetBody(BaseModel):
    """预算请求体。**每一项都卡上限**（见 `MAX_*` 常量的注释）。"""

    model_config = ConfigDict(extra="forbid")

    token_total: int | None = Field(default=None, ge=1, le=MAX_TOKEN_TOTAL)
    cost_total_usd: float | None = Field(default=None, ge=0, le=MAX_COST_USD)
    tool_calls_total: int | None = Field(default=None, ge=1, le=MAX_TOOL_CALLS)
    max_steps: int | None = Field(default=None, ge=1, le=MAX_STEPS)


class TurnRequest(BaseModel):
    """`POST /v1/turns`、`POST /v1/turns/stream` 的请求体。

    `extra="forbid"`：拼错字段（`principle` / `deadline`）必须 422 而不是被静默忽略。
    "客户端以为设了 deadline、实际没设"是一个会烧真实预算的静默故障。
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    goal: str = Field(..., min_length=1, max_length=MAX_GOAL_CHARS,
                      description="本轮目标（会进 Context 种子）")
    # 可选：启用鉴权时身份来自凭据，请求体不必重复"我是谁"。
    # 未启用鉴权时由 `_missing_identity` 在端点里判必填（模式相关，不能在模型层判）。
    principal: PrincipalBody = Field(default_factory=PrincipalBody)
    task_id: str | None = Field(default=None, description="客户端指定的任务 id")
    ctx_id: str | None = Field(default=None, description="续跑已落盘的 Context")
    deadline_ms: float | None = Field(
        default=None, gt=0,
        description="相对毫秒截止；Gateway 换算成绝对 epoch ms 后写进 ExecCtx.deadline")
    budget: BudgetBody | None = Field(
        default=None,
        description="token_total / cost_total_usd / tool_calls_total / max_steps，"
                    "每项都有上限（**单请求不得放大自身额度**）")
    session_id: str | None = Field(
        default=None, max_length=128,
        description="会话 id。带上 = 承接该会话前几轮的问答骨架（需服务端开 "
                    "AGENT_RUNTIME_SESSION=1）；不带 = 新会话，结果里会返回一个")
    decision: bool | None = Field(
        default=None,
        description="**仅当 ctx_id 指向一个挂起等待确认的任务时有意义**。"
                    "true = 批准、false = 拒绝。不给则拒绝续跑并返回 "
                    "resume_needs_decision（没表态不等于同意）。"
                    "普通回合传了会被忽略")

    def to_gateway_request(self) -> dict:
        """→ Gateway 的 request dict。

        **只透传，不加工**：`deadline_ms` 原样交给 Gateway 换算（基准点必须是
        "收到请求的那一刻"，服务层如果自己算一遍，排队时间就会被算进宽限里）。
        值为 None 的可选字段直接不带 —— Gateway 对"缺字段"和"给 None"是同一语义，
        但少一层 None 少一次将来实现分叉的机会。
        """
        request: dict[str, Any] = {"goal": self.goal,
                                   "principal": self.principal.to_gateway()}
        # session_id 必须原样透传：SessionGateway 靠它找历史。
        # 早期漏了它 —— pydantic 的 extra="forbid" 会把请求里的 session_id 直接拒掉（422），
        # 表面上"多轮对话不工作"，实际是字段没进模型。
        for name in ("task_id", "ctx_id", "deadline_ms", "session_id", "decision"):
            value = getattr(self, name)
            if value is not None:
                request[name] = value
        # `budget` 要转回**纯 dict**：Gateway 的 `_check_shape` 吃 dict，
        # 传 pydantic 模型过去会让它"看起来有预算、实际一项都没生效"
        # （`Budget(**shape["budget"])` 对模型对象做 `**` 解包会失败/被忽略）。
        # 只带用户显式给了的键 —— `None` 表示"用默认值"，不能塞进去。
        if self.budget is not None:
            budget = {k: v for k, v in self.budget.model_dump().items()
                      if v is not None}
            if budget:
                request["budget"] = budget
        return request


# ══════════════════════════════════════════════════════════════
# 二之二、鉴权接线的小工具（中间件注入身份 → 端点使用身份）
# ══════════════════════════════════════════════════════════════
def _header_principal(request: Request) -> Principal | None:
    """取 `auth.AuthMiddleware` 注入的身份；**未启用鉴权时恒为 None**。"""
    state = getattr(request, "state", None)
    if state is None:
        return None
    principal = getattr(state, "principal", None)
    return principal if isinstance(principal, Principal) else None


def _principal_dict(principal: Principal) -> dict:
    """Principal → Gateway `_resolve_principal` 吃的 dict（只给契约字段）。"""
    return {"user_id": principal.user_id, "tenant": principal.tenant,
            "roles": list(principal.roles), "scopes": sorted(principal.scopes)}


def _missing_identity(body: Any, authed: Any) -> list[dict]:
    """未启用鉴权时，请求体必须给出 user_id / tenant（它们是 Gateway 的分区键）。

    启用鉴权时两者都可省：身份已由凭据给出。这条判定放在端点里而不是
    pydantic 模型里，因为**同一个请求体在两种模式下必填性不同**。
    """
    if authed is not None:
        return []
    principal = getattr(body, "principal", None)
    missing: list[dict] = []
    for name in ("user_id", "tenant"):
        if not str(getattr(principal, name, "") or "").strip():
            missing.append({"field": f"principal.{name}",
                            "message": ("Field required（未启用鉴权时必须给；"
                                        "启用鉴权后由凭据提供）"),
                            "type": "missing"})
    return missing


def _validation_payload(path: str, fields: list[dict]) -> dict:
    return {"error": "validation_error", "detail": "缺少必填字段", "fields": fields,
            "status": 422, "path": path}


def _principal_conflict(body: PrincipalBody, authed: Principal) -> str:
    """请求体 principal 与凭据身份是否冲突 → 冲突原因（空串 = 一致）。

    规则（方向只有一个：**body 不能比凭据更宽**）：

    1. `user_id` / `tenant` **留空 = 未声明**（执行时用凭据身份）；写了就必须与
       凭据一致 —— 写了别人的身份就是冒名，这是这一步要挡的东西。
       为什么允许留空：API key 的默认 user_id 是**不可逆别名**（`svc_xxxxxxxx`），
       运维如果不写 `#user_id` 就无从得知；强求回填只会逼客户端去猜。
    2. body 声明的 `roles` / `scopes` 必须是凭据授予集合的**子集**：允许少声明
       （客户端可能只想声明这次需要的权限），但**不许**声明凭据没有的
       （那就是"自己给自己升权限"）。
    3. 一致时 body 的 roles/scopes **被忽略**：执行一律用凭据身份 —— 否则
       "声明得越小执行得越少"会让同一个 token 的行为取决于客户端心情。
    """
    claimed_user = str(body.user_id or "").strip()
    claimed_tenant = str(body.tenant or "").strip()
    if claimed_user and claimed_user != str(authed.user_id or "").strip():
        return "请求体 principal.user_id 与凭据身份不一致"
    if claimed_tenant and claimed_tenant != str(authed.tenant or "").strip():
        return "请求体 principal.tenant 与凭据身份不一致"

    granted_scopes = {str(item).strip() for item in (authed.scopes or ())}
    extra_scopes = sorted({str(item).strip() for item in (body.scopes or ())}
                          - granted_scopes)
    if extra_scopes:
        return f"请求体 principal.scopes 超出凭据授予的范围：{extra_scopes}"

    granted_roles = {str(item).strip() for item in (authed.roles or ())}
    extra_roles = sorted({str(item).strip() for item in (body.roles or ())}
                         - granted_roles)
    if extra_roles:
        return f"请求体 principal.roles 超出凭据授予的角色：{extra_roles}"
    return ""


def _conflict_response(path: str, detail: str) -> JSONResponse:
    """身份冲突的出口：403（与 Gateway 的 `forbidden` 同一状态码，语义同为"不许"）。"""
    return JSONResponse(
        content={"error": "forbidden",
                 "detail": f"principal_conflict: {detail}",
                 "status": 403, "path": path},
        status_code=403)


# ══════════════════════════════════════════════════════════════
# 二之三、Workflow 显式入口（B）：静态 DAG 的 HTTP 面
# ══════════════════════════════════════════════════════════════
# 为什么这一层存在（`docs/DESIGN-C.md` §5）：
#
#   C（回合内编排）由**模型**决定流程，适合开放式排查；
#   B（显式 workflow）由**人**提交 DAGSpec 决定流程，适合发布/回滚这类确定性 SOP。
#   后者的价值全在「事前声明、事后比对、可审计、可续跑」——所以它必须有一个
#   显式入口，而不是"在 prompt 里求模型按顺序做"。
#
# `plane/dag.py` 已经有 `DAG` / `DAGRunner`（拓扑校验 + 按层调度），但**没有 HTTP 面**，
# 只能在 demo 里跑。本段只做三件服务层的事，不复制任何一条编排判断：
#
#   ① 登记与有界淘汰（WorkflowRegistry）  ② 后台执行（线程池里跑 DAGRunner）
#   ③ 状态快照（层 / 节点 / 步骤 / 错误，给 GET 用）

#: 内存里最多跟踪多少个 workflow。**必须有界**：不设上限时，一个每分钟提交一次的
#: 脚本跑一周就是几十万条状态对象常驻内存 —— 那就是内存泄漏，只是没人给它起名字。
DEFAULT_WORKFLOW_MAX = 200

#: 后台跑 DAG 的线程数。与 uvicorn/HTTP 的线程池**分开**：一条跑几分钟的流水线
#: 不该占掉跑 `/v1/turns` 的 worker（否则交互式请求会排在批处理后面）。
DEFAULT_WORKFLOW_WORKERS = 2

#: workflow 级状态（与节点级状态是两套词汇，刻意不合并）
WF_PENDING = "pending"
WF_RUNNING = "running"
WF_DONE = "done"
WF_FAILED = "failed"
#: 进程重启导致的中断。**不同于 failed**：它不是"跑错了"，是"没跑完"，
#: 需要人决定是重跑还是接着做 —— 所以不能混进 `WF_FINISHED`（否则会被当成终态淘汰）。
WF_INTERRUPTED = "interrupted"
WF_FINISHED = frozenset({WF_DONE, WF_FAILED})

#: 节点级状态。`skipped` 是 `DAGRunner` 的短路语义（上游没跑赢 → 不派发），
#: 与 `failed`（真跑了但失败）必须分开：前者没花钱，后者花了钱。
NODE_PENDING = "pending"
NODE_RUNNING = "running"
NODE_DONE = "done"
NODE_FAILED = "failed"
NODE_SKIPPED = "skipped"


@dataclass
class WorkflowNodeState:
    """一个 DAG 节点的可观测事实。

    字段就是 `docs/DESIGN-C.md` §4 说的 `workflow_state`：`{dag_id, layer, node,
    status, started_at, ended_at, skip_reason}` 的 HTTP 版 —— 时间用 epoch 秒
    （float），未开始/未结束保持 0.0（不编一个假时间出来）。
    """

    name: str
    status: str = NODE_PENDING
    steps: int = 0
    error: str = ""
    started_at: float = 0.0
    ended_at: float = 0.0

    def to_dict(self) -> dict:
        return {"name": self.name, "status": self.status, "steps": int(self.steps),
                "error": self.error,
                "started_at": round(float(self.started_at), 6),
                "ended_at": round(float(self.ended_at), 6)}


@dataclass
class WorkflowState:
    """一条 workflow 的全部状态（内存里唯一的真源，`GET` 直接读它）。

    `nodes` 用 dict 且**按层序插入**（提交时一次建齐）：这样 `to_dict()` 出来的
    节点顺序 = 层的拓扑序，不受线程完成先后影响 —— 同一个 DAG 两次查询得到
    逐字节相同的 JSON（顺序即语义，`plane/dag.py` 的确定性基准同一条）。
    """

    id: str
    spec_name: str
    layers: list[list[str]] = field(default_factory=list)
    nodes: dict[str, WorkflowNodeState] = field(default_factory=dict)
    status: str = WF_PENDING
    created_at: float = field(default_factory=time.time)
    finished_at: float = 0.0
    error: str = ""
    #: 提交序。同毫秒创建时用它定序，保证列表顺序**完全确定**（不靠 dict 巧合）。
    seq: int = 0
    #: 提交者的身份（只存 user_id/tenant/roles/scopes —— 审计要能回答"谁提交的"）
    principal: dict = field(default_factory=dict)
    max_parallel: int = 1
    #: 节点级 goal 覆盖（续跑要用；提交时给过就记住）。
    goals: dict = field(default_factory=dict)
    #: 已续跑过几次（0 = 从未续跑）。运维据此判断"这条流水线是不是反复断"。
    resumed_count: int = 0

    @property
    def is_finished(self) -> bool:
        return self.status in WF_FINISHED

    def node_order(self) -> list[str]:
        """拓扑序的节点名（= `flatten(layers)`）。"""
        return [name for layer in self.layers for name in layer]

    def to_dict(self) -> dict:
        nodes = [node.to_dict() for node in self.nodes.values()]
        counts = {key: 0 for key in
                  (NODE_PENDING, NODE_RUNNING, NODE_DONE, NODE_FAILED, NODE_SKIPPED)}
        for node in self.nodes.values():
            counts[node.status] = counts.get(node.status, 0) + 1
        finished = counts[NODE_DONE] + counts[NODE_FAILED] + counts[NODE_SKIPPED]
        return {
            "id": self.id,
            "status": self.status,
            "spec_name": self.spec_name,
            "layers": [list(layer) for layer in self.layers],
            "nodes": nodes,
            "progress": {**counts, "total": len(nodes), "finished": finished},
            "created_at": round(float(self.created_at), 6),
            "finished_at": round(float(self.finished_at), 6),
            "error": self.error,
            "principal": dict(self.principal),
            "max_parallel": int(self.max_parallel),
            # 续跑次数：>0 说明这条流水线断过。运维据此判断"是不是反复断"。
            "resumed_count": int(self.resumed_count),
        }


# ── 请求体（pydantic）：形状校验在这里，语义（拓扑）校验在 validate_workflow_spec ──
#: 单条 workflow 的节点数上限。**必须有** —— 一个 1000 节点的 DAG 是
#: "单请求放大"（每节点跑一个回合，烧预算/线程），不是持续攻击。
DEFAULT_MAX_WORKFLOW_NODES = 64


class DAGNodeBody(BaseModel):
    """线上形状的 DAG 节点，与 `contracts.DAGNode` 一一对应。

    `extra="forbid"`：拼错字段（`depends` / `depend_on`）必须 422。否则会得到一个
    "看起来有依赖、实际全是并行"的 DAG —— 那是最难排查的一类错（它能跑，只是顺序错）。
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=128,
                      description="节点唯一名（DAG 里的身份，重复 → 400）")
    agent: str = Field("", max_length=128, description="该节点指派给谁（可空）")
    depends_on: list[str] = Field(default_factory=list,
                                 description="必须全部成功的前置节点名")
    goal: str = Field("", max_length=MAX_GOAL_CHARS,
                      description="节点目标（空 = `{workflow 名}:{节点名}`）")


class WorkflowRequest(BaseModel):
    """`POST /v1/workflows` 的请求体 = 一份 DAGSpec + 身份声明。

    刻意**不**套一层 `{"dag": {...}}`：`DAGSpec` 本身就是 `{name, nodes}`，
    再包一层只是让客户端多写一个键、多一处拼错的机会。
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=128,
                      description="workflow 名（进审计与所有节点默认 goal）")
    nodes: list[DAGNodeBody] = Field(
        default_factory=list,
        # **上限是必须的**：每个节点会跑一个回合（花 token、调工具、可能起线程）。
        # 没有上限时，一个 1000 节点的 DAG 会被接受（实测 202），
        # 然后排队烧光预算/线程 —— 而且它是**一次请求**造成的，
        # 不是持续攻击。这类"单请求放大"必须在入口拦住。
        # 64 的依据：DAG 典型是"检查→备份→灰度→验证"这种 3~10 步；
        # 64 已经覆盖到极复杂的流水线，再大基本是配错了。
        max_length=DEFAULT_MAX_WORKFLOW_NODES,
        description=f"节点表（书写顺序不影响拓扑，最多 {DEFAULT_MAX_WORKFLOW_NODES} 个）")
    # 与 TurnRequest 同一套身份规则：未启用鉴权时 user_id/tenant 必填
    # （由 `_missing_identity` 在端点里判），启用鉴权时它们是"一致性声明"。
    principal: PrincipalBody = Field(default_factory=PrincipalBody)
    goals: dict[str, str] = Field(
        default_factory=dict, description="按节点名覆盖 goal（DAGRunner 的 goals 参数）")
    max_parallel: int = Field(default=1, ge=1, le=32,
                              description="层内并行度（1 = 串行，名字序）")

    def to_spec(self) -> DAGSpec:
        """→ `contracts.DAGSpec`（节点顺序保持请求里的书写顺序）。"""
        return DAGSpec(name=self.name, nodes=tuple(
            DAGNode(name=node.name, agent=node.agent,
                    depends_on=tuple(node.depends_on), goal=node.goal)
            for node in self.nodes))


def validate_workflow_spec(spec: DAGSpec) -> list[str]:
    """DAGSpec 的静态校验：返回**全部**错误（空列表 = 可提交）。

    `plane/dag.py` 的 `DAG.validate()` 报四类：重名 / 依赖不存在 / 自依赖 / 成环
    （成环会报出**具体是哪几个节点**，不只说"有环"）。

    这里额外拦一条**空图**。`DAG.validate()` 认为空 DAG 是合法的（没有节点就没有
    错误），但提交一个什么都不做的 workflow 只会白占一个追踪位（有界容量），
    并在审计里留下一条"成功"的假记录 —— 那比 400 更糟。所以入口拒掉，理由写在这里。
    """
    errors: list[str] = []
    if not tuple(getattr(spec, "nodes", ()) or ()):
        errors.append("nodes 不能为空：空 DAG 没有任何可执行节点")
    try:
        errors.extend(DAG(spec).validate())
    except ValueError as exc:            # spec 形状坏（不是 DAGSpec）：也算校验失败
        errors.append(f"DAGSpec 形状不合法: {exc}")
    return errors


# ── 结果读取：`DAGRunner` 只做鸭子类型，所以这里也不许假定 SubagentResult ──
def _node_facts(raw: Any) -> tuple[bool, int, str, str]:
    """把 runner 的任意返回值读成 `(ok, steps, error, term_reason)`。

    `DAGRunner` 的 `_payload` 已经做了同样的事，但它只在**跑完后**的 report 里
    给出结果；状态屋（`WorkflowState`）要在 dispatch 返回的那一刻就更新，
    所以这里必须自己读一遍。两种读法保持一致：优先 `to_dict()`，否则 Mapping，
    再否则逐字段 getattr。
    """
    if isinstance(raw, Mapping):
        data: Mapping[str, Any] = raw
    else:
        dumper = getattr(raw, "to_dict", None)
        blob: Any = None
        if callable(dumper):
            try:
                blob = dumper()
            except Exception:               # noqa: BLE001 - 坏 to_dict 不该炸状态更新
                blob = None
        if isinstance(blob, Mapping):
            data = blob
        else:
            data = {"ok": getattr(raw, "ok", False),
                    "steps": getattr(raw, "steps", 0),
                    "error": getattr(raw, "error", ""),
                    "term_reason": getattr(raw, "term_reason", "")}
    try:
        steps = max(0, int(data.get("steps") or 0))
    except (TypeError, ValueError):
        steps = 0
    return (bool(data.get("ok")),
            steps,
            str(data.get("error") or ""),
            str(data.get("term_reason") or ""))


def _node_skipped(name: str, reason: str) -> Any:
    """节点未执行的统一返回（**要留痕**，不能假装成功）。"""
    from contracts import SubagentResult
    return SubagentResult(name=name, ok=False, summary="", steps=0,
                          term_reason="ABORTED", error=reason)


class GatewayNodeRunner:
    """**内置的最小 runner**：一个 DAG 节点 = 一次「用 goal 跑一个回合」。

    为什么要内置一个，而不是直接用 `plane/workflow.py` 的 `SubagentRunner`：

    `SubagentRunner(dispatcher, builder, llm_factory, ...)` 要的是运行时的**内部装配**
    （工具分发器、Prompt 构造器、每个子 Agent 的 LLM）。这三件都在
    `build_gateway_from_env` 里面，serve 层拿不到，也不该为了一个端点把它再拼一遍 ——
    两份装配早晚会漂（模块 docstring 硬纪律 1 的同一条理由）。
    所以这里走**唯一已经装配好的公开入口**：`gateway.handle({"goal": ...})` ——
    鉴权、配额、输入护栏、审计、观测全都在那条路径上，一处都不绕过。

    **局限（诚实写在这里，不当它不存在）**：

    1. **节点之间不共享子 Context**：每个节点是独立回合，节点 A 的取证结论不会
       自动出现在节点 B 的 Context 里（要靠 goal 传递或外部状态/记忆）。
    2. **无降权**：不满足 INV-18（子 Agent 的 principal 只剩 `*.read`、预算切半、
       `depth+1`）。这里节点的权限 = 提交者的权限，因为跑的本来就是提交者的回合。
    3. **无白名单回写**：`SubagentRunner._merge_back`（observations / artifacts）
       不参与 —— DAGRunner 本身也不写父 Context，两边口径一致。

    第 2、3 条属于 C 的能力（`ExecCtx.child()` + `_GuardedDispatcher`）。要在 B 上拿到，
    把 `SubagentRunner` 通过 `WorkflowRegistry(runner_factory=...)` 注进来即可；
    `dispatch` 的签名不变，本类不参与。

    失败语义：`gateway.handle` 契约上**永不抛异常**（失败用 `ok=False`/`error` 表达），
    所以这里把它的结果折成 `SubagentResult`；真抛了就是基础设施故障，交给
    `DAGRunner` 自己收敛成该节点 `ok=False`（它已有那条兜底，这里不重复实现）。
    """

    #: 单节点的预算上限（不给就是用 total/node_count 的均分）。
    #:
    #: **为什么必须有**：不传 budget 时每个节点都会用 Gateway 的默认额度
    #: （2 万 token / 20 次工具）。64 个节点 = 128 万 token 的放大 ——
    #: 而它来自**一次请求**。这是"单请求放大"的又一种形态，只不过
    #: 放大器是节点数而不是参数值。
    DEFAULT_NODE_TOKEN_TOTAL = 30_000
    DEFAULT_NODE_TOOL_CALLS = 12
    DEFAULT_NODE_MAX_STEPS = 8

    def __init__(self, gateway: Any, principal: Mapping[str, Any],
                 *, node_budget: Mapping[str, Any] | None = None) -> None:
        if not callable(getattr(gateway, "handle", None)):
            raise TypeError("GatewaNodeRunner 需要一个有 handle(request) 的 gateway")
        self._gateway = gateway
        self._principal = dict(principal or {})
        self._node_budget = dict(node_budget) if node_budget else {}

    def _budget_for_node(self, parent_e: Any) -> dict:
        """给单个节点的预算：注入口 > 父 ExecCtx 均分 > 常量上限。"""
        if self._node_budget:
            return dict(self._node_budget)
        budget = getattr(parent_e, "budget", None) if parent_e is not None else None
        total_tokens = int(getattr(budget, "token_total", 0) or 0)
        total_calls = int(getattr(budget, "tool_calls_total", 0) or 0)
        total_steps = int(getattr(budget, "max_steps", 0) or 0)
        # 父预算摊到节点上（父预算默认是 0/未设 → 用常量上限兜底）
        return {
            "token_total": min(total_tokens or self.DEFAULT_NODE_TOKEN_TOTAL,
                               self.DEFAULT_NODE_TOKEN_TOTAL),
            "tool_calls_total": min(total_calls or self.DEFAULT_NODE_TOOL_CALLS,
                                    self.DEFAULT_NODE_TOOL_CALLS),
            "max_steps": min(total_steps or self.DEFAULT_NODE_MAX_STEPS,
                             self.DEFAULT_NODE_MAX_STEPS),
        }

    def dispatch(self, spec: SubagentSpec, parent_ctx: Any, parent_e: Any) -> SubagentResult:
        name = str(getattr(spec, "name", "") or "")
        goal = str(getattr(spec, "goal", "") or "") or name
        # 欠预算 / 已取消：**不发起**这个节点。否则父层已经没额度了，
        # 节点还会各自拿一份默认额度跑下去。
        if parent_e is not None and getattr(parent_e, "aborted", False):
            return _node_skipped(name, "父级已取消，节点未执行")
        result = self._gateway.handle({"goal": goal,
                                       "principal": dict(self._principal),
                                       "budget": self._budget_for_node(parent_e)})
        return SubagentResult(
            name=name,
            ok=bool(getattr(result, "ok", False)),
            summary=str(getattr(result, "text", "") or ""),
            steps=int(getattr(result, "steps", 0) or 0),
            term_reason=str(getattr(result, "term_reason", "") or ""),
            error=str(getattr(result, "error", "") or ""),
        )


class _NodeTrackingRunner:
    """装饰器 runner：把「节点开始跑 / 跑完了」实时写进 `WorkflowState`。

    为什么需要它：`DAGRunner` 只在**跑完**给一份 report（`last_run`）。没有这一层，
    一条跑 5 分钟的三层 DAG 在 `GET /v1/workflows/{id}` 里会一直显示 `pending` ——
    "提交后看不见进度"等于这个端点没做。

    它只观察、不改语义：`dispatch` 的入参与返回**原样透传**，异常也照样往上抛
    （由 `DAGRunner` 收敛），这里只保证"抛出前把节点标成失败"。
    """

    def __init__(self, inner: Any, on_start: Callable[[str], None],
                 on_finish: Callable[[str, Any], None]) -> None:
        self._inner = inner
        self._on_start = on_start
        self._on_finish = on_finish

    def dispatch(self, spec: SubagentSpec, parent_ctx: Any, parent_e: Any) -> Any:
        name = str(getattr(spec, "name", "") or "")
        self._on_start(name)
        try:
            raw = self._inner.dispatch(spec, parent_ctx, parent_e)
        except BaseException as exc:                 # noqa: BLE001 - 先记账再抛
            self._on_finish(name, {"ok": False,
                                   "error": f"dispatch 异常: {type(exc).__name__}: {exc}",
                                   "term_reason": ""})
            raise
        self._on_finish(name, raw)
        return raw


class WorkflowSubmitError(Exception):
    """提交阶段的拒绝。`status` 直接就是 HTTP 状态码（与 `STATUS_BY_CODE` 同一口径）。

    为什么用异常而不是返回值：`submit()` 有四个前置判定（校验 / 容量 / 执行器 / 关停），
    用返回码时"某个分支忘了写 return"就等于放行 —— 这与 `core/gateway.py` 用
    `_Reject` 异常表达拒绝是同一个理由。
    """

    def __init__(self, message: str, *, status: int = 500,
                 retry_after: float | None = None) -> None:
        self.status = int(status)
        self.retry_after = retry_after
        super().__init__(message)


class WorkflowRegistry:
    """内存里的 workflow 状态表 + 后台执行器（`app.state.workflows`）。

    三条硬约束，每条都对应一个会出事的具体后果：

    1. **异步**：`submit()` 只做校验 + 登记 + 丢给线程池，**立刻返回**。
       同步等一个可能跑几分钟的 DAG，等于让 HTTP 超时机制去砍一条正在干活的流水线
       （而 CPython 杀不掉线程，只能"停止等待"——那条流水线会继续烧预算）。
    2. **有界**：最多 `max_tracked` 条状态；超限淘汰**最老的已结束**的。
       一条可淘汰的都没有（全在跑）时**拒绝新提交**（429 + Retry-After）：
       宁可让客户端晚点再来，也不让内存无界增长，也不偷偷 kill 一条正在跑的流水线
       （kill = 半写状态 + 白烧预算，比慢一点贵得多）。
    3. **可查**：层 / 节点 / 每个节点的步骤数与错误都在 `WorkflowState` 里，
       `GET /v1/workflows/{id}` 直接读它，不重新推算任何东西。

    线程安全：`_lock`（RLock）保护 `_states` 与所有节点写入。为什么要锁：层的并行
    执行在多个 worker 线程里同时 dispatch，节点状态是被**并发写**的；没有一个
    统一入口，`to_dict()` 就会读到"节点还 running，但它的后继已经 done"的撕裂快照。
    `to_dict()` 也在锁里做，所以读到的永远是一个一致的时间点。
    """

    def __init__(self, *, gateway: Any,
                 max_tracked: int = DEFAULT_WORKFLOW_MAX,
                 workers: int = DEFAULT_WORKFLOW_WORKERS,
                 default_parallel: int = 1,
                 tracer: Any = None,
                 logger: Any = None,
                 runner_factory: Callable[[dict], Any] | None = None,
                 store: Any = None) -> None:
        self._gateway = gateway
        self._max_tracked = max(1, int(max_tracked))
        self._workers = max(1, int(workers))
        self._default_parallel = max(1, int(default_parallel))
        self._tracer = tracer
        self._logger = logger
        #: 注入口：`(principal_dict) -> runner`，用来换成真 `SubagentRunner`。
        #: 不给 = 用内置 `GatewayNodeRunner`（见它的 docstring 与局限）。
        self._runner_factory = runner_factory
        #: 持久化后端（可选）。给了就在状态变化时落盘，进程重启能读回来。
        #:
        #: **为什么必须持久化**：workflow 是"发布/回滚"这类长流程，
        #: 进程重启丢状态 = 发布做了一半没人知道。内存状态只适合演示。
        #: 形状要求很弱：只要 `save(key, payload)` / `load(key)` / `list_pending()`，
        #: `core.store` 的三个实现（memory/file/sqlite）都满足。
        self._store = store
        self._lock = threading.RLock()
        #: dict 保序 = 提交序，"最老"就是第一个（与 `created_at` 排序无关）
        self._states: dict[str, WorkflowState] = {}
        #: workflow_id → DAGSpec。**续跑必需**：没有 DAG 定义就没法重放未完成的节点。
        #: 只存在内存（spec 是代码产物，不值得持久化；重启后提供不了就明确报 409）。
        self._specs: dict[str, Any] = {}
        self._seq = 0
        self._pool = ThreadPoolExecutor(max_workers=self._workers,
                                       thread_name_prefix="workflow")
        #: 累计淘汰 / 累计因容量拒绝（运维要能看出"容量被打满过"）
        self.evicted = 0
        self.rejected = 0

    # ── 持久化 ──────────────────────────────────────────────
    # `core.store` 的 ctx_id 只允许 `[A-Za-z0-9_.-]`（要能当文件名），
    # 所以不能用 `workflow:<id>` 这种带冒号的形态。双下划线既合法又不撞 ctx_ 前缀。
    _WF_KEY = "workflow__{}"

    def _persist(self, state: WorkflowState) -> None:
        """把状态落盘。**fail-open**：存储坏了不该让流水线跑不下去。

        但要有痕迹（`workflow.persist_failed`）——"持久化静默失效"比"没持久化"更坏：
        前者会让人以为重启能恢复。
        """
        if self._store is None:
            return
        save = getattr(self._store, "save", None)
        if not callable(save):
            return
        try:
            payload = json.dumps(state.to_dict(), ensure_ascii=False, default=str)
            save(self._WF_KEY.format(state.id), payload)
        except Exception as exc:                               # noqa: BLE001
            try:
                if self._logger is not None:
                    self._logger.warn("workflow.persist_failed",
                                      workflow_id=state.id,
                                      reason=f"{type(exc).__name__}: {exc}")
            except Exception:                                  # noqa: BLE001
                pass

    def restore(self) -> int:
        """从存储读回**未结束**的 workflow，标记为 `interrupted`。返回条数。

        **不自动续跑**：重启后原地重放 DAG 会重复执行已经做完的节点
        （发布流程里"重复执行"可能是灾难）。所以只做两件事：
          1. 把状态读回来，让 `GET /v1/workflows/{id}` 还能查到
          2. 标成 `interrupted`，告诉人"它是在这里断的，需要你决定"

        真正的续跑需要节点级幂等 + checkpoint，是另一个量级的工程。
        """
        if self._store is None:
            return 0
        pending = getattr(self._store, "list_pending", None)
        load = getattr(self._store, "load", None)
        if not callable(pending) or not callable(load):
            return 0
        restored = 0
        try:
            keys = self._iter_workflow_keys(pending)
        except Exception:                                      # noqa: BLE001
            return 0
        for key in keys:
            try:
                raw = load(key)      # store 实例的 load(ctx_id) -> str | None
                payload = json.loads(raw) if isinstance(raw, str) else None
            except Exception:                                  # noqa: BLE001
                continue
            if not isinstance(payload, dict):
                continue
            state = self._state_from_payload(payload)
            if state is None:
                continue
            with self._lock:
                # **已完成的也要读回来**：那是审计轨迹。
                # 早先只恢复未完成的，结果"重启后查不到上周的发布记录" ——
                # 持久化的意义不只是"接着跑"，还包括"事后能查"。
                if state.status not in (WF_DONE, WF_FAILED):
                    state.status = WF_INTERRUPTED
                    state.error = state.error or (
                        "进程重启导致中断（未自动续跑：重放会重复执行已完成节点）")
                if len(self._states) >= self._max_tracked:
                    break                     # 容量上限同样适用于恢复
                self._states[state.id] = state
                restored += 1
        if restored and self._logger is not None:
            try:
                self._logger.info("workflow.restored", count=restored)
            except Exception:                                  # noqa: BLE001
                pass
        return restored

    def _iter_workflow_keys(self, pending: Any) -> list[str]:
        """从 store 的 `list_pending()` 里挑出 workflow 的键。

        `core.store` 三个实现的返回形状不完全一致（有的是 `[ctx_id]`，
        有的是 `[(ctx_id, payload)]`），所以这里都认。
        """
        try:
            rows = pending() or []
        except TypeError:
            return []
        keys: list[str] = []
        for row in rows:
            key = row[0] if isinstance(row, (tuple, list)) else row
            key = str(key)
            if key.startswith("workflow__"):
                keys.append(key)
        return keys

    def _state_from_payload(self, payload: dict) -> Any:
        state = WorkflowState(
            id=str(payload.get("id") or ""),
            spec_name=str(payload.get("spec_name") or ""),
            principal=dict(payload.get("principal") or {}),
            created_at=float(payload.get("created_at") or 0.0),
            max_parallel=int(payload.get("max_parallel") or 1),
        )
        if not state.id:
            return None
        state.status = str(payload.get("status") or WF_PENDING)
        state.finished_at = payload.get("finished_at")
        state.error = str(payload.get("error") or "")
        layers = payload.get("layers")
        if isinstance(layers, list):
            state.layers = [[str(x) for x in layer] for layer in layers
                            if isinstance(layer, list)]
        for node in payload.get("nodes") or []:
            if not isinstance(node, dict):
                continue
            ns = WorkflowNodeState(name=str(node.get("name") or ""))
            ns.status = str(node.get("status") or "")
            ns.steps = int(node.get("steps") or 0)
            ns.error = str(node.get("error") or "")
            ns.started_at = node.get("started_at")
            ns.ended_at = node.get("ended_at")
            # `WorkflowState.nodes` 是 **dict[name -> node]**（保序 + O(1) 查），
            # 不是 list —— 早先按 list 写会 AttributeError。
            state.nodes[ns.name] = ns
        return state

    # ── 只读属性 ────────────────────────────────────────────
    @property
    def max_tracked(self) -> int:
        return self._max_tracked

    @property
    def workers(self) -> int:
        return self._workers

    def tracked(self) -> int:
        with self._lock:
            return len(self._states)

    def active_count(self) -> int:
        """未结束的工作流数（pending + running）。`/v1/info` 的 `running` 用它。"""
        with self._lock:
            return sum(1 for state in self._states.values() if not state.is_finished)

    def info(self) -> dict:
        """`/v1/info` 的 `workflow` 段（启动即可读，不探测任何东西）。"""
        with self._lock:
            return {
                "enabled": True,
                "max_tracked": self._max_tracked,
                "tracked": len(self._states),
                # pending 也算"在跑"：它已经占了一个追踪位、且即将/正在执行
                "running": sum(1 for s in self._states.values() if not s.is_finished),
                "workers": self._workers,
                "evicted": int(self.evicted),
                "rejected": int(self.rejected),
            }

    # ── 提交 ────────────────────────────────────────────────
    def submit(self, *, spec: DAGSpec, principal: Principal,
               principal_dict: Mapping[str, Any] | None = None,
               goals: Mapping[str, str] | None = None,
               max_parallel: int | None = None) -> WorkflowState:
        """校验 → 登记 → 丢线程池 → 返回状态对象（**不等待执行**）。

        Raises:
            WorkflowSubmitError: 400（DAG 不合法）/ 429（容量满且无可淘汰）/ 503（执行器已关）。
        """
        errors = validate_workflow_spec(spec)
        if errors:
            raise WorkflowSubmitError("DAG 不合法：" + "；".join(errors), status=400)
        # 校验通过后再取层：`layers()` 自己也会再验一次（幂等），但它抛的是异常，
        # 上面那次是为了拿到**全部**错误明细。
        try:
            layers = DAG(spec).layers()
        except DAGValidationError as exc:
            raise WorkflowSubmitError("DAG 不合法：" + "；".join(exc.errors),
                                      status=400) from exc

        parallel = max(1, int(max_parallel if max_parallel is not None
                              else self._default_parallel))
        identity = dict(principal_dict or {})
        if not identity:
            identity = {"user_id": principal.user_id, "tenant": principal.tenant,
                        "roles": list(principal.roles),
                        "scopes": sorted(principal.scopes)}

        with self._lock:
            self._prune_locked()
            if len(self._states) >= self._max_tracked:
                self.rejected += 1
                raise WorkflowSubmitError(
                    f"workflow 容量已满（max_tracked={self._max_tracked}，"
                    f"且没有已结束的可淘汰：正在跑的不会被淘汰）",
                    status=429, retry_after=5.0)
            self._seq += 1
            order = [name for layer in layers for name in layer]
            state = WorkflowState(
                id=self._new_id_locked(),
                spec_name=str(getattr(spec, "name", "") or ""),
                layers=[list(layer) for layer in layers],
                nodes={name: WorkflowNodeState(name=name) for name in order},
                seq=self._seq, principal=identity, max_parallel=parallel)
            state.goals = dict(goals or {})
            self._states[state.id] = state
            self._specs[state.id] = spec      # 续跑要用（没有它就重放不了）

        # **提交即落盘**（pending）。顺序很关键：先落盘再进线程池 ——
        # 反过来会有一个窗口"线程已经开跑、盘上还没有"，此时崩溃就彻底丢了。
        # 这个窗口很短但真实存在（重启测试正好打中过）。
        self._persist(state)

        try:
            self._pool.submit(self._execute, state, spec,
                              dict(goals or {}), parallel, identity)
        except RuntimeError as exc:          # 线程池已 shutdown（进程正在退出）
            with self._lock:
                self._states.pop(state.id, None)
            raise WorkflowSubmitError(
                f"workflow 执行器已关闭: {type(exc).__name__}: {exc}",
                status=503) from exc
        return state

    # ── 查询 ────────────────────────────────────────────────
    def get(self, workflow_id: str) -> WorkflowState | None:
        """活对象（测试/内部用）。HTTP 出口用 `snapshot()`，避免读到撕裂状态。"""
        with self._lock:
            return self._states.get(str(workflow_id or ""))

    def snapshot(self, workflow_id: str) -> dict | None:
        """一条 workflow 的一致快照；不存在 → None。"""
        with self._lock:
            state = self._states.get(str(workflow_id or ""))
            return state.to_dict() if state is not None else None

    def list_states(self, *, limit: int = 20, offset: int = 0) -> tuple[list[dict], int]:
        """分页列表：**(created_at, seq) 倒序**（最新在前），返回 `(窗口, 总数)`。

        排序键带 `seq`：两次提交可能落在同一毫秒，只按 `created_at` 排会让顺序
        取决于 dict 迭代的巧合 —— 分页就会漏条/重条。
        """
        with self._lock:
            states = sorted(self._states.values(),
                            key=lambda s: (s.created_at, s.seq), reverse=True)
            total = len(states)
            start = max(0, int(offset))
            size = max(0, int(limit))
            window = states[start:start + size] if size else []
            return [state.to_dict() for state in window], total

    # ── 收口 ────────────────────────────────────────────────
    def close(self) -> None:
        """关停执行器：不再接新任务，**不等**已提交的跑完。

        为什么 `wait=False`：一条 DAG 可能跑几分钟，优雅关停被它拖住就等于
        "进程卡死不退出"（k8s 会在宽限期到了以后 SIGKILL，那时 flush 也没了）。
        DAG 的执行是可丢弃的（状态是 poke 出来的、不落盘），所以这里选择守时。
        """
        try:
            self._pool.shutdown(wait=False, cancel_futures=True)
        except TypeError:                    # pragma: no cover - 老版本没有 cancel_futures
            self._pool.shutdown(wait=False)
        except Exception:                    # noqa: BLE001 - 关停路径 fail-open
            pass

    # ── 内部：容量 ──────────────────────────────────────────
    def _new_id_locked(self) -> str:
        """`wf_<12 hex>`。**唯一性在锁里保证**（撞了就重摇，绝不返回重复 id）。"""
        while True:
            candidate = f"wf_{uuid.uuid4().hex[:12]}"
            if candidate not in self._states:
                return candidate

    def _prune_locked(self) -> int:
        """淘汰最老的**已结束**工作流，直到能塞下新的一条（或没有可淘汰的）。

        绝不能淘汰 running/pending 的：杀掉一条正在跑的流水线会留下半写状态、
        白烧的预算，而"容量满时拒绝新提交"只是让客户端等 5 秒。
        """
        evicted = 0
        while len(self._states) >= self._max_tracked:
            victim = next((wid for wid, state in self._states.items()
                           if state.is_finished), None)
            if victim is None:               # 全在跑：一条都不动
                break
            del self._states[victim]
            # spec 也要清：不清就是内存泄漏（每条 spec 都带着整棵 DAG）
            self._specs.pop(victim, None)
            evicted += 1
        if evicted:
            self.evicted += evicted
        return evicted

    # ── 内部：执行（后台线程，永不把异常漏给线程池）──────────
    def resume(self, workflow_id: str, *, principal: Principal,
               principal_dict: Mapping[str, Any] | None = None) -> WorkflowState:
        """续跑一条 `interrupted`（或 failed）的 workflow：**跳过已完成的节点**。

        为什么必须跳过：重放已完成的节点会**重复产生副作用**（重复发版、
        重复删库）。这是续跑与"重跑一遍"的唯一区别，也是它能用在发布流程里的前提。

        只允许 `interrupted` / `failed`：
          * `pending` / `running` → 409（已在跑，重复 resume 会双跑）
          * `done` → 409（没有可续的）

        Raises:
            WorkflowSubmitError: 404（不存在）/ 409（状态不允许）/ 429（容量满）
        """
        with self._lock:
            state = self._states.get(workflow_id)
            if state is None:
                raise WorkflowSubmitError(
                    f"workflow 不存在：{workflow_id}", status=404)
            if state.status not in (WF_INTERRUPTED, WF_FAILED):
                raise WorkflowSubmitError(
                    f"workflow 状态为 {state.status!r}，只有 interrupted / failed "
                    f"可以续跑（pending/running 重复 resume 会双跑）", status=409)
            # 已完成节点：**只认 done**。failed 的节点需要重跑（那正是续跑要做的），
            # 所以不能把它也塞进 skip。
            done = tuple(n for n, ns in state.nodes.items()
                         if ns.status == NODE_DONE)
            prev_results = {n: {"name": n, "ok": True, "steps": state.nodes[n].steps,
                                "text": "", "error": "", "term_reason": "RESUMED",
                                "resumed_ok": True}
                            for n in done}
            # 重开状态：清掉上一轮的终态与错误，节点里 done 的保留
            state.status = WF_PENDING
            # `finished_at` 是 float（0.0 = 未结束），**不是 None** ——
            # `to_dict()` 会 `float()` 它，设成 None 直接 TypeError。
            state.finished_at = 0.0
            state.error = ""
            state.resumed_count = getattr(state, "resumed_count", 0) + 1
            for name, ns in state.nodes.items():
                if name not in done:
                    ns.status = NODE_PENDING
                    ns.error = ""
                    ns.steps = 0
                    ns.started_at = 0.0
                    ns.ended_at = 0.0
            spec = self._specs.get(workflow_id)
            if spec is None:
                raise WorkflowSubmitError(
                    f"workflow {workflow_id} 的 DAG 定义不在内存里，无法续跑"
                    f"（进程重启后 spec 未持久化）", status=409)

        self._persist(state)
        identity = dict(principal_dict or state.principal or {})
        goals = dict(getattr(state, "goals", {}) or {})
        parallel = int(getattr(state, "max_parallel", 1) or 1)
        try:
            self._pool.submit(self._execute, state, spec, goals, parallel, identity,
                              skip=done, skip_results=prev_results)
        except RuntimeError as exc:                            # noqa: BLE001
            with self._lock:
                state.status = WF_FAILED
                state.error = f"执行器已关闭：{exc}"
            self._persist(state)
            raise WorkflowSubmitError("执行器已关闭", status=503) from exc
        return state

    def _execute(self, state: WorkflowState, spec: DAGSpec,
                 goals: dict[str, str], parallel: int,
                 principal_dict: Mapping[str, Any],
                 *, skip: tuple[str, ...] = (),
                 skip_results: Mapping[str, dict] | None = None) -> None:
        """跑一条 DAG，把每个节点的结果抄进状态。

        这里**必须**把一切异常收敛成状态（`failed` + error）：线程池里的异常没人接，
        漏出去就只剩一条 stderr traceback，而 `GET /v1/workflows/{id}` 会永远
        显示 running —— "看不见的失败"比失败本身糟。
        """
        self._mark_running(state)
        try:
            principal = _principal_from_dict(principal_dict)
            # parent_e 是**这条 workflow 自己的**执行上下文（principal / 预算 / 追踪）。
            parent_e = ExecCtx(principal=principal)
            runner = self._make_runner(principal_dict)
            # runner 可以自带 `parent_ctx`（例如注入了真 SubagentRunner + 真 Context）：
            # 那时 `DAGRunner` 会把执行事实（层 / 节点 / 跳过原因）落到那个 Context 的
            # `workflow_state` 里（DESIGN-C §4 的可复盘要求）。内置 runner 不带 Context
            # —— 于是这里是 None，而 `DAGRunner` 的 stamping 是 **fail-open** 的
            # （parent_ctx 没有 apply 就跳过），不会因此失败。
            # `parent_ctx`：**每条 workflow 一个独立 Context**，用来落 `workflow_state`
            # （层结构 / 节点顺序 / 跳过与失败原因）。
            #
            # 为什么必须给 Context：`DAGRunner` 的复盘能力依赖它 ——
            # 没有它，"跑到第几层、哪个节点被跳过"不留任何痕迹，出事后无法追。
            #
            # 为什么是**独立**的而非复用回合的：workflow 是独立执行单元。与回合内编排（C）
            # 共享同一个 Context 会撞 `bind_engine` 互斥（两者都写 observations，
            # 顺序与归属会不确定）。给它自己的 Context，互斥自然通过，归属也唯一。
            #
            # 注入的 runner 若自带 `parent_ctx`（真 SubagentRunner + 真 Context），
            # 优先用它的 —— 那种场景下执行事实应落到那个 Context 上。
            parent_ctx = (getattr(runner, "parent_ctx", None)
                          or self._make_workflow_ctx(spec, principal_dict, state))
            tracked = _NodeTrackingRunner(
                runner,
                on_start=lambda name: self._node_started(state, name),
                on_finish=lambda name, raw: self._node_finished(state, name, raw))
            dag_runner = DAGRunner(tracked, tracer=self._tracer, max_parallel=parallel)
            report = dag_runner.run(spec, parent_ctx, parent_e, goals,
                                    skip=skip, skip_results=skip_results)
            self._apply_report(state, report)
        except DAGValidationError as exc:
            self._fail_all_pending(state, "未执行（DAG 校验失败）")
            self._finish(state, WF_FAILED,
                         "DAG 校验失败：" + "；".join(exc.errors))
        except BaseException as exc:                  # noqa: BLE001 - 线程里不许漏
            self._fail_all_pending(state, f"未执行（{type(exc).__name__}）")
            self._finish(state, WF_FAILED,
                         f"workflow 执行异常: {type(exc).__name__}: {exc}")

    def _make_runner(self, principal_dict: Mapping[str, Any]) -> Any:
        """拿 runner：注入了 `runner_factory` 就用它，否则用内置的最小型。

        注入的装配**坏了**（抛异常）时退回内置 runner 并留一行日志：一个坏插件
        不该让所有 workflow 一起提交失败，但也不能静默 —— 否则"降权没了"没人知道。
        """
        if self._runner_factory is not None:
            try:
                runner = self._runner_factory(dict(principal_dict))
                if callable(getattr(runner, "dispatch", None)):
                    return runner
                self._warn("workflow.runner_factory_invalid",
                           reason=f"返回对象没有 dispatch: {type(runner).__name__}")
            except Exception as exc:                  # noqa: BLE001 - fail-open 到内置
                self._warn("workflow.runner_factory_failed",
                           reason=f"{type(exc).__name__}: {exc}")
        return GatewayNodeRunner(self._gateway, principal_dict)

    # ── 内部：状态写入（全部在锁里）──────────────────────────
    @staticmethod
    def _make_workflow_ctx(spec: Any, principal_dict: dict, state: Any) -> Any:
        """给一条 workflow 造它**自己的** Context（不与他人共享）。

        `bind_engine("dag")` 让互斥检查生效：同一个 Context 上不能再跑回合内编排。
        失败返回 None（`DAGRunner` 的 stamping 是 fail-open）——
        观测面不该因为造不出 Context 就让整条流水线失败。
        """
        try:
            from core.context import ContextHandle
            ctx = ContextHandle(initial={
                "goal": f"workflow:{getattr(spec, 'name', '')}",
                "principal": dict(principal_dict or {}),
                "workflow_mode": True,
            }, budget=None)
            ctx.bind_engine("dag")
            return ctx
        except Exception:                                      # noqa: BLE001
            return None

    def running_count(self) -> int:
        """当前 running 的 workflow 数（喂给 `workflow_running` gauge）。"""
        with self._lock:
            return sum(1 for s in self._states.values() if not s.is_finished)

    def _mark_running(self, state: WorkflowState) -> None:
        with self._lock:
            if state.status == WF_PENDING:
                state.status = WF_RUNNING

        self._persist(state)

    def _node_started(self, state: WorkflowState, name: str) -> None:
        with self._lock:
            node = state.nodes.get(name)
            if node is None:                  # 兜底：report 里出现未知节点名也能记上
                node = WorkflowNodeState(name=name)
                state.nodes[name] = node
            node.status = NODE_RUNNING
            node.started_at = time.time()
            node.error = ""
            if state.status == WF_PENDING:
                state.status = WF_RUNNING

    def _node_finished(self, state: WorkflowState, name: str, raw: Any) -> None:
        ok, steps, error, term_reason = _node_facts(raw)
        with self._lock:
            node = state.nodes.get(name)
            if node is None:
                node = WorkflowNodeState(name=name)
                state.nodes[name] = node
            node.steps = steps
            node.error = error
            node.ended_at = time.time()
            node.status = (NODE_DONE if ok
                           else NODE_SKIPPED if term_reason == SKIP_TERM
                           else NODE_FAILED)

    def _apply_report(self, state: WorkflowState, report: Mapping[str, Any]) -> None:
        """把 `DAGRunner` 的最终 report 抄进状态（**权威**：含被短路的节点）。

        report 是唯一一份"所有节点都有结论"的账本（`results` + `layers` +
        `short_circuited`），所以最终状态以它为准，逐字段覆盖实时追踪写下的值。
        """
        results = dict(report.get("results") or {})
        with self._lock:
            for name, payload in results.items():
                node = state.nodes.get(name)
                if node is None:
                    node = WorkflowNodeState(name=name)
                    state.nodes[name] = node
                ok, steps, error, term_reason = _node_facts(payload)
                node.steps = steps
                node.error = error
                node.status = (NODE_DONE if ok
                               else NODE_SKIPPED if term_reason == SKIP_TERM
                               else NODE_FAILED)
                if not node.ended_at:         # 短路节点没跑过：给一个"落定"时间
                    node.ended_at = time.time()
            for node in state.nodes.values():
                if node.status == NODE_PENDING:   # 不该发生，发生了也不能留假象
                    node.status = NODE_FAILED
                    node.error = node.error or "未执行（DAG 未派发该节点）"
                    node.ended_at = node.ended_at or time.time()
            summary = _workflow_failure_summary(state)
        self._finish(state, WF_FAILED if summary else WF_DONE, summary)

    def _fail_all_pending(self, state: WorkflowState, reason: str) -> None:
        with self._lock:
            now = time.time()
            for node in state.nodes.values():
                if node.status in (NODE_PENDING, NODE_RUNNING):
                    node.status = NODE_FAILED
                    node.error = node.error or reason
                    node.ended_at = node.ended_at or now

    def _finish(self, state: WorkflowState, status: str, error: str = "") -> None:
        with self._lock:
            state.status = status
            state.error = error
            state.finished_at = time.time()
        self._persist(state)

    def _warn(self, event: str, **fields: Any) -> None:
        """观测面 fail-open：日志坏了不影响执行。"""
        logger = self._logger
        if logger is None:
            return
        try:
            logger.warn(event, **fields)
        except Exception:                             # noqa: BLE001
            pass


def _principal_from_dict(data: Mapping[str, Any]) -> Principal:
    """身份 dict → `Principal`（只认契约字段，多余键忽略）。"""
    if isinstance(data, Principal):
        return data
    blob = dict(data or {})
    roles = blob.get("roles") or ()
    scopes = blob.get("scopes") or ()
    return Principal(
        user_id=str(blob.get("user_id") or ""),
        tenant=str(blob.get("tenant") or ""),
        roles=tuple(str(item) for item in roles),
        scopes=frozenset(str(item) for item in scopes),
    )


def _workflow_failure_summary(state: WorkflowState) -> str:
    """把节点的失败/短路折成一句人能读的 workflow 级 error（空串 = 全部成功）。

    `failed` 与 `skipped` 分开写：前者花了钱但没成，后者**根本没派发**
    （上游没跑赢）—— 运维要能一眼分清"哪一段真的坏了"和"哪一段被连累没跑"。
    """
    failed = sorted(n.name for n in state.nodes.values() if n.status == NODE_FAILED)
    skipped = sorted(n.name for n in state.nodes.values() if n.status == NODE_SKIPPED)
    parts: list[str] = []
    if failed:
        parts.append("节点失败: " + ", ".join(failed))
    if skipped:
        parts.append("被短路跳过: " + ", ".join(skipped))
    return "；".join(parts)


def _workflow_info_section(state: Any) -> dict:
    """`/v1/info` 的 `workflow` 段（表不存在时 self-report `enabled: False`）。"""
    registry = getattr(state, "workflows", None)
    info = getattr(registry, "info", None)
    if callable(info):
        try:
            return dict(info())
        except Exception as exc:                      # noqa: BLE001 - 观测面 fail-open
            return {"enabled": False, "reason": f"{type(exc).__name__}: {exc}"}
    return {"enabled": False, "reason": "workflow registry 未装配"}


# ══════════════════════════════════════════════════════════════
# 三、依赖自检（/readyz、/v1/info、启动快照共用同一套探针）
# ══════════════════════════════════════════════════════════════
#: 探测键：store 的 `exists()` 用它（只问"在不在"，不写入）。
#: 探针**不许有副作用** —— 一个会在生产库里写 key 的 health check 就是 bug。
_PROBE_KEY = "__readyz__"


@dataclass(frozen=True)
class DepSpec:
    """一个待自检的依赖：名字 + 对象 + 是否关键。

    `critical=False` 用于"坏了只降级、不该摘流"的依赖（例如可选的缓存层）。
    默认 True：**默认严格**，降级必须显式声明 —— 反过来会让"忘了标关键"静默变成
    "不关键"，那正是"永远 ready"的探针的起点。
    """
    name: str
    obj: Any
    critical: bool = True


class _Unavailable:
    """构造失败的依赖占位：**不假装可用**，`info()` 直接报告失败原因。

    为什么需要它：`create_app` 里 "vector 后端配置了 milvus 但连不上"必须能被
    `/readyz` 表达出来。若这时把依赖置成 None 或干脆不放进 deps，进程就会报 ready
    —— 那正是"配了却没生效"这个项目反复踩的坑。
    """

    def __init__(self, name: str, reason: str) -> None:
        self.name = name
        self.reason = reason

    def info(self) -> dict:
        return {"kind": self.name, "available": False, "reason": self.reason}

    def __repr__(self) -> str:          # pragma: no cover - 调试用
        return f"<Unavailable {self.name}: {self.reason}>"


def _as_dep(name: Any, value: Any) -> DepSpec:
    """把 `deps=` 里的一项归一成 DepSpec。三种写法都认：

        deps={"store": store}                                  # 对象
        deps={"store": {"obj": store, "critical": False}}      # 带策略
        deps=[DepSpec("store", store, critical=False)]          # 直接给 spec
    """
    if isinstance(value, DepSpec):
        return DepSpec(str(name or value.name), value.obj, value.critical)
    if isinstance(value, Mapping) and "obj" in value:
        return DepSpec(str(name), value["obj"], bool(value.get("critical", True)))
    return DepSpec(str(name), value, True)


def _dep_specs(deps: Any) -> list[DepSpec]:
    """归一 deps 参数；None = 按环境变量造默认三件（store / kv / vector）。"""
    if deps is None:
        return _default_dep_specs()
    if isinstance(deps, Mapping):
        return [_as_dep(name, value) for name, value in deps.items()]
    if isinstance(deps, Sequence) and not isinstance(deps, (str, bytes, bytearray)):
        out: list[DepSpec] = []
        for item in deps:
            if isinstance(item, DepSpec):
                out.append(item)
            elif isinstance(item, (tuple, list)) and len(item) == 2:
                out.append(_as_dep(item[0], item[1]))
            else:
                raise TypeError(f"deps 元素必须是 DepSpec 或 (name, obj)，收到 {item!r}")
        return out
    raise TypeError(f"deps 必须是 Mapping / Sequence / None，收到 {type(deps).__name__}")


def _default_dep_specs(env: Mapping[str, str] | None = None,
                      logger: Any = None) -> list[DepSpec]:
    """默认三件依赖：store / kv / vector。

    默认全是**零依赖、进程内**实现（memory）：服务能起来、探针能真实回答，
    且测试不需要 Redis/Milvus。生产用环境变量显式换成持久化/分布式实现 ——
    "默认可跑、显式才接外部服务" 是这个项目一贯的降级方向。
    """
    e = dict(os.environ if env is None else env)
    return [DepSpec("store", _make_store(e, logger=logger)),
            DepSpec("kv", _make_kv(e)),
            DepSpec("vector", _make_vector(e))]


def _make_memory_provider(env: Mapping[str, str]) -> Any:
    """Memory 的 Provider（不是 Store —— 抽象层级别搞混）。

    `MemoryRuntime(provider=...)` 吃的是 `plane.memory.Provider` 协议：
    put/get/scan/delete 四个方法。所以这里造的是 **Provider**，
    而不是 Context 那种"按 ctx_id 存检查点"的 Store。

    可选值：
      * `memory`（默认）—— `InMemoryProvider`，进程内 dict，重启即失
      * `vector`         —— `VectorProvider`，向量检索（需配 `VECTOR_BACKEND`/embedding）
      * `agents.xxx:YYY` —— 自定义 Provider（Agent 层实现，Runtime 按配置加载）
    """
    kind = str(env.get("AGENT_RUNTIME_MEMORY_PROVIDER") or "memory").strip()
    low = kind.lower()
    if low == "memory":
        from plane.memory import InMemoryProvider
        return InMemoryProvider()
    if low == "vector":
        from plane.vector_store import VectorProvider, configure_vector_provider
        provider = configure_vector_provider(env)
        if provider is None:
            raise RuntimeError(
                "AGENT_RUNTIME_MEMORY_PROVIDER=vector 但向量后端不可用"
                "（检查 VECTOR_BACKEND / VECTOR_DIM / 对应服务的连接）")
        return provider
    # 其余当成 import 路径（必须含冒号才算合法 import 路径）
    if ":" not in kind:
        raise RuntimeError(
            f"未知 AGENT_RUNTIME_MEMORY_PROVIDER={kind!r}"
            f"（可选 memory / vector / 'module:attr'）")
    obj = load_extension(kind, kind="memory_provider")
    return _instantiate(obj)


def _make_embedder(env: Mapping[str, str]) -> Any:
    """造一个 embedding 提供者（给语义召回 / 向量检索用）。

    只认显式配置，**不猜**：没配就返回 None，由调用方决定"没配"该报错还是降级。
    这一条很重要 —— 早期几个 factory 用"猜一个默认值"，表现是配错了却
    "看起来能跑"，而效果和没配一样（比如语义召回静默退回 bigram）。

    支持的键（按优先级）：

        AGENT_RUNTIME_EMBEDDING_BASE_URL     专用（推荐）
        AGENT_RUNTIME_VECTOR_BASE_URL        向量后端那套的兼容名

    model 同理：`AGENT_RUNTIME_EMBEDDING_MODEL` / `AGENT_RUNTIME_VECTOR_MODEL`。
    """
    base = (env.get("AGENT_RUNTIME_EMBEDDING_BASE_URL")
            or env.get("AGENT_RUNTIME_VECTOR_BASE_URL") or "").strip()
    if not base:
        return None
    model = (env.get("AGENT_RUNTIME_EMBEDDING_MODEL")
             or env.get("AGENT_RUNTIME_VECTOR_MODEL") or "").strip()
    try:
        from adapters.embeddings import OpenAICompatEmbeddings
        return OpenAICompatEmbeddings(
            base_url=base, model=model or "text-embedding-3-small",
            api_key=(env.get("AGENT_RUNTIME_EMBEDDING_API_KEY")
                     or env.get("OPENAI_API_KEY") or ""),
            dim=int(env.get("AGENT_RUNTIME_EMBEDDING_DIM") or 0) or None)
    except Exception as exc:                                   # noqa: BLE001
        raise RuntimeError(
            f"embedding 提供者装配失败（base_url={base!r}）："
            f"{type(exc).__name__}: {exc}") from exc


def _store_env_notice(kind: str, env: Mapping[str, str], logger: Any = None) -> None:
    """store 的路径变量**每个后端只认一个名字**，配错要出声。

    现实里最容易踩的两脚（都**静默**，表面上一切正常）：

      * `STORE=sqlite` + `STORE_ROOT`：sqlite 只读 `STORE_PATH`/`STORE_DB`，
        `STORE_ROOT` 被忽略 → 落到 `:memory:`。重启后 session/checkpoint 全丢，
        而 `/readyz` 是绿的。
      * `STORE=file` + `STORE_PATH`：file 只读 `STORE_ROOT`，`STORE_PATH` 被
        忽略 → 落到默认 `/tmp/agent-runtime-store`。表现是
        `/v1/turns/{ctx_id}/resume` 一直 404 ctx_not_found —— 看起来像"ctx 不存在"，
        实际是服务在另一个目录里找。

    不改行为（沿用 `AGENT_RUNTIME_KV` 的口径：可以兼容别名，但必须留痕），
    只把"配了却被忽略"这件事说清楚。
    """
    if logger is None:
        return
    root = str(env.get("AGENT_RUNTIME_STORE_ROOT") or "").strip()
    path = str(env.get("AGENT_RUNTIME_STORE_PATH") or "").strip()
    db = str(env.get("AGENT_RUNTIME_DB") or "").strip()
    try:
        if kind == "sqlite":
            if not (path or db) and root:
                logger.warn(
                    "store.env_ignored",
                    backend="sqlite",
                    ignored="AGENT_RUNTIME_STORE_ROOT",
                    hint="sqlite 认 AGENT_RUNTIME_STORE_PATH / AGENT_RUNTIME_DB；"
                         "当前落 :memory:，重启即丢",
                    effective=":memory:")
            elif path and db and path != db:
                logger.warn("store.env_conflict", backend="sqlite",
                            used="AGENT_RUNTIME_STORE_PATH", ignored=db)
        elif kind == "file":
            if path and not root:
                logger.warn(
                    "store.env_alias",
                    backend="file",
                    note="file 后端读 AGENT_RUNTIME_STORE_ROOT；"
                         "本次把 AGENT_RUNTIME_STORE_PATH 当 root 用",
                    effective=path)
            elif root and path and root != path:
                logger.warn("store.env_conflict", backend="file",
                            used="AGENT_RUNTIME_STORE_ROOT", ignored=path)
    except Exception:                   # noqa: BLE001 - 观测面 fail-open
        pass


def _make_store(env: Mapping[str, str], logger: Any = None) -> Any:
    """按 `AGENT_RUNTIME_STORE` 造 Context 存储（memory / sqlite / file / redis）。"""
    kind = str(env.get("AGENT_RUNTIME_STORE") or "memory").strip().lower()
    _store_env_notice(kind, env, logger)
    try:
        if kind == "memory":
            from core.store import MemoryStore
            return MemoryStore()
        if kind == "sqlite":
            from core.sqlite_store import SQLiteStore
            # 别名兼容：`AGENT_RUNTIME_DB` 是更直观的名字（文档/直觉都用它），
            # 但早期实现只认 `AGENT_RUNTIME_STORE_PATH` —— 配了前者会**静默退化成
            # 内存库**（surface 表现是"重启后数据没了"）。两个都认，冲突时以显式
            # 的 STORE_PATH 为准；被忽略的那个由 `_store_env_notice` 出声。
            path = (env.get("AGENT_RUNTIME_STORE_PATH")
                    or env.get("AGENT_RUNTIME_DB") or ":memory:")
            return SQLiteStore(path)
        if kind == "file":
            from core.store import FileStore
            # **`STORE_PATH` 也认**：它比 `STORE_ROOT` 更通用，绝大多数人会先
            # 试它；只认 ROOT 的结果是"配了路径却落在默认目录"，且只在
            # `/v1/turns/{ctx_id}/resume` 404 时才暴露。
            root = (env.get("AGENT_RUNTIME_STORE_ROOT")
                    or env.get("AGENT_RUNTIME_STORE_PATH") or "")
            return FileStore(root) if root else FileStore()
        if kind == "redis":
            # **会话/检查点跨副本的唯一路径**。
            #
            # 之前 `RedisStore` 已实现但这里没有分支，于是"想跨副本"只能选
            # sqlite/file —— 而那两种是**单机文件**，多副本各写各的，
            # 会话读不到对方写的轮次。实测过：A 写 B 读，`turn_index` 归零。
            #
            # 这里显式要求 KV 是 redis（而不是复用 `AGENT_RUNTIME_KV`）：
            # 因为 Context 存储与限流计数是两个关注点，后者可能是 memory
            # （单机够用），但前者跨副本必须共享。
            from core.kv import RESPKV
            from core.redis_store import RedisStore
            kv = RESPKV(host=env.get("AGENT_RUNTIME_REDIS_HOST") or "127.0.0.1",
                        port=int(env.get("AGENT_RUNTIME_REDIS_PORT") or 6379),
                        db=int(env.get("AGENT_RUNTIME_REDIS_DB") or 0),
                        password=env.get("AGENT_RUNTIME_REDIS_PASSWORD") or "",
                        key_prefix=env.get("AGENT_RUNTIME_KEY_PREFIX") or "rt:")
            if not kv.ping():
                raise RuntimeError(
                    "AGENT_RUNTIME_STORE=redis 但 Redis 连不上"
                    "（跨副本存储必须是共享的，不能静默退回进程内）")
            ttl = float(env.get("AGENT_RUNTIME_STORE_TTL_S") or 86400)
            return RedisStore(kv, ttl_s=ttl, prefix="ctx:")
        return _Unavailable(
            "store", f"未知 AGENT_RUNTIME_STORE={kind!r}（memory/sqlite/file/redis）")
    except Exception as exc:            # noqa: BLE001 - 构造失败 = 不 ready，不是崩溃
        return _Unavailable("store", f"{type(exc).__name__}: {exc}")


def _make_kv(env: Mapping[str, str]) -> Any:
    """按 `AGENT_RUNTIME_KV` 造原子 KV（memory / redis）。"""
    kind = str(env.get("AGENT_RUNTIME_KV") or "memory").strip().lower()
    try:
        if kind == "memory":
            from core.kv import InMemoryKV
            return InMemoryKV()
        if kind == "redis":
            from core.kv import RESPKV
            return RESPKV(host=env.get("AGENT_RUNTIME_REDIS_HOST") or "127.0.0.1",
                          port=int(env.get("AGENT_RUNTIME_REDIS_PORT") or 6379),
                          db=int(env.get("AGENT_RUNTIME_REDIS_DB") or 0),
                          password=env.get("AGENT_RUNTIME_REDIS_PASSWORD") or "",
                          key_prefix=env.get("AGENT_RUNTIME_KEY_PREFIX") or "rt:")
        return _Unavailable("kv", f"未知 AGENT_RUNTIME_KV={kind!r}（memory/redis）")
    except Exception as exc:            # noqa: BLE001
        return _Unavailable("kv", f"{type(exc).__name__}: {exc}")


def _make_vector(env: Mapping[str, str]) -> Any:
    """按 `VECTOR_BACKEND` 造向量后端（默认 memory）。

    刻意**不**用 `configure_vector_backend()` 的 `auto`：auto 会挑第一个装了库的后端
    （本机是 qdrant），于是"服务启动时到底连了什么"取决于机器上装了什么 ——
    这种不可复现的默认值在生产里是事故来源。默认 memory，要外部后端就显式声明。
    """
    kind = str(env.get("VECTOR_BACKEND") or "memory").strip().lower()
    try:
        from plane.vector_backends import configure_vector_backend
        result = configure_vector_backend({
            "VECTOR_BACKEND": kind,
            "VECTOR_DIM": str(env.get("VECTOR_DIM") or 0),
            "QDRANT_URL": str(env.get("QDRANT_URL") or ""),
            "QDRANT_API_KEY": str(env.get("QDRANT_API_KEY") or ""),
            "QDRANT_LOCATION": str(env.get("QDRANT_LOCATION") or ":memory:"),
            "QDRANT_COLLECTION": str(env.get("QDRANT_COLLECTION") or "vectors"),
            "MILVUS_URI": str(env.get("MILVUS_URI") or "http://127.0.0.1:19530"),
            "MILVUS_TOKEN": str(env.get("MILVUS_TOKEN") or ""),
            "MILVUS_COLLECTION": str(env.get("MILVUS_COLLECTION") or "vectors"),
        })
        backend = result.get("backend")
        if backend is None:
            return _Unavailable("vector", str(result.get("reason") or "后端不可用"))
        return backend
    except Exception as exc:            # noqa: BLE001
        return _Unavailable("vector", f"{type(exc).__name__}: {exc}")


def _info_dict(payload: Any) -> dict:
    """把各种 `info()` 的返回值归一成 dict（Mapping / dataclass / 有 to_dict 的对象）。"""
    if isinstance(payload, Mapping):
        return dict(payload)
    to_dict = getattr(payload, "to_dict", None)
    if callable(to_dict):
        try:
            data = to_dict()
            if isinstance(data, Mapping):
                return dict(data)
        except Exception:               # noqa: BLE001
            pass
    if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
        try:
            return dataclasses.asdict(payload)
        except Exception:               # noqa: BLE001
            pass
    return {"note": str(payload)}


#: URL 里的 userinfo（`redis://user:pass@host` → `redis://user:***@host`）
_CREDENTIAL_IN_URL = re.compile(r"(?i)(://[^/\s:@]+:)[^/\s@]+@")
#: `key=value` 形状的凭证（`api_key=sk-...` → `api_key=***`）
_KEY_VALUE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|authorization)\b\s*[=:]\s*[^\s,;}\]]+")


def _scrub(text: str) -> str:
    """把探针/自述里的凭证形状抹掉。

    探针的 detail 是给运维看的，可能原样带上连接串（Redis URL / Milvus token）。
    `/readyz` 通常挂在**不需要鉴权**的端口上 —— 那就是一个凭证泄漏面。
    """
    if not text:
        return ""
    out = _CREDENTIAL_IN_URL.sub(r"\1***@", str(text))
    return _KEY_VALUE.sub(lambda m: f"{m.group(1)}=***", out)


def _probe_inner(spec: DepSpec) -> dict:
    """单依赖探测。优先 ping()，其次 info()，再退化到 stats()/exists()。"""
    obj = spec.obj
    base: dict[str, Any] = {"critical": bool(spec.critical)}
    if obj is None:
        return {**base, "ok": False, "probe": "none", "detail": "依赖对象为 None"}

    # ① ping()：最能表达"服务端还活着"（Redis 就是这个）
    ping = getattr(obj, "ping", None)
    if callable(ping):
        ok = bool(ping())
        return {**base, "ok": ok, "probe": "ping",
                "detail": "" if ok else "ping() 返回 False（服务端不可达）"}

    # ② info()：后端自述。`available=False` 就是"配了但没生效"
    info = getattr(obj, "info", None)
    if callable(info):
        data = _info_dict(info())
        available = data.get("available")
        ok = True if available is None else bool(available)
        detail = data.get("reason") or data.get("note") or ""
        return {**base, "ok": ok, "probe": "info",
                "kind": str(data.get("kind") or data.get("backend") or ""),
                "detail": _scrub(str(detail))}

    # ③ stats()：SQLite/File store 的自述（读一次元数据，不写）
    stats = getattr(obj, "stats", None)
    if callable(stats):
        payload = stats()
        return {**base, "ok": True, "probe": "stats",
                "kind": type(obj).__name__,
                "detail": _scrub(json.dumps(payload, ensure_ascii=False, default=str)[:200])}

    # ④ exists()：只读探针（MemoryStore 只有这个）。返回 False 也正常：key 就是不存在
    exists = getattr(obj, "exists", None)
    if callable(exists):
        exists(_PROBE_KEY)
        return {**base, "ok": True, "probe": "exists",
                "kind": type(obj).__name__, "detail": ""}

    # ⑤ 什么探针都没有：只证明对象存在，**如实标注**探针级别
    return {**base, "ok": True, "probe": "none", "kind": type(obj).__name__,
            "detail": "该依赖没有可调用探针（只证明对象已构造）"}


def _probe(spec: DepSpec) -> dict:
    """`_probe_inner` + 耗时。异常一律变成 `ok=False`（探针不许抛）。"""
    started = time.perf_counter()
    try:
        result = _probe_inner(spec)
    except Exception as exc:            # noqa: BLE001 - 探针自己炸了 = 依赖不可用
        result = {"critical": bool(spec.critical), "ok": False, "probe": "error",
                  "detail": _scrub(f"{type(exc).__name__}: {exc}")}
    result["latency_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
    return result


def _probe_bounded(spec: DepSpec, timeout_s: float) -> dict:
    """带超时的单依赖探测（启动快照用）。

    为什么必须**有界**：`create_app` 是同步的，一个连不上的 Redis / 挂住的 Milvus
    会让进程在"启动中"无限等待 —— 那时既没有 `/healthz` 也没有日志，运维只能看到
    "容器起来了但一直不 ready"。

    实现取舍：守护线程 + join(timeout)。**不用** ThreadPoolExecutor：它的
    `__exit__` 会 join 所有 worker，超时语义立刻失效（超时后仍然要等挂住的那个）。
    守护线程不阻止进程退出，超时后如实报告 `probe="timeout"`，线程自己随请求结束。
    """
    box: dict = {}
    started = time.perf_counter()

    def _run() -> None:
        box.update(_probe(spec))

    thread = threading.Thread(target=_run, name=f"readyz-probe-{spec.name}",
                              daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        return {"critical": bool(spec.critical), "ok": False, "probe": "timeout",
                "detail": f"探针超过 {timeout_s:.1f}s 未返回",
                "latency_ms": round((time.perf_counter() - started) * 1000.0, 3)}
    if not box:
        return {"critical": bool(spec.critical), "ok": False, "probe": "error",
                "detail": "探针没有返回任何结果"}
    return box


def probe_all(specs: Sequence[DepSpec],
              timeout_s: float = READY_TIMEOUT_S) -> dict[str, dict]:
    """同步探一遍（`create_app` 的启动快照用；此时还没有事件循环）。

    逐个探（启动期并发收益 < 复杂度），每个都带独立超时。
    """
    return {spec.name: _probe_bounded(spec, timeout_s) for spec in specs}


async def probe_all_async(specs: Sequence[DepSpec],
                          timeout_s: float = READY_TIMEOUT_S) -> dict[str, dict]:
    """并发探一遍，每个探针独立超时（`/readyz` 用）。

    并发而不是串行：三个依赖各 2s 超时串起来就是 6s，kubelet 的默认 timeout 是 1s ——
    探针自己成了超时源。独立超时：一个挂住的依赖不该掩盖另外两个的真实状态。
    """
    async def _one(spec: DepSpec) -> dict:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_probe_bounded, spec, timeout_s), timeout_s + 0.5)
        except (asyncio.TimeoutError, TimeoutError):
            return {"critical": bool(spec.critical), "ok": False, "probe": "timeout",
                    "detail": f"探针超过 {timeout_s:.1f}s 未返回"}
        except Exception as exc:        # noqa: BLE001
            return {"critical": bool(spec.critical), "ok": False, "probe": "error",
                    "detail": _scrub(f"{type(exc).__name__}: {exc}")}

    results = await asyncio.gather(*(_one(spec) for spec in specs))
    return {spec.name: result for spec, result in zip(specs, results)}


def _ready(checks: Mapping[str, Mapping[str, Any]]) -> bool:
    """关键依赖全 ok 才是 ready。**空集也算 ready**（没有依赖就没有依赖故障）。"""
    return all(bool(check.get("ok"))
               for check in checks.values() if check.get("critical", True))


# ══════════════════════════════════════════════════════════════
# 四、SSE（Server-Sent Events）
# ══════════════════════════════════════════════════════════════
def sse_frame(event: str, data: Mapping[str, Any],
              seq: int | None = None) -> str:
    """一条 SSE 事件：`event: <type>` + `data: <json>` + 空行。

    空行是**分隔符**，不是排版：少了它，客户端会把两条事件拼成一条，
    而 JSON 恰好能拼出合法的第二行 —— 故障表现为"少了一个事件"，不报错。

    `seq` 必须带上：增量流里事件多而杂（accepted / started / step* / completed），
    客户端要能发现"中间漏了一条"。缺了它，断线重连后无法对账。
    """
    body = dict(data)
    if seq is not None:
        body["seq"] = int(seq)
    payload = json.dumps(body, ensure_ascii=False, separators=(",", ":"),
                         default=str, sort_keys=False)
    return f"event: {event}\ndata: {payload}\n\n"


def _advance(iterator: Any) -> Any:
    """取下一个事件；耗尽返回 None。

    在**线程池**里调用（`handle_stream` 是同步生成器）：返回 None 而不是让
    `StopIteration` 穿过 future（`Future.set_exception(StopIteration)` 会被
    asyncio 判为非法）。
    """
    try:
        return next(iterator)
    except StopIteration:
        return None


def _close_quietly(iterator: Any) -> None:
    """关掉同步生成器（触发它的 finally / with）。关闭失败不掩盖真正的原因。"""
    close = getattr(iterator, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception:                   # noqa: BLE001
        pass


#: SSE 心跳间隔（秒）。**不是可选项**：回合里"模型思考 30 秒"是常态，
#: 这段时间一个事件都没有，而 nginx 默认 `proxy_read_timeout 60s`、
#: 很多 CDN 是 30s —— 连接会被上游直接掐断，客户端看到的是"流莫名其妙结束"。
#: SSE 规范里的注释行（以 `:` 开头）客户端会忽略，正好用来保活。
SSE_HEARTBEAT_S = 15.0

def _sse_heartbeat_from_env(env: Mapping[str, str] | None = None) -> float:
    """心跳间隔：环境变量 → 常量。非法值报错（不静默用默认）。"""
    import os as _os
    raw = (env if env is not None else _os.environ).get(
        "AGENT_RUNTIME_SSE_HEARTBEAT_S")
    if raw is None or str(raw).strip() == "":
        return SSE_HEARTBEAT_S
    try:
        val = float(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"AGENT_RUNTIME_SSE_HEARTBEAT_S 不是数字: {raw!r}") from exc
    if val <= 0:
        raise ValueError(
            f"AGENT_RUNTIME_SSE_HEARTBEAT_S 必须 > 0（收到 {val}）；"
            f"要关心跳请设很大的值，而不是 0")
    return val


def sse_heartbeat(comment: str = "keepalive") -> str:
    """一条 SSE 注释帧。客户端按规范忽略，但字节流动着，代理就不会掐连接。"""
    return f": {comment}\n\n"


async def _event_stream(gateway: Any, request: dict,
                        shutdown: ShutdownCoordinator,
                        logger: JsonLogger | None = None,
                        heartbeat_s: float | None = None) -> AsyncIterator[str]:
    """把 Gateway 的同步事件流变成 SSE 字节流。

    四件事必须同时成立：
    1. **不阻塞事件循环**：`next()` 丢线程池（LLM/工具是阻塞 IO）。
    2. **静默期要保活**：见 `SSE_HEARTBEAT_S`。为此用一个后台搬运任务 +
       `asyncio.Queue`，主循环 `wait_for(queue.get(), timeout)` 超时就吐心跳。
       **不能**直接 `await to_thread(next, it)` —— 那会卡在 future 上，
       超时也没机会 yield（这正是早期版本 5 秒静默的原因）。
    3. **客户端断开不崩**：`CancelledError` / `GeneratorExit` 是"接收方走了"，
       只做资源收口（关生成器 + 飞行中计数归零），不向上抛。
    4. **不吞掉真正的故障**：迭代中抛出的其它异常 → 补一条 `completed(ok=false)`，
       让客户端状态机仍然能收口（它不该靠"连接突然断了"判断失败）。

    `heartbeat_s=None` 时读 `AGENT_RUNTIME_SSE_HEARTBEAT_S`，再退到常量。
    **不能在签名里写 `= SSE_HEARTBEAT_S`**：那是 import 期绑定，
    之后改环境变量/模块常量都不生效（实测踩过，心跳永远是旧值）。
    """
    if heartbeat_s is None:
        heartbeat_s = _sse_heartbeat_from_env()
    iterator: Any = None
    tracker: Any = None
    pump: asyncio.Task[None] | None = None
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1024)
    _DONE = object()

    def _run_pump() -> None:
        """在线程里把同步生成器抽干，塞进 asyncio 队列。"""
        try:
            while True:
                ev = _advance(iterator)
                if ev is None:
                    break
                asyncio.run_coroutine_threadsafe(queue.put(ev), loop).result()
        except Exception as exc:                                # noqa: BLE001
            asyncio.run_coroutine_threadsafe(
                queue.put({"__error__": f"{type(exc).__name__}: {exc}"}), loop).result()
        finally:
            asyncio.run_coroutine_threadsafe(queue.put(_DONE), loop).result()

    try:
        loop = asyncio.get_running_loop()
        iterator = gateway.handle_stream(request)
        # 飞行中计数：SIGTERM 的 drain 必须知道"还有一个流没吐完"
        tracker = shutdown.track("turn_stream")
        tracker.__enter__()

        # 注意：**不发 `retry:` 字段**。这个端点是 POST + 请求体，
        # 浏览器 EventSource 无法重放同一个回合，发 `retry:` 等于告诉客户端
        # "重连就能续上"——续不上。做不到的事不该在协议里承诺。
        # 需要断点续传的场景走 `ctx_id` 续跑（Gateway 本来就支持）。
        pump = asyncio.create_task(asyncio.to_thread(_run_pump))
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=heartbeat_s)
            except asyncio.TimeoutError:
                # 静默期：吐一条注释保活。客户端忽略内容，但连接不会闲置超时。
                yield sse_heartbeat()
                continue
            if item is _DONE:
                break
            if isinstance(item, Mapping) and "__error__" in item:
                raise RuntimeError(str(item["__error__"]))
            if isinstance(item, Mapping):
                kind = str(item.get("type") or "message")
                data = item.get("data") if isinstance(item.get("data"), Mapping) else {}
                # **与 HTTP 状态码保持一致**：`ok=True` 在 JSON 端点会因
                # `term_reason=UNRECOVERABLE` 被判成 502（依赖故障）。流式端点
                # 只有 `completed` 一个终态事件，客户端就靠它的 `ok` 判断成败 ——
                # 这里不改的话，同一个故障在 JSON 端点是 502、在 SSE 里是"成功"。
                if kind == "completed" and isinstance(data, Mapping):
                    if str(data.get("term_reason") or "") == "UNRECOVERABLE":
                        data = {**data, "ok": False,
                                "upstream_unavailable": True}
                # seq 原样带出（增量流靠它检测丢帧/断点）
                yield sse_frame(kind, data, item.get("seq"))
    except (asyncio.CancelledError, GeneratorExit):
        # 客户端断开：响应体已经没有接收方。这里**不能** yield（GeneratorExit 之后
        # yield 会变成 RuntimeError），只做清理然后收口。
        if logger is not None:
            try:
                logger.debug("sse.client_gone", inflight=shutdown.inflight())
            except Exception:           # noqa: BLE001 - 观测面 fail-open
                pass
    except Exception as exc:            # noqa: BLE001
        if logger is not None:
            try:
                logger.error("sse.stream_failed",
                             error=f"{type(exc).__name__}: {exc}")
            except Exception:           # noqa: BLE001
                pass
        yield sse_frame("completed", {
            "ok": False, "text": "", "steps": 0, "phase": "", "term_reason": "",
            "error": f"internal_error: {type(exc).__name__}: {exc}", "audit_id": "",
        })
    finally:
        if pump is not None:
            pump.cancel()
        _close_quietly(iterator)
        if tracker is not None:
            try:
                tracker.__exit__(None, None, None)
            except Exception:           # noqa: BLE001
                pass


# ══════════════════════════════════════════════════════════════
# 五、SIGTERM 接线
# ══════════════════════════════════════════════════════════════
class SignalDriver:
    """信号 → `ShutdownCoordinator.begin()` → `wait()` → `on_drained(report)`。

    为什么不在信号处理函数里直接 `wait()`：信号处理函数在事件循环线程里跑
    （`loop.add_signal_handler` 的回调就是循环回调），在里面阻塞等待等于停掉整个
    循环 —— 飞行中的请求再也没机会完成，drain 必然"超时"，而且超时前
    `/readyz` 也已经不响应了。所以这里只做两件轻活：`begin()`（停接新）+ 起一个
    后台任务去做阻塞的 `wait()`。

    幂等：第二次 SIGTERM（运维等不及又按了一次 Ctrl-C）不会再起一个 drain 任务，
    否则钩子会被"再排一次队"，而 flush 只需要一次。
    """

    #: 接管哪两个信号。SIGINT 也接：Ctrl-C 在容器里等价于"请你体面退出"。
    SIGNALS: tuple[int, ...] = (signal.SIGTERM, signal.SIGINT)

    def __init__(self, shutdown: ShutdownCoordinator, *,
                 logger: JsonLogger | None = None,
                 on_drained: Callable[[dict], None] | None = None) -> None:
        self.shutdown = shutdown
        self.logger = logger
        self.on_drained = on_drained
        self._loop: Any = None
        self._task: Any = None
        self._triggered = False
        #: 真正注册成功的信号（非主线程 / 不支持 add_signal_handler 的平台会是空）
        self.installed: list[int] = []

    # ── 注册 ────────────────────────────────────────────────
    def install(self, loop: Any) -> list[int]:
        """注册信号处理，返回成功的信号列表。

        失败**不抛**：`loop.add_signal_handler` 只能在主线程的循环里用
        （TestClient、子线程里的 uvicorn worker、Windows 都会失败）。失败时如实
        返回空列表 + warn —— 启动不该因为"信号接不上"而失败，但运维必须看见。
        """
        self._loop = loop
        installed: list[int] = []
        for sig in self.SIGNALS:
            try:
                loop.add_signal_handler(int(sig), self.trigger)
            except (AttributeError, NotImplementedError, RuntimeError,
                    ValueError, OSError) as exc:
                self._log("warn", "signal.install_failed",
                          signal=int(sig), error=f"{type(exc).__name__}: {exc}")
                continue
            installed.append(int(sig))
        self.installed = installed
        return installed

    # ── 触发 ────────────────────────────────────────────────
    def trigger(self) -> Any:
        """进入 draining 并排空。返回后台任务（无可用事件循环时返回 None）。

        信号回调、测试、`main()` 的兜底路径都走这一个入口：**只有一条关停路径**，
        否则"信号关停"和"代码关停"会各有一套时序。
        """
        if self._triggered:
            return self._task
        self._triggered = True
        self.shutdown.begin()
        self._log("warn", "server.draining",
                  inflight=self.shutdown.inflight(),
                  active=self.shutdown.active_names())
        loop = self._loop
        if loop is None or not getattr(loop, "is_running", lambda: False)():
            # 没有正在跑的循环：同步排空（调用方自己承担阻塞，通常是 main 的兜底）
            report = self.shutdown.wait()
            self._finish(report)
            return None
        self._task = loop.create_task(self._drain())
        return self._task

    async def _drain(self) -> dict:
        """在线程里等 `wait()`：它内部是阻塞的 Condition.wait，且会跑清理钩子。"""
        try:
            report = await asyncio.to_thread(self.shutdown.wait)
        except Exception as exc:        # noqa: BLE001 - 关停路径不许抛
            report = {"drained": False, "remaining": -1,
                      "error": f"{type(exc).__name__}: {exc}"}
        self._finish(report)
        return report

    def _finish(self, report: Any) -> None:
        self._log("info", "server.drained", report=report)
        if self.on_drained is None:
            return
        try:
            self.on_drained(report if isinstance(report, dict) else {"report": report})
        except Exception:               # noqa: BLE001
            pass

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            getattr(self.logger, level)(event, **fields)
        except Exception:               # noqa: BLE001 - 日志坏了不影响关停
            pass


# ══════════════════════════════════════════════════════════════
# 六、观测面小工具（全部 fail-open）
# ══════════════════════════════════════════════════════════════
def _bootstrap_metrics(metrics: Any) -> None:
    """先落一个 `up` gauge。

    不这么做，一个"刚起来还没跑过回合"的进程 `/metrics` 是**空 body** ——
    抓取端看到空 exposition 会以为指标端点坏了，而实际上只是还没有样本。
    先声明再采样：抓取端拿到的至少是合法文本（含 # HELP / # TYPE）。
    """
    for call in (lambda: metrics.describe("up", "Runtime process is up (1 = alive)"),
                 lambda: metrics.gauge("up", 1.0, {"version": VERSION}),
                 # 这三个是**告警规则要查的**指标。没有它们，三条告警就写不出来，
                 # 只能靠人工核对 /v1/info —— 而那在夜里没人做。
                 lambda: metrics.describe(
                     "info", "Runtime build/config info (always 1)"),
                 lambda: metrics.describe(
                     "checkpoint_errors_total", "Side-effect checkpoint write failures"),
                 lambda: metrics.describe(
                     "workflow_running", "Workflows currently running"),
                 lambda: metrics.describe("http_requests_total",
                                          "HTTP requests by path and status")):
        try:
            call()
        except Exception:               # noqa: BLE001 - 注入的假 metrics 可能没这些方法
            pass


#: 允许作为指标 label 出现的路径（**白名单**）。
#: 为什么需要它：鉴权拒绝发生在路由之前，`path` 完全是攻击者可控的输入。
#: 把随机 URL 直接当 label，prometheus_client 会为每个 URL 建一个时间序列 ——
#: 几万个随机路径就能把进程内存打爆（指标本身成了 DoS 面）。
_METRIC_PATHS = frozenset({"/v1/turns", "/v1/turns/stream", "/metrics",
                           "/healthz", "/readyz", "/v1/info", "/v1/whoami",
                           # workflow 的三个路径共用同一个 label：`/v1/workflows/{id}`
                           # 里的 id 是客户端给的，绝不能进指标（基数爆炸）。
                           "/v1/workflows"})


def _metric_path(path: Any) -> str:
    """路径 → 指标 label 的安全取值：白名单外一律归到 `<other>`。"""
    text = str(path or "")
    return text if text in _METRIC_PATHS else "<other>"


def _observe_http(metrics: Any, path: str, status: int) -> None:
    """记一次 HTTP 请求（fail-open：指标坏了不影响响应）。"""
    try:
        metrics.counter("http_requests_total",
                        {"path": str(path), "status": str(int(status))})
    except Exception:                   # noqa: BLE001
        pass


def _flush_observability(metrics: Any, logger: Any) -> None:
    """关停钩子：渲染一次指标 + 记一行日志。

    为什么要"渲染一次"而不是什么都不做：`Metrics` 是进程内的，随进程消失。
    在退出前把 exposition 渲染一遍，能让"最后一次抓取失败"的部署至少留下一个
    字节数证据（日志里 bytes=N），也能暴露"渲染本身坏了"这个问题。
    """
    try:
        text = metrics.render()
    except Exception as exc:            # noqa: BLE001
        try:
            logger.error("metrics.flush_failed",
                         error=f"{type(exc).__name__}: {exc}")
        except Exception:               # noqa: BLE001
            pass
        return
    size = len(text) if isinstance(text, (bytes, bytearray, str)) else 0
    try:
        logger.info("metrics.flushed", bytes=int(size))
    except Exception:                   # noqa: BLE001
        pass


def _http_error_name(status: int) -> str:
    return {400: "bad_request", 401: "unauthorized", 403: "forbidden",
            404: "not_found", 405: "method_not_allowed",
            422: "validation_error"}.get(int(status), "http_error")


#: `/v1/info` 允许回显的环境变量（**白名单**，永远不 dump os.environ）
_ENV_ALLOWLIST = ("VECTOR_BACKEND", "VECTOR_DIM", "AGENT_RUNTIME_STORE",
                  "AGENT_RUNTIME_KV", "AGENT_RUNTIME_LLM",
                  "AGENT_RUNTIME_PROMPT_TOKENS", "AGENT_RUNTIME_METRIC_PREFIX",
                  "AGENT_RUNTIME_ALLOW_ANONYMOUS", "AGENT_RUNTIME_AUTH",
                  # 三道资源边界的开关：运维要能一眼看到"到底关了没有"
                  # （值只有数字，没有半个密钥）
                  "AGENT_RUNTIME_MAX_BODY_BYTES", "AGENT_RUNTIME_HTTP_RATE",
                  "AGENT_RUNTIME_REQUEST_TIMEOUT_S",
                  # workflow 的容量与后台线程数（同样是数字，没有密钥）
                  "AGENT_RUNTIME_WORKFLOW_MAX", "AGENT_RUNTIME_WORKFLOW_WORKERS")

#: `/v1/info` 里自报"装没装"的第三方模块（可选依赖的可用性是部署事实）
_MODULE_PROBES = ("fastapi", "uvicorn", "pydantic", "prometheus_client", "redis",
                  "tiktoken", "jsonschema", "qdrant_client", "pymilvus",
                  "milvus_lite", "opentelemetry.sdk")


def _module_available(name: str) -> bool:
    """模块在不在（不 import：import 一个后端库可能有副作用）。"""
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, AttributeError, ValueError, ModuleNotFoundError):
        return False


def _env_snapshot() -> dict:
    """只回显白名单里的环境变量，值过 scrub。

    为什么不 dump 整个 `os.environ`：里面有 API key、DB 密码、token。`/v1/info`
    常常被放在不需要鉴权的内部端口上 —— 那就是一次全量凭证泄漏。
    """
    out: dict[str, str] = {}
    for name in _ENV_ALLOWLIST:
        value = os.environ.get(name)
        if value is None:
            continue
        out[name] = _scrub(str(value))[:128]
    return out


def _auth_snapshot(state: Any) -> dict:
    """鉴权自述：只回答"有没有"，**绝不**回答"是什么"。

    为什么不给密钥长度：密钥若由低熵口令派生，长度会缩小搜索空间。
    连"有几把 key"都只给数量（`api_keys: 2`），不给任何可枚举的标识
    （key 的哈希也不行 —— 它可被拿去撞库比对）。
    """
    authenticator = getattr(state, "authenticator", None)
    config = getattr(state, "auth_config", None) or {}
    # `mode` 是给运维的**一眼结论**：enforced = 装了鉴权；disabled = 显式放行
    # （principal 由请求体给出 = 任何客户端都能自称任意租户）。它和 `enabled`
    # 是同一件事的两种说法，但"disabled"这个词不会被误读成"暂时没数据"。
    mode = str(getattr(state, "auth_mode", "") or
               ("enforced" if authenticator is not None else "disabled"))
    if authenticator is None:
        return {"enabled": False, "mode": mode, "schemes": [], "api_keys": 0,
                "jwt": False, "allow_anonymous": False, "exempt_paths": [],
                "metrics_requires_auth": None, "metrics_token": False}
    return {
        "enabled": True,
        "mode": mode,
        "schemes": list(getattr(authenticator, "schemes", ()) or ()),
        "api_keys": len(getattr(authenticator, "api_keys", {}) or {}),
        "jwt": bool(getattr(authenticator, "jwt_secret", "")),
        "jwt_algorithms": list(getattr(authenticator, "jwt_algorithms", ()) or ()),
        "jwt_issuer": bool(getattr(authenticator, "jwt_issuer", "")),
        "jwt_audience": bool(getattr(authenticator, "jwt_audience", "")),
        "allow_anonymous": bool(getattr(authenticator, "allow_anonymous", False)),
        "exempt_paths": list(config.get("exempt_paths", ()) or ()),
        # None = 除豁免外全保护（默认拒绝），别把它显示成 []（那读起来像"谁都不保护"）
        "protect_paths": (None if config.get("protect_paths") is None
                          else list(config["protect_paths"])),
        "metrics_requires_auth": config.get("metrics_requires_auth"),
        "metrics_token": bool(config.get("metrics_token")),
    }


# ══════════════════════════════════════════════════════════════
# 六之二、启动期鉴权判定（fail-closed）
# ══════════════════════════════════════════════════════════════
#: 一份**可以照着敲**的修复建议。它必须出现在 `StartupRefused` 的消息里：
#: 读到这条消息的人是在终端前，不是在文档前，他手里只有这一个终端。
STARTUP_REFUSED_MESSAGE = """拒绝启动：未配置任何凭据方式。
修复方式（任选其一）：
  export AGENT_RUNTIME_API_KEYS='sk-xxx:acme:runtime:run tool.read#u_alben'
  export AGENT_RUNTIME_JWT_SECRET=$(openssl rand -base64 48)
  export AGENT_RUNTIME_ALLOW_ANONYMOUS=1   # 仅限本机/内网，principal 由请求体给"""


class StartupRefused(RuntimeError):
    """拒绝启动：配置不足以安全地对外服务。

    为什么是异常而不是"WARN 一行继续跑"：不装鉴权时 principal 由**请求体**给出，
    任何客户端都能自称任意租户/任意 scope。这样的进程起在公网上，每次成功响应
    都是一次越权；而"启动失败"发生在任何人连上之前 —— 最便宜的失败点是启动。

    消息必须带可执行的修复命令（`STARTUP_REFUSED_MESSAGE`）：`"配置错误"` 这四个字
    修不好任何东西，尤其是在凌晨三点的值班终端上。

    继承 `RuntimeError` 而不是自定义基类：调用方（`scripts/`、`main()`）现有的
    `except RuntimeError` 兜底逻辑会自动接住它，不会变成一个没人处理的 traceback。
    """

    #: 固定的修复指引（测试断言它包含三个环境变量名）
    FIX = STARTUP_REFUSED_MESSAGE

    def __init__(self, message: str = STARTUP_REFUSED_MESSAGE) -> None:
        super().__init__(str(message))
        self.message = str(message)


#: `AGENT_RUNTIME_AUTH` 的"关"取值（显式开关：压过一切配置）
AUTH_OFF_VALUES = frozenset({"off", "0", "false", "no", "disabled", "none"})
#: `AGENT_RUNTIME_AUTH` 的"开"取值（压过 ALLOW_ANONYMOUS 的残留）
AUTH_ON_VALUES = frozenset({"on", "1", "true", "yes", "enforced", "required"})


def _has_credentials(env: Mapping[str, str]) -> bool:
    """配了**可校验的**凭据方式吗（API key 表 / JWT secret）。

    只看"有没有"，不看"对不对"：格式错误由 `parse_api_keys_spec` 抛 `ValueError`
    —— "你写了但写错了"与"你什么都没写"必须分开报，否则运维会去翻错文档。
    """
    spec = str(env.get("AGENT_RUNTIME_API_KEYS") or "").strip()
    secret = str(env.get("AGENT_RUNTIME_JWT_SECRET") or "").strip()
    return bool(spec) or bool(secret)


def resolve_auth_mode(*, env: Mapping[str, str] | None = None,
                      explicit: bool | None = None) -> str:
    """本次启动是"强制鉴权"还是"显式放行"：返回 `'enforced'` | `'disabled'`。

    判定顺序（先看显式意图，再看配置，最后落到**默认拒绝**）：

        1. `explicit`（调用方直接给 True/False）       → 最高优先级
        2. `AGENT_RUNTIME_AUTH=off/0/false/...`        → disabled（硬开关，压过配置）
           `AGENT_RUNTIME_AUTH=on/1/true/enforced/...`  → enforced
        3. 配了 API key 表或 JWT secret                 → enforced
        4. `AGENT_RUNTIME_ALLOW_ANONYMOUS=1`            → disabled
        5. 都没配                                       → **enforced**（默认拒绝）

    第 3 条压过第 4 条是刻意的：`ALLOW_ANONYMOUS=1` 常常是本机调试留下的残留，而
    "已经配好 key、却仍在裸奔"是最难被发现的事故（它看起来一切正常）。要让两台都
    放开，用第 2 条 —— 那是一个不会在"只是试一下"时被顺手留下的显式开关。

    返回 `'enforced'` **不等于**"一定能启动"：没凭据时它会交给
    `require_auth_config` 抛 `StartupRefused`。模式是"意图"，启动是"结果"。
    """
    e = dict(os.environ if env is None else env)
    if explicit is not None:
        return "enforced" if explicit else "disabled"
    switch = str(e.get("AGENT_RUNTIME_AUTH") or "").strip().lower()
    if switch in AUTH_OFF_VALUES:
        return "disabled"
    if switch in AUTH_ON_VALUES:
        return "enforced"
    if _has_credentials(e):
        return "enforced"
    if parse_bool(e.get("AGENT_RUNTIME_ALLOW_ANONYMOUS"), default=False):
        return "disabled"
    return "enforced"


def _startup_hint(env: Mapping[str, str]) -> str:
    """给 `StartupRefused` 补一句"你的配置错在哪"（最常见误配：布尔写成 0/false）。"""
    raw = env.get("AGENT_RUNTIME_ALLOW_ANONYMOUS")
    if raw is None or str(raw).strip() == "":
        return ""
    if parse_bool(raw):
        return ""
    return (f"\n注意：AGENT_RUNTIME_ALLOW_ANONYMOUS={str(raw)!r} 不是真值"
            f"（接受 1/true/yes/on），所以它没有放行本进程。")


def require_auth_config(*, env: Mapping[str, str] | None = None,
                        allow_anonymous: bool = False) -> Authenticator | None:
    """生产入口：**没有可用凭据就拒绝启动**；只有显式匿名才返回 None。

    返回 `None` 的语义只有一种：调用方**显式**要求匿名（`allow_anonymous=True`，
    或环境里 `AGENT_RUNTIME_ALLOW_ANONYMOUS=1` / `AGENT_RUNTIME_AUTH=off`）。
    此时进程不装鉴权中间件，principal 由**请求体**给出 —— 这是"知道自己在做什么的
    本机/内网 demo"所需要的，也是唯一被允许的裸奔路径；`main()` 会为它记一条 WARN。

    其余情况一律返回 `Authenticator`；拿不到就抛 `StartupRefused`（带修复命令）。
    配置写错（key 表格式不对、JWT 算法不认识）由 `build_authenticator_from_env`
    抛 `ValueError` —— 那是"写了但写错"，与"什么都没写"是两种事故，分开报。
    """
    e = dict(os.environ if env is None else env)
    mode = resolve_auth_mode(env=e, explicit=(False if allow_anonymous else None))
    if mode == "disabled":
        return None
    authenticator = build_authenticator_from_env(env=e)
    if authenticator is None:
        raise StartupRefused(STARTUP_REFUSED_MESSAGE + _startup_hint(e))
    return authenticator


# ══════════════════════════════════════════════════════════════
# 六之三、HTTP 层加固（请求体上限 / 限流 / 超时）
# ══════════════════════════════════════════════════════════════
# 为什么都写成**纯 ASGI** 中间件而不是 `BaseHTTPMiddleware`：后者会把响应包成
# `StreamingResponse` 再转发，SSE（`/v1/turns/stream`）在它中间多一次缓冲，
# 断连语义（`http.disconnect`）也跟着变复杂。这三件事都只需要"读几个 header、
# 必要时直接回一个响应"，纯 ASGI 足够且对数据流零介入。

#: 请求体上限默认值：1 MiB（一个 turn 的 JSON 远小于此；1 MiB 是"防手滑"而非"防大文件"）
DEFAULT_MAX_BODY_BYTES = 1024 * 1024
#: 每 IP 每分钟请求数上限；0 = 关闭（默认关：单机开发不该被自己的压测脚本卡住）
DEFAULT_RATE_PER_MINUTE = 0
#: 限流窗口（秒）
RATE_WINDOW_S = 60.0
#: 请求超时（秒）；0 = 关闭
DEFAULT_REQUEST_TIMEOUT_S = 0.0
#: 限流只作用于这些路径。探针/指标**必须**豁免 —— 限流把 `/healthz` 打到 429，
#: 编排器会判定实例已死并重启它，而实例其实是被自己的监控探针"限死"的。
DEFAULT_RATE_PATHS = ("/v1/*", "/v1")
#: 体量超限后仍"丢弃性读掉"的额度：为了连接可复用（见 `BodyLimitMiddleware`）
_BODY_DRAIN_BUDGET = 256 * 1024
#: 取消协程后的收尾宽限（秒）：给被取消的协程一点时间退出，别在 loop 里留 pending task
_CANCEL_GRACE_S = 0.05


def _env_number(env: Mapping[str, str], name: str, *, default: Any,
                cast: Callable[[Any], Any]) -> Any:
    """读一个数值型环境变量；空/未设 → default；写了非数字 → `ValueError`。

    **不许静默降级**：`AGENT_RUNTIME_MAX_BODY_BYTES=lO24m`（字母 O）如果被当成
    "没配"而回落到默认值，事故的报告就会变成"我们配了但没生效"，而不是"配错了"。
    """
    raw = env.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    text = str(raw).strip()
    try:
        return cast(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是数字，实际是 {text!r}") from exc


def _pick(env: Mapping[str, str], name: str, explicit: Any, *,
          default: Any, cast: Callable[[Any], Any]) -> Any:
    """显式参数 > 环境变量 > 默认值（三者的优先级必须写在一处，别散在调用点）。"""
    if explicit is not None:
        return cast(explicit)
    return _env_number(env, name, default=default, cast=cast)


def _raw_header(scope: Mapping[str, Any], name: bytes) -> str:
    """从 ASGI `scope["headers"]` 取一个 header（bytes 键、bytes 值、可重复）。"""
    for raw_name, raw_value in scope.get("headers") or ():
        if bytes(raw_name).lower() == name:
            return bytes(raw_value).decode("latin-1")
    return ""


def _declared_length(scope: Mapping[str, Any]) -> int | None:
    """`Content-Length`；没有/坏值 → None。"""
    raw = _raw_header(scope, b"content-length").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value >= 0 else None


async def _send_json(send: Any, status: int, payload: Mapping[str, Any], *,
                     extra_headers: Sequence[tuple[bytes, bytes]] = ()) -> None:
    """在**中间件层**直接回一个 JSON 响应（走不到 FastAPI 的错误出口，得自己拼）。

    形状与 `serve.py` 的其它错误一致：`{"error": <码>, "detail": <人读的>, "status": <码>}`。
    客户端只该学一套解析 —— 413 也不能是一段纯文本。
    """
    body = json.dumps(dict(payload), ensure_ascii=False).encode("utf-8")
    headers = [(b"content-type", b"application/json; charset=utf-8"),
               (b"content-length", str(len(body)).encode("latin-1"))]
    headers.extend((bytes(k), bytes(v)) for k, v in extra_headers)
    await send({"type": "http.response.start", "status": int(status),
                "headers": headers})
    await send({"type": "http.response.body", "body": body, "more_body": False})


def _safe_log(logger: Any, level: str, event: str, **fields: Any) -> None:
    """日志是观测面：**fail-open**（坏 sink 不能把请求带崩）。"""
    if logger is None:
        return
    try:
        getattr(logger, level)(event, **{k: v for k, v in fields.items()})
    except Exception:                   # noqa: BLE001 - 观测面永不抛
        pass


async def _read_bounded(receive: Any, limit: int) -> tuple[bytes, bool, bool]:
    """读到 `limit` 字节为止。

    返回 `(已收 body, 是否超限, 是否还有未读数据)`。超限时**立刻停手**：把剩下的
    字节继续往内存里收，正是这道限制要防的事。
    """
    chunks: list[bytes] = []
    total = 0
    while True:
        message = await receive()
        if message.get("type") != "http.request":
            # 客户端断开：交给下游（它自己的 receive 会拿到同样的信号）
            return b"".join(chunks), False, False
        chunk = message.get("body", b"") or b""
        total += len(chunk)
        if total > limit:
            # 越界的那一块也丢掉：反正整个请求都要被 413
            return b"".join(chunks), True, bool(message.get("more_body", False))
        chunks.append(chunk)
        if not message.get("more_body", False):
            return b"".join(chunks), False, False


async def _drain_bounded(receive: Any, budget: int) -> bool:
    """把剩余请求体**丢弃性**读掉，最多 `budget` 字节；返回是否读干净。

    为什么值得读：HTTP/1.1 的 keep-alive 要求服务器在回响应前消费掉请求体，否则
    这一条连接只能关掉。读掉一小段（默认 256 KiB）就能让"客户端多发了一点"这种
    常见情况保住连接复用；真正的洪水炸弹超过额度就 `Connection: close`。
    """
    total = 0
    while True:
        try:
            message = await receive()
        except Exception:               # noqa: BLE001 - 断连/协议错都当作"读不干净"
            return False
        if message.get("type") != "http.request":
            return True
        total += len(message.get("body", b"") or b"")
        if not message.get("more_body", False):
            return True
        if total > budget:
            return False


def _replay_receive(body: bytes, real_receive: Any) -> Any:
    """把一个**已经读完**的 body 伪装成 ASGI `receive`。

    第一次调用给已缓冲的 body，之后**一律转交给真正的 `receive`**。这里踩过一个
    必须记下来的坑：起初第二次调用直接返回 `http.disconnect`（"别让协程挂住"），
    结果 chunked 上传的 SSE 请求一帧事件都不发 —— Starlette 的 `StreamingResponse`
    用 `receive` 监听客户端断开，一个伪造的 `disconnect` 会让它立刻收掉整个流。
    生成的 `receive` 不能凭空编造连接状态，只能转交。
    """
    state = {"sent": False}

    async def receive() -> dict:
        if not state["sent"]:
            state["sent"] = True
            return {"type": "http.request", "body": body, "more_body": False}
        return await real_receive()

    return receive


#: ctx_id 的合法字符集。和 `core.store` 的约束保持一致（要能当文件名用），
#: 在端点上先拦一次：非法字符不该进 store 的键空间。
_CTX_ID_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}")


class ResumeRequest(BaseModel):
    """`POST /v1/turns/{ctx_id}/resume` 的请求体。

    **和 `POST /v1/turns` 带 decision 的区别**：这个端点的语义是"我在处理一个
    已挂起的任务"，路径里就写明 ctx_id，客户端不需要知道 goal、也不会误建新回合。
    `decision` 在这里**必填** —— 这个端点的唯一目的就是表态。

    `extra="forbid"`：写错字段必须 422，不能让"我以为批准了、实际没传"静默通过 ——
    那正是这个端点存在要解决的那类故障。
    """

    model_config = ConfigDict(extra="forbid")

    decision: bool = Field(..., description="true = 批准执行挂起的动作；false = 拒绝")
    reason: str = Field(default="", max_length=MAX_GOAL_CHARS,
                        description="人类给的理由（会写进 history，模型据此换方案）")
    principal: PrincipalBody = Field(default_factory=PrincipalBody)
    deadline_ms: float | None = Field(default=None, gt=0)
    budget: BudgetBody | None = Field(default=None)


class BodyLimitMiddleware:
    """请求体大小上限（纯 ASGI；**读 header 就拒**，不把超限的 body 收进内存）。

    两条路径：

      1. 有 `Content-Length` 且超过上限 → 立刻 413，**一个字节都不读**。
         "读完再拒"等于按攻击者声明的长度先把内存消耗一遍，限制形同虚设。
         代价：请求体没被消费，这条连接不能复用（响应带 `Connection: close`）。
      2. 无 `Content-Length`（chunked 上传）→ 有界缓冲，边读边量：越界立刻停手回
         413，再把剩余部分丢弃性读掉 `_BODY_DRAIN_BUDGET` 字节以内（连接可复用）；
         超出额度才 `Connection: close`。

    为什么不用 Starlette 内建的 `RequestBodyLimitMiddleware`：它的出口是
    `PlainTextResponse("Content Too Large")`，而本文件的纪律是"错误一律 JSON"
    （客户端只写一套解析）；它也没有 `Connection` 语义与 413 的指标/日志。
    """

    def __init__(self, app: Any, *, max_bytes: int = DEFAULT_MAX_BODY_BYTES,
                 logger: Any = None,
                 on_reject: Callable[[str, int], None] | None = None) -> None:
        self.app = app
        self.max_bytes = int(max_bytes)
        self.logger = logger
        self.on_reject = on_reject

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or self.max_bytes <= 0:
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path") or "")
        declared = _declared_length(scope)
        if declared is not None and declared > self.max_bytes:
            await self._reject(send, path=path, declared=declared, closed=True)
            return
        if declared is None:
            body, over, more_pending = await _read_bounded(receive, self.max_bytes)
            if over:
                drained = (not more_pending) or await _drain_bounded(
                    receive, max(_BODY_DRAIN_BUDGET, self.max_bytes))
                await self._reject(send, path=path, declared=None,
                                   closed=not drained)
                return
            await self.app(scope, _replay_receive(body, receive), send)
            return
        # 声明长度在上限内：直接透传（不为诚实客户端多复制一份 body）
        await self.app(scope, receive, send)

    async def _reject(self, send: Any, *, path: str, declared: int | None,
                      closed: bool) -> None:
        if self.on_reject is not None:
            try:
                self.on_reject(path, 413)
            except Exception:           # noqa: BLE001
                pass
        _safe_log(self.logger, "warn", "http.body_too_large",
                  path=str(path)[:128], max_bytes=self.max_bytes,
                  declared=declared, connection_closed=bool(closed))
        extra: list[tuple[bytes, bytes]] = []
        if closed:
            extra.append((b"connection", b"close"))
        await _send_json(
            send, 413,
            {"error": "payload_too_large",
             "detail": f"请求体超过上限（{self.max_bytes} 字节），未读取",
             "status": 413},
            extra_headers=extra)


def _ip_in_networks(ip: str, networks: Sequence[str]) -> bool:
    """`ip` 是否落在任一 CIDR 里，或**字面量相等**。

    为什么要有字面量比较：不是所有 "对端标识" 都是 IP —— TestClient 的
    `scope["client"][0]` 是 `"testclient"`，unix socket 上可能是路径。
    这些场景下 `ipaddress` 解析失败，但"运维显式写了这个字符串"就是信任声明。

    解析失败且无字面量匹配 → **不匹配**（宁可不信任）。
    """
    import ipaddress
    raw = str(ip).strip()
    for candidate in networks:
        text = str(candidate or "").strip()
        if text and text == raw and "/" not in text:
            return True
    try:
        addr = ipaddress.ip_address(raw)
    except Exception:                                          # noqa: BLE001
        return False
    for raw in networks:
        text = str(raw or "").strip()
        if not text:
            continue
        try:
            if "/" in text:
                if addr in ipaddress.ip_network(text, strict=False):
                    return True
            elif addr == ipaddress.ip_address(text):
                return True
        except Exception:                                      # noqa: BLE001
            continue
    return False


def _socket_peer(scope: Mapping[str, Any]) -> str:
    client = scope.get("client")
    if isinstance(client, (tuple, list)) and client:
        return str(client[0])[:64]
    return ""


def _client_ip(scope: Mapping[str, Any],
               trusted_proxies: Sequence[str] = ()) -> str:
    """限流用的客户端标识。

    **信任边界（这是 IPv6 伪造问题的正解）**：
    `X-Forwarded-For` 只有在本进程前面**确实有一个会重写它的可信反代**时才可信。
    直连暴露时任何人都能塞 `XFF: 1.2.3.4` 拿到全新配额 —— 限流被完全绕过。

    规则：
      * `trusted_proxies` 为空（默认）→ **完全忽略 XFF**，只用 socket 对端地址。
      * 有配置 → 只有当 socket 对端落在可信网段内，才采信 XFF 的第一段。
        对端不可信时仍然用 socket 地址（攻击者伪造的 XFF 被丢弃）。

    取 XFF **第一段**：`a, b, c` 里 a 是原始客户端，b/c 是中间代理；取最后一段
    会把所有请求算到最外层代理头上（等于没限流）。

    注意：限流只是**资源保护**，从来不是鉴权 —— 即使 IP 可信，它也只是配额维度。
    """
    peer = _socket_peer(scope)
    if trusted_proxies and peer and _ip_in_networks(peer, trusted_proxies):
        forwarded = _raw_header(scope, b"x-forwarded-for")
        if forwarded:
            first = forwarded.split(",")[0].strip()
            if first:
                return first[:64]
    return peer[:64] or "unknown"


class KvRateWindow:
    """**跨副本**限流窗口：用 KV 的原子 `incr` 计数。

    为什么需要它：`RateLimitMiddleware` 默认是每进程内存计数，
    N 个副本 ≈ 实际阈值 ×N —— 部署 5 个副本，`limit=60` 实际放行 300。
    多副本部署必须换成这个。

    **它和内存版有一个语义差别**（必须说清楚，不能假装一样）：
      * 内存版是**滑动窗口**（精确，但需要每条请求的时间戳）
      * KV 版是**固定窗口**（`incr` + TTL 是原子的，但没有 zset 可用）
    固定窗口在边界处允许"两倍突发"（窗口末打满 + 下一窗口初打满）。
    取舍理由：**分布式原子性 > 窗口精度**。跨副本各算各的滑动窗口比"两倍突发"更糟
    （配额直接乘副本数）。要精确滑动窗口得把 KV SPI 扩成带 zset 的形态，成本不划算。

    KV 不可用时**降级到内存窗口**（fail-open 到"至少单个副本有限流"），
    并记一条 warn —— 静默变成不限流是不可接受的。
    """

    def __init__(self, kv: Any, *, limit_per_minute: int, window_s: float = RATE_WINDOW_S,
                 clock: Callable[[], float] | None = None,
                 fallback: "RateLimitMiddleware | None" = None,
                 logger: Any = None) -> None:
        self.kv = kv
        self.limit = int(limit_per_minute)
        self.window_s = float(window_s)
        self._clock = clock or time.time
        self.fallback = fallback
        self.logger = logger
        self.degraded = False

    def _hit(self, ip: str) -> int | None:
        now = self._clock()
        bucket = int(now // self.window_s)
        key = f"rl:{ip}:{bucket}"
        try:
            # TTL 给 2 个窗口：跨桶的旧键自然过期，不用手动清理
            used = int(self.kv.incr(key, 1, ttl_s=self.window_s * 2))
        except Exception as exc:                               # noqa: BLE001
            if not self.degraded:
                self.degraded = True
                _safe_log(self.logger, "warn", "http.rate_limit.degraded",
                          reason=f"{type(exc).__name__}: {exc}",
                          note="KV 不可用 → 降级为单副本内存限流（实际阈值×副本数）")
            return self.fallback._hit(ip) if self.fallback is not None else None
        if used <= self.limit:
            return None
        retry = self.window_s - (now % self.window_s)
        return max(1, int(retry) + 1)


class RateLimitMiddleware:
    """每 IP 每分钟请求数上限（滑动窗口；只作用于 `/v1/*`）。

    为什么用滑动窗口而不是固定窗口：固定窗口在边界处允许"两倍突发"（59 秒打满
    一窗、61 秒再打满下一窗），对保护后端毫无意义。

    为什么只限 `/v1/*`：`/healthz`（liveness）与 `/metrics`（抓取）被限流 → 编排器
    判定实例已死并重启它、监控把"限流 429"当成"服务挂了"。用探针把自己拖死，是
    这类中间件最经典的翻车方式。

    内存边界：每个 IP 一个小列表（长度 ≤ limit），并设总条数上限；被 IP 洪水刷爆时
    淘汰最老的一批桶并记一条 WARN —— 否则"限流器本身"就成了内存 DoS 面。
    """

    def __init__(self, app: Any, *, limit_per_minute: int = DEFAULT_RATE_PER_MINUTE,
                 window_s: float = RATE_WINDOW_S,
                 path_patterns: Sequence[str] = DEFAULT_RATE_PATHS,
                 logger: Any = None,
                 on_reject: Callable[[str, int], None] | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 max_tracked_ips: int = 4096,
                 window_backend: Any = None,
                 trusted_proxies: Sequence[str] = ()) -> None:
        self.app = app
        self.limit = int(limit_per_minute)
        self.window_s = float(window_s)
        self.path_patterns = tuple(str(p) for p in path_patterns)
        self.logger = logger
        self.on_reject = on_reject
        self.clock = clock
        self.max_tracked_ips = int(max_tracked_ips)
        self._hits: dict[str, list[float]] = {}
        self._lock = threading.Lock()
        #: 给了就走向量窗口（跨副本）。None = 内存滑动窗口（单副本）。
        self.window_backend = window_backend
        self.trusted_proxies = tuple(trusted_proxies or ())

    def _applies(self, path: str) -> bool:
        return any(fnmatch.fnmatchcase(path, pattern)
                   for pattern in self.path_patterns)

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if self.limit < 1 or scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path") or "")
        if not self._applies(path):
            await self.app(scope, receive, send)
            return
        ip = _client_ip(scope, self.trusted_proxies)
        retry_after = (self.window_backend._hit(ip) if self.window_backend is not None
                       else self._hit(ip))
        if retry_after is None:
            await self.app(scope, receive, send)
            return
        if self.on_reject is not None:
            try:
                self.on_reject(path, 429)
            except Exception:           # noqa: BLE001
                pass
        _safe_log(self.logger, "warn", "http.rate_limited", path=str(path)[:128],
                  limit_per_minute=self.limit, retry_after_s=int(retry_after))
        await _send_json(
            send, 429,
            {"error": "rate_limited",
             "detail": f"每 IP 每 {int(self.window_s)} 秒最多 {self.limit} 个请求",
             "status": 429},
            extra_headers=[(b"retry-after", str(int(retry_after)).encode("latin-1"))])

    # ── 内部 ────────────────────────────────────────────────
    def _hit(self, ip: str) -> int | None:
        """记一次；放行返回 None，超限返回建议重试秒数。"""
        now = self.clock()
        cutoff = now - self.window_s
        with self._lock:
            hits = self._hits.setdefault(ip, [])
            while hits and hits[0] <= cutoff:
                hits.pop(0)
            if len(hits) >= self.limit:
                # 最早那次滑出窗口时才有配额，向上取整（`Retry-After` 只收整数秒，
                # 向下取整会教客户端提前重试、然后立刻又吃一个 429）
                oldest = hits[0]
                return max(1, int(math.ceil(self.window_s - (now - oldest))))
            hits.append(now)
            self._evict(now)
            return None

    def _evict(self, now: float) -> None:
        """桶太多时清理（在锁内调用）：先删空桶，再按插入顺序删最老的一批。"""
        if len(self._hits) <= self.max_tracked_ips:
            return
        cutoff = now - self.window_s
        stale = [ip for ip, hits in self._hits.items()
                 if not hits or hits[-1] <= cutoff]
        for ip in stale:
            self._hits.pop(ip, None)
        if len(self._hits) <= self.max_tracked_ips:
            return
        overflow = len(self._hits) - self.max_tracked_ips
        victims = list(self._hits.keys())[:max(overflow, len(self._hits) // 4)]
        for ip in victims:
            self._hits.pop(ip, None)
        _safe_log(self.logger, "warn", "http.rate_limit.evicted",
                  evicted=len(victims), tracked=len(self._hits),
                  hint="IP 多样性异常（或 XFF 被伪造）：限流器只能保护资源，不能替代鉴权")


class TimeoutMiddleware:
    """请求超时 → 504，并**触发协作式中止**（不是只停止等待）。

    实现：把下游 app 作为 task 跑，`asyncio.wait(timeout=...)` 竞速。超时就取消 task
    并回 504 —— 前提是**响应头还没发出去**；已经发了（SSE 已经开始吐事件）就只记
    一条 `http.timeout_after_response_started`，因为 HTTP 响应一旦开始就无法改状态码。

    **真实边界（这条要说清楚，不能假装）**：
        `Gateway.handle` 跑在 `asyncio.to_thread` 的线程里。取消协程只是不再 await
        它，OS 线程会继续把这一回合跑完（继续烧 token、调工具、写 Context）。
        CPython 没有安全的强制终止线程手段：`PyThreadState_SetAsyncExc` 只在字节码
        边界插异常，线程可能停在"持着锁"或"写了一半文件"的状态 —— 那比慢更危险。
        所以本中间件**记录**这件事（`http.timeout_detached`），不假装线程被杀了。
        真正的中止点在 Gateway 的 deadline/预算，不在 HTTP 层。
    """

    def __init__(self, app: Any, *, timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
                 logger: Any = None,
                 on_reject: Callable[[str, int], None] | None = None) -> None:
        self.app = app
        self.timeout_s = float(timeout_s)
        self.logger = logger
        self.on_reject = on_reject

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or self.timeout_s <= 0:
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path") or "")
        state = {"started": False}

        async def guarded_send(message: dict) -> None:
            if message.get("type") == "http.response.start":
                state["started"] = True
            await send(message)

        # **真中止的入口**：把一个跨线程 Event 塞进 ASGI scope，
        # 端点会把它放进 Gateway 的请求 → ExecCtx.cancel_event。
        # 超时时 set() 它，跑回合的那个线程会在下一个检查点看到 `aborted` 并退出
        # （adapter 每次重试前、每轮循环开头都会看一眼）。
        #
        # 这**不是**强杀线程（CPython 做不到安全的强杀），但它把"跑完整个回合"
        # 缩短成"跑完当前这一步"。配合 adapter 的读超时，卡在 socket 上的调用
        # 也能真的结束。
        import threading as _threading
        cancel = _threading.Event()
        try:
            scope = dict(scope)
            scope["_cancel_event"] = cancel
            # **把 HTTP 超时转成隐式 deadline**（见端点里的用法）：
            # 这样"中止"只有一个权威来源（ExecCtx.deadline），而不是
            # HTTP 层与 Gateway 各自算一套、互相不知道对方何时放弃。
            scope["_http_timeout_s"] = self.timeout_s
        except Exception:           # noqa: BLE001 - 只读 scope 时降级
            pass
        task = asyncio.ensure_future(self.app(scope, receive, guarded_send))
        done, _pending = await asyncio.wait({task}, timeout=self.timeout_s)
        if task in done:
            return task.result()        # 正常返回；异常原样抛给 ServerErrorMiddleware
        # 先置位（让工作线程尽早退出），再取消协程
        cancel.set()
        task.cancel()
        # 别让被取消的 task 在 GC 时抱怨"exception was never retrieved"
        task.add_done_callback(
            lambda t: None if t.cancelled() else t.exception())
        if self.on_reject is not None:
            try:
                self.on_reject(path, 504)
            except Exception:           # noqa: BLE001
                pass
        if state["started"]:
            # 响应头已经发出：改不了状态码，只能如实记下（流式回合会表现为被截断）
            _safe_log(self.logger, "warn", "http.timeout_after_response_started",
                      path=str(path)[:128], timeout_s=self.timeout_s,
                      hint="响应已开始，无法回 504；客户端看到的是被截断的响应")
        else:
            _safe_log(self.logger, "warn", "http.timeout_detached",
                      path=str(path)[:128], timeout_s=self.timeout_s,
                      hint=("已停止等待并回 504；Gateway 线程仍在跑完本回合"
                            "（Python 不能安全杀线程）"))
            await _send_json(
                send, 504,
                {"error": "request_timeout",
                 "detail": (f"处理超过 {self.timeout_s}s，已停止等待"
                            f"（服务端回合仍在后台收尾）"),
                 "status": 504})
        # 收尾宽限：给被取消的协程一点时间退出，别在事件循环里留 pending task
        with contextlib.suppress(BaseException):
            await asyncio.wait({task}, timeout=_CANCEL_GRACE_S)


def _resolve_trusted_proxies(env: Mapping[str, str],
                            explicit: Sequence[str] | None = None) -> tuple[str, ...]:
    """可信反代网段：显式参数 > `AGENT_RUNTIME_TRUSTED_PROXIES`（逗号分隔）> 空。

    **空是安全默认**：不信任任何 XFF，限流按 socket 对端算。配错了后果是
    "限流比预期更严"（所有人共享一个桶），而不是"被绕过"—— 方向是对的。
    """
    if explicit is not None:
        return tuple(str(x).strip() for x in explicit if str(x).strip())
    raw = str(env.get("AGENT_RUNTIME_TRUSTED_PROXIES") or "")
    return tuple(x.strip() for x in raw.split(",") if x.strip())


def _metric_counter_sink(metrics: Any) -> Any:
    """把 `metrics` 包成 Gateway 要的 `(name, delta, labels) -> None`。

    Gateway 在 `core/`，必须零第三方依赖；`prometheus_client` 是可选件。
    用回调把"记指标"这件事留在接入层，core 只负责"什么时候该记"。
    """
    def _sink(name: str, delta: float = 1.0, labels: dict | None = None) -> None:
        try:
            metrics.counter(name, delta, labels or {})
        except Exception:                   # noqa: BLE001 - 观测面 fail-open
            pass
    return _sink


def _assert_kv_reachable(kv: Any) -> None:
    """分布式模式下，启动时**真连一次** KV。

    为什么必需：`RESPKV` 是**惰性连接** —— 构造不碰网络，`info().shared`
    也恒为 True。于是"配了分布式限流 + Redis 根本连不上"会**启动成功**，
    然后在第一个请求时才失败。实测过：Redis 端口关着，服务照样起来，
    `/readyz` 也是 200。

    淡性连接对"可选依赖"是对的（不该因为没配就拖慢启动），
    但对"必须共享"的配置是错的 —— 它把配置错误推迟成了运行期故障。
    """
    ping = getattr(kv, "ping", None)
    if not callable(ping):
        return                       # 后端没有 ping 就跳过（不假装探过）
    try:
        alive = ping()
    except Exception as exc:          # noqa: BLE001
        raise RuntimeError(
            f"AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED=1 但 KV 连不上："
            f"{type(exc).__name__}: {exc}") from exc
    if not alive:
        raise RuntimeError(
            "AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED=1 但 KV 探活失败"
            "（检查 Redis 地址/端口/密码；这类配置错误必须在启动时暴露，"
            "而不是等到第一个请求）")


class RedisQuotaCounter:
    """跨副本租户配额计数器（`Gateway(quota_counter=...)`）。

    只用 `INCR` + `EXPIRE` 两个原子命令：
      * `INCR` 保证多副本读到的 `used` 是同一个数
      * `EXPIRE` 只在**首次**设（`used == 1` 时），否则会把窗口无限延长

    与 `KvRateWindow` 的分工：那个管**每 IP 的 HTTP 请求**，这个管
    **每租户的回合数**（成本闸）。两者都必须在多副本下共享，否则阈值 × 副本数。
    """

    #: 窗口长度（秒）。与 Gateway 的内存窗口保持同一口径。
    WINDOW_S = 60.0

    def __init__(self, client: Any, *, window_s: float = WINDOW_S) -> None:
        self._client = client
        self._window_s = float(window_s)

    def incr(self, key: str) -> tuple[int, float]:
        """原子自增并返回 `(当前值, 剩余 TTL 秒)`。"""
        used = int(self._client.incr(key))
        if used == 1:
            self._client.expire(key, int(self._window_s))
        ttl = self._client.ttl(key)
        # Redis 的 `ttl` 对无过期键返回 -1、不存在返回 -2，都归零处理
        return used, float(ttl) if ttl and ttl > 0 else 0.0


def _build_quota_counter(env: Mapping[str, str], enabled: bool) -> Any:
    """`AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED=1` 时给 Gateway 一个跨副本计数器。

    与 HTTP 层共用同一个开关：多副本部署不该记两个名字。
    KV 不是共享实现时**不静默降级** —— 建配置就报错，
    因为"以为跨副本、其实各算各的"正是这条配置要防的事。
    """
    if not enabled:
        return None
    kv = _make_kv(env)
    if not getattr(getattr(kv, "info", lambda: None)(), "shared", False):
        raise RuntimeError(
            "AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED=1 但租户配额拿不到共享 KV"
            "（检查 AGENT_RUNTIME_KV=redis）")
    _assert_kv_reachable(kv)
    client = getattr(kv, "client", None) or getattr(kv, "_client", None)
    if client is None:
        raise RuntimeError(
            f"KV {type(kv).__name__} 没暴露 redis client，无法做原子配额计数")
    return RedisQuotaCounter(client)


def _build_rate_window(env: Mapping[str, str], explicit: bool | None,
                       *, limit: int, logger: Any = None) -> Any:
    """分布式限流窗口：显式参数 > `AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED`。

    默认**关闭**（用每进程内存窗口）：单副本部署没必要连 Redis，
    而多副本部署"忘了开"的后果是阈值×副本数 —— 所以 `/v1/info` 会自述
    `rate_limit_distributed=false`，让这个错配可见。
    """
    if limit < 1:
        return None
    enabled = (bool(explicit) if explicit is not None
               else _truthy(env.get("AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED")))
    if not enabled:
        return None
    try:
        kv = _make_kv(env)
    except Exception as exc:                                   # noqa: BLE001
        raise RuntimeError(
            f"AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED=1 但 KV 不可用："
            f"{type(exc).__name__}: {exc}") from exc
    if not getattr(getattr(kv, "info", lambda: None)(), "shared", False):
        # 配了"分布式"却拿到进程内 KV = 配了没生效。必须报错。
        raise RuntimeError(
            "AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED=1 但当前 KV 是进程内的"
            "（检查 AGENT_RUNTIME_KV=redis 与 REDIS_* 配置）")
    _assert_kv_reachable(kv)
    return KvRateWindow(kv, limit_per_minute=limit, logger=logger)


def install_hardening(app: Any, *,
                      max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
                      rate_limit_per_minute: int = DEFAULT_RATE_PER_MINUTE,
                      request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
                      logger: Any = None,
                      metrics: Any = None,
                      trusted_proxies: Sequence[str] = (),
                      rate_window: Any = None) -> dict:
    """装三道资源边界，返回它们的**生效配置**（给 `/v1/info` 自述，不含任何密钥）。

    顺序（后 `add_middleware` 的在外层，见 Starlette 的 `build_middleware_stack`）：

        限流 → 超时 → 体量 → （鉴权）→ 路由

    限流在最外：一次坏凭据爆破必须被配额挡住，而不是打满鉴权器再被 401。
    体量在鉴权之内：等体量判定先拒掉一个 4 GB 的 chunked 上传，比先算一遍
    鉴权更划算 —— 但两者都不读 body 就能拒，差别只在"谁先回"。

    任何一项设成 0/负数 = 关闭该项（显式关闭，不是"配错了"）。
    """
    metrics_cb = (lambda path, status: _observe_http(metrics, _metric_path(path),
                                                     status)) if metrics is not None else None

    app.add_middleware(BodyLimitMiddleware, max_bytes=int(max_body_bytes),
                       logger=logger, on_reject=metrics_cb)
    app.add_middleware(TimeoutMiddleware, timeout_s=float(request_timeout_s),
                       logger=logger, on_reject=metrics_cb)
    app.add_middleware(RateLimitMiddleware,
                       limit_per_minute=int(rate_limit_per_minute),
                       logger=logger, on_reject=metrics_cb,
                       window_backend=rate_window,
                       trusted_proxies=tuple(trusted_proxies or ()))
    return {
        "max_body_bytes": int(max_body_bytes),
        "body_limit": bool(int(max_body_bytes) > 0),
        "rate_limit_per_minute": int(rate_limit_per_minute),
        "rate_limit": bool(int(rate_limit_per_minute) > 0),
        "request_timeout_s": float(request_timeout_s),
        "timeout": bool(float(request_timeout_s) > 0),
        "rate_paths": list(DEFAULT_RATE_PATHS),
        # 自述：分布式限流是否生效、信任哪些反代。**运维一眼要能看出**
        # "我配了 Redis，但限流还是每进程算的"这种错配。
        "rate_limit_distributed": bool(rate_window is not None),
        "trusted_proxies": list(trusted_proxies or ()),
    }


# ══════════════════════════════════════════════════════════════
# 七、应用装配
# ══════════════════════════════════════════════════════════════
def create_app(*, gateway: Any = None, metrics: Any = None,
               logger: Any = None, shutdown: Any = None,
               deps: Any = None,
               authenticator: Authenticator | None = None,
               auth_required: bool = False,
               protect_paths: Sequence[str] | None = None,
               exempt_paths: Sequence[str] | None = DEFAULT_EXEMPT_PATHS,
               metrics_requires_auth: bool = True,
               metrics_token: str = "",
               admin_token: str = "",
               max_body_bytes: int | None = None,
               rate_limit_per_minute: int | None = None,
               request_timeout_s: float | None = None,
               trusted_proxies: Sequence[str] | None = None,
               distributed_rate_limit: bool | None = None,
               workflow_max: int | None = None,
               workflow_workers: int | None = None,
               workflow_runner_factory: Callable[[dict], Any] | None = None,
               workflow_store: Any = None) -> FastAPI:
    """造 FastAPI 应用。五个依赖都可注入（缺省各自 new 一个）。

    Args:
        gateway:  `Gateway` 或任何有 `handle(request) -> TurnResult` /
                  `handle_stream(request) -> Iterator[dict]` 的对象。
                  缺省用 `build_gateway_from_env()` 装配一个真实运行时的最小形态。
                  注入的对象会被 `instrument_gateway` 包一层：观测与关停纪律对
                  **所有**接入路径生效，不依赖调用方记得装配。
        metrics:  `Metrics` 或任何有 `render()` 的对象（/metrics 直接用它）。
        logger:   `JsonLogger` 或任何有 `info/warn/error/debug(event, **fields)` 的对象。
        shutdown: `ShutdownCoordinator` 或任何有 `is_shutting_down()` / `track()` /
                  `wait()` / `on_shutdown()` 的对象。
        deps:     依赖自检表（见 `_dep_specs`）。None = store/kv/vector 的默认三件。
        authenticator: 鉴权器（`auth.Authenticator`）。**None = 不装鉴权**，保持
                  "principal 由请求体给出"的历史行为 —— 既有 55 个 serve 用例
                  全部走这条路径，它们的语义不因为本参数存在而改变。
        auth_required: **fail-closed 开关**。True 且 `authenticator is None` →
                  抛 `StartupRefused`（消息里带修复命令）。默认 False = 保留旧行为。
                  生产入口（`main()`）永远传 True；测试/嵌入式装配可以显式说 False。
        protect_paths: 只保护命中的路径（fnmatch）。None = 除 exempt 外全保护。
        exempt_paths:  豁免路径，默认只有 `/healthz`（liveness 失败会触发重启，
                  重启会砍掉最后的 drain/flush）。`/readyz` 默认**不**豁免。
        metrics_requires_auth: `/metrics` 是否要鉴权，默认 True（暴露业务量）。
        metrics_token: `/metrics` 的专用 Bearer 令牌（抓取端没有用户身份时用）。
        max_body_bytes: 请求体上限（字节）。None = 读 `AGENT_RUNTIME_MAX_BODY_BYTES`，
                  再缺省 `DEFAULT_MAX_BODY_BYTES`（1 MiB）；0 = 关闭。
        rate_limit_per_minute: 每 IP 每分钟请求数。None = 读 `AGENT_RUNTIME_HTTP_RATE`，
                  再缺省 0（关闭）。只对 `/v1/*` 生效。
        request_timeout_s: 请求超时（秒）。None = 读 `AGENT_RUNTIME_REQUEST_TIMEOUT_S`，
                  再缺省 0（关闭）。超时回 504，但**不杀线程**（见模块 docstring）。
        workflow_max: 内存里最多跟踪多少条 workflow。None = 读
                  `AGENT_RUNTIME_WORKFLOW_MAX`，再缺省 `DEFAULT_WORKFLOW_MAX`(200)。
                  超限淘汰最老的**已结束**的；全在跑时**拒绝新提交**（429）。
        workflow_workers: 后台跑 DAG 的线程数。None = 读
                  `AGENT_RUNTIME_WORKFLOW_WORKERS`，再缺省 `DEFAULT_WORKFLOW_WORKERS`(2)。
                  与 HTTP 的线程池分开，长流水线不该占掉交互式请求的 worker。
        workflow_runner_factory: `(principal_dict) -> runner`，用来把 DAG 节点换成
                  真 `SubagentRunner`（带 INV-18 降权）。None = 内置最小 runner
                  （`GatewayNodeRunner`，一个节点 = 一次 gateway.handle 回合）。

    为什么启动就探一次：`/readyz` 是"现在能不能接流量"，但"启动时到底探到了什么"
    是排障时更常见的问题（"这个进程是从什么时候开始不 ready 的"）。两份快照
    分开存：`/readyz` 实时探，`/v1/info` 给启动快照。
    """
    # ── fail-closed：**最先**判定，早于任何副作用（不建 gateway、不探依赖）──
    # 一个起不来的进程比一个裸奔的进程便宜得多；也没必要先花时间探一遍 Redis 再死。
    if authenticator is None and auth_required:
        raise StartupRefused(STARTUP_REFUSED_MESSAGE)

    logger = logger if logger is not None else JsonLogger()
    metrics = metrics if metrics is not None else Metrics()
    shutdown = shutdown if shutdown is not None else ShutdownCoordinator()
    specs = _dep_specs(deps)
    started_at = time.time()
    startup_checks = probe_all(specs)

    built_here = gateway is None
    if gateway is None:
        # **探针对象必须就是业务对象**：/readyz 探一个 gateway 根本没用到的 store，
        # 等于在检查一个不存在的依赖（MemoryStore 时看不出差别，sqlite/redis 时就是假绿）。
        gw_store = next((spec.obj for spec in specs
                         if spec.name == "store" and not isinstance(spec.obj, _Unavailable)),
                        None)
        gateway = build_gateway_from_env(logger=logger, store=gw_store,
                                         metrics=metrics)
    # **工具面必须在包装之前抓下来。**
    # `StreamingGateway.__getattr__` 刻意拒绝下划线开头的属性（隐私防护，见
    # core/streaming.py），所以一旦包上，`getattr(gateway, "_tool_registry")`
    # 只会拿到 default —— 于是真实启动（`create_app()` 无参路径）里
    # `app.state.tools` 恒为 None：
    #   * `/v1/tools` 返回 `{"count": 0, "note": "本实例未装配工具表"}`，
    #     而启动日志里 `sandbox.tool name=sandbox_run` 明明已经装配 —— 端点
    #     在**撒谎**，且撒的是"能力不存在"这种最难查的谎；
    #   * `/v1/admin/tools/reload` 恒 503「工具表未装配」。
    # 注入 gateway 的测试路径（`built_here=False`）不包 Streaming，所以一直绿 ——
    # 21 处 `create_app(...)` 里 8 处传替身、3 处默认启动，默认启动那 3 处
    # 只测身份校验和 SSE，没有一处读 `/v1/tools`。
    _tool_registry = getattr(gateway, "_tool_registry", None)
    _tool_dispatcher = getattr(gateway, "_tool_dispatcher", None)
    _tool_loader_env = getattr(gateway, "_tool_loader_env", None)

    # 真增量流式：在**观测层里面**包（顺序很重要）。
    #
    #   InstrumentedGateway(StreamingGateway(真实 Gateway))
    #   ────────────────┬───────────────────┬──────────────
    #                   │                   └─ 产出增量事件（step/...）
    #                   └─ 观测到的是**同一个**事件流（指标不会漏）
    #
    # 反过来包会让 StreamingGateway 只能看到 "accepted/started/completed"，
    # 因为它拿到的是已经收口过的流 —— 那就退回"跑完再发"，白做。
    #
    # 默认开启：`Gateway.handle_stream` 本身形状不变（仍是三事件），
    # 增量只是**追加** step 事件，既有客户端不受影响。
    # `AGENT_RUNTIME_STREAMING=0` 可关（比如排查"事件太多"的问题）。
    # 只包装**我们自己造的** gateway。注入进来的对象不碰 —— 那是调用方的，
    # 它可能已经自带流式实现，或者是个只实现 handle() 的测试替身；
    # 擅自包装会改变它的语义（本项目被"偷偷包装私有属性"坑过一次）。
    if built_here and _truthy(os.environ.get("AGENT_RUNTIME_STREAMING", "1")):
        try:
            from core.streaming import StreamingGateway
            gateway = StreamingGateway(gateway)
        except Exception as exc:                               # noqa: BLE001
            # 流式装不上 = 退回"跑完再发"，但必须留痕（静默降级是本项目反复踩的坑）
            if logger is not None:
                logger.warn("streaming.disabled",
                            reason=f"{type(exc).__name__}: {exc}")
    # 幂等：已经是 InstrumentedGateway 就原样返回（见 observability 的实现）
    gateway = instrument_gateway(gateway, logger=logger, metrics=metrics,
                                shutdown=shutdown)
    _bootstrap_metrics(metrics)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """启动装信号处理；退出兜底排空。

        信号在 lifespan 里装（而不是创建 app 时）：那时才有 running loop，且
        一定晚于 uvicorn 自己装的处理器（后装的生效，我们接管 SIGTERM）。
        """
        loop = asyncio.get_running_loop()
        server = getattr(app.state, "server", None)
        driver = SignalDriver(shutdown, logger=logger,
                              on_drained=_exit_server(server))
        installed = driver.install(loop)
        app.state.signal_driver = driver
        app.state.signals = installed
        try:
            logger.info("server.started", version=VERSION, signals=installed,
                        ready=_ready(startup_checks))
        except Exception:               # noqa: BLE001
            pass
        try:
            yield
        finally:
            # 兜底：正常退出（非信号）也要跑一遍 begin+wait，钩子只执行一次
            shutdown.begin()
            report: Any = {}
            try:
                report = await asyncio.to_thread(shutdown.wait)
            except Exception as exc:    # noqa: BLE001
                report = {"drained": False, "error": f"{type(exc).__name__}: {exc}"}
            try:
                logger.info("server.stopped", report=report)
            except Exception:           # noqa: BLE001
                pass
            # 收口 MCP 子进程：不关会变孤儿（只在 ps 里看得见，日志干净）。
            # 放在 drain 之后：正在跑的工具调用需要连接还活着。
            try:
                tools_obj = getattr(getattr(app.state, "gateway", None),
                                    "_loaded_tools", None)
                if tools_obj is not None:
                    tools_obj.close()
                    logger.info("tools.closed")
            except Exception as exc:    # noqa: BLE001 - 关停路径 fail-open
                try:
                    logger.warn("tools.close_failed",
                                reason=f"{type(exc).__name__}: {exc}")
                except Exception:       # noqa: BLE001
                    pass
            # 收口 workflow 执行器：不等在跑的 DAG 收尾（一条流水线可能几分钟，
            # 优雅关停被它拖住 = 进程卡死不退出）。执行器不再接新任务即可。
            try:
                registry_obj = getattr(app.state, "workflows", None)
                if registry_obj is not None and getattr(
                        app.state, "workflows_built_here", False):
                    registry_obj.close()
                    logger.info("workflows.closed")
            except Exception as exc:    # noqa: BLE001 - 关停路径 fail-open
                try:
                    logger.warn("workflows.close_failed",
                                reason=f"{type(exc).__name__}: {exc}")
                except Exception:       # noqa: BLE001
                    pass

    app = FastAPI(title="Agent Runtime", version=VERSION,
                  description="HTTP 服务层：/v1/turns、SSE 流、指标与探针",
                  lifespan=lifespan)

    # ── 共享状态（测试与运维都能从 app.state 读到同一份事实）──
    app.state.gateway = gateway
    app.state.metrics = metrics
    app.state.logger = logger
    app.state.shutdown = shutdown
    app.state.deps = specs
    app.state.startup_checks = startup_checks
    app.state.started_at = started_at
    app.state.version = VERSION
    app.state.signals = []
    app.state.signal_driver = None
    # 工具表从 gateway 取（装配在 `build_gateway_from_env` 那层）。
    # 注入的假 gateway 没有这些属性 → 保持 None，端点优雅降级成空表。
    app.state.tools = _tool_registry
    app.state.tool_dispatcher = _tool_dispatcher
    app.state.tool_loader_env = _tool_loader_env or {}

    # ── Workflow 显式入口（B）的执行器 ──
    # **总是装配**（默认开启）：`/v1/workflows` 是确定性流程的正式入口，
    # 让它需要额外开关才能用，等于鼓励大家继续"在 prompt 里描述 SOP"。
    # 不用它的人只多了一个空表 + 两个空闲线程，代价远小于"能力存在但没人知道"。
    # 容量与环境变量在**创建时**读一次并存进 registry：运行中改环境不会悄悄改行为，
    # 而 `/v1/info` 自述的正好是"启动时到底生效了什么"（与 hardening 同一口径）。
    env = os.environ
    app.state.workflows = WorkflowRegistry(
        gateway=gateway,
        max_tracked=_pick(env, "AGENT_RUNTIME_WORKFLOW_MAX", workflow_max,
                          default=DEFAULT_WORKFLOW_MAX, cast=int),
        workers=_pick(env, "AGENT_RUNTIME_WORKFLOW_WORKERS", workflow_workers,
                      default=DEFAULT_WORKFLOW_WORKERS, cast=int),
        # 层内并行度默认 1：静态 DAG 的价值在确定性，并行是显式选择（`max_parallel`）
        default_parallel=1,
        logger=logger,
        runner_factory=workflow_runner_factory,
        # 持久化：给了 store 就落盘，进程重启能读回来（标记 interrupted）
        store=workflow_store if workflow_store is not None else _make_store(os.environ, logger=logger),
    )
    #: 谁装配谁负责收口：注入方自己管生命周期（与 `built_here` 处理 gateway 同一口径）
    app.state.workflows_built_here = True

    # 把 running 数喂给指标（告警规则 `WorkflowStuck` 查的就是它）。
    # 用 gauge 覆盖写：gauge 是"当前值"，累加就是 bug。
    def _observe_workflows() -> None:
        try:
            metrics.gauge("workflow_running",
                          float(app.state.workflows.running_count()))
        except Exception:               # noqa: BLE001 - 观测面 fail-open
            pass
    app.state.observe_workflows = _observe_workflows
    _observe_workflows()

    # 配置自述做成指标：`rate_limit_distributed` 只在 /v1/info 里，
    # 而告警规则查不到 JSON —— 没有这条就写不出"多副本没开分布式限流"的告警。
    try:
        metrics.gauge("info", 1.0, {
            "version": VERSION,
            "rate_limit_distributed": str(bool(
                _truthy(env.get("AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED")))).lower(),
        })
    except Exception:                   # noqa: BLE001
        pass
    # 启动时把上次没跑完的 workflow 读回来（标 interrupted，**不自动续跑** ——
    # 重放会重复执行已完成节点，发布流程里那是灾难）。
    try:
        restored = app.state.workflows.restore()
        if restored:
            logger.info("workflow.restored_on_start", count=restored)
    except Exception as exc:            # noqa: BLE001 - 恢复失败不阻塞启动
        logger.warn("workflow.restore_failed", reason=f"{type(exc).__name__}: {exc}")

    # ── 鉴权（可选）：只有显式给 authenticator 才装 ──
    # 不给 = 保持历史行为（principal 由请求体给出）。这条"默认不变"是硬要求：
    # 既有 55 个 serve 用例注入的都是假件，装上一层鉴权会把它们全变成 401。
    app.state.authenticator = None
    app.state.auth_middleware = None
    app.state.auth_config = {}
    #: 热加载端点的独立门槛。**空串 = 端点不注册**（不是"不校验"）。
    #: 为什么不复用主鉴权：改运行时能力是管理动作，不该只凭一个普通用户的
    #: `runtime:run` scope 就放行。
    app.state.admin_token = str(admin_token or "").strip()
    if authenticator is not None:
        install_auth(
            app, authenticator,
            protect_paths=protect_paths,
            exempt_paths=exempt_paths,
            metrics_requires_auth=metrics_requires_auth,
            metrics_token=metrics_token,
            logger=logger,
            # 拒绝也进 HTTP 指标：401/403 的突增是最早能看到的攻击/配置事故信号。
            # 路径先过白名单归一 —— 被拒的请求根本没到路由，path 是攻击者可控的。
            on_reject=lambda path, status: _observe_http(metrics,
                                                         _metric_path(path), status),
        )
        try:
            logger.info("auth.enabled", schemes=list(authenticator.schemes),
                        exempt=list(app.state.auth_config.get("exempt_paths", ())),
                        metrics_requires_auth=bool(metrics_requires_auth),
                        allow_anonymous=bool(authenticator.allow_anonymous))
        except Exception:               # noqa: BLE001 - 观测面 fail-open
            pass

    # 鉴权**模式**（给 /v1/info 与运维看）：enforced = 装了鉴权中间件；disabled =
    # 显式放行，principal 由请求体给出。两者都不是"配错了"，但必须一眼能分清。
    app.state.auth_mode = "enforced" if authenticator is not None else "disabled"

    # ── HTTP 层加固（三道资源边界；顺序见 install_hardening）──
    # 环境变量在调用时读一次并存进 app.state：运行中改环境不会悄悄改行为，
    # 而 /v1/info 自述的正好是"启动时到底生效了什么"。
    hardening_env = os.environ
    app.state.hardening = install_hardening(
        app,
        max_body_bytes=_pick(hardening_env, "AGENT_RUNTIME_MAX_BODY_BYTES",
                             max_body_bytes, default=DEFAULT_MAX_BODY_BYTES,
                             cast=int),
        rate_limit_per_minute=_pick(hardening_env, "AGENT_RUNTIME_HTTP_RATE",
                                    rate_limit_per_minute,
                                    default=DEFAULT_RATE_PER_MINUTE, cast=int),
        request_timeout_s=_pick(hardening_env, "AGENT_RUNTIME_REQUEST_TIMEOUT_S",
                                request_timeout_s,
                                default=DEFAULT_REQUEST_TIMEOUT_S, cast=float),
        trusted_proxies=_resolve_trusted_proxies(hardening_env, trusted_proxies),
        rate_window=_build_rate_window(
            hardening_env, distributed_rate_limit,
            limit=int(_pick(hardening_env, "AGENT_RUNTIME_HTTP_RATE",
                            rate_limit_per_minute,
                            default=DEFAULT_RATE_PER_MINUTE, cast=int)),
            logger=logger),
        logger=logger, metrics=metrics,
    )
    try:
        logger.info("http.hardening", **dict(app.state.hardening))
    except Exception:                   # noqa: BLE001 - 观测面 fail-open
        pass

    # 关停钩子：flush 指标 + 日志。注册在 create_app 里，保证"谁装配谁负责 flush"。
    shutdown.on_shutdown(lambda: _flush_observability(metrics, logger))

    # ── ① 同步回合 ─────────────────────────────────────────
    @app.post("/v1/turns", tags=["turns"],
              summary="跑一个回合，返回 TurnResult JSON")
    async def post_turn(body: TurnRequest, request: Request) -> Response:
        if shutdown.is_shutting_down():
            _observe_http(metrics, "/v1/turns", 503)
            return JSONResponse(content=_shutting_down_payload(), status_code=503)
        # 未启用鉴权时，请求体是**唯一**的身份来源，必须给
        missing = _missing_identity(body, _header_principal(request))
        if missing:
            _observe_http(metrics, "/v1/turns", 422)
            return JSONResponse(
                content=_validation_payload(request.url.path, missing),
                status_code=422)
        gateway_request = body.to_gateway_request()
        # 把超时中间件注入的取消信号透传给 Gateway（→ ExecCtx.cancel_event）。
        # 没有它，504 之后那一回合会继续跑完、继续烧 token。
        _ce = request.scope.get("_cancel_event")
        if _ce is not None:
            gateway_request["_cancel_event"] = _ce
        # 进度侧通道：只有**流式**端点需要（子 Agent 的 start/step/finish
        # 要实时推给客户端）。JSON 端点不看播报，注入了也是空转。
        _sink = request.scope.get("_progress_sink")
        if _sink is not None:
            gateway_request["_progress"] = _sink
        # 隐式 deadline：没显式给 deadline 时，用「HTTP 超时 × 0.9」当硬截止。
        # 留 10% 余量的目的：让 Gateway 有机会**返回一个正常的 DEADLINE 结果**
        # （带已完成步数与部分文本），而不是被中间件直接 504 砍断 ——
        # 前者对客户端更有用，后者只剩一个状态码。
        _htt = request.scope.get("_http_timeout_s")
        if _htt and float(_htt) > 0:
            implicit = float(_htt) * 1000.0 * 0.9
            cur = gateway_request.get("deadline_ms")
            gateway_request["deadline_ms"] = (implicit if cur is None
                                              else min(float(cur), implicit))
        # 启用鉴权后：凭据身份是**权威**，请求体只做一致性声明（冲突 → 403）
        authed = _header_principal(request)
        if authed is not None:
            conflict = _principal_conflict(body.principal, authed)
            if conflict:
                _observe_http(metrics, "/v1/turns", 403)
                return _conflict_response(request.url.path, conflict)
            gateway_request["principal"] = _principal_dict(authed)
        try:
            # Gateway 是同步的：丢线程池，别把事件循环钉住（否则 /healthz 一起超时）
            result = await asyncio.to_thread(gateway.handle, gateway_request)
        except Exception as exc:        # noqa: BLE001 - 出口永不 500 成 HTML
            _observe_http(metrics, "/v1/turns", 500)
            return JSONResponse(
                content={"error": "internal_error",
                         "detail": f"{type(exc).__name__}: {exc}",
                         "status": 500},
                status_code=500)
        response = _result_response(result)
        _observe_http(metrics, "/v1/turns", response.status_code)
        return response

    # ── ①c 资源清单（只读）──────────────────────────────────
    @app.get("/v1/tools", tags=["ops"],
             summary="本实例已注册的工具（只读，不含 secrets）")
    async def list_tools() -> Response:
        """运行时**实际注册**的工具清单。

        和读 YAML 的区别：配置加载会校验、会拒绝、会告警 —— 文件里有一项
        不代表运行时真注册上了。这个端点给的是注册后的事实。

        为什么需要：此前没有任何 HTTP 途径能回答"这个实例能干什么"。
        运维翻 YAML、客户端猜工具名，而 `needs_approval` 这种信息
        （哪些工具会挂起等审批）直接决定了前端要不要准备审批 UI。

        **不暴露 `secrets`**：见 `_tool_public_view` 的白名单说明。
        """
        registry = getattr(app.state, "tools", None)
        names = getattr(registry, "names", None)
        if registry is None or not callable(names):
            # 没装配工具表是**合法状态**（serve 默认不配 tools.yaml）。
            # 200 + 空表，而不是 404：能力存在，只是这一项为空。
            return JSONResponse(content={"tools": [], "count": 0,
                                         "note": "本实例未装配工具表"}, status_code=200)
        views = [_tool_public_view(m) for m in registry.manifests()]
        views.sort(key=lambda v: (v["risk_level"], v["name"]))
        return JSONResponse(content={
            "tools": views,
            "count": len(views),
            "needs_approval": sum(1 for v in views if v["needs_approval"]),
        }, status_code=200)

    # ── ①d 工具热加载（管理动作，独立门槛）──────────────────
    if app.state.admin_token:
        @app.post("/v1/admin/tools/reload", tags=["admin"],
                  summary="重新读取工具配置并原子替换工具表（需 admin token）")
        async def reload_tools(request: Request) -> Response:
            """重读 `AGENT_RUNTIME_TOOLS` 指向的配置并**原地**替换工具表。

            为什么需要它：改一项工具参数就要重启服务，而重启会中断正在跑的
            回合（除非先 drain 到手）。运维调白名单不该有这个代价。

            为什么是**原地替换**：`_loop_factory` 闭包捕获了 dispatcher，
            dispatcher 持有 registry 引用。新建 registry 再替换 app.state
            只影响之后创建的 Loop，**正在跑的 Loop 仍然用旧表** ——
            那种"热加载看起来成功了"更难查。

            门槛：`Authorization: Bearer <AGENT_RUNTIME_ADMIN_TOKEN>`。
            没配这个环境变量时**端点根本不注册**（404），不暴露存在性。

            失败语义（fail-closed）：**任何一步出错都不替换**。
            新配置坏掉时保留旧表，服务继续用上一份能工作的工具集 ——
            换上一张坏表会让"改配置"变成"停服"。
            """
            token = ""
            header = request.headers.get("authorization") or ""
            if header.lower().startswith("bearer "):
                token = header[7:].strip()
            token = token or (request.headers.get("x-admin-token") or "").strip()
            # 定长比较，避免按字节提前返回泄露前缀
            if not token or not secrets.compare_digest(token, app.state.admin_token):
                _observe_http(metrics, "/v1/admin/tools/reload", 403)
                return JSONResponse(
                    content={"error": "forbidden", "status": 403,
                             "detail": "需要 admin token"},
                    status_code=403)

            registry = getattr(app.state, "tools", None)
            if registry is None or not hasattr(registry, "replace_all"):
                _observe_http(metrics, "/v1/admin/tools/reload", 503)
                return JSONResponse(
                    content={"error": "unavailable", "status": 503,
                             "detail": "工具表未装配，无法热加载"},
                    status_code=503)

            loader_env = getattr(app.state, "tool_loader_env", {}) or {}
            try:
                from toolconfig import load_tools_from_env
                loaded = load_tools_from_env(loader_env)

                # 先装进**临时** registry：中途任何一项不合法都在这里炸，
                # 真正那张表一动不动。
                staging = type(registry)()
                loaded.register_into(staging)
                items = [(m, staging.get(m.name)[1]) for m in staging.manifests()]
            except Exception as exc:                           # noqa: BLE001
                # 旧表保留 —— 这是 fail-closed 的关键动作
                _observe_http(metrics, "/v1/admin/tools/reload", 422)
                try:
                    logger.warn("tools.reload_failed",
                                reason=f"{type(exc).__name__}: {exc}")
                except Exception:                              # noqa: BLE001
                    pass
                return JSONResponse(
                    content={"error": "reload_failed", "status": 422,
                             "detail": f"{type(exc).__name__}: {exc}",
                             "note": "旧工具表保持不动"},
                    status_code=422)

            diff = registry.replace_all(items)
            try:
                logger.info("tools.reloaded", **{k: v for k, v in diff.items()
                                                 if k != "total"},
                            total=diff.get("total", 0))
            except Exception:                                  # noqa: BLE001
                pass
            _observe_http(metrics, "/v1/admin/tools/reload", 200)
            return JSONResponse(content={"ok": True, "diff": diff}, status_code=200)

    @app.get("/v1/capabilities", tags=["ops"],
             summary="各资源面与派发器申报的能力（只读）")
    async def list_capabilities() -> Response:
        """SPI 实现申报的能力，供客户端做能力探测。

        `dispatch` 来自 `Dispatcher.capabilities()` —— 那份申报与
        `tests/test_dispatch_capabilities.py` 的行为测试一一对应，
        所以它是**可验证的声明**，不是自说自话。
        """
        dispatcher = getattr(app.state, "tool_dispatcher", None)
        dispatch: list[str] = []
        if dispatcher is not None:
            caps = getattr(dispatcher, "capabilities", None)
            if callable(caps):
                try:
                    dispatch = sorted(str(c) for c in caps())
                except Exception:                              # noqa: BLE001
                    dispatch = []
        registry = getattr(app.state, "tools", None)
        tools_count = len(registry) if hasattr(registry, "__len__") else 0
        return JSONResponse(content={
            "dispatch": dispatch,
            "counts": {"tools": tools_count},
        }, status_code=200)

    # ── ①b 续跑挂起的回合（审批）──────────────────────────────
    @app.post("/v1/turns/{ctx_id}/resume", tags=["turns"],
              summary="批准或拒绝一个挂起等待确认的回合")
    async def post_turn_resume(ctx_id: str, body: ResumeRequest,
                               request: Request) -> Response:
        """**审批的 HTTP 入口**。

        在此之前，审批状态机只有 Python API 能用：`loop.resume()` 存在、
        `resume_from_store()` 存在，但 `/v1/turns` 里 `decision` 是硬编码
        `True` —— 等于"拿着 ctx_id 再发一次请求就自动批准"。拒绝路径根本
        不存在，而这恰恰是审批要防的事。

        状态码：

            200  批准后跑完，或拒绝后模型已换方案跑完（都是**成功**的回合结果）
            404  ctx_id 不存在 / 已过期 / 不属于调用方（不泄露"存在但不是你的"）
            409  ctx_id 存在但不在 WAITING_USER（没什么可批的）
            422  请求体不合法
        """
        if shutdown.is_shutting_down():
            _observe_http(metrics, "/v1/turns/resume", 503)
            return JSONResponse(content=_shutting_down_payload(), status_code=503)

        missing = _missing_identity(body, _header_principal(request))
        if missing:
            _observe_http(metrics, "/v1/turns/resume", 422)
            return JSONResponse(
                content=_validation_payload(request.url.path, missing),
                status_code=422)

        # ctx_id 直接进 store 的键空间，先按 `core.store` 的字符集校验，
        # 避免把路径穿越一类的东西带进文件型 store。
        if not _CTX_ID_RE.fullmatch(ctx_id):
            _observe_http(metrics, "/v1/turns/resume", 422)
            return JSONResponse(
                content={"error": "validation_failed", "status": 422,
                         "detail": f"ctx_id={ctx_id!r} 含非法字符",
                         "failed": ["ctx_id"]},
                status_code=422)

        gateway_request: dict[str, Any] = {
            "goal": "继续",              # 挂起的回合不重新规划目标，这里有值即可
            "ctx_id": ctx_id,
            "decision": bool(body.decision),
            "principal": body.principal.to_gateway(),
        }
        if body.reason:
            gateway_request["reason"] = body.reason
        if body.deadline_ms is not None:
            gateway_request["deadline_ms"] = body.deadline_ms
        if body.budget is not None:
            dumped = {k: v for k, v in body.budget.model_dump().items()
                      if v is not None}
            if dumped:
                gateway_request["budget"] = dumped
        authed = _header_principal(request)
        if authed is not None:
            conflict = _principal_conflict(body.principal, authed)
            if conflict:
                _observe_http(metrics, "/v1/turns/resume", 403)
                return _conflict_response(request.url.path, conflict)
            gateway_request["principal"] = _principal_dict(authed)

        try:
            result = await asyncio.to_thread(gateway.handle, gateway_request)
        except Exception as exc:        # noqa: BLE001
            _observe_http(metrics, "/v1/turns/resume", 500)
            return JSONResponse(
                content={"error": "internal_error",
                         "detail": f"{type(exc).__name__}: {exc}",
                         "status": 500},
                status_code=500)

        # 把 Gateway 的拒绝翻译成这个端点自己的语义。
        # `resume_failed` = 载入失败/不存在 → 404（同时也是防枚举：不区分
        # "不存在"与"存在但不是你的"）。
        if not result.ok and result.error.startswith("resume_failed"):
            _observe_http(metrics, "/v1/turns/resume", 404)
            return JSONResponse(
                content={"error": "ctx_not_found", "status": 404,
                         "detail": f"ctx_id={ctx_id} 不存在、已过期或无权访问"},
                status_code=404)
        # 载入了但没在等确认 → 409：客户端调错了端点，不是资源缺失。
        if (not result.ok and result.error.startswith("resume_needs_decision")):
            _observe_http(metrics, "/v1/turns/resume", 409)
            return JSONResponse(
                content={"error": "not_awaiting_confirmation", "status": 409,
                         "detail": "该 ctx_id 不处于等待确认状态（可能已处理或从未挂起）"},
                status_code=409)

        response = _result_response(result)
        _observe_http(metrics, "/v1/turns/resume", response.status_code)
        return response

    # ── ② 流式回合（SSE）────────────────────────────────────
    @app.post("/v1/turns/stream", tags=["turns"],
              summary="跑一个回合，逐条 SSE 推送事件")
    async def post_turn_stream(body: TurnRequest, request: Request) -> Response:
        if shutdown.is_shutting_down():
            _observe_http(metrics, "/v1/turns/stream", 503)

            async def _one() -> AsyncIterator[str]:
                # 只有一条 rejected：客户端状态机据此收口（不启动"半条流"）
                yield sse_frame("rejected", {"error": SHUTTING_DOWN, "audit_id": ""})

            return StreamingResponse(_one(), media_type="text/event-stream",
                                     headers=dict(SSE_HEADERS))
        missing = _missing_identity(body, _header_principal(request))
        if missing:
            _observe_http(metrics, "/v1/turns/stream", 422)
            return JSONResponse(content=_validation_payload(
                request.url.path, missing), status_code=422)
        gateway_request = body.to_gateway_request()
        # 把超时中间件注入的取消信号透传给 Gateway（→ ExecCtx.cancel_event）。
        # 没有它，504 之后那一回合会继续跑完、继续烧 token。
        _ce = request.scope.get("_cancel_event")
        if _ce is not None:
            gateway_request["_cancel_event"] = _ce
        # 进度侧通道：只有**流式**端点需要（子 Agent 的 start/step/finish
        # 要实时推给客户端）。JSON 端点不看播报，注入了也是空转。
        _sink = request.scope.get("_progress_sink")
        if _sink is not None:
            gateway_request["_progress"] = _sink
        # 隐式 deadline：没显式给 deadline 时，用「HTTP 超时 × 0.9」当硬截止。
        # 留 10% 余量的目的：让 Gateway 有机会**返回一个正常的 DEADLINE 结果**
        # （带已完成步数与部分文本），而不是被中间件直接 504 砍断 ——
        # 前者对客户端更有用，后者只剩一个状态码。
        _htt = request.scope.get("_http_timeout_s")
        if _htt and float(_htt) > 0:
            implicit = float(_htt) * 1000.0 * 0.9
            cur = gateway_request.get("deadline_ms")
            gateway_request["deadline_ms"] = (implicit if cur is None
                                              else min(float(cur), implicit))
        authed = _header_principal(request)
        if authed is not None:
            conflict = _principal_conflict(body.principal, authed)
            if conflict:
                # 身份冲突在**开流之前**就拒：不能让客户端先收到 accepted 再失败
                # （"流里有半条成功"比直接 403 更难排查）。用 JSON 而不是 SSE 帧：
                # 状态码本身就是给客户端状态机的信号。
                _observe_http(metrics, "/v1/turns/stream", 403)
                return _conflict_response(request.url.path, conflict)
            gateway_request["principal"] = _principal_dict(authed)
        # 进度侧通道：子 Agent 的 start/step/finish 要靠它实时推给客户端。
        # 有界队列（满了丢弃并计数）—— 播报不能把主链路拖死。
        try:
            from core.observability import QueueProgressSink
            request.scope["_progress_sink"] = QueueProgressSink(
                maxsize=int(os.environ.get("AGENT_RUNTIME_PROGRESS_QUEUE") or 1024))
        except Exception:           # noqa: BLE001 - 观测面 fail-open
            pass
        if request.scope.get("_progress_sink") is not None:
            gateway_request["_progress"] = request.scope["_progress_sink"]
        _observe_http(metrics, "/v1/turns/stream", 200)
        return StreamingResponse(_event_stream(gateway, gateway_request, shutdown, logger),
                                 media_type="text/event-stream",
                                 headers=dict(SSE_HEADERS))

    # ── ②之二 Workflow 显式入口（B）────────────────────────
    # 与 `/v1/turns` 的三条一致性（任务书 ④：鉴权/配额必须同源）：
    #
    #   1. **同一套中间件栈**：路由注册在同一个 app 上 → 限流 / 体量 / 超时 / 鉴权
    #      一并生效。端点里再判一次鉴权就是两套口径（模块 docstring 硬纪律 1）。
    #   2. **同一套身份规则**：未启用鉴权时 body 必须给 user_id/tenant
    #      （`_missing_identity`）；启用鉴权时凭据身份是权威，body 只做一致性声明，
    #      冲突 → 403（`_principal_conflict`）。
    #   3. **同一套关停纪律**：shutting_down → 503 + `_shutting_down_payload()`。
    #
    # 与 turns 唯一的结构差异：这里**不接受** deadline/超时语义。一个 DAG 可能跑
    # 几分钟，把它绑在 HTTP 请求的生命周期上等于"API 一超时流水线就成孤儿"。
    # 所以提交立即返回，执行在后台线程池里跑完（进程级生命周期，不是请求级）。

    @app.post("/v1/workflows", tags=["workflows"], status_code=202,
              summary="提交静态 DAG（异步执行，立即返回 202 + workflow_id）")
    async def post_workflow(body: WorkflowRequest, request: Request) -> Response:
        if shutdown.is_shutting_down():
            _observe_http(metrics, "/v1/workflows", 503)
            return JSONResponse(content=_shutting_down_payload(), status_code=503)
        authed = _header_principal(request)
        missing = _missing_identity(body, authed)
        if missing:
            _observe_http(metrics, "/v1/workflows", 422)
            return JSONResponse(content=_validation_payload(request.url.path, missing),
                                status_code=422)
        if authed is not None:
            conflict = _principal_conflict(body.principal, authed)
            if conflict:
                _observe_http(metrics, "/v1/workflows", 403)
                return _conflict_response(request.url.path, conflict)
            principal = authed
            principal_dict = _principal_dict(authed)
        else:
            # 未启用鉴权：请求体是唯一身份来源（已经过 `_missing_identity` 必填校验）。
            # scope 由客户端声明 —— 这是"不装鉴权的部署"本身的性质，不是本端点放松的。
            principal = Principal(
                user_id=str(body.principal.user_id or "").strip(),
                tenant=str(body.principal.tenant or "").strip(),
                roles=tuple(body.principal.roles or ()),
                scopes=frozenset(body.principal.scopes or ()),
            )
            principal_dict = _principal_dict(principal)

        spec = body.to_spec()
        errors = validate_workflow_spec(spec)
        if errors:
            # 400 而不是 422：请求**形状**完全合法（pydantic 已过），错的是**拓扑语义**
            # （成环 / 未知依赖）。422 会让客户端以为"改字段格式再试"，而正确动作是
            # "改 DAG 结构"。`errors` 逐条给出，成环会点名是哪几个节点。
            _observe_http(metrics, "/v1/workflows", 400)
            return JSONResponse(
                content={"error": "invalid_dag",
                         "detail": "DAG 不合法：" + "；".join(errors),
                         "errors": list(errors), "status": 400,
                         "path": request.url.path},
                status_code=400)

        try:
            # registry.submit 内部会再验一次（直接调用方也受保护），幂等且便宜
            state = await asyncio.to_thread(
                app.state.workflows.submit,
                spec=spec, principal=principal, principal_dict=principal_dict,
                goals=dict(body.goals or {}), max_parallel=body.max_parallel)
        except WorkflowSubmitError as exc:
            _observe_http(metrics, "/v1/workflows", exc.status)
            headers = {}
            if exc.retry_after is not None:
                headers["Retry-After"] = str(max(1, int(math.ceil(exc.retry_after))))
            return JSONResponse(
                content={"error": ("workflow_capacity" if exc.status == 429
                                   else "invalid_dag" if exc.status == 400
                                   else "workflow_unavailable"),
                         "detail": str(exc), "status": exc.status,
                         "path": request.url.path},
                status_code=exc.status, headers=headers or None)

        _observe_http(metrics, "/v1/workflows", 202)
        return JSONResponse(
            content={"workflow_id": state.id,
                     "status": "accepted",
                     "spec_name": state.spec_name,
                     "layers": [list(layer) for layer in state.layers],
                     "node_count": len(state.nodes),
                     "max_parallel": state.max_parallel,
                     "created_at": round(float(state.created_at), 6),
                     "links": {"self": f"/v1/workflows/{state.id}"}},
            status_code=202,
            headers={"Location": f"/v1/workflows/{state.id}"})

    @app.get("/v1/workflows/{workflow_id}/stream", tags=["workflows"],
             summary="SSE 推送 workflow 进度（节点状态变化 + 终态）")
    async def stream_workflow(workflow_id: str, request: Request) -> Response:
        """把流水线进度实时推出去。

        实现是**轮询式**（默认 200ms 取一次快照，只在有变化时发帧）而不是事件总线：
          * 状态本来就在内存里，读一次很便宜
          * 事件总线要在每处状态写入点埋钩子，漏一处就少一种事件
          * 轮询天然**幂等**——重连的客户端不会漏掉"已经是 done"这种既成事实

        代价是延迟上限 = 轮询间隔（默认 200ms）。对"流水线跑到第几层"这个
        粒度完全够用；要更细就把 `AGENT_RUNTIME_WORKFLOW_STREAM_MS` 调小。
        """
        if app.state.workflows.snapshot(workflow_id) is None:
            _observe_http(metrics, "/v1/workflows", 404)
            return JSONResponse(
                content={"error": "workflow_not_found", "status": 404,
                         "detail": f"workflow 不存在：{workflow_id}"},
                status_code=404)
        interval = float(os.environ.get("AGENT_RUNTIME_WORKFLOW_STREAM_MS") or 200) / 1000.0
        interval = min(max(interval, 0.02), 5.0)

        async def _frames() -> AsyncIterator[str]:
            seq = 0
            last_sig: tuple | None = None
            try:
                yield sse_frame("accepted", {"workflow_id": workflow_id,
                                             "interval_ms": int(interval * 1000)}, seq)
                seq += 1
                while True:
                    snap = app.state.workflows.snapshot(workflow_id)
                    if snap is None:            # 被容量淘汰了
                        yield sse_frame("completed", {
                            "ok": False, "status": "evicted",
                            "error": "workflow 已被容量淘汰，无法继续观察",
                        }, seq)
                        return
                    # 只在**有变化**时发帧：轮询 5 分钟不动不该刷 1500 条相同事件
                    sig = (snap["status"],
                           tuple((n["name"], n["status"]) for n in snap["nodes"]))
                    if sig != last_sig:
                        last_sig = sig
                        yield sse_frame("progress", {
                            "status": snap["status"],
                            "progress": snap["progress"],
                            "nodes": [{"name": n["name"], "status": n["status"],
                                       "steps": n["steps"], "error": n["error"]}
                                      for n in snap["nodes"]],
                        }, seq)
                        seq += 1
                    if snap["status"] in (WF_DONE, WF_FAILED, WF_INTERRUPTED):
                        yield sse_frame("completed", {
                            "ok": snap["status"] == WF_DONE,
                            "status": snap["status"],
                            "error": snap.get("error", ""),
                            "progress": snap["progress"],
                            "resumed_count": snap.get("resumed_count", 0),
                        }, seq)
                        return
                    await asyncio.sleep(interval)
            except (asyncio.CancelledError, GeneratorExit):
                # 客户端走了：不 yield（GeneratorExit 之后 yield 会 RuntimeError）
                return

        _observe_http(metrics, "/v1/workflows", 200)
        return StreamingResponse(_frames(), media_type="text/event-stream",
                                 headers=dict(SSE_HEADERS))

    @app.post("/v1/workflows/{workflow_id}/resume", tags=["workflows"],
              status_code=202,
              summary="续跑一条 interrupted/failed 的 workflow（**跳过已完成节点**）")
    async def resume_workflow(workflow_id: str, request: Request) -> Response:
        """续跑。**这是"续跑"与"重跑一遍"的唯一区别：跳过已完成的节点。**

        重放已完成的节点会重复产生副作用（重复发版、重复删库）——
        发布流程里那是灾难。所以 `done` 的节点只回填结果，不重新执行。
        """
        if shutdown.is_shutting_down():
            _observe_http(metrics, "/v1/workflows", 503)
            return JSONResponse(content=_shutting_down_payload(), status_code=503)
        authed = _header_principal(request)
        if authed is not None:
            principal = authed
            principal_dict = _principal_dict(authed)
        else:
            # 未启用鉴权：续跑要沿用**原提交者**的身份（不能凭空换人）。
            snap = app.state.workflows.snapshot(workflow_id) or {}
            principal_dict = dict(snap.get("principal") or {})
            try:
                principal = _principal_from_dict(principal_dict)
            except Exception as exc:                       # noqa: BLE001
                _observe_http(metrics, "/v1/workflows", 409)
                return JSONResponse(
                    content={"error": "resume_failed", "status": 409,
                             "detail": f"无法复原原提交者身份：{exc}"},
                    status_code=409)
        try:
            state = await asyncio.to_thread(
                app.state.workflows.resume, workflow_id,
                principal=principal, principal_dict=principal_dict)
        except WorkflowSubmitError as exc:
            status = int(getattr(exc, "status", 400))
            _observe_http(metrics, "/v1/workflows", status)
            body = {"error": "resume_failed", "detail": str(exc), "status": status}
            headers: dict[str, str] = {}
            retry = getattr(exc, "retry_after", None)
            if retry is not None:
                headers["Retry-After"] = str(int(retry))
            return JSONResponse(content=body, status_code=status, headers=headers)
        except Exception as exc:                           # noqa: BLE001
            _observe_http(metrics, "/v1/workflows", 500)
            return JSONResponse(
                content={"error": "internal_error", "status": 500,
                         "detail": f"{type(exc).__name__}: {exc}"},
                status_code=500)
        _observe_http(metrics, "/v1/workflows", 202)
        getattr(app.state, "observe_workflows", lambda: None)()
        return JSONResponse(content=state.to_dict(), status_code=202,
                            headers={"Location": f"/v1/workflows/{workflow_id}"})

    @app.get("/v1/workflows/{workflow_id}", tags=["workflows"],
             summary="查一条 workflow 的状态（层 / 节点 / 进度 / 每个节点的结果）")
    async def get_workflow(workflow_id: str) -> Response:
        snapshot = app.state.workflows.snapshot(workflow_id)
        if snapshot is None:
            # 404 的语义是"这个 id 我没见过"。它**同时**覆盖三种情况：
            # 从未提交 / 打错字 / 已被容量淘汰 —— 客户端都只能重新提交，
            # 所以不细分（细分会把"哪条被淘汰了"变成可枚举的信息）。
            _observe_http(metrics, "/v1/workflows", 404)
            return JSONResponse(
                content={"error": "workflow_not_found",
                         "detail": f"没有这条 workflow: {workflow_id}",
                         "status": 404, "path": f"/v1/workflows/{workflow_id}"},
                status_code=404)
        _observe_http(metrics, "/v1/workflows", 200)
        return JSONResponse(content=snapshot, status_code=200)

    @app.get("/v1/workflows", tags=["workflows"],
             summary="列出 workflow（按创建时间倒序，limit/offset 分页）")
    async def list_workflows(limit: int = Query(20, ge=1, le=100),
                             offset: int = Query(0, ge=0)) -> Response:
        window, total = app.state.workflows.list_states(limit=limit, offset=offset)
        _observe_http(metrics, "/v1/workflows", 200)
        return JSONResponse(
            content={"workflows": window, "total": int(total),
                     "limit": int(limit), "offset": int(offset),
                     "max_tracked": int(app.state.workflows.max_tracked)},
            status_code=200)

    # ── ③ Prometheus 指标 ──────────────────────────────────
    @app.get("/metrics", tags=["ops"], summary="Prometheus text exposition")
    async def get_metrics() -> Response:
        try:
            # render() 会拿 Metrics 的 RLock 并遍历 registry：可能慢，不能阻塞循环
            text = await asyncio.to_thread(metrics.render)
        except Exception as exc:        # noqa: BLE001
            try:
                logger.error("metrics.render_failed",
                             error=f"{type(exc).__name__}: {exc}")
            except Exception:           # noqa: BLE001
                pass
            # **不返回空 body**：空 exposition 会被抓取端当成"指标没了"，
            # 而真实情况是"渲染坏了"。500 + 原因才能让人去修。
            return JSONResponse(
                content={"error": "metrics_render_failed",
                         "detail": f"{type(exc).__name__}: {exc}"},
                status_code=500)
        if isinstance(text, (bytes, bytearray)):
            text = bytes(text).decode("utf-8", "replace")
        return Response(str(text or ""), media_type=PROMETHEUS_CONTENT_TYPE)

    # ── ④ 存活探针 ─────────────────────────────────────────
    @app.get(HEALTH_PATH, tags=["ops"], summary="存活探针（进程活着就 200）")
    async def healthz() -> Response:
        """**只看进程**：不探依赖、不判关停。

        关停期间仍然 200 是刻意的：liveness 失败会触发**重启**，而一个正在优雅
        排空的进程被重启，等于把最后的 flush 和审计路径直接砍掉。摘流是
        `/readyz` 的事，两件事不能混。
        """
        return JSONResponse(content={
            "status": "alive", "version": VERSION, "pid": os.getpid(),
            "uptime_s": round(time.time() - started_at, 3),
            "shutting_down": bool(shutdown.is_shutting_down()),
        }, status_code=200)

    # ── ⑤ 就绪探针 ─────────────────────────────────────────
    @app.get("/readyz", tags=["ops"], summary="就绪探针（依赖自检）")
    async def readyz() -> Response:
        checks = await probe_all_async(specs, READY_TIMEOUT_S)
        shutting = bool(shutdown.is_shutting_down())
        ready = _ready(checks) and not shutting
        if ready:
            reason = ""
        elif shutting:
            reason = SHUTTING_DOWN
        else:
            bad = [name for name, check in checks.items()
                   if check.get("critical", True) and not check.get("ok")]
            reason = "unhealthy_dependencies: " + ", ".join(bad)
        return JSONResponse(
            content={"ready": ready, "checks": checks, "reason": reason,
                     "shutting_down": shutting},
            status_code=200 if ready else 503)

    # ── ⑥ 运行时自述 ───────────────────────────────────────
    @app.get("/v1/info", tags=["ops"], summary="运行时自述（版本/依赖/后端）")
    async def runtime_info() -> Response:
        # 启动快照（不是实时探针）：info 回答"起来时是什么"，readyz 回答"现在能不能接"
        deps_info: dict[str, dict] = {}
        backends: dict[str, str] = {}
        for name, check in startup_checks.items():
            kind = str(check.get("kind") or "")
            deps_info[name] = {"available": bool(check.get("ok")),
                               "critical": bool(check.get("critical", True)),
                               "probe": str(check.get("probe") or ""),
                               "kind": kind,
                               "detail": str(check.get("detail") or "")}
            backends[name] = kind
        return JSONResponse(content={
            "service": SERVICE_NAME,
            "version": VERSION,
            "python": sys.version.split()[0],
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "pid": os.getpid(),
            "uptime_s": round(time.time() - started_at, 3),
            "modules": {name: _module_available(name) for name in _MODULE_PROBES},
            "deps": deps_info,
            "backends": backends,
            "features": {
                "sync_turns": True,
                "stream_turns": True,
                "prometheus_metrics": True,
                "dependency_probes": True,
                "signal_driven_shutdown": True,
                "explicit_workflows": True,
            },
            "config": _env_snapshot(),          # 白名单 + scrub，绝不 dump os.environ
            "auth": _auth_snapshot(app.state),
            # Workflow 显式入口（B）的**生效值**：enabled / max_tracked / running。
            # 为什么自述它：一个"提交了但查不到"的工单，第一个要确认的就是
            # "这个进程到底有没有 workflow 面、容量多大、当前有几条在跑"。
            "workflow": _workflow_info_section(app.state),
            # 三道资源边界的**生效值**（都不是密钥，运维需要一眼看到"到底关了没有"）
            "hardening": dict(getattr(app.state, "hardening", {}) or {}),
            "signals": list(getattr(app.state, "signals", []) or []),
            "shutting_down": bool(shutdown.is_shutting_down()),
        }, status_code=200)

    # ── ⑦ 身份自述（**只在启用鉴权时注册**）────────────────────
    # 为什么条件注册：没装鉴权时"我是谁"没有答案（身份来自请求体，等于客户端自报）。
    # 让它 404 比让它 500/返回一个默认身份都更诚实 —— 能力不存在，而不是能力坏了。
    #
    # 为什么需要它：API key 的默认 user_id 是不可逆别名（`svc_xxxxxxxx`），
    # 客户端要在请求体里声明身份（一致性校验）时得先知道自己是谁。这里给的只是
    # **调用方自己的**身份，不是别人的，所以不构成信息泄露。
    if authenticator is None:
        # **未启用鉴权时的显式 404**。
        # 状态码仍是 404（能力不存在，不是能力坏了），但 body 说清"为什么"和
        # "怎么开"。裸 `{"detail":"Not Found"}` 会让客户端以为是路径写错，
        # 去翻文档、改 URL —— 而它需要的是改一个环境变量。
        @app.get("/v1/whoami", tags=["ops"], include_in_schema=False)
        async def whoami_disabled() -> Response:
            return JSONResponse(content={
                "error": "whoami_requires_auth",
                "status": 404,
                "detail": "本实例未启用鉴权，/v1/whoami 不存在。"
                          "未启用鉴权时身份由请求体给出，没有『我是谁』这个服务端事实。",
                "enable": "AGENT_RUNTIME_AUTH=1（并配 API key 或 JWT）",
            }, status_code=404)

    if authenticator is not None:
        @app.get("/v1/whoami", tags=["ops"], summary="凭据对应的身份（需鉴权）")
        async def whoami(request: Request,
                         principal: Principal = Depends(current_principal)) -> Response:
            auth_result = getattr(request.state, "auth", None)
            return JSONResponse(content={
                "principal": principal.to_dict(),
                "scheme": str(getattr(auth_result, "scheme", "") or ""),
                # 令牌快到期时给客户端一个"该刷新了"的信号（0 = 不需要）
                "retry_after_s": float(getattr(auth_result, "retry_after_s", 0.0) or 0.0),
            }, status_code=200)

    # ── 错误出口：一律 JSON（FastAPI 默认的 404 是 HTML，客户端 parse 不了）──
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request,
                                     exc: StarletteHTTPException) -> JSONResponse:
        detail = exc.detail
        if not isinstance(detail, str):
            detail = json.dumps(jsonable_encoder(detail), ensure_ascii=False)
        return JSONResponse(
            content={"error": _http_error_name(exc.status_code),
                     "detail": detail, "status": int(exc.status_code),
                     "path": request.url.path},
            status_code=int(exc.status_code),
            headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request,
                                           exc: RequestValidationError) -> JSONResponse:
        """422 且**说清缺哪个字段**：`{"field": "goal", "message": "Field required"}`。"""
        fields: list[dict] = []
        for err in exc.errors():
            loc = [str(part) for part in err.get("loc", ()) if str(part) != "body"]
            fields.append({"field": ".".join(loc) or "body",
                           "message": str(err.get("msg", "")),
                           "type": str(err.get("type", ""))})
        missing = [item["field"] for item in fields if item["type"] == "missing"]
        detail = (f"缺少必填字段: {', '.join(missing)}" if missing
                  else "请求体校验失败")
        return JSONResponse(
            content={"error": "validation_error", "detail": detail,
                     "fields": fields, "status": 422, "path": request.url.path},
            status_code=422)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request,
                                          exc: Exception) -> JSONResponse:
        try:
            logger.error("http.unhandled",
                         path=request.url.path, error=f"{type(exc).__name__}: {exc}")
        except Exception:               # noqa: BLE001
            pass
        return JSONResponse(
            content={"error": "internal_error",
                     "detail": f"{type(exc).__name__}: {exc}",
                     "status": 500, "path": request.url.path},
            status_code=500)

    return app


def _tool_public_view(m: Any) -> dict[str, Any]:
    """`ToolManifest` → 可安全暴露的只读视图。

    **白名单，不是黑名单**：只挑明确安全的字段出来。`ToolManifest` 上有
    `secrets`（要注入的环境变量名），那是部署内部信息，出网就是泄露面。
    用黑名单（"除了 secrets 都放"）迟早会漏掉将来新增的敏感字段。
    """
    tier = getattr(m, "risk_tier", None)
    try:
        tier_name = tier.name if hasattr(tier, "name") else str(tier)
        tier_value = int(tier) if tier is not None else 0
    except (TypeError, ValueError):
        tier_name, tier_value = "UNKNOWN", -1
    side = getattr(m, "side_effect", None)
    side_name = side.value if hasattr(side, "value") else str(side)
    return {
        "name": getattr(m, "name", ""),
        "description": getattr(m, "description", ""),
        "side_effect": side_name,
        "risk_tier": tier_name,
        "risk_level": tier_value,
        # 需不需要人工确认：T2+ 且没带 approval 就会被拦成 needs_confirmation。
        # 客户端据此决定"要不要准备审批 UI"，所以直接给结论而不是让前端自己算。
        "needs_approval": tier_value >= 2,
        "idempotent": getattr(m, "idempotent", None),
        "required_scope": getattr(m, "required_scope", ""),
        "per_turn_limit": getattr(m, "per_turn_limit", 0),
        "timeout_ms": getattr(m, "timeout_ms", 0),
        "max_result_bytes": getattr(m, "max_result_bytes", 0),
        "input_schema": getattr(m, "input_schema", None),
    }


def _result_response(result: Any) -> Response:
    """TurnResult → JSONResponse（状态码按映射表，429 带 Retry-After）。"""
    status = status_for_result(result)
    headers: dict[str, str] = {}
    if status == 429:
        retry_after = _retry_after(result)
        headers["Retry-After"] = str(retry_after if retry_after is not None else 1)
    payload = jsonable_encoder(result_payload(result))
    return JSONResponse(content=payload, status_code=status, headers=headers)


def _exit_server(server: Any) -> Callable[[dict], None]:
    """drain 完了让 uvicorn 退出（`should_exit` 是它公开的退出开关）。"""
    def _callback(report: dict) -> None:
        if server is None:
            return
        with contextlib.suppress(Exception):
            server.should_exit = True
    return _callback


# ══════════════════════════════════════════════════════════════
# 八、默认装配（uvicorn 入口用）
# ══════════════════════════════════════════════════════════════
def _otel_headers(env: Mapping[str, str]) -> dict[str, str]:
    """构造 OTLP 导出用的鉴权头。

    两种来源，优先级从高到低：
      1. `OTEL_EXPORTER_OTLP_HEADERS`（标准变量，`k=v,k2=v2`）
      2. `LANGFUSE_PUBLIC_KEY` + `LANGFUSE_SECRET_KEY` → `Authorization: Basic ...`
         （Langfuse 的 OTLP 端点就吃这个）

    **密钥只在内存里拼一次，不写日志、不进 /v1/info。**
    """
    import base64
    raw = str(env.get("OTEL_EXPORTER_OTLP_HEADERS") or "").strip()
    if raw:
        out: dict[str, str] = {}
        for pair in raw.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                out[k.strip()] = v.strip()
        if out:
            return out
    pk = str(env.get("LANGFUSE_PUBLIC_KEY") or "").strip()
    sk = str(env.get("LANGFUSE_SECRET_KEY") or "").strip()
    if pk and sk:
        token = base64.b64encode(f"{pk}:{sk}".encode()).decode()
        return {"Authorization": f"Basic {token}"}
    return {}


def load_extension(spec: str | None, *, kind: str, logger: Any = None) -> Any:
    """`"module:attr"` → 对象。空/None → None（表示该能力不启用）。

    **这是 Runtime 不依赖 Agent 层的关键。**
    Runtime 只认"配置里给了个可导入路径"，自己从不 `import agents`——
    边界测试（`test_builder_root_does_not_import_agents`）会因此保持绿，
    而部署方照样能插自己的 Extractor / Planner / Router。

    解析失败一律**抛错**，不静默返回 None：`AGENT_RUNTIME_EXTRACTOR` 写错了
    却"看起来正常启动"，是最难查的一类故障（记忆永远为空，没人报错）。
    """
    raw = str(spec or "").strip()
    if not raw:
        return None
    if ":" not in raw:
        raise ValueError(
            f"{kind} 配置格式错误：{raw!r}。应为 'module:attr'，"
            f"例如 'agents.extractor:OpsExtractor'")
    mod_name, _, attr_path = raw.partition(":")
    try:
        mod = importlib.import_module(mod_name.strip())
    except Exception as exc:                                   # noqa: BLE001
        raise RuntimeError(
            f"{kind} 加载失败：导入模块 {mod_name!r} 出错 "
            f"（{type(exc).__name__}: {exc}）") from exc
    obj: Any = mod
    for part in attr_path.strip().split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError as exc:
            raise RuntimeError(
                f"{kind} 加载失败：{mod_name!r} 里没有 {attr_path!r}") from exc
    if logger is not None:
        try:
            logger.info("extension.loaded", kind=kind, spec=raw)
        except Exception:           # noqa: BLE001
            pass
    return obj


def _instantiate(obj: Any, *args: Any, **kw: Any) -> Any:
    """扩展点可能是类，也可能是工厂函数，也可能已经是实例。

    **普通函数必须当工厂调用**。早先的判据是 "callable 且没有 `__dict__` 才调用"，
    而 Python 的函数对象**有** `__dict__` —— 于是 `module:make_template` 这种
    文档里推荐的写法拿到的是函数本身，不是它返回的对象。症状极隐蔽：

      * `/v1/tools`、启动日志、`/v1/capabilities` 全部正常（装配确实"成功"了）；
      * 真用起来才炸 —— 例：`AGENT_RUNTIME_PROMPT_TEMPLATE` 拿到函数后
        `template.system` 抛 AttributeError，被 `core/loop.py` 吞成一句
        `[builder 失败：...] 请基于 history 继续。`，模型从此看不到上下文。

    所以判据改成"**实例**才原样返回，其余 callable（类/函数/可调用对象）一律调用"。
    实例的识别用 `types`：函数、方法、内建函数都是工厂；其余 callable 若是
    某个类的实例（有 `__dict__` 或 `__slots__`），仍然当实例。
    """
    if obj is None:
        return None
    if isinstance(obj, (types.FunctionType, types.BuiltinFunctionType,
                        types.MethodType, functools.partial)):
        try:
            return obj(*args, **kw)
        except TypeError:
            return obj()
    if isinstance(obj, type):
        try:
            return obj(*args, **kw)
        except TypeError:
            return obj()
    # 剩下的 callable 实例（有 __dict__/__slots__ 的对象）按实例处理：
    # 它们往往自己就是实现（例如带 `complete()` 的 LLM adapter）。
    return obj


def _truthy(value: Any) -> bool:
    """环境变量布尔解析：1/true/yes/on 为真（大小写不敏感）。"""
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def build_gateway_from_env(*, logger: Any = None,
                           env: Mapping[str, str] | None = None,
                           store: Any = None,
                           metrics: Any = None) -> Any:
    """按环境变量装配一个**真实**的 Gateway（Loop + Governance + Store）。

    这是"最小可用运行时"：一个 PromptBuilder、一个 LLM adapter、一张（默认为空的）
    工具表。工具表留空是刻意的 —— 服务层不替部署方决定"这个进程能干什么"，
    工具注入属于业务装配（见 `demo/scenario_fullchain.py` 的 `_loop_factory_factory`）。

    环境变量：
        AGENT_RUNTIME_LLM          scripted（默认，确定性 mock）| openai
        AGENT_RUNTIME_PROMPT_TOKENS  Prompt 预算（默认 8000）
        AGENT_RUNTIME_RATE_LIMIT     每租户每 60s 的回合上限（默认 60）
        AGENT_RUNTIME_SESSION        1 = 启用会话多轮承接（默认 0，单轮语义）
        AGENT_RUNTIME_SESSION_MAX_HISTORY  会话保留的最大轮数（默认 20）
        AGENT_RUNTIME_SESSION_TTL_S        会话 TTL 秒（默认 7 天）
        OPENAI_*                    AGENT_RUNTIME_LLM=openai 时由 adapter 自己读

    Args:
        store: 显式注入的 Context 存储。`create_app` 会把 `/readyz` 探的**同一个**
               对象交进来 —— 探针与业务必须共用一份依赖，否则探针是假绿。

    为什么默认是 scripted mock：一个刚拉起来的进程应该能回答请求（证明整条链路
    通了），而不该在"没配 key"时启动失败。真 LLM 用 `AGENT_RUNTIME_LLM=openai`
    显式打开 —— 配置错误要报错，但**默认要能跑**。
    """
    e = dict(os.environ if env is None else env)
    try:
        from adapters.llm_mock import CallableLLM
        from builder import DEFAULT_TEMPLATE, PromptBuilder
        from core.loop import Loop
        from plane.governance import Governance, InputGuard, OutputGuard, RBAC
        from plane.tool import Dispatcher, ToolRegistry
    except Exception as exc:            # noqa: BLE001 - 缺件要立刻说清，不能静默降级
        raise RuntimeError(
            f"默认装配失败（缺 Runtime 组件）：{type(exc).__name__}: {exc}") from exc

    prompt_tokens = int(e.get("AGENT_RUNTIME_PROMPT_TOKENS") or 8000)
    rate_limit = int(e.get("AGENT_RUNTIME_RATE_LIMIT") or 60)
    llm_kind = str(e.get("AGENT_RUNTIME_LLM") or "scripted").strip().lower()

    # ── 扩展点：Prompt 模板 / profile（Agent 层内容，Runtime 按配置加载）──
    profile_factory = load_extension(e.get("AGENT_RUNTIME_PROMPT_PROFILE"),
                                     kind="prompt_profile", logger=logger)
    template_factory = load_extension(e.get("AGENT_RUNTIME_PROMPT_TEMPLATE"),
                                      kind="prompt_template", logger=logger)
    profile = _instantiate(profile_factory) if profile_factory else None
    template = _instantiate(template_factory) if template_factory else None
    if profile is None:
        # **默认用实测标定档，不用保守默认档。**
        #
        # 这是一条踩过的坑：`DEFAULT_PROFILE` 刻意高估（CJK 1.0 tok/字），
        # 而实测是 0.638。纯中文语料下默认档高估 **1.57x** ——
        # `budget=20000` 实际只能用到 12760 tok，白扔 36% 预算，
        # 而且会提前触发压缩、多烧摘要调用。
        #
        # 高危默认值的正确方向：**生产路径用标定值，保守值留给"没有实测数据"的
        # 纯手工装配**。反过来（默认保守、要自己记得切标定）必然有人忘。
        #
        # 可用 `AGENT_RUNTIME_PROMPT_PROFILE` 显式覆盖；也可以设
        # `AGENT_RUNTIME_TOKEN_PROFILE=conservative` 主动退回高估档
        # （换到未标定的模型时该这么做）。
        mode = str(e.get("AGENT_RUNTIME_TOKEN_PROFILE") or "calibrated").strip().lower()
        if mode in ("conservative", "default", "safe"):
            from builder import DEFAULT_PROFILE
            profile = DEFAULT_PROFILE
        else:
            from builder import CALIBRATED
            profile = CALIBRATED
        try:
            logger.info("prompt.token_profile", name=getattr(profile, "name", "?"),
                        cjk_per_token=getattr(profile, "cjk_per_token", 0),
                        other_chars_per_token=getattr(profile, "other_chars_per_token", 0))
        except Exception:                   # noqa: BLE001 - 观测面 fail-open
            pass
    # **`template or DEFAULT_TEMPLATE`：不配模板时必须落到内置模板。**
    # `PromptBuilder(template=None)` 不会退回默认值（默认值是参数缺省才生效），
    # 构造出的 builder 每次渲染都在 `template.system` 上抛 AttributeError，
    # 而 `core/loop.py` 把它吞成一句
    #   `[builder 失败：AttributeError: ...] 请基于 history 继续。`
    # —— 表现是**模型收到的 prompt 只有 88 字节、连 goal 都不在里面**，
    # 而 HTTP 面一切正常（200/DONE）。这条路径没有用例覆盖：
    # demo 自带 builder，单测用自己的，默认装配反而没人验。
    builder = PromptBuilder(total_tokens=prompt_tokens,
                            template=template or DEFAULT_TEMPLATE,
                            profile=profile)
    registry = ToolRegistry()

    # ── 工具表：声明式配置（YAML），不配置就是空表 ──────────────────
    #
    # 为什么不做成内置：服务层不该替部署方决定"这个进程能干什么"。
    # 但**必须**有声明入口，否则加一个工具要改 Python —— 运维不该为此动源码。
    # 配置错误一律 fail-closed：工具装了却调不到，比启动失败更难查。
    tool_manifests: list[Any] = []
    loaded_tools: Any = None
    try:
        from toolconfig import load_tools_from_env
    except Exception as exc:                                   # noqa: BLE001
        raise RuntimeError(
            f"工具配置模块不可用：{type(exc).__name__}: {exc}") from exc
    try:
        loaded_tools = load_tools_from_env(e)
    except Exception as exc:                                   # noqa: BLE001
        # 说清"是哪个文件、哪一项"，别只说加载失败
        raise RuntimeError(f"工具配置加载失败：{exc}") from exc
    for warn in getattr(loaded_tools, "warnings", ()) or ():
        if logger is not None:
            logger.warn("tools.config_warning", detail=str(warn))
    try:
        loaded_tools.register_into(registry)
    except Exception as exc:                                   # noqa: BLE001
        loaded_tools.close()
        raise RuntimeError(
            f"工具注册失败：{type(exc).__name__}: {exc}") from exc
    tool_manifests = list(getattr(loaded_tools, "manifests", ()) or ())
    if logger is not None:
        logger.info("tools.loaded", count=len(tool_manifests),
                    names=[getattr(m, "name", "?") for m in tool_manifests][:20],
                    source=str(e.get("AGENT_RUNTIME_TOOLS") or ""))

    dispatcher = Dispatcher(registry)

    # 治理面配额（`QuotaGuard`）与 Gateway 的 `rate_limit_per_minute` 是**两道独立的闸**：
    # 前者按 `kind` 分桶（`runtime_turn` / 工具 / LLM 调用各算各的），后者只管回合数。
    # 早先 serve 没暴露前者，于是压测时会被它的默认值 60/60s 拦住 ——
    # 而报错只说 "quota exceeded"，很容易误判成 Gateway 的配额没配。
    # `AGENT_RUNTIME_GOVERNANCE_QUOTA=0` 关闭它（与 `RATE_LIMIT=0` 同口径）。
    from plane.governance import QuotaGuard
    _gq_raw = e.get("AGENT_RUNTIME_GOVERNANCE_QUOTA")
    if _gq_raw is None:
        _quota_guard = QuotaGuard()
    else:
        # `0` = 关闭（与 `AGENT_RUNTIME_RATE_LIMIT=0` 同口径）。
        # 必须显式写进 `per_minute`：空表会 fallback 到 DEFAULT_LIMIT=60，
        # 那是"配置了却更严"的静默反转。
        _n = int(_gq_raw)
        _quota_guard = QuotaGuard(per_minute={
            "runtime_turn": _n, "tool_call": _n, "llm_call": _n})
    governance = Governance(rbac=RBAC(), input_guard=InputGuard(),
                            output_guard=OutputGuard(), quota=_quota_guard)

    def _scripted(goal: str) -> Any:
        """确定性回执。**不回显 goal**：服务自己的日志/响应不该成为注入回显通道。"""
        return CallableLLM(lambda _i, _prompt: _final_answer(goal))

    def _real() -> Any:
        from adapters.openai_compat import from_env as _from_env
        # 前缀可配：`AGENT_RUNTIME_LLM_PREFIX`（默认 OPENAI）。
        # 早期硬编码 "OPENAI"，导致用 BABEL_* 那套凭据时恒为 None ——
        # 而 `scripts/real_model_smoke.py` 用的正是 BABEL_*，两处口径不一致，
        # 表现为"冒烟能过、服务起不来"。前缀一配就对齐了。
        prefix = str(e.get("AGENT_RUNTIME_LLM_PREFIX") or "OPENAI").strip().upper()
        adapter = _from_env(prefix)
        if adapter is None:
            raise RuntimeError(
                f"AGENT_RUNTIME_LLM=openai 但 {prefix}_API_KEY / {prefix}_BASE_URL "
                f"不完整（前缀由 AGENT_RUNTIME_LLM_PREFIX 决定，当前 {prefix}）")
        return adapter

    # ── Memory Plane：Extractor 由配置提供（Agent 层），Runtime 管管道 ──
    memory_rt = None
    extractor_factory = load_extension(e.get("AGENT_RUNTIME_EXTRACTOR"),
                                       kind="extractor", logger=logger)
    if extractor_factory is not None:
        try:
            from plane.memory import MemoryRuntime
            mem_provider = _make_memory_provider(e)
            # ── 语义召回（可选）──────────────────────────────
            # 默认用 bigram 相似度（纯本地、无外部依赖）。
            # 开 `AGENT_RUNTIME_SEMANTIC=1` 且配了 embedding 网关时，
            # 换成语义向量 —— **降级安全**：网关挂了 `SemanticSimilarity`
            # 内部回落 bigram，召回不中断。
            #
            # 为什么做成子类而不是改 MemoryRuntime：那个模块是纯 bigram 的
            # 稳定实现（45 个用例），语义召回依赖外部网关。组合让两边独立演进。
            mem_cls: Any = MemoryRuntime
            if _truthy(e.get("AGENT_RUNTIME_SEMANTIC")):
                try:
                    from plane.wiring import semantic_memory
                    sem_provider = _make_embedder(e)
                    if sem_provider is None:
                        raise RuntimeError(
                            "AGENT_RUNTIME_SEMANTIC=1 但 embedding 网关未配置"
                            "（需要 AGENT_RUNTIME_EMBEDDING_BASE_URL 或 "
                            "AGENT_RUNTIME_VECTOR_BASE_URL）")
                    # `semantic_memory()` 返回的是**实例**（它内部定义了子类并
                    # 立即实例化）。取 `type()` 拿到类，才能用本进程的
                    # provider/extractor 构造 —— 否则那两个参数会被丢掉，
                    # 表现是"开了语义召回但记忆用的是默认 provider"。
                    mem_cls = type(semantic_memory(MemoryRuntime,
                                                   provider=sem_provider))
                    if logger is not None:
                        logger.info("memory.semantic", enabled=True,
                                    cls=mem_cls.__name__)
                except Exception as exc:                       # noqa: BLE001
                    # 显式开了却装不上 → 报错。静默退回 bigram 等于
                    # "配了语义召回但效果和没配一样"，最难查。
                    raise RuntimeError(
                        f"语义召回装配失败：{type(exc).__name__}: {exc}") from exc
            memory_rt = mem_cls(provider=mem_provider,
                                extractor=_instantiate(extractor_factory))
            if logger is not None:
                logger.info("memory.enabled",
                            extractor=getattr(extractor_factory, "__name__", "?"),
                            semantic=mem_cls is not MemoryRuntime)
        except Exception as exc:                               # noqa: BLE001
            # 配了 Extractor 却装不上 → 报错。静默不装等于"记忆永远为空"。
            raise RuntimeError(f"Memory 装配失败：{type(exc).__name__}: {exc}") from exc

    # **配了记忆面却没配 Extractor：出声。**
    # `AGENT_RUNTIME_SEMANTIC` / `AGENT_RUNTIME_RECALL_K` 都嵌在
    # `if extractor_factory is not None:` 里 —— 语义上没错（没有记忆就没有召回），
    # 但"配了不生效、日志一个字不提"正是本仓反复踩的那类坑：
    # 运维会以为语义召回开着，实际连记忆面都没装。
    if extractor_factory is None and logger is not None:
        for key, event in (("AGENT_RUNTIME_SEMANTIC", "memory.semantic_ignored"),
                           ("AGENT_RUNTIME_RECALL_K", "memory.recall_k_ignored")):
            raw = str(e.get(key) or "").strip()
            if raw and raw not in ("0", "false", "False"):
                try:
                    logger.warn(event, reason="未配置 AGENT_RUNTIME_EXTRACTOR，"
                                              "记忆面未装配，该开关不会生效")
                except Exception:            # noqa: BLE001 - 观测面 fail-open
                    pass

    # ── 其余可选能力：Router / Skills / 规划器 / 历史压缩 ──
    router_obj = _instantiate(load_extension(e.get("AGENT_RUNTIME_ROUTER"),
                                             kind="router", logger=logger))
    skills_obj = _instantiate(load_extension(e.get("AGENT_RUNTIME_SKILLS"),
                                             kind="skills", logger=logger))
    planner_factory = load_extension(e.get("AGENT_RUNTIME_PLANNER"),
                                     kind="planner", logger=logger)
    # **装配期**就实例化，不留到回合里。理由：构造失败（比如 StaticDAGPlanner
    # 少传 dags）如果拖到回合中，表现是"每一轮都 loop_error"，而根因在启动配置。
    planner_obj = None
    if planner_factory is not None:
        try:
            planner_obj = _instantiate(planner_factory)
        except Exception as exc:                               # noqa: BLE001
            raise RuntimeError(
                f"AGENT_RUNTIME_PLANNER 实例化失败：{type(exc).__name__}: {exc}\n"
                f"提示：需要参数的实现要写成工厂函数，"
                f"例如 'agents.planner:make_planner'") from exc
    # ── 资源面（可选）：把外部资源暴露成只读工具 ──────────────
    # `AGENT_RUNTIME_RESOURCES='module:factory'`，factory 返回
    # `[(ResourceRef, reader), ...]` —— reader 是 `uri -> str` 的读取函数。
    #
    # 为什么需要它：`ResourceRuntime` 实现了权限（`resource.*` scope）、缓存、
    # 大小上限，但**没有任何生产入口构造它**（早先只有 demo 用）。
    # 没有这一行，`RuntimeWiring.resources` 永远是空元组，
    # 架构图上的「资源面」在 HTTP 面等于不存在。
    resource_rt = None
    resource_refs: tuple[Any, ...] = ()
    res_factory = load_extension(e.get("AGENT_RUNTIME_RESOURCES"),
                                 kind="resources", logger=logger)
    if res_factory is not None:
        try:
            from plane.resources import ResourceRegistry, ResourceRuntime
            # **不直接用 `_instantiate`**：它把"有 `__dict__` 的 callable"
            # 当成实例原样返回，而普通函数也有 `__dict__` —— 于是工厂函数
            # 不会被调用，拿到的是函数对象本身。
            # 这里显式区分：类是构造器，函数是工厂。
            items = (res_factory() if not isinstance(res_factory, type)
                     else res_factory())
            if not isinstance(items, (list, tuple)):
                raise TypeError(
                    f"AGENT_RUNTIME_RESOURCES 的工厂必须返回 [(ResourceRef, reader), ...]，"
                    f"收到 {type(items).__name__}")
            res_reg = ResourceRegistry()
            refs: list[Any] = []
            for entry in items:
                try:
                    ref, reader = entry
                except (TypeError, ValueError) as exc:
                    raise TypeError(
                        f"每一项必须是 (ResourceRef, reader) 二元组，"
                        f"收到 {entry!r}") from exc
                res_reg.register(ref, reader)
                refs.append(ref)
            resource_rt = ResourceRuntime(res_reg, governance=governance)
            resource_refs = tuple(refs)
            if logger is not None:
                logger.info("resources.enabled", count=len(refs),
                            uris=[str(getattr(r, "uri", "?")) for r in refs][:10])
        except Exception as exc:                               # noqa: BLE001
            # 配了却装不上 → 报错。静默跳过等于"资源面配了但模型看不见"。
            raise RuntimeError(
                f"资源面装配失败：{type(exc).__name__}: {exc}") from exc

    # ── 知识面（可选）：文档入库 + 检索暴露成工具 ──────────────
    # `AGENT_RUNTIME_KNOWLEDGE='module:factory'`，factory 返回
    # `[Document, ...]`（`plane.knowledge.Document`）。
    #
    # 为什么需要它：`KnowledgeBase`（551 行、23 个用例）实现了 BM25 检索、
    # 版本线、分块，但**没有任何生产入口构造它** —— 架构图画着「四资源面」，
    # 而 HTTP 面只装了 Tool / Memory / Workflow 三个。这一行补上第四个。
    knowledge_rt = None
    knowledge_docs: tuple[Any, ...] = ()
    kn_factory = load_extension(e.get("AGENT_RUNTIME_KNOWLEDGE"),
                                kind="knowledge", logger=logger)
    if kn_factory is not None:
        try:
            from plane.knowledge import KnowledgeBase
            docs = kn_factory() if not isinstance(kn_factory, type) else kn_factory()
            if not isinstance(docs, (list, tuple)):
                raise TypeError(
                    f"AGENT_RUNTIME_KNOWLEDGE 的工厂必须返回 [Document, ...]，"
                    f"收到 {type(docs).__name__}")
            kb = KnowledgeBase()
            kb.add_many(docs)
            # `rebuild()` 才真正建索引；不调它 `retrieve` 会拿到空结果。
            # 早先 demo 里就踩过：add 完不 rebuild，检索永远 0 命中。
            kb.rebuild()
            # **存 `reader()`，不是 `kb` 本身**：`KnowledgeBase` 上没有
            # `retrieve` —— 检索方法长在只读门面 `KnowledgeReader` 上
            # （INV-11：Agent 侧拿到的句柄不应该有 add/retire）。
            # 早先这里存的是 `kb`，表现是「工具注册成功、`/v1/tools` 看得见、
            # 一调用就 AttributeError」—— 比没接线更难查，因为症状出现在
            # 第一次真实检索，而不是启动。
            knowledge_rt = kb.reader()
            knowledge_docs = tuple(docs)
            if logger is not None:
                logger.info("knowledge.enabled", docs=len(docs),
                            chunks=(kb.stats() or {}).get("chunks", 0))
        except Exception as exc:                               # noqa: BLE001
            raise RuntimeError(
                f"知识面装配失败：{type(exc).__name__}: {exc}") from exc

    wiring = None
    try:
        from plane.wiring import RuntimeWiring
        from core.summary import HistoryCompressor
        wiring = RuntimeWiring(router=router_obj, memory=memory_rt,
                               resource_rt=resource_rt, resources=resource_refs,
                               compressor=HistoryCompressor() if memory_rt else None,
                               recall_k=int(e.get("AGENT_RUNTIME_RECALL_K") or 5))
    except Exception as exc:                                   # noqa: BLE001
        if logger is not None:
            logger.warn("wiring.disabled", reason=f"{type(exc).__name__}: {exc}")

    # 把资源配置注册成**只读工具**：注册后模型才能在 `manifests()` 里看见它们。
    # 必须在 `_loop_factory` 之前完成 —— Builder 从 registry 生成工具描述，
    # 晚一步注册等于"配了但模型永远看不到"。
    if wiring is not None and resource_refs:
        try:
            wiring.attach_tools(registry)
            if logger is not None:
                logger.info("resources.tools", count=len(wiring.resource_tools),
                            names=[m.name for m in wiring.resource_tools][:10])
        except Exception as exc:                               # noqa: BLE001
            raise RuntimeError(
                f"资源注册成工具失败：{type(exc).__name__}: {exc}") from exc

    # ── 共享黑板（可选）：多 agent 的 CAS 状态面 ─────────────
    # `AGENT_RUNTIME_BLACKBOARD=1` 开启。
    #
    # 后端自动选择：KV 是 redis → `DistributedBlackboard`（跨副本 CAS 由服务端
    # 保证，两个副本各持实例也只有一方成功）；否则 `Blackboard`（进程内锁）。
    #
    # 为什么需要它：`plane/blackboard.py`（660 行、35 个用例）实现了分区、
    # CAS、版本线、历史，但**没有任何生产入口构造它** —— 只在 demo 里跑过。
    # 不接线等于"多 agent 协调能力配了但没有"。
    blackboard_rt = None
    if _truthy(e.get("AGENT_RUNTIME_BLACKBOARD")):
        try:
            # **这里必须自己造 KV，不能指望 `kv` 变量。**
            # 早先写的是 `kv if "kv" in locals() else None` —— 而
            # `build_gateway_from_env` 里**从来没有 `kv` 这个局部变量**
            # （它只活在 `_dep_specs()` 和 `main()` 里），于是判断恒为假，
            # `AGENT_RUNTIME_KV=redis` 配了也照样退回进程内黑板：
            # 单进程内 CAS 正常、HTTP 面正常、日志说 `blackboard.enabled`，
            # 只有跨副本才发现"两个副本各写各的"。
            _kv_kind = str(e.get("AGENT_RUNTIME_KV") or "memory").strip().lower()
            _kv = _make_kv(e) if _kv_kind not in ("", "memory") else None
            if _kv is not None and isinstance(_kv, _Unavailable):
                # 显式点名了外部 KV 却连不上 → 报错。静默退回进程内会让
                # 「多副本 CAS」变成「各写各的」，而那正是黑板存在的理由。
                raise RuntimeError(f"AGENT_RUNTIME_KV={_kv_kind} 不可用：{_kv.reason}")
            if _kv is not None:
                # 连不上就别装 distributed：`DistributedBlackboard` 的构造不碰网络，
                # 装上之后要等第一次 CAS 才报错 —— 而那时人已经在看业务问题了。
                _ping = getattr(_kv, "ping", None)
                if callable(_ping) and not _ping():
                    raise RuntimeError(
                        f"AGENT_RUNTIME_KV={_kv_kind} 不可达（{type(_kv).__name__} "
                        f"ping=False）；要么把 KV 起来，要么改回 "
                        f"AGENT_RUNTIME_KV=memory 并接受单进程黑板")
                from plane.distributed import DistributedBlackboard
                blackboard_rt = DistributedBlackboard(
                    _kv, prefix=str(e.get("AGENT_RUNTIME_BLACKBOARD_PREFIX")
                                   or "bb:"))
                _bb_mode = "distributed"
            else:
                from plane.blackboard import Blackboard
                blackboard_rt = Blackboard()
                _bb_mode = "in-process"
            if logger is not None:
                logger.info("blackboard.enabled", mode=_bb_mode)
        except Exception as exc:                               # noqa: BLE001
            raise RuntimeError(
                f"共享黑板装配失败：{type(exc).__name__}: {exc}") from exc

    # ── 执行沙箱（可选）：把 `sandbox_run` 注册成 T2 工具 ─────
    # `AGENT_RUNTIME_SANDBOX_TOOL=1` 开启；`AGENT_RUNTIME_SANDBOX_ALLOW` 给白名单
    # （逗号分隔，如 `git,grep,ls`），不给就用 `DEFAULT_ALLOWED_COMMANDS`。
    #
    # 为什么需要它：`plane/tool.py` 里的 `SubprocessSandbox` 只是**库**。
    # 没有 `ToolManifest` 它就不在 `registry.manifests()` 里，模型永远看不到 ——
    # 沙箱存在却不被使用，等于不存在。走 `register_sandbox` 才能保证
    # scope / policy / 审批 / 预算 / 限流 / 审计 / 脱敏 全都生效。
    if _truthy(e.get("AGENT_RUNTIME_SANDBOX_TOOL")):
        try:
            from contracts import RiskTier
            from plane.sandbox_tool import register_sandbox
            _allow_raw = str(e.get("AGENT_RUNTIME_SANDBOX_ALLOW") or "").strip()
            _allow = ([x.strip() for x in _allow_raw.split(",") if x.strip()]
                      or None)
            # 风险档可下调（`AGENT_RUNTIME_SANDBOX_TIER=1` 免审批，仅测试用）。
            # 上调到 T3 合法但 Dispatcher 只会 denied —— 那是刻意的硬边界。
            _tier = int(e.get("AGENT_RUNTIME_SANDBOX_TIER") or 2)
            _m = register_sandbox(
                registry, allowed_commands=_allow,
                risk_tier=RiskTier(_tier))
            if logger is not None:
                logger.info("sandbox.tool", name=_m.name,
                            risk_tier=_m.risk_tier.name,
                            allowlist=_allow or "default")
        except Exception as exc:                               # noqa: BLE001
            raise RuntimeError(
                f"沙箱工具装配失败：{type(exc).__name__}: {exc}") from exc

    # 黑板注册成只读工具 —— 模型能看共享状态，写入走程序（CAS 不适合模型）。
    if blackboard_rt is not None:
        try:
            from contracts import RiskTier, SideEffect, ToolManifest

            def _bb_read(args: dict, e: ExecCtx) -> dict:
                key = str(args.get("key") or "").strip()
                if not key:
                    raise ValueError("key 不能为空")
                entry = blackboard_rt.read(
                    key, agent=str((args.get("agent") or "")).strip())
                if entry is None:
                    return {"key": key, "found": False}
                return {"key": key, "found": True,
                        "value": entry.value, "version": entry.version,
                        "owner": entry.owner}

            registry.register(
                ToolManifest(
                    name="blackboard_read",
                    side_effect=SideEffect.READ_ONLY,
                    risk_tier=RiskTier.T0_SAFE,
                    description="读取多 agent 共享黑板上的某个键（含版本号）。",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "agent": {"type": "string",
                                      "description": "以哪个 agent 身份读（影响私有分区可见性）"},
                        },
                        "required": ["key"],
                        "additionalProperties": False,
                    },
                ),
                _bb_read,
            )
            if logger is not None:
                logger.info("blackboard.tools", names=["blackboard_read"])
        except Exception as exc:                               # noqa: BLE001
            raise RuntimeError(
                f"黑板注册成工具失败：{type(exc).__name__}: {exc}") from exc

    # 知识面注册成 `search_knowledge` 工具 —— 模型通过它检索文档。
    # 不注册的话，文档入了库却没有任何调用路径（等于"配了知识库但模型不知道"）。
    if knowledge_rt is not None:
        try:
            from contracts import RiskTier, SideEffect, ToolManifest

            def _search_knowledge(args: dict, e: ExecCtx) -> dict:
                query = str(args.get("query") or "").strip()
                if not query:
                    raise ValueError("query 不能为空")
                k = int(args.get("k") or 4)
                k = max(1, min(k, 20))          # 上限 20：防止一次拉爆上下文
                hits = knowledge_rt.retrieve(query, k=k)
                return {
                    "query": query,
                    "hits": [
                        {"doc_id": h.doc_id, "text": h.text,
                         "score": round(float(h.score), 4),
                         "span": h.span, "version": h.version}
                        for h in hits
                    ],
                    "count": len(hits),
                }

            registry.register(
                ToolManifest(
                    name="search_knowledge",
                    side_effect=SideEffect.READ_ONLY,
                    risk_tier=RiskTier.T0_SAFE,
                    description="检索内部知识库（BM25）。返回相关文档片段与得分。",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string",
                                      "description": "检索词（自然语言）"},
                            "k": {"type": "integer", "minimum": 1,
                                  "maximum": 20, "default": 4,
                                  "description": "返回条数"},
                        },
                        "required": ["query"],
                        "additionalProperties": False,
                    },
                ),
                _search_knowledge,
            )
            if logger is not None:
                logger.info("knowledge.tools", names=["search_knowledge"])
        except Exception as exc:                               # noqa: BLE001
            raise RuntimeError(
                f"知识注册成工具失败：{type(exc).__name__}: {exc}") from exc

    def _loop_factory(goal: str, _tenant_ctx: Any) -> Any:
        llm = _real() if llm_kind == "openai" else _scripted(goal)
        # 工具表来自声明式配置（`AGENT_RUNTIME_TOOLS` 指向 tools.yaml）。
        # 未配置时是**显式空表**："这个进程没有工具"，不是"工具丢了"——
        # 两者在排障时完全不同（前者是设计，后者是故障）。
        # 单轮内并行执行工具调用：**默认开**。
        # 真模型实测一轮会发 5~17 个独立读调用，串行是 p99 的主要来源。
        # 关掉：AGENT_RUNTIME_PARALLEL_TOOLS=0（行为回退到逐条串行）。
        return Loop(builder, llm, dispatcher, tools=list(tool_manifests),
                    planner=planner_obj,
                    parallel_tools=_truthy(e.get("AGENT_RUNTIME_PARALLEL_TOOLS", "1")),
                    max_parallel_tools=int(e.get("AGENT_RUNTIME_MAX_PARALLEL_TOOLS") or 4))

    gw_store = store if store is not None else _make_store(e, logger=logger)

    # ── 回合级钩子：把 wiring 接到 Gateway 上 ──────────────────
    # `before_turn` 必须在第一次调模型前跑完（Memory 召回 / 技能装载 / 路由），
    # 否则模型看不到历史与技能索引 —— 表现是"配了记忆但模型像没记忆"。
    on_start = on_end = None
    if wiring is not None:
        def on_start(ctx, goal, principal):                    # noqa: ANN001
            return wiring.before_turn(ctx, goal, principal=principal)

        def on_end(ctx, result):                               # noqa: ANN001
            # 把这一轮问答回写成"记忆原料"：只带骨架，不带工具回包全量
            turn = {
                "user": str(ctx.get("goal") or ""),
                "assistant": str(getattr(result, "text", "") or "") if result else "",
                "tools": [],
                "ok": bool(getattr(result, "ok", False)) if result else False,
            }
            try:
                principal = ctx.get("principal") or {}
                from contracts import Principal as _P
                p_obj = _P(user_id=principal.get("user_id", ""),
                           tenant=principal.get("tenant", ""),
                           roles=tuple(principal.get("roles") or ()),
                           scopes=tuple(principal.get("scopes") or ()))
            except Exception:                                  # noqa: BLE001
                p_obj = None
            return wiring.after_turn(ctx, turn=turn, principal=p_obj)

    gateway = Gateway(_loop_factory, governance=governance, store=gw_store,
                      rate_limit_per_minute=rate_limit,
                      # 多副本：租户配额走共享计数，否则阈值 × 副本数
                      quota_counter=_build_quota_counter(
                          e, _truthy(e.get("AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED"))),
                      # 指标回调：core/ 不能依赖 prometheus_client（可选件），
                      # 这里把 metrics 包成 `(name, delta, labels)` 形状传进去
                      metric_sink=(_metric_counter_sink(metrics)
                                   if metrics is not None else None),
                      on_turn_start=on_start, on_turn_end=on_end)

    # 会话多轮承接：默认**关闭**（向后兼容 —— 单轮调用方不受影响）。
    # 打开后同一个 session_id 的多轮请求会带上前几轮的问答骨架。
    # 只承接骨架、不承接 tool_results：工具回包是单次任务的事实，
    # 跨轮带过来会把过期/越权观测喂给模型（见 core/session.py 的裁决）。
    if _truthy(e.get("AGENT_RUNTIME_SESSION")):
        try:
            from core.session import SessionStore
            from core.session_gateway import SessionGateway
            from core.summary import HistoryCompressor
            sessions = SessionStore(
                store=gw_store,
                max_history=int(e.get("AGENT_RUNTIME_SESSION_MAX_HISTORY") or 20),
                summarizer=HistoryCompressor(),
                ttl_s=float(e.get("AGENT_RUNTIME_SESSION_TTL_S") or 7 * 86400))
            gateway = SessionGateway(gateway, sessions)
        except Exception as exc:                               # noqa: BLE001
            # 装配失败必须报错，不许静默退化成"没有会话"（用户开了它就该生效）
            raise RuntimeError(
                f"AGENT_RUNTIME_SESSION=1 但会话层装配失败："
                f"{type(exc).__name__}: {exc}") from exc
    # 把 MCP 连接挂到 gateway 上，供 lifespan 退出时 close()。
    # 不挂的话子进程会变成孤儿 —— 只在 `ps` 里看得出来。
    try:
        gateway._loaded_tools = loaded_tools            # noqa: SLF001
    except Exception:                                      # noqa: BLE001
        if logger is not None:
            logger.warn("tools.detach", reason="无法挂载 loaded_tools，子进程可能残留")
    # 工具表与派发器挂到 gateway 上，供 `create_app` 取出来放进 `app.state`
    # （`/v1/tools` 只读查询、`/v1/admin/tools/reload` 原地热替换）。
    # 走 gateway 而不是直接写 app，是因为 registry/dispatcher 在这层装配，
    # 而 app 在上一层 —— 这里没有 app 可写。
    try:
        gateway._tool_registry = registry               # noqa: SLF001
        gateway._tool_dispatcher = dispatcher           # noqa: SLF001
        gateway._tool_loader_env = dict(e) if hasattr(e, "keys") else {}
    except Exception:                                      # noqa: BLE001
        if logger is not None:
            logger.warn("tools.attach", reason="无法挂载工具表，/v1/tools 将为空")
    return gateway


def _final_answer(goal: str) -> Any:
    """scripted LLM 的终答（惰性 import 契约类型，避免模块级耦合）。"""
    from contracts import LLMResponse, Usage
    return LLMResponse(
        text=(f"运行时已就绪（scripted LLM）。本轮收到 {len(goal)} 字符的目标，"
              f"未执行任何工具。接真实模型请设 AGENT_RUNTIME_LLM=openai。"),
        tool_calls=(), usage=Usage(), finish_reason="stop")


def build_authenticator_from_env(*, env: Mapping[str, str] | None = None,
                                 logger: Any = None) -> Authenticator | None:
    """按环境变量装配鉴权器。**什么都没配 → 返回 None（不装鉴权）**。

    环境变量（下面这段也是 `.env` 的示例）::

        # API key 表：key:tenant:scopes[:user_id]，条目用 ; 或换行分隔
        #   scopes 用空格/逗号分隔；* 表示该租户的全部权限
        AGENT_RUNTIME_API_KEYS=sk-acme-1:acme:runtime:run tool.read;sk-beta-1:beta:runtime:run
        # JWT（HS256）：给 iss/aud 就强校验它们
        AGENT_RUNTIME_JWT_SECRET=<32 字节以上的随机串>
        AGENT_RUNTIME_JWT_ALGORITHMS=HS256
        AGENT_RUNTIME_JWT_ISSUER=https://idp.example.com
        AGENT_RUNTIME_JWT_AUDIENCE=agent-runtime
        # 无凭据是否放行（默认 0；放行拿到的是**空 scope** 的匿名身份）
        AGENT_RUNTIME_ALLOW_ANONYMOUS=0
        # 路径策略：不设 = 除 /healthz 外全保护
        AGENT_RUNTIME_AUTH_EXEMPT=/healthz,/readyz
        AGENT_RUNTIME_AUTH_PROTECT=
        # 抓取端的独立令牌（与用户凭据分开，最小权限）
        AGENT_RUNTIME_METRICS_AUTH=1
        AGENT_RUNTIME_METRICS_TOKEN=<另一个随机串>

    返回 None 的语义：这个进程**没有配置任何凭据方式**。`main()` 会 warn 一行
    （不是报错）—— "本机直接跑 demo"是合法用法，但这件事必须被说出来：
    没装鉴权时 principal 来自请求体，任何客户端都能自称任意租户。

    配置错误用 `ValueError` 抛出且**不回显条目内容**（一条坏配置行里就有半个密钥）。
    """
    e = dict(os.environ if env is None else env)
    spec = str(e.get("AGENT_RUNTIME_API_KEYS") or "")
    secret = str(e.get("AGENT_RUNTIME_JWT_SECRET") or "").strip()
    allow_anonymous = parse_bool(e.get("AGENT_RUNTIME_ALLOW_ANONYMOUS"), default=False)
    if not spec.strip() and not secret and not allow_anonymous:
        return None

    keys: dict[str, Principal] = {}
    for key, meta in parse_api_keys_spec(spec).items():
        keys[key] = Principal(user_id=meta["user_id"], tenant=meta["tenant"],
                              roles=(), scopes=frozenset(meta["scopes"]))

    algorithms = tuple(part.strip() for part in
                       str(e.get("AGENT_RUNTIME_JWT_ALGORITHMS") or "HS256").split(",")
                       if part.strip())
    try:
        leeway = float(e.get("AGENT_RUNTIME_JWT_LEEWAY_S") or 0.0)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "AGENT_RUNTIME_JWT_LEEWAY_S 必须是数字（秒）") from exc

    authenticator = Authenticator(
        api_keys=keys,
        jwt_secret=secret,
        jwt_algorithms=algorithms or ("HS256",),
        jwt_issuer=str(e.get("AGENT_RUNTIME_JWT_ISSUER") or ""),
        jwt_audience=str(e.get("AGENT_RUNTIME_JWT_AUDIENCE") or ""),
        allow_anonymous=allow_anonymous,
        jwt_leeway_s=leeway,
    )
    if logger is not None:
        try:
            logger.info("auth.configured", schemes=list(authenticator.schemes),
                        api_keys=len(keys), jwt=bool(secret),
                        allow_anonymous=allow_anonymous)
        except Exception:               # noqa: BLE001
            pass
    return authenticator


def _csv(value: Any) -> list[str] | None:
    """逗号/空格分隔的列表 → list；空/None → None（"未设置"与"设成空"要能区分）。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return []
    return [part for part in (chunk.strip() for chunk in
                              text.replace(",", " ").split()) if part]


def main() -> None:
    """uvicorn 入口。读环境变量 → 装配 → 装信号 → 起服务。

    环境变量：
        AGENT_RUNTIME_HOST / AGENT_RUNTIME_PORT        监听地址（默认 127.0.0.1:8000）
        AGENT_RUNTIME_LOG_LEVEL                        DEBUG/INFO/WARN/ERROR
        AGENT_RUNTIME_DRAIN_TIMEOUT_S                  优雅关闭等待上限（默认 10）
        AGENT_RUNTIME_METRIC_PREFIX                    指标前缀（默认 agent_runtime）
        AGENT_RUNTIME_API_KEYS / AGENT_RUNTIME_JWT_SECRET /
        AGENT_RUNTIME_JWT_ALGORITHMS / _ISSUER / _AUDIENCE /
        AGENT_RUNTIME_ALLOW_ANONYMOUS                  鉴权（见 build_authenticator_from_env）
        AGENT_RUNTIME_AUTH_EXEMPT / _AUTH_PROTECT      路径策略（逗号分隔）
        AGENT_RUNTIME_AUTH=off                         硬开关：连鉴权一起关掉
        AGENT_RUNTIME_MAX_BODY_BYTES                   请求体上限（默认 1 MiB，0=关）
        AGENT_RUNTIME_HTTP_RATE                        每 IP 每分钟请求数（默认 0=关）
        AGENT_RUNTIME_REQUEST_TIMEOUT_S                请求超时秒（默认 0=关）
        AGENT_RUNTIME_METRICS_AUTH / _METRICS_TOKEN    /metrics 的独立门槛
        其余见 build_gateway_from_env / _default_dep_specs

    信号：SIGTERM / SIGINT → `ShutdownCoordinator.begin()` → `wait()` → flush → 退出。
    uvicorn 自己的信号处理会被 lifespan 里后注册的处理器**接管**（后注册生效），
    所以退出一定走"先排空再退"，不是"立刻断开连接"。
    """
    import uvicorn                     # 局部 import：库的安装不该影响模块可 import

    env = os.environ
    host = str(env.get("AGENT_RUNTIME_HOST") or "127.0.0.1")
    port = int(env.get("AGENT_RUNTIME_PORT") or 8000)
    log_level = str(env.get("AGENT_RUNTIME_LOG_LEVEL") or "INFO")
    drain_timeout = float(env.get("AGENT_RUNTIME_DRAIN_TIMEOUT_S") or 10.0)
    prefix = str(env.get("AGENT_RUNTIME_METRIC_PREFIX") or "agent_runtime")

    logger = JsonLogger(level=log_level, service=SERVICE_NAME)
    metrics = Metrics(prefix=prefix)
    shutdown = ShutdownCoordinator(drain_timeout_s=drain_timeout)

    # ── Trace 导出（OTLP）──────────────────────────────────────
    # 不配 `OTEL_EXPORTER_OTLP_ENDPOINT` 就只在进程内留 Tracer（不导出）。
    # **必须显式 install**：OTel 的 set_tracer_provider 只能生效一次且撤不掉，
    # 没有 endpoint 时装上去只会让退出时刷 "Failed to export spans" 噪声。
    try:
        from plane.quality import configure_otel
        otel_info = configure_otel(
            str(env.get("OTEL_EXPORTER_OTLP_ENDPOINT") or ""),
            service_name=SERVICE_NAME,
            headers=_otel_headers(env),
            install=True)
        if otel_info.get("installed"):
            logger.info("otel.installed", endpoint=otel_info.get("endpoint", ""))
        else:
            logger.info("otel.disabled", reason=otel_info.get("reason", ""))
    except Exception as exc:            # noqa: BLE001 - 观测面 fail-open
        logger.warn("otel.setup_failed", reason=f"{type(exc).__name__}: {exc}")

    # ── 鉴权：**默认拒绝**（fail-closed）──
    # 这是本文件唯一一处"主动让进程起不来"的地方，而它正是最该存在的一处：
    # 一个裸奔的进程（principal 由请求体给出）比一个起不来的进程危险得多。
    mode = resolve_auth_mode(env=env)
    authenticator: Authenticator | None = None
    if mode == "enforced":
        authenticator = require_auth_config(env=env)   # 缺凭据 → StartupRefused
        try:
            logger.info("auth.enforced", schemes=list(authenticator.schemes),
                        api_keys=len(authenticator.api_keys),
                        jwt=bool(authenticator.jwt_secret),
                        allow_anonymous=bool(authenticator.allow_anonymous))
            if authenticator.allow_anonymous:
                # 装了鉴权但允许匿名：拿到的是**空 scope** 的匿名身份，不是任意租户。
                # 这件事必须说出来，否则会被当成"和裸奔一样"（它确实不一样）。
                logger.warn(
                    "auth.anonymous_allowed",
                    hint=("AGENT_RUNTIME_ALLOW_ANONYMOUS=1：无凭据请求会拿到空 scope "
                          "的匿名身份（进不了 runtime:run）；要彻底关掉请去掉它"))
        except Exception:                 # noqa: BLE001
            pass
    else:
        authenticator = None
        try:
            logger.warn(
                "auth.disabled",
                mode=mode,
                hint=("显式放行（AGENT_RUNTIME_AUTH=off 或 "
                      "AGENT_RUNTIME_ALLOW_ANONYMOUS=1）：未安装鉴权中间件，"
                      "principal 由**请求体**给出，任何客户端都能自称任意租户。"
                      "仅限本机/内网：公网暴露前请改配 AGENT_RUNTIME_API_KEYS "
                      "或 AGENT_RUNTIME_JWT_SECRET"))
            if _has_credentials(env):
                # 配了凭据却被显式关掉：这几乎一定是遗留的开关，必须点名
                logger.warn(
                    "auth.credentials_ignored",
                    hint=("配置了 AGENT_RUNTIME_API_KEYS / AGENT_RUNTIME_JWT_SECRET，"
                          "但被 AGENT_RUNTIME_AUTH=off 显式关闭 —— 凭据未被使用"))
        except Exception:                 # noqa: BLE001
            pass

    raw_exempt = env.get("AGENT_RUNTIME_AUTH_EXEMPT")
    exempt_paths: Sequence[str] = (DEFAULT_EXEMPT_PATHS if raw_exempt is None
                                   else (_csv(raw_exempt) or ()))

    app = create_app(metrics=metrics, logger=logger, shutdown=shutdown,
                     authenticator=authenticator,
                     # 生产入口永远 fail-closed：上面的 require_auth_config 已经保证
                     # enforced 模式下 authenticator 非 None，这里是**第二道锁**
                     # （将来有人改错了上面的分支，也会在 create_app 里炸掉）
                     auth_required=(mode == "enforced"),
                     protect_paths=_csv(env.get("AGENT_RUNTIME_AUTH_PROTECT")),
                     exempt_paths=exempt_paths,
                     metrics_requires_auth=parse_bool(
                         env.get("AGENT_RUNTIME_METRICS_AUTH"), default=True),
                     metrics_token=str(env.get("AGENT_RUNTIME_METRICS_TOKEN") or "").strip(),
                     admin_token=str(env.get("AGENT_RUNTIME_ADMIN_TOKEN") or "").strip())
    # 多 worker：uvicorn 的 `workers` 只在传**导入字符串**时生效，
    # 传 app 对象会被忽略（uvicorn 会静默单进程跑，这点很容易误判）。
    workers = int(env.get("AGENT_RUNTIME_WORKERS") or 0)
    if workers > 1:
        logger.info("server.multi_worker", workers=workers,
                    note="进程内状态不共享；限额需 DISTRIBUTED=1 + 共享 KV")
        uvicorn.run("serve:app_factory", host=host, port=port,
                    log_level="info", access_log=False, lifespan="on",
                    workers=workers)
        return

    server = uvicorn.Server(uvicorn.Config(
        app, host=host, port=port, log_level="info",
        # access log 关掉：本服务自己用 JsonLogger 记结构化访问事件，
        # 两条日志通道会给出两个"事实来源"（而且 uvicorn 的格式无法被字段检索）
        access_log=False, lifespan="on",
    ))
    app.state.server = server             # lifespan 里的 drain 回调要它来置 should_exit
    try:
        logger.info("server.listen", host=host, port=port,
                    drain_timeout_s=drain_timeout, version=VERSION)
    except Exception:                     # noqa: BLE001
        pass
    server.run()


def app_factory() -> Any:
    """uvicorn 多 worker 用的**模块级工厂**。

    为什么必须有它：`uvicorn --workers N` 只接受 `"module:factory"` 形式的
    导入字符串，不接受已构造的 app 对象 —— 每个 worker 是独立进程，
    必须各自 import 并构造。所以 `create_app()` 的关键字参数在这里取不到，
    一切从环境变量读。

    **多进程带来的语义变化**（必须知道，否则会误判）：

      * 进程内状态**不再共享**：限流窗口、配额、`audit_log`、workflow 状态表
        各进程一份 → 实际阈值 × worker 数
      * 多 worker 想正确限额，必须 `AGENT_RUNTIME_RATE_LIMIT_DISTRIBUTED=1`
        且 KV 是共享实现（Redis）
      * `/metrics` 只有一个 worker 的数据（除非用 `prometheus_client`
        的多进程模式，本仓未做）

    所以多 worker 不是"免费加速"，它等价于多副本部署，只是少了一个反向代理。
    """
    return create_app()


if __name__ == "__main__":                # pragma: no cover - 手动启动路径
    try:
        main()
    except StartupRefused as refusal:
        # 不给运维看一屏 traceback：他要的是那三条命令。栈回溯属于"代码坏了"，
        # 这里不是代码坏了，是配置缺了。退出码 2 与"跑起来后自己挂了"（1）分开。
        print(str(refusal), file=sys.stderr)
        raise SystemExit(2) from None
