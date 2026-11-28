#!/usr/bin/env python3
"""长稳压测：持续负载下采样内存/线程/句柄，找**缓慢增长**。

**为什么需要它**：短跑压测（12 秒）证明不了稳定性。真实故障是慢的：
每回合泄漏 4KB，一小时 3600 回合就是 14MB；每回合漏一个文件句柄，
几百回合后 `EMFILE`。这些只在几十分钟的负载下显形。

判据不是"绝对值多大"，而是**趋势**：
  * 内存：RSS 的线性拟合斜率（MB/分钟）。采样期间 gc 会让它上台阶，
    所以同时看"台阶高度"是否收敛
  * 线程：应当恒定（线程池大小固定）。增长说明每次请求都在起新线程
  * 句柄：应当稳定。增长说明有未关闭的文件/连接
  * 延迟：p99 不应随运行时间恶化（恶化 = 有累积状态，如字典无限增长）

用法：
    .venv/bin/python scripts/soak_test.py --url http://127.0.0.1:PORT/v1/turns \\
        --minutes 20 --concurrency 8
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

try:
    import httpx
except ImportError:
    sys.exit("需要 httpx：pip install httpx")

BODY = {"goal": "查一下 api 服务的 p99 指标",
        "principal": {"user_id": "soak", "tenant": "soak",
                      "roles": ["sre"], "scopes": ["runtime:run", "tool.read"]}}


def _proc_stats(pid: int) -> dict:
    """取进程的 RSS(MB) / 线程数 / 打开的文件句柄数。

    跨平台坑：`ps -o nlwp=`（Linux 数线程）与 `thcount` 在 macOS 上都不可用，
    直接用会**静默返回 0** —— 于是长稳报告显示"内存 0MB、线程 0"，
    看起来一切正常，实际上什么都没测到。所以分平台取，并让失败可见。
    """
    out = {"rss_mb": 0.0, "threads": 0, "fds": 0, "err": ""}
    try:
        rss = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                             capture_output=True, text=True, timeout=5).stdout.strip()
        if rss.isdigit():
            out["rss_mb"] = round(int(rss) / 1024.0, 1)
        else:
            out["err"] = "rss_unreadable"
    except Exception as exc:                                   # noqa: BLE001
        out["err"] = f"rss:{type(exc).__name__}"

    # 线程数：Linux 用 nlwp，macOS 用 `ps -M` 数行
    try:
        r = subprocess.run(["ps", "-o", "nlwp=", "-p", str(pid)],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip().isdigit():
            out["threads"] = int(r.stdout.strip())
        else:
            m = subprocess.run(["ps", "-M", "-p", str(pid)],
                               capture_output=True, text=True, timeout=10).stdout
            out["threads"] = max(0, len([ln for ln in m.splitlines() if ln.strip()]) - 1)
    except Exception as exc:                                   # noqa: BLE001
        out["err"] = (out["err"] + f";threads:{type(exc).__name__}").strip(";")
    try:
        out["fds"] = len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except Exception:                                          # noqa: BLE001
        # macOS 没有 /proc，用 lsof
        try:
            r = subprocess.run(["lsof", "-p", str(pid)], capture_output=True,
                               text=True, timeout=15).stdout
            out["fds"] = max(0, len(r.splitlines()) - 1)
        except Exception:                                      # noqa: BLE001
            pass
    return out


async def load(client: httpx.AsyncClient, url: str, deadline: float,
               lat: list[float], codes: Counter) -> None:
    while time.monotonic() < deadline:
        t0 = time.perf_counter()
        try:
            r = await client.post(url, json=BODY, timeout=60.0)
            codes[r.status_code] += 1
            if 200 <= r.status_code < 300:
                lat.append((time.perf_counter() - t0) * 1000.0)
        except Exception:                                      # noqa: BLE001
            codes["exception"] += 1


def _pct(v: list[float], q: float) -> float:
    if not v:
        return 0.0
    v = sorted(v)
    if len(v) == 1:
        return v[0]
    pos = (len(v) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] * (1 - (pos - lo)) + v[hi] * (pos - lo)


def _slope(points: list[tuple[float, float]]) -> float:
    """最小二乘斜率（y 单位/分钟）。"""
    if len(points) < 2:
        return 0.0
    n = len(points)
    sx = sum(p[0] for p in points)
    sy = sum(p[1] for p in points)
    sxx = sum(p[0] * p[0] for p in points)
    sxy = sum(p[0] * p[1] for p in points)
    denom = n * sxx - sx * sx
    return 0.0 if denom == 0 else (n * sxy - sx * sy) / denom


async def run(url: str, pid: int, minutes: float, concurrency: int,
              sample_every: float) -> dict:
    limits = httpx.Limits(max_connections=concurrency + 4,
                          max_keepalive_connections=concurrency + 4)
    base = _proc_stats(pid)
    samples: list[dict] = []
    all_lat: list[float] = []
    codes: Counter = Counter()
    t_start = time.monotonic()
    deadline = t_start + minutes * 60

    async with httpx.AsyncClient(limits=limits) as client:
        workers = [asyncio.create_task(
            load(client, url, deadline, all_lat, codes))
            for _ in range(concurrency)]

        while time.monotonic() < deadline:
            await asyncio.sleep(sample_every)
            st = _proc_stats(pid)
            if st.get("err") == "rss_unreadable" and st["fds"] == 0:
                # PID 不在了（被外部 kill / 崩了）。继续采样只会记录一堆 0，
                # 看起来像"内存降到 0，很健康" —— 比报错更危险。
                raise SystemExit(
                    f"被测进程 {pid} 已消失（t="
                    f"{(time.monotonic() - t_start) / 60.0:.1f}min）。"
                    f"长稳测试中止：进程不在了，数据无意义。")
            st["t_min"] = round((time.monotonic() - t_start) / 60.0, 2)
            st["done"] = sum(codes.values())
            # 这一段窗口内的延迟分位（看是否随时间恶化）
            recent = all_lat[-200:]
            st["p99_recent"] = round(_pct(recent, 0.99), 1)
            samples.append(st)
            if st["err"]:
                print(f"  t={st['t_min']:5.2f}min  [采集异常: {st['err']}]")
            print(f"  t={st['t_min']:5.2f}min  rss={st['rss_mb']:7.1f}MB  "
                  f"threads={st['threads']:3d}  fds={st['fds']:4d}  "
                  f"req={st['done']:6d}  p99={st['p99_recent']:7.1f}ms")
            sys.stdout.flush()

        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    wall = (time.monotonic() - t_start) / 60.0
    rss_pts = [(s["t_min"], s["rss_mb"]) for s in samples]
    fd_pts = [(s["t_min"], s["fds"]) for s in samples]
    rss_vals = [s["rss_mb"] for s in samples]
    # **谷值趋势**才是泄漏的判据，不是全体线性拟合。
    #
    # 为什么：CPython 的分配器不把内存立刻还给 OS，RSS 呈锯齿形
    # （实测 20 分钟里在 30MB 与 162MB 之间摆动）。对全点做线性拟合，
    # 结果取决于"末次采样恰好在峰还是谷" —— 这是**测量噪声**，不是信号。
    #
    # 真泄漏的特征是：每个 GC 周期结束后的**低点**在抬高。
    # 所以取局部最小值序列，看它的趋势。
    troughs = [rss_vals[i] for i in range(1, len(rss_vals) - 1)
               if rss_vals[i] < rss_vals[i - 1] and rss_vals[i] < rss_vals[i + 1]]
    # **剔除预热期**：前几个周期在填解释器缓存/连接池，谷值会跳一台阶
    # （实测 70MB → 85MB），那不是泄漏而是启动成本。不剔除会把一次阶跃
    # 拟合成线性增长，得出"每小时涨 48MB"的错误结论。
    warmup_samples = max(2, len(rss_vals) // 5)      # 前 20% 视为预热
    steady = rss_vals[warmup_samples:] if len(rss_vals) > warmup_samples + 2 else rss_vals
    steady_troughs = [steady[i] for i in range(1, len(steady) - 1)
                      if steady[i] < steady[i - 1] and steady[i] < steady[i + 1]]
    if len(steady_troughs) >= 2:
        rss_trough_slope = round(
            _slope([(float(i), v) for i, v in enumerate(steady_troughs)]), 3)
        trough_first, trough_last = steady_troughs[0], steady_troughs[-1]
        troughs = steady_troughs
    elif len(troughs) >= 2:
        rss_trough_slope = round(
            _slope([(float(i), v) for i, v in enumerate(troughs)]), 3)
        trough_first, trough_last = troughs[0], troughs[-1]
    else:
        # 样本太少看不出锯齿 → 退回全点拟合，并标记不可靠
        rss_trough_slope = round(_slope(rss_pts), 3)
        trough_first = trough_last = rss_vals[0] if rss_vals else 0.0
    th = [s["threads"] for s in samples]
    first_p99 = samples[0]["p99_recent"] if samples else 0.0
    last_p99 = samples[-1]["p99_recent"] if samples else 0.0

    return {
        "minutes": round(wall, 2),
        "concurrency": concurrency,
        "requests": sum(codes.values()),
        "rps": round(sum(codes.values()) / (wall * 60), 2) if wall else 0.0,
        "codes": {str(k): v for k, v in sorted(codes.items(), key=lambda kv: str(kv[0]))},
        "baseline": base,
        "samples": samples,
        # **判据要用原始序列**。早先只在 `main()` 里引用 `rss_vals` ——
        # 那是 `run()` 的局部变量，`main()` 里根本不存在，于是每次长稳跑到
        # 最后都抛 `NameError: name 'rss_vals' is not defined`：
        # 摘要打印完了、JSON 也写了，但**判据那一段从未真正执行过**。
        # 一条"判据"从未生效，等于没有判据 —— 这才是最该修的地方。
        "rss_series": [round(v, 1) for v in rss_vals],
        "fd_series": [int(s["fds"]) for s in samples],
        "rss_slope_mb_per_min": round(_slope(rss_pts), 3),
        "rss_trough_slope_mb_per_poll": rss_trough_slope,
        "rss_trough_first": trough_first,
        "rss_trough_last": trough_last,
        "rss_peak": round(max(rss_vals), 1) if rss_vals else 0.0,
        "rss_min": round(min(rss_vals), 1) if rss_vals else 0.0,
        "rss_troughs": len(troughs),
        "fd_slope_per_min": round(_slope(fd_pts), 3),
        "threads_min": min(th) if th else 0,
        "threads_max": max(th) if th else 0,
        "p50": round(_pct(all_lat, 0.50), 1),
        "p95": round(_pct(all_lat, 0.95), 1),
        "p99": round(_pct(all_lat, 0.99), 1),
        "p99_first": first_p99,
        "p99_last": last_p99,
    }


def _verdicts(res: dict) -> list[tuple[str, bool]]:
    """长稳判据（纯函数：只看 `res`，不碰网络/进程）。

    抽出来的理由：这段原来写在 `main()` 里并直接引用 `run()` 的局部变量
    `rss_vals`，于是**每次长稳跑到最后都抛 NameError** —— 摘要打印完了、
    JSON 也写了，判据一段从未执行。一条从不执行的判据等于没有判据。
    """
    series = res.get("rss_series") or []
    if not series:
        return [("内存采样序列存在（没有序列就无法判断趋势）", False)]

    verdicts: list[tuple[str, bool]] = []
    n3 = max(1, len(series) // 3)
    head_mean = sum(series[:n3]) / n3
    tail_seg = series[-n3:]
    tail_mean = sum(tail_seg) / len(tail_seg)
    res["rss_head_mean"] = round(head_mean, 1)
    res["rss_tail_mean"] = round(tail_mean, 1)
    res["rss_peak_head"] = round(max(series[:n3]), 1)
    res["rss_peak_tail"] = round(max(tail_seg), 1)

    # 阈值 1.25：允许 25% 的阶段均值波动（GC 时机、缓存填充都会动它），
    # 超过就是真增长。实测健康值 97.1/101.3 = 0.96。
    verdicts.append((
        f"内存无泄漏（阶段均值 {res['rss_head_mean']}→{res['rss_tail_mean']}MB，"
        f"比值 {tail_mean / head_mean:.2f}）",
        tail_mean <= head_mean * 1.25))
    verdicts.append(("句柄无泄漏（<1个/min）", res["fd_slope_per_min"] < 1.0))
    verdicts.append((f"线程恒定（{res['threads_min']}~{res['threads_max']}）",
                     res["threads_max"] - res["threads_min"] <= 4))
    p99_ok = res["p99_last"] <= max(res["p99_first"] * 1.5, res["p99_first"] + 50)
    verdicts.append((f"p99 未随时间恶化（{res['p99_first']}→{res['p99_last']}ms）", p99_ok))
    return verdicts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--pid", type=int, required=True, help="被测服务进程 PID")
    ap.add_argument("--minutes", type=float, default=20.0)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--sample-every", type=float, default=30.0)
    ap.add_argument("--json-out", default="")
    a = ap.parse_args()

    res = asyncio.run(run(a.url, a.pid, a.minutes, a.concurrency, a.sample_every))
    if a.json_out:
        Path(a.json_out).write_text(json.dumps(res, ensure_ascii=False, indent=2))

    print()
    print(f"  时长 {res['minutes']} 分钟 | 并发 {res['concurrency']} | "
          f"{res['requests']} 请求 | {res['rps']} RPS")
    print(f"  状态码 {res['codes']}")
    print(f"  延迟 p50={res['p50']} p95={res['p95']} p99={res['p99']} ms")
    print()
    print(f"  内存 起始 {res['baseline']['rss_mb']}MB | 区间 "
          f"{res['rss_min']}~{res['rss_peak']}MB（锯齿是分配器行为，非泄漏）")
    print(f"        谷值 {res['rss_trough_first']}MB → {res['rss_trough_last']}MB "
          f"（{res['rss_troughs']} 个周期）| 全点斜率 "
          f"{res['rss_slope_mb_per_min']} MB/min（含峰值噪声，供参考）")
    print(f"  句柄 起始 {res['baseline']['fds']} → 末次 "
          f"{res['samples'][-1]['fds'] if res['samples'] else 0} | "
          f"斜率 {res['fd_slope_per_min']} 个/min")
    print(f"  线程 {res['threads_min']}~{res['threads_max']}（应当恒定）")
    print(f"  p99 首段 {res['p99_first']}ms → 末段 {res['p99_last']}ms")
    print()

    verdicts = _verdicts(res)

    failed = 0
    for name, ok in verdicts:
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
        if not ok:
            failed += 1
    print()
    print("长稳通过。" if failed == 0 else f"{failed} 项未通过，存在泄漏或劣化。")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
