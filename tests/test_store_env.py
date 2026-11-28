"""store 的路径变量：配错名字必须出声，而且不能把配好的路径丢掉。

现实里踩过两脚，都**没有日志**，表面一切正常：

  * `STORE=sqlite` + `STORE_ROOT=/data` → sqlite 只读 `STORE_PATH`/`STORE_DB`，
    于是落到 `:memory:`。重启后 session/checkpoint 全丢，`/readyz` 仍然是绿的。
  * `STORE=file` + `STORE_PATH=/tmp/x` → file 只读 `STORE_ROOT`，于是落到默认
    `/tmp/agent-runtime-store`。表现是 `/v1/turns/{ctx_id}/resume` 一直 404
    ctx_not_found —— 看起来像"ctx 不存在"，实际是服务在另一个目录里找。

修法沿用 `AGENT_RUNTIME_KV` 的口径：**能兼容就兼容，但必须留痕**；不改已生效
配置的行为。
"""
from __future__ import annotations

import unittest

import serve


class _Log:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def info(self, ev, **kw): self.events.append((ev, kw))
    def warn(self, ev, **kw): self.events.append((ev, kw))
    def error(self, ev, **kw): self.events.append((ev, kw))
    def debug(self, *a, **k): pass

    def has(self, prefix: str) -> list[dict]:
        return [kw for ev, kw in self.events if ev.startswith(prefix)]


def _store(env: dict):
    log = _Log()
    st = serve._make_store(env, logger=log)
    return st, log


class TestStoreEnvNames(unittest.TestCase):
    def test_file_accepts_store_path(self) -> None:
        """`file` + `STORE_PATH` 必须真的用那个路径（原来会被静默忽略）。"""
        st, log = _store({"AGENT_RUNTIME_STORE": "file",
                          "AGENT_RUNTIME_STORE_PATH": "/tmp/turing-test-root"})
        self.assertEqual(str(st.root), "/tmp/turing-test-root")
        self.assertTrue(log.has("store.env_alias"), "用了别名却不留痕")

    def test_sqlite_root_is_reported_as_ignored(self) -> None:
        """`sqlite` + `STORE_ROOT` 落 :memory:（重启即丢），必须 warn。"""
        st, log = _store({"AGENT_RUNTIME_STORE": "sqlite",
                          "AGENT_RUNTIME_STORE_ROOT": "/data"})
        self.assertEqual(str(st.path), ":memory:")
        notices = log.has("store.env_ignored")
        self.assertTrue(notices, "静默落 :memory: = 重启后数据全丢却没人知道")
        self.assertEqual(notices[0]["effective"], ":memory:")

    def test_sqlite_path_still_wins(self) -> None:
        """显式 `STORE_PATH` 的行为不变，且不该误报。"""
        st, log = _store({"AGENT_RUNTIME_STORE": "sqlite",
                          "AGENT_RUNTIME_STORE_PATH": "/tmp/turing-test.db",
                          "AGENT_RUNTIME_STORE_ROOT": "/data"})
        self.assertEqual(str(st.path), "/tmp/turing-test.db")
        self.assertFalse(log.has("store.env_ignored"))

    def test_memory_store_says_nothing(self) -> None:
        """默认 memory 不该刷无意义的告警。"""
        _st, log = _store({})
        self.assertFalse([ev for ev, _ in log.events])


class TestMemorySwitchesWithoutExtractor(unittest.TestCase):
    def test_semantic_and_recall_k_report_ineffective(self) -> None:
        """没配 Extractor 时，SEMANTIC / RECALL_K 配了也不生效 —— 要说出来。"""
        log = _Log()
        serve.build_gateway_from_env(logger=log, env={
            "AGENT_RUNTIME_SEMANTIC": "1",
            "AGENT_RUNTIME_RECALL_K": "9",
        })
        self.assertTrue(log.has("memory.semantic_ignored"))
        self.assertTrue(log.has("memory.recall_k_ignored"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
