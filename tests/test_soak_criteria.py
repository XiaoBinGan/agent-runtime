"""长稳判据本身的自测。

`scripts/soak_test.py` 的判据段曾经**从未执行**：它写在 `main()` 里、却引用
`run()` 的局部变量 `rss_vals`，于是每次长稳跑到最后都抛
`NameError: name 'rss_vals' is not defined` —— 摘要打印完了、JSON 也写了，
判据一段直接崩溃退出。12 小时长稳的最后一行是 traceback，而不是结论。

判据是一道"防线"，没被执行过的防线等于没有。这里把它锁成纯函数并验三种形态。
"""
from __future__ import annotations

import importlib.util
import pathlib
import unittest

_PATH = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "soak_test.py"
_spec = importlib.util.spec_from_file_location("soak_test_under_test", _PATH)
assert _spec and _spec.loader
soak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(soak)


def _res(series, *, fd_slope=0.1, threads=(77, 77), p99=(45.0, 47.0)) -> dict:
    return {"rss_series": list(series), "fd_slope_per_min": fd_slope,
            "threads_min": threads[0], "threads_max": threads[1],
            "p99_first": p99[0], "p99_last": p99[1]}


class TestSoakCriteria(unittest.TestCase):
    def test_flat_series_passes(self) -> None:
        verdicts = soak._verdicts(_res([100.0] * 30))
        self.assertEqual([ok for _name, ok in verdicts], [True] * 4)

    def test_growing_series_fails(self) -> None:
        """阶段均值 100 → 220MB 必须判为泄漏（这正是本次 12 小时长稳的形态）。"""
        verdicts = soak._verdicts(_res([100.0] * 10 + [150.0] * 10 + [220.0] * 10))
        self.assertFalse(verdicts[0][1], "线性增长没被判出来")
        self.assertTrue(all(ok for _n, ok in verdicts[1:]),
                        "只该内存那条红，别连坐")

    def test_missing_series_fails_instead_of_crashing(self) -> None:
        """没有序列 → 明确失败。**不能**像修之前那样抛 NameError。"""
        verdicts = soak._verdicts({})
        self.assertEqual(len(verdicts), 1)
        self.assertFalse(verdicts[0][1])

    def test_fd_and_thread_growth_fail(self) -> None:
        verdicts = soak._verdicts(_res([100.0] * 30, fd_slope=2.5, threads=(10, 90)))
        self.assertFalse(verdicts[1][1], "句柄增长未判出")
        self.assertFalse(verdicts[2][1], "线程增长未判出")

    def test_p99_degradation_fails(self) -> None:
        verdicts = soak._verdicts(_res([100.0] * 30, p99=(40.0, 400.0)))
        self.assertFalse(verdicts[3][1], "p99 恶化未判出")

    def test_run_returns_series_needed_by_criteria(self) -> None:
        """`run()` 必须把判据要的原始序列带出来（这是 NameError 的根因）。"""
        src = _PATH.read_text(encoding="utf-8")
        self.assertIn('"rss_series"', src)
        self.assertIn('"fd_series"', src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
