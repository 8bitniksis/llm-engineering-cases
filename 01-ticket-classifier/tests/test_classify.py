import asyncio
import contextlib
import io
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import classify  # noqa: E402
import fake_llm  # noqa: E402

FAST = dict(timeout=0.05, pause_base=0.001, pause_cap=0.005)


class Fake:
    """Детерминированная подмена fake_llm.classify: сценарий по тексту, счётчики вызовов."""

    def __init__(self, script=None, delay=0.0):
        self.script = script or {}   # text -> список ответов по порядку; Exception — бросить
        self.delay = delay
        self.calls = 0
        self.by_text = {}
        self.in_flight = 0
        self.peak = 0

    async def __call__(self, text):
        self.calls += 1
        n = self.by_text[text] = self.by_text.get(text, 0) + 1
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            await asyncio.sleep(self.delay)
            steps = self.script.get(text)
            if steps is None:
                return "карты"
            step = steps[min(n, len(steps)) - 1]
            if isinstance(step, Exception):
                raise step
            if callable(step):
                return await step()
            return step
        finally:
            self.in_flight -= 1


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.db = self.dir / "t.db"

    def make_input(self, name, tickets):
        p = self.dir / f"{name}.jsonl"
        p.write_text("".join(json.dumps({"id": i, "text": t}, ensure_ascii=False) + "\n" for i, t in tickets),
                     encoding="utf-8")
        return p

    def run_cls(self, path, fake, **kw):
        params = {**FAST, **kw}
        with mock.patch.object(fake_llm, "classify", fake), contextlib.redirect_stdout(io.StringIO()) as out:
            code = classify.run(path, self.db, self.dir, self.dir / "c.lock", **params)
        self.out = out.getvalue()
        return code

    def rows(self, name, kind):
        p = self.dir / f"{name}.{kind}.jsonl"
        return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()]

    def row(self, tid):
        with contextlib.closing(sqlite3.connect(self.db)) as c:
            return c.execute("SELECT status, category, reason, runs, attempts FROM results WHERE id=?", (tid,)).fetchone()


class ParseTest(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(classify.parse_answer("Карты."), ("карты", "norm"))
        self.assertEqual(classify.parse_answer("fraud"), ("мошенничество", "syn"))
        self.assertEqual(classify.parse_answer("вклады"), ("вклады", "ok"))
        self.assertEqual(classify.parse_answer("не знаю"), (None, None))


class RunTest(Base):
    def test_retry_after_server_error(self):
        f = Fake({"a": [fake_llm.ServerError("503"), "вклады"]})
        inp = self.make_input("b1", [("1", "a")])
        self.assertEqual(self.run_cls(inp, f), 0)
        self.assertEqual(f.calls, 2)
        self.assertEqual(self.row("1")[:2], ("ok", "вклады"))

    def test_all_attempts_fail_then_retry_then_exhausted(self):
        f = Fake({"a": [fake_llm.ServerError("503")]})
        for run_no in (1, 2):
            inp = self.make_input(f"b{run_no}", [("1", "a")] if run_no == 1 else [])
            self.run_cls(inp, f)
            self.assertEqual(self.row("1")[0], "retry")
            self.assertEqual(self.row("1")[3], run_no)
        self.assertEqual(f.calls, 8)
        self.assertEqual([r["id"] for r in self.rows("b2", "failed")], [])
        self.assertEqual(self.rows("b2", "out"), [])
        self.assertEqual([r["id"] for r in json.loads("[" + ",".join(
            (self.dir / "pending.jsonl").read_text(encoding="utf-8").splitlines()) + "]")], ["1"])
        self.run_cls(self.make_input("b3", []), f)
        status, _, reason, runs, attempts = self.row("1")
        self.assertEqual((status, reason, runs, attempts), ("failed", "retry_exhausted: server_error", 3, 12))
        self.assertEqual([r["id"] for r in self.rows("b3", "failed")], ["1"])
        self.assertEqual((self.dir / "pending.jsonl").read_text(encoding="utf-8"), "")

    def test_invalid_category_two_calls_and_failed(self):
        f = Fake({"a": ["не знаю"]})
        self.run_cls(self.make_input("b1", [("1", "a")]), f)
        self.assertEqual(f.calls, 2)
        self.assertEqual(self.row("1")[:3], ("failed", None, "invalid_category: не знаю"))
        self.assertIn("не знаю", self.out)

    def test_next_run_picks_up_retry_without_old_ids(self):
        f = Fake({"bad": [fake_llm.ServerError("x")]})
        self.run_cls(self.make_input("b1", [("1", "good"), ("2", "bad")]), f)
        self.assertEqual(self.row("2")[0], "retry")
        f.script = {}  # кластер отпустило
        self.run_cls(self.make_input("b2", [("3", "good")]), f)
        self.assertEqual(self.row("2")[:2], ("ok", "карты"))
        self.assertEqual(sorted(r["id"] for r in self.rows("b2", "out")), ["2", "3"])
        self.assertEqual([r["id"] for r in self.rows("b1", "out")], ["1"])

    def test_rerun_same_file_same_output(self):
        f = Fake({"bad": ["не знаю"]})
        inp = self.make_input("b1", [("1", "good"), ("2", "bad")])
        self.run_cls(inp, f)
        out1 = (self.dir / "b1.out.jsonl").read_bytes()
        failed1 = (self.dir / "b1.failed.jsonl").read_bytes()
        calls = f.calls
        self.run_cls(inp, f)
        self.assertEqual(f.calls, calls)  # и ни одного нового вызова
        self.assertEqual((self.dir / "b1.out.jsonl").read_bytes(), out1)
        self.assertEqual((self.dir / "b1.failed.jsonl").read_bytes(), failed1)

    def test_rerun_after_crash_between_mark_and_write(self):
        # п. 9: пометили пакет, файлы не записали; перезапуск пишет те же файлы
        f = Fake()
        inp = self.make_input("b1", [("1", "a")])
        self.run_cls(inp, f)
        (self.dir / "b1.out.jsonl").unlink()
        self.run_cls(inp, f)
        self.assertEqual(self.rows("b1", "out"), [{"id": "1", "category": "карты"}])

    def test_concurrency_limit(self):
        f = Fake(delay=0.01)
        n = 300
        self.run_cls(self.make_input("b1", [(str(i), f"t{i}") for i in range(n)]), f, timeout=5)
        self.assertEqual(f.calls, n)
        self.assertLessEqual(f.peak, 20)
        self.assertGreaterEqual(f.peak, 15)

    def test_window_bounds_tasks(self):
        f = Fake(delay=0.01)
        self.run_cls(self.make_input("b1", [(str(i), f"t{i}") for i in range(30)]), f, window=5, timeout=5)
        self.assertEqual(f.calls, 30)
        self.assertLessEqual(f.peak, 5)  # задач в работе не больше окна, хотя квота 20

    def test_timeout_is_temporary_error(self):
        async def hang():
            await asyncio.sleep(5)

        f = Fake({"a": [hang, "кредиты"]})
        self.run_cls(self.make_input("b1", [("1", "a")]), f)
        self.assertEqual(f.calls, 2)
        self.assertEqual(self.row("1")[:2], ("ok", "кредиты"))
        f = Fake({"h": [hang]})
        self.run_cls(self.make_input("b2", [("2", "h")]), f)
        self.assertEqual(f.calls, 4)
        self.assertEqual(self.row("2")[:3], ("retry", None, "timeout"))

    def test_warning_exit_code_2(self):
        f = Fake({"bad": [fake_llm.ServerError("x")]})
        code = self.run_cls(self.make_input("b1", [("1", "bad"), ("2", "ok")]), f)
        self.assertEqual(code, 2)
        self.assertIn("WARNING", self.out)

    def test_lock_busy_exits_zero_without_calls(self):
        held = classify.Lock(self.dir / "c.lock")
        self.assertTrue(held.acquire())
        try:
            f = Fake()
            self.assertEqual(self.run_cls(self.make_input("b1", [("1", "a")]), f), 0)
            self.assertEqual(f.calls, 0)
        finally:
            held.release()


if __name__ == "__main__":
    unittest.main()
