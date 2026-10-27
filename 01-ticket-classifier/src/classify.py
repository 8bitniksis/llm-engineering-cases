"""Пакетная классификация обращений через LLM API. Решения — в PROMPT.md (номера пунктов в комментариях)."""
import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import sqlite3
import string
import sys
from collections import Counter
from pathlib import Path

import fake_llm

log = logging.getLogger("classify")

# п. 7: пополняется по логам ответов вне списка
SYNONYMS = {
    "fraud": "мошенничество",
    "ипотека": "кредиты",
}
MAX_RUNS = 3          # п. 8
TECH_ALERT = 0.05     # п. 10

SCHEMA = """CREATE TABLE IF NOT EXISTS results(
    id TEXT PRIMARY KEY, text TEXT, status TEXT, category TEXT, reason TEXT,
    runs INTEGER, attempts INTEGER, batch TEXT)"""


def parse_answer(raw):
    """Возвращает (категория или None, как исправлено: ok / norm / syn)."""
    if raw in fake_llm.CATEGORIES:
        return raw, "ok"
    norm = raw.strip().lower().strip(string.punctuation + "«»“”… \t\r\n")
    if norm in fake_llm.CATEGORIES:
        return norm, "norm"
    if norm in SYNONYMS:
        return SYNONYMS[norm], "syn"
    return None, None


class Lock:
    """Блокировка ОС без ожидания (п. 1); снимается сама при падении процесса."""

    def __init__(self, path):
        self.f = open(path, "a+")

    def acquire(self):
        try:
            self.f.seek(0)
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(self.f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            self.f.close()
            return False

    def release(self):
        self.f.close()  # закрытие дескриптора снимает блокировку на обеих ОС


def tickets_to_process(conn, input_path, seen, stats):
    """Сначала все retry из базы, потом строки входа, которых нет в базе (п. 2)."""
    for row in conn.execute("SELECT id, text, runs, attempts FROM results WHERE status='retry'").fetchall():
        seen.add(row[0])
        yield row
    with open(input_path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                t = json.loads(line)
                tid, text = t["id"], t["text"]
            except (ValueError, KeyError, TypeError):
                stats["bad_lines"] += 1
                log.warning("битая строка входа: %r", line[:100])
                continue
            if tid in seen:
                continue
            if conn.execute("SELECT 1 FROM results WHERE id=?", (tid,)).fetchone():
                stats["skipped"] += 1
                continue
            seen.add(tid)
            yield tid, text, 0, 0


def save(conn, tid, text, status, category, reason, runs, attempts):
    if status != "retry":
        text = None  # текст обращения — персональные данные; хранится, только пока нужен для retry
    conn.execute(
        """INSERT INTO results(id, text, status, category, reason, runs, attempts, batch)
           VALUES(?,?,?,?,?,?,?,NULL)
           ON CONFLICT(id) DO UPDATE SET text=excluded.text, status=excluded.status,
               category=excluded.category, reason=excluded.reason, runs=excluded.runs,
               attempts=excluded.attempts""",
        (tid, text, status, category, reason, runs, attempts))
    conn.commit()  # п. 2: коммит на каждую запись


async def process(conn, tickets, stats, *, timeout, attempts, pause_base, pause_cap, window, concurrency,
                  deadline):
    sem = asyncio.Semaphore(concurrency)  # п. 3

    async def handle(tid, text, runs, prev_attempts):
        calls = 0
        invalid = 0
        err = None
        raw = None
        for n in range(1, attempts + 1):
            err = None
            async with sem:  # слот только на время вызова
                calls += 1
                try:
                    raw = await asyncio.wait_for(fake_llm.classify(text), timeout)  # п. 5
                except fake_llm.RateLimitError:
                    err = "rate_limit"
                except fake_llm.ServerError:
                    err = "server_error"
                except asyncio.TimeoutError:
                    err = "timeout"
                except Exception as e:
                    # неизвестная ошибка API (ConnectionResetError и т. п.) не роняет пакет:
                    # считаем временной, предел MAX_RUNS не даёт одному обращению валить каждый запуск
                    err = f"unexpected: {type(e).__name__}"
                    log.exception("обращение %s: неизвестная ошибка API", tid)
            if err is None:
                cat, how = parse_answer(raw)
                if cat is not None:
                    if how != "ok":
                        stats["invalid_" + how] += 1
                    save(conn, tid, text, "ok", cat, None, runs + 1, prev_attempts + calls)
                    break
                stats["invalid_unfixed"] += 1
                stats["unfixed_raw"][raw] += 1
                invalid += 1
                if invalid == 2:  # п. 7: ровно одна повторная попытка
                    save(conn, tid, text, "failed", None, f"invalid_category: {raw}", runs + 1, prev_attempts + calls)
                    stats["failed"]["invalid_category"] += 1
                    break
                stats["retries"] += 1
                continue
            if n < attempts:
                stats["retries"] += 1
                await asyncio.sleep(random.uniform(0, min(pause_cap, pause_base * 2 ** n)))  # п. 6
        else:
            # попытки кончились; последняя причина — err, либо ответ вне списка при исчерпанных попытках
            if err is None:
                save(conn, tid, text, "failed", None, f"invalid_category: {raw}", runs + 1, prev_attempts + calls)
                stats["failed"]["invalid_category"] += 1
            elif runs + 1 < MAX_RUNS:  # п. 8
                save(conn, tid, text, "retry", None, err, runs + 1, prev_attempts + calls)
                stats["retry"] += 1
            else:
                save(conn, tid, text, "failed", None, f"retry_exhausted: {err}", runs + 1, prev_attempts + calls)
                stats["failed"]["retry_exhausted"] += 1
                stats["tech_failed"] += 1
        stats["processed"] += 1

    pending = set()
    try:
        # дедлайн запуска: зависший запуск не держит блокировку вечно и не глушит следующие
        async with asyncio.timeout(deadline):
            for t in tickets:
                while len(pending) >= window:  # п. 4
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for d in done:
                        d.result()
                pending.add(asyncio.create_task(handle(*t)))
            await asyncio.gather(*pending)
    except TimeoutError:
        stats["deadline"] = True
    except asyncio.CancelledError:
        stats["interrupted"] = True
    finally:
        for p in pending:
            p.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


def write_jsonl(path, rows):
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, path)


def export(conn, batch, out_dir):
    """п. 9: сначала пометить пакет в базе, потом писать файлы."""
    conn.execute("UPDATE results SET batch=? WHERE batch IS NULL AND status IN ('ok','failed')", (batch,))
    conn.commit()
    q = "SELECT {} FROM results WHERE batch=? AND status=? ORDER BY id"
    write_jsonl(out_dir / f"{batch}.out.jsonl",
                [{"id": i, "category": c} for i, c in conn.execute(q.format("id, category"), (batch, "ok"))])
    write_jsonl(out_dir / f"{batch}.failed.jsonl",
                [{"id": i, "reason": r, "runs": n} for i, r, n in conn.execute(q.format("id, reason, runs"), (batch, "failed"))])
    write_jsonl(out_dir / "pending.jsonl",
                [{"id": i, "reason": r, "runs": n} for i, r, n in
                 conn.execute("SELECT id, reason, runs FROM results WHERE status='retry' ORDER BY id")])


def print_summary(stats):
    print("=== Сводка ===")
    print(f"обработано за запуск: {stats['processed']}")
    print(f"пропущено как уже готовые: {stats['skipped']}")
    print(f"повторов: {stats['retries']}")
    print(f"битых строк входа: {stats['bad_lines']}")
    print(f"ответов вне списка: исправлено нормализацией {stats['invalid_norm']}, "
          f"словарём {stats['invalid_syn']}, не исправлено {stats['invalid_unfixed']}")
    for raw, n in stats["unfixed_raw"].most_common():
        print(f"    не исправлено: {raw!r} x{n}")
    print(f"ушло в retry: {stats['retry']}")
    print(f"ушло в failed: {sum(stats['failed'].values())} {dict(stats['failed'])}")
    print(f"fake_llm.calls: {fake_llm.calls}")
    if stats["interrupted"]:
        print("запуск прерван (Ctrl+C), готовое сохранено в базе, выгрузка — при перезапуске")
    if stats["deadline"]:
        print("ERROR: дедлайн запуска истёк, пакет не доделан — перезапустить на том же файле")


def batch_name(path):
    """Имя файла + хэш содержимого: тот же файл после сбоя даёт то же имя (выгрузка идемпотентна),
    другой файл с тем же именем — другое (иначе tickets.jsonl каждые 15 минут копил бы один пакет)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return f"{path.stem}-{h.hexdigest()[:8]}"


def run(input_path, db_path="classify.db", out_dir=None, lock_path="classify.lock", *,
        timeout=10.0, attempts=4, pause_base=0.5, pause_cap=8.0, window=100, concurrency=20,
        deadline=720.0):
    """Код выхода: 0 — ок, 2 — системные технические отказы, 3 — блокировка занята,
    4 — истёк дедлайн, 130 — прервано."""
    input_path = Path(input_path)
    out_dir = Path(out_dir) if out_dir else input_path.parent
    lock = Lock(lock_path)
    if not lock.acquire():
        # дедлайн (12 мин) меньше интервала (15 мин), так что занятая блокировка — аномалия:
        # ненулевой код, чтобы планировщик это показал, а не молчал сутки
        log.error("предыдущий запуск ещё работает")
        return 3
    stats = Counter()
    stats["failed"] = Counter()
    stats["unfixed_raw"] = Counter()
    conn = sqlite3.connect(db_path)
    try:
        # WAL + NORMAL: коммит переживает падение процесса и Ctrl+C без fsync на каждую запись.
        # При потере питания хвост WAL может пропасть; если это важно — synchronous=FULL (~0,1 с на коммит)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(SCHEMA)
        try:
            asyncio.run(process(conn, tickets_to_process(conn, input_path, set(), stats), stats,
                                timeout=timeout, attempts=attempts, pause_base=pause_base,
                                pause_cap=pause_cap, window=window, concurrency=concurrency,
                                deadline=deadline))
        except KeyboardInterrupt:
            stats["interrupted"] = True
        # п. 9: неполный пакет не выгружаем — его могут забрать и разослать до перезапуска;
        # готовое лежит в базе и уйдёт в выгрузку перезапуска или следующего пакета
        if not stats["interrupted"] and not stats["deadline"]:
            export(conn, batch_name(input_path), out_dir)
    finally:
        conn.close()
        lock.release()
    print_summary(stats)
    tech = stats["retry"] + stats["tech_failed"]
    if stats["processed"] and tech / stats["processed"] > TECH_ALERT:
        print(f"WARNING: технических отказов {tech} из {stats['processed']} (> {TECH_ALERT:.0%})")
        return 2
    if stats["deadline"]:
        return 4
    return 130 if stats["interrupted"] else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input")
    ap.add_argument("--db", default="classify.db")
    ap.add_argument("--out-dir", default=None, help="по умолчанию — папка входного файла")
    ap.add_argument("--lock", default="classify.lock")
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--attempts", type=int, default=4)
    ap.add_argument("--pause-base", type=float, default=0.5)
    ap.add_argument("--pause-cap", type=float, default=8.0)
    ap.add_argument("--deadline", type=float, default=720.0, help="секунд на запуск, меньше интервала планировщика")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(run(a.input, a.db, a.out_dir, a.lock, timeout=a.timeout, attempts=a.attempts,
                 pause_base=a.pause_base, pause_cap=a.pause_cap, deadline=a.deadline))


if __name__ == "__main__":
    main()
