import json, random
from pathlib import Path

random.seed(0)
samples = [
    "Не приходит смс с кодом", "Списали комиссию за перевод", "Как закрыть вклад досрочно",
    "Заблокировали карту за границей", "Звонили из банка и просили код", "Не проходит платёж по QR",
    "Хочу снизить ставку по кредиту", "Приложение вылетает при входе",
]
out = Path(__file__).resolve().parent.parent / "data" / "sample.jsonl"
out.parent.mkdir(exist_ok=True)
with open(out, "w", encoding="utf-8", newline="\n") as f:
    for i in range(2000):
        f.write(json.dumps({"id": f"t-{i:06d}", "text": random.choice(samples)}, ensure_ascii=False) + "\n")
