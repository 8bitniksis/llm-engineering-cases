import asyncio
import random

CATEGORIES = [
    "карты", "кредиты", "вклады", "переводы и платежи",
    "мобильное приложение", "мошенничество", "тарифы и комиссии", "другое",
]
MAX_CONCURRENT = 20
_in_flight = 0
calls = 0  # сколько платных запросов ушло в API


class RateLimitError(Exception):
    pass


class ServerError(Exception):
    pass


async def classify(text: str) -> str:
    global _in_flight, calls
    calls += 1
    if _in_flight >= MAX_CONCURRENT:
        raise RateLimitError("429 Too Many Requests")
    _in_flight += 1
    try:
        r = random.random()
        if r < 0.02:
            await asyncio.sleep(60)          # зависание
        else:
            await asyncio.sleep(random.uniform(0.05, 0.3))
        if r < 0.05:
            raise ServerError("503 Service Unavailable")
        if r < 0.08:
            return random.choice(["Карты.", "fraud", "не знаю", "ипотека"])  # мусор от модели
        return random.choice(CATEGORIES)
    finally:
        _in_flight -= 1
