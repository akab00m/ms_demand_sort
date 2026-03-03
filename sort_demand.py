"""Сортировка товаров в отгрузке МойСклад по ячейкам склада.

Использование:
    python sort_demand.py                          # стандартный запуск
    python sort_demand.py --days 14               # за 14 дней
    python sort_demand.py --apply                 # + сохранить порядок в МойСклад
    python sort_demand.py --state-name "сборка"  # другое название статуса
    python sort_demand.py --cell-attr "Место"    # другое название атрибута ячейки
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import sys
import threading
import time

import keyring
import requests
import tyro
from colorama import Fore, Style, init

init(autoreset=True)

__version__ = "1.0.0"

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"


# ── Config ────────────────────────────────────────────────────────────────────


@dataclasses.dataclass
class AppConfig:
    """Параметры сортировки отгрузок МойСклад."""

    days: int = 30
    """Количество дней назад для выборки отгрузок."""

    state_name: str = "на сборке"
    """Название статуса для фильтрации отгрузок."""

    cell_attr: str = "Ячейка"
    """Название пользовательского атрибута ячейки у товара."""

    apply: bool = False
    """Сохранить отсортированный порядок позиций в МойСклад."""

    debug: bool = False
    """Выводить сырой ответ API для диагностики."""


# ── Rate Limiter ──────────────────────────────────────────────────────────────

# МойСклад: Bearer-токен → 45 запросов за 3-секундное окно
# Берём 40 для запаса; при 429 ждём Retry-After (или экспоненциальный backoff)
_RATE_LIMIT_REQUESTS = 40
_RATE_LIMIT_WINDOW = 3.0  # секунды
_MAX_RETRIES = 5


class _RateLimiter:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, rate: int, window: float) -> None:
        self._rate = rate          # токенов на окно
        self._window = window      # размер окна в секундах
        self._tokens = float(rate)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Блокирует вызывающий поток до получения разрешения на запрос."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            # Пополняем токены пропорционально прошедшему времени
            self._tokens = min(
                float(self._rate),
                self._tokens + elapsed * self._rate / self._window,
            )
            self._last = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) * self._window / self._rate
                time.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


_rate_limiter = _RateLimiter(_RATE_LIMIT_REQUESTS, _RATE_LIMIT_WINDOW)


# ── API Client ────────────────────────────────────────────────────────────────


class MoySkladClient:
    def __init__(self, token: str) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept-Encoding": "gzip",
                "Content-Type": "application/json",
            }
        )

    # ── internal ──────────────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        json: dict | list | None = None,
    ) -> dict | list:
        """Выполнить запрос с rate limiting и retry при 429/5xx."""
        for attempt in range(1, _MAX_RETRIES + 1):
            _rate_limiter.acquire()
            try:
                resp = self._session.request(
                    method, url, params=params, json=json, timeout=30
                )
            except requests.Timeout:
                wait = 2 ** attempt
                print(
                    f"{Fore.YELLOW}[WARN] Timeout, повтор через {wait}с "
                    f"(попытка {attempt}/{_MAX_RETRIES})…{Style.RESET_ALL}"
                )
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                print(
                    f"{Fore.YELLOW}[WARN] 429 Rate limit, ожидание {retry_after:.1f}с "
                    f"(попытка {attempt}/{_MAX_RETRIES})…{Style.RESET_ALL}"
                )
                time.sleep(retry_after)
                continue

            if resp.status_code >= 500 and attempt < _MAX_RETRIES:
                wait = 2 ** attempt
                print(
                    f"{Fore.YELLOW}[WARN] HTTP {resp.status_code}, повтор через {wait}с "
                    f"(попытка {attempt}/{_MAX_RETRIES})…{Style.RESET_ALL}"
                )
                time.sleep(wait)
                continue

            if not resp.ok:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text
                raise RuntimeError(
                    f"HTTP {resp.status_code}\n"
                    f"  request.url = {resp.request.url}\n"
                    f"  resp.url    = {resp.url}\n"
                    f"{detail}"
                )

            # 204 No Content — некоторые bulk-операции не возвращают тело
            if not resp.content:
                return {}
            return resp.json()

        raise RuntimeError(f"Превышено {_MAX_RETRIES} попыток для {url}")

    # ── public ────────────────────────────────────────────────────────────────

    def get(self, path: str, params: dict | None = None) -> dict:
        return self._request("GET", f"{BASE_URL}{path}", params=params)

    def get_by_href(self, href: str, params: dict | None = None) -> dict:
        return self._request("GET", href, params=params)

    def post(self, path: str, payload: dict | list) -> dict | list:
        return self._request("POST", f"{BASE_URL}{path}", json=payload)


# ── Auth ──────────────────────────────────────────────────────────────────────


def load_token() -> str:
    token = keyring.get_password("moysklad", "access_token")
    if not token:
        _err(
            "Токен не найден. Запустите get_token.py для авторизации.",
            exit_code=1,
        )
    return token  # type: ignore[return-value]


# ── Business Logic ────────────────────────────────────────────────────────────


def find_state_href(client: MoySkladClient, state_name: str) -> str:
    """Найти href статуса по имени в метаданных отгрузок."""
    meta = client.get("/entity/demand/metadata")
    states: list[dict] = meta.get("states", [])
    for state in states:
        if state["name"].lower() == state_name.lower():
            return state["meta"]["href"]

    available = ", ".join(f"'{s['name']}'" for s in states)
    _err(
        f"Статус '{state_name}' не найден. Доступные статусы: {available}",
        exit_code=1,
    )
    return ""  # unreachable


def fetch_demands(
    client: MoySkladClient,
    since: datetime.datetime,
    state_href: str,
) -> list[dict]:
    """Получить отгрузки за период с нужным статусом (все страницы)."""
    moment_str = since.strftime("%Y-%m-%d %H:%M:%S")
    rows: list[dict] = []
    offset = 0
    limit = 100

    while True:
        data = client.get(
            "/entity/demand",
            params={
                "filter": f"moment>{moment_str};state={state_href}",
                "limit": limit,
                "offset": offset,
                "expand": "state,agent",
                "order": "moment,desc",
            },
        )
        page = data.get("rows", [])
        rows.extend(page)
        if len(page) < limit:
            break
        offset += limit

    return rows


def fetch_positions(
    client: MoySkladClient,
    demand_id: str,
    cell_attr: str,
    debug: bool = False,
) -> list[dict]:
    """Получить позиции отгрузки со всеми страницами, каждой назначить ячейку.

    Ячейка берётся из поля `slot` позиции (место хранения МойСклад).
    Если slot отсутствует — fallback на пользовательский атрибут товара.
    """
    # Шаг 1: href позиций из самого документа
    demand_data = client.get(f"/entity/demand/{demand_id}")
    positions_meta: dict = demand_data.get("positions", {}).get("meta", {})
    positions_href: str = positions_meta.get("href", "")
    total_size: int = positions_meta.get("size", 0)

    if debug:
        print(
            f"{Fore.YELLOW}[DEBUG] positions href: {positions_href}"
            f"\n[DEBUG] total size: {total_size}{Style.RESET_ALL}"
        )

    if not positions_href:
        print(f"{Fore.RED}[WARN] Не удалось определить href позиций.{Style.RESET_ALL}")
        return []

    # Шаг 2: пагинация, раскрываем и assortment, и slot
    positions: list[dict] = []
    offset = 0
    limit = 100

    while True:
        data = client.get_by_href(
            positions_href,
            params={
                "limit": limit,
                "offset": offset,
                "expand": "assortment,slot",
            },
        )

        if isinstance(data, list):
            page = data
        else:
            page = data.get("rows", [])

        if debug and page:
            first = page[0]
            slot_raw = first.get("slot")
            print(
                f"{Fore.YELLOW}[DEBUG] rows={len(page)}"
                f" | первая позиция: {first.get('assortment', {}).get('name', '—')[:40]}"
                f" | slot: {slot_raw}{Style.RESET_ALL}"
            )

        positions.extend(page)

        if len(page) < limit:
            break
        offset += limit

    # Шаг 3: определяем ячейку для каждой позиции
    # Приоритет: slot.name → атрибут товара (fallback)
    product_cell_cache: dict[str, str] = {}  # href → ячейка из атрибутов

    for pos in positions:
        # Способ 1: slot из позиции (место хранения)
        slot: dict | None = pos.get("slot")
        if slot and isinstance(slot, dict) and slot.get("name"):
            pos["_cell"] = slot["name"]
            continue

        # Способ 2: пользовательский атрибут на товаре (fallback)
        assortment: dict = pos.get("assortment") or {}
        if not assortment.get("name"):
            # expand не раскрыл assortment — отдельный запрос
            pos_href = pos.get("meta", {}).get("href", "")
            if pos_href:
                try:
                    full = client.get_by_href(pos_href, params={"expand": "assortment,slot"})
                    pos["assortment"] = full.get("assortment", {})
                    slot = full.get("slot")
                    if slot and isinstance(slot, dict) and slot.get("name"):
                        pos["_cell"] = slot["name"]
                        continue
                    assortment = pos["assortment"]
                except RuntimeError as exc:
                    print(f"{Fore.RED}[WARN] Не удалось получить позицию: {exc}{Style.RESET_ALL}")

        product_href = _product_href(assortment)
        if product_href not in product_cell_cache:
            product_cell_cache[product_href] = _extract_cell_from_attr(
                client, assortment, product_href, cell_attr, debug=debug
            )
        pos["_cell"] = product_cell_cache[product_href]

    return positions


def _product_href(assortment: dict) -> str:
    """Вернуть href товара (для варианта — href родительского товара)."""
    meta = assortment.get("meta", {})
    entity_type = meta.get("type", "")

    # Вариант → берём href родительского product
    if entity_type == "variant":
        product = assortment.get("product")
        if product:
            return product.get("meta", {}).get("href", "")

    return meta.get("href", "")


def _extract_cell_from_attr(
    client: MoySkladClient,
    assortment: dict,
    product_href: str,
    cell_attr: str,
    debug: bool = False,
) -> str:
    """Fallback: поиск ячейки в пользовательских атрибутах товара."""
    # Атрибуты могут присутствовать сразу (если API вернул их)
    cell = _find_attr_value(assortment.get("attributes", []), cell_attr)
    if cell:
        return cell

    # Атрибуты не возвращаются через expand → запрашиваем товар отдельно
    if product_href:
        try:
            product_data = client.get_by_href(product_href)
            if debug:
                attrs = product_data.get("attributes", [])
                print(
                    f"{Fore.YELLOW}[DEBUG] Атрибуты товара ({product_href.split('/')[-1]}): "
                    f"{[a.get('name') for a in attrs]}{Style.RESET_ALL}"
                )
            cell = _find_attr_value(product_data.get("attributes", []), cell_attr)
        except RuntimeError as exc:
            print(f"{Fore.RED}[WARN] Не удалось получить атрибуты товара: {exc}{Style.RESET_ALL}")

    return cell


def _find_attr_value(attributes: list[dict], attr_name: str) -> str:
    """Найти значение атрибута по имени."""
    for attr in attributes:
        if attr.get("name", "").lower() == attr_name.lower():
            raw = attr.get("value", "")
            return str(raw) if raw is not None else ""
    return ""


def sort_key(cell: str) -> tuple:
    """
    Ключ натуральной сортировки для ячеек вида 'A-1-2-3', 'E-6-2-1'.

    Буквенные части сортируются лексикографически,
    числовые — численно. Пустые ячейки — в конец.
    """
    if not cell:
        return (chr(0x10FFFF),)  # пустые → в самый конец

    parts = cell.split("-")
    result: list[tuple] = []
    for part in parts:
        try:
            result.append((0, int(part), ""))
        except ValueError:
            result.append((1, 0, part.upper()))
    return tuple(result)  # type: ignore[return-value]


def apply_sort_to_demand(
    client: MoySkladClient,
    demand_id: str,
    sorted_positions: list[dict],
    debug: bool = False,
) -> None:
    """
    Пересортировать позиции отгрузки через batch-DELETE + POST bulk.

    PUT /entity/demand/{id} игнорирует порядок positions. Единственный
    рабочий способ:
      1. POST /entity/demand/{id}/positions/delete  — batch-удаление (1 запрос)
      2. POST  /entity/demand/{id}/positions        — создать в нужном порядке
    """
    total = len(sorted_positions)

    # Шаг 1: batch-удаляем все текущие позиции одним запросом
    delete_payload = [{"meta": pos["meta"]} for pos in sorted_positions]
    if debug:
        import json as _json_d
        print(
            f"  [DEBUG] POST /positions/delete — {total} позиций.\n"
            f"  Первый элемент: {_json_d.dumps(delete_payload[0], ensure_ascii=False)}"
        )
    print(f"  Удаление {total} позиций (batch)…", end="", flush=True)
    client.post(f"/entity/demand/{demand_id}/positions/delete", delete_payload)
    print(" ✓")

    # Шаг 2: создаём все позиции bulk-запросом в нужном порядке
    create_payload: list[dict] = []
    for pos in sorted_positions:
        entry: dict = {
            "assortment": {"meta": pos["assortment"]["meta"]},
            "quantity": pos["quantity"],
            "price": pos.get("price", 0),
        }
        for field in (
            "discount", "vat", "vatEnabled",
            "overhead", "cost", "trackingCodes",
            "things", "gtd", "country",
        ):
            val = pos.get(field)
            if val is not None:
                entry[field] = val
        slot = pos.get("slot")
        if slot and isinstance(slot, dict) and slot.get("meta"):
            entry["slot"] = {"meta": slot["meta"]}
        create_payload.append(entry)

    if debug:
        import json as _json
        print(
            f"  [DEBUG] POST {len(create_payload)} позиций. Первая:\n"
            f"{_json.dumps(create_payload[0], ensure_ascii=False, indent=2)}"
        )

    print(f"  Создание {len(create_payload)} позиций в новом порядке…")
    client.post(f"/entity/demand/{demand_id}/positions", create_payload)


# ── Verify ────────────────────────────────────────────────────────────────────────────


def _make_snapshot(positions: list[dict]) -> dict:
    """Слепок позиций: кол-во строк, итоговое qty, сумма, Counter и порядок ключей."""
    total_qty = 0.0
    total_sum = 0.0
    items: collections.Counter = collections.Counter()
    order_keys: list[tuple] = []

    for pos in positions:
        qty = float(pos.get("quantity", 0))
        price = float(pos.get("price", 0))
        href = (pos.get("assortment") or {}).get("meta", {}).get("href", "?")
        cell = pos.get("_cell", "")

        total_qty += qty
        total_sum += price * qty
        items[(href, cell)] += qty
        order_keys.append((href, cell))

    return {
        "count": len(positions),
        "total_qty": total_qty,
        "total_sum": total_sum,
        "items": items,
        "order_keys": order_keys,
    }


def _print_verify(before: dict, after: dict) -> bool:
    """Сравнить снапшоты до и после. Возвращает True если всё совпало."""
    count_ok = before["count"] == after["count"]
    qty_ok = abs(before["total_qty"] - after["total_qty"]) < 0.001
    # price в МойСклад хранится в денежных единицах (сотые копейки)
    sum_ok = abs(before["total_sum"] - after["total_sum"]) < 1.0

    def _c(ok: bool) -> str:
        return Fore.GREEN if ok else Fore.RED

    def _i(ok: bool) -> str:
        return "✓" if ok else "✗"

    # Перевод суммы из ден. единиц в рубли (делим на 100 центов и на 100)
    b_rub = before["total_sum"] / 10_000
    a_rub = after["total_sum"] / 10_000

    print(f"\n  {'\u041fараметр':<24} {'\u0414о':>14} {'\u041fосле':>14}")
    print(f"  {'─' * 56}")
    print(
        f"  {'\u0421трок позиций':<24}"
        f" {before['count']:>14} {after['count']:>14}"
        f"  {_c(count_ok)}{_i(count_ok)}{Style.RESET_ALL}"
    )
    print(
        f"  {'\u0418того единиц (шт)':<24}"
        f" {before['total_qty']:>14.0f} {after['total_qty']:>14.0f}"
        f"  {_c(qty_ok)}{_i(qty_ok)}{Style.RESET_ALL}"
    )
    print(
        f"  {'\u0421умма (руб.)':<24}"
        f" {b_rub:>14.2f} {a_rub:>14.2f}"
        f"  {_c(sum_ok)}{_i(sum_ok)}{Style.RESET_ALL}"
    )

    # Позицийный дифф: сравниваем Counterы
    missing: list = []
    extra: list = []
    changed: list = []
    for key in set(before["items"]) | set(after["items"]):
        b_qty = before["items"].get(key, 0.0)
        a_qty = after["items"].get(key, 0.0)
        if abs(b_qty - a_qty) >= 0.001:
            href, cell = key
            sku = href.rsplit("/", 1)[-1][:12]
            label = f"ячейка={cell or '—':8} артикуль=…{sku}"
            if b_qty > 0 and a_qty == 0:
                missing.append((label, b_qty))
            elif b_qty == 0 and a_qty > 0:
                extra.append((label, a_qty))
            else:
                changed.append((label, b_qty, a_qty))

    if missing or extra or changed:
        print(f"\n  {Fore.RED}Расхождения по позициям:{Style.RESET_ALL}")
        for label, qty in missing:
            print(f"  {Fore.RED}\u2717 ПОТЕРЯНО   {label}  qty={qty:.0f}{Style.RESET_ALL}")
        for label, qty in extra:
            print(f"  {Fore.YELLOW}\u2717 ЛИШНЕЕ    {label}  qty={qty:.0f}{Style.RESET_ALL}")
        for label, b, a in changed:
            print(f"  {Fore.YELLOW}\u2717 ИЗМЕНЕНО  {label}  qty={b:.0f}→{a:.0f}{Style.RESET_ALL}")

    # Проверка порядка
    exp = before["order_keys"]
    got = after["order_keys"]
    order_ok = (exp == got)
    order_label = "Порядок позиций"
    print(
        f"  {order_label:<24}"
        f" {'(отправлено)':>14} {'(из API)':>14}"
        f"  {_c(order_ok)}{_i(order_ok)}{Style.RESET_ALL}"
    )
    if not order_ok:
        # Показываем первые расхождения (макс 5)
        diffs = [
            i for i, (e, g) in enumerate(zip(exp, got), 1) if e != g
        ]
        extra_count = len(exp) - len(got) if len(exp) != len(got) else 0
        shown = diffs[:5]
        print(f"  {Fore.RED}  Позиции не в том порядке. Первые расхождения: строки {shown}{Style.RESET_ALL}")
        if extra_count:
            print(f"  {Fore.RED}  Длины не совпадают: отправлено {len(exp)}, получено {len(got)}{Style.RESET_ALL}")

    return count_ok and qty_ok and sum_ok and order_ok and not (missing or extra or changed)


# ── Display ────────────────────────────────────────────────────────────────────────────

_BORDER = "─"


def _hr(width: int = 78) -> None:
    print(_BORDER * width)


def print_demands_table(demands: list[dict]) -> None:
    header = (
        f"{'№':<4} {'Дата':<12} {'Номер':<16} "
        f"{'Контрагент':<32} {'Статус'}"
    )
    print(f"\n{Fore.CYAN}{header}{Style.RESET_ALL}")
    _hr()
    for i, d in enumerate(demands, 1):
        moment = d.get("moment", "")[:10]
        name = d.get("name", "—")
        agent_name = (d.get("agent") or {}).get("name", "—")[:30]
        state_name = (d.get("state") or {}).get("name", "—")
        print(
            f"{Fore.WHITE}{i:<4}{Style.RESET_ALL}"
            f" {moment:<12} {name:<16} {agent_name:<32} {state_name}"
        )


def print_positions_table(sorted_positions: list[dict]) -> None:
    header = (
        f"{'№':<4} {'Ячейка':<14} {'Наименование':<46} {'Кол-во':>7}"
    )
    print(f"\n{Fore.CYAN}{header}{Style.RESET_ALL}")
    _hr()

    for i, pos in enumerate(sorted_positions, 1):
        assortment = pos.get("assortment", {})
        product_name = assortment.get("name", "—")[:44]
        qty = pos.get("quantity", 0)
        cell = pos.get("_cell", "")

        if cell:
            cell_str = f"{Fore.YELLOW}{cell:<14}{Style.RESET_ALL}"
        else:
            cell_str = f"{Fore.RED}{'—':<14}{Style.RESET_ALL}"

        print(f"{i:<4} {cell_str} {product_name:<46} {qty:>7.0f}")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _err(msg: str, exit_code: int = 1) -> None:
    print(f"{Fore.RED}{msg}{Style.RESET_ALL}", file=sys.stderr)
    sys.exit(exit_code)


def _demand_id_from(demand: dict) -> str:
    """Извлечь UUID отгрузки. Берём поле id напрямую — безопаснее, чем парсить href."""
    return demand["id"]


def _pick_demand(demands: list[dict]) -> dict:
    """Интерактивный выбор отгрузки по номеру."""
    total = len(demands)
    while True:
        try:
            raw = input(
                f"\n{Fore.CYAN}Введите номер отгрузки "
                f"[1–{total}] (0 — выход): {Style.RESET_ALL}"
            ).strip()
            num = int(raw)
        except (ValueError, EOFError):
            print(f"{Fore.RED}Введите целое число.{Style.RESET_ALL}")
            continue

        if num == 0:
            print("Выход.")
            sys.exit(0)
        if 1 <= num <= total:
            return demands[num - 1]
        print(f"{Fore.RED}Введите число от 1 до {total}.{Style.RESET_ALL}")


# ── Entry Point ───────────────────────────────────────────────────────────────


def main(config: AppConfig) -> None:  # noqa: D401
    token = load_token()
    client = MoySkladClient(token)

    # 1. Найти статус
    print(f"{Fore.CYAN}Поиск статуса «{config.state_name}»…{Style.RESET_ALL}")
    state_href = find_state_href(client, config.state_name)
    print(f"{Fore.GREEN}✓ Статус найден{Style.RESET_ALL}")

    # 2. Загрузить отгрузки
    since = datetime.datetime.now() - datetime.timedelta(days=config.days)
    print(
        f"Загрузка отгрузок за последние {config.days} дн. "
        f"(с {since.strftime('%Y-%m-%d')})…"
    )
    demands = fetch_demands(client, since, state_href)

    if not demands:
        print(f"{Fore.YELLOW}Отгрузок не найдено.{Style.RESET_ALL}")
        return

    print(f"{Fore.GREEN}✓ Найдено: {len(demands)}{Style.RESET_ALL}")

    # 3. Показать таблицу и получить выбор пользователя
    print_demands_table(demands)
    selected = _pick_demand(demands)
    demand_id = _demand_id_from(selected)
    demand_name = selected.get("name", demand_id)

    # 4. Загрузить позиции и определить ячейки
    print(
        f"\nЗагрузка позиций «{Fore.CYAN}{demand_name}{Style.RESET_ALL}»…"
    )
    positions = fetch_positions(client, demand_id, config.cell_attr, debug=config.debug)

    if not positions:
        print(f"{Fore.YELLOW}В отгрузке нет позиций.{Style.RESET_ALL}")
        return

    without_cell = sum(1 for p in positions if not p.get("_cell"))
    if without_cell:
        print(
            f"{Fore.YELLOW}⚠ {without_cell} из {len(positions)} позиций "
            f"без атрибута «{config.cell_attr}» — будут в конце списка.{Style.RESET_ALL}"
        )

    # 5. Отсортировать и отобразить
    sorted_positions = sorted(
        positions, key=lambda p: sort_key(p.get("_cell", ""))
    )
    print_positions_table(sorted_positions)

    # 6. Применить в МойСклад
    # --config.apply = автоподтверждение; без флага — всегда спрашиваем
    if config.apply:
        confirm = "y"
    else:
        confirm = input(
            f"\n{Fore.YELLOW}Сохранить порядок в МойСклад? [y/N]: {Style.RESET_ALL}"
        ).strip().lower()

    if confirm == "y":
        # Снапшот ДО сохранения
        before_snap = _make_snapshot(sorted_positions)

        print("Применяю сортировку…")
        apply_sort_to_demand(client, demand_id, sorted_positions, debug=config.debug)

        # Снапшот ПОСЛЕ: читаем позиции заново из АПИ
        print("  Сверка данных…")
        after_positions = fetch_positions(
            client, demand_id, config.cell_attr, debug=False
        )
        after_snap = _make_snapshot(after_positions)

        ok = _print_verify(before_snap, after_snap)

        if ok:
            print(
                f"\n{Fore.GREEN}✓ Порядок обновлён. Данные совпали.{Style.RESET_ALL}"
            )
        else:
            print(
                f"\n{Fore.RED}✗ ВНИМАНИЕ: несоответствие данных после сохранения!{Style.RESET_ALL}"
            )
    else:
        print("Отменено.")


if __name__ == "__main__":
    tyro.cli(main)
