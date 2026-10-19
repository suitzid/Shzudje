"""Бот-оркестратор. ROLE=manager | worker (env).

Manager: меню с кнопками (Добавить бота / Инструкция / Поддержка),
         админка (/admin) с полной настройкой, HTTP- и Telegram-пинг ботов.
Worker:  отвечает "pong" на /ping@ЭтотБот.

Защита от циклов: дедуп по update_id, лимитер исходящих в группу,
глубина цепочки = 1 (pong никогда не вызывает ответ).
"""
import asyncio
import json
import logging
import os
import re
import time
from collections import OrderedDict

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

ROLE = os.getenv("ROLE", "worker")
TOKEN = os.environ["BOT_TOKEN"]
SECRET = os.environ["WEBHOOK_SECRET"]
BASE_URL = os.environ["RENDER_EXTERNAL_URL"].rstrip("/")
PORT = int(os.getenv("PORT", "10000"))
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "")   # для worker
OWNER_ID = int(os.getenv("ADMIN_ID", "0"))             # главный админ (твой user id)
CFG_FILE = os.getenv("CFG_FILE", "config.json")
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL", "")  # опционально: постоянное хранилище
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

DEFAULT_INSTRUCTION = (
    "Как подключить бота:\n"
    "1. Создай бота в @BotFather.\n"
    "2. @BotFather → /mybots → бот → Bot Settings → Mode Settings → "
    "включи Bot-to-Bot Communication Mode.\n"
    "3. Задеплой его на Render как Web Service с маршрутом GET /health.\n"
    "4. Добавь бота в общую группу.\n"
    "5. Нажми «Добавить бота» и пришли @username и URL сервиса. "
    "Заявку подтвердит админ."
)

cfg = {
    "targets": json.loads(os.getenv("TARGETS", "[]")),  # [{"username","url"}]
    "pending": {},            # username -> {"url", "from"}
    "admins": [],             # доп. админы (id)
    "instruction": DEFAULT_INSTRUCTION,
    "support": "Поддержка пока не настроена.",
    "group_id": 0,
    "interval": 600,          # сек между авто-HTTP-пингами
    "min_gap": 3.0,           # сек между сообщениями бота в группу
    "auto": True,             # включён ли авто-пинг
}
state = {}  # username -> результаты последних пингов

bot = Bot(TOKEN)
dp = Dispatcher()  # FSM-хранилище в памяти


# ---------- хранение настроек ----------
def sync_state():
    names = {t["username"] for t in cfg["targets"]}
    for n in names:
        state.setdefault(n, {"http_ok": None, "http_ms": None, "tg_sent": 0.0, "tg_pong": 0.0})
    for n in list(state):
        if n not in names:
            del state[n]


async def _upstash(cmd):
    async with aiohttp.ClientSession() as s:
        async with s.post(UPSTASH_URL, json=cmd, timeout=aiohttp.ClientTimeout(total=15),
                          headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}) as r:
            return (await r.json()).get("result")


async def load_cfg():
    data = None
    try:
        if UPSTASH_URL:
            raw = await _upstash(["GET", "botcfg"])
            data = json.loads(raw) if raw else None
        elif os.path.exists(CFG_FILE):
            with open(CFG_FILE, encoding="utf-8") as f:
                data = json.load(f)
    except Exception:
        log.exception("load_cfg")
    if data:
        cfg.update(data)
    sync_state()


async def save_cfg():
    try:
        if UPSTASH_URL:
            await _upstash(["SET", "botcfg", json.dumps(cfg, ensure_ascii=False)])
        else:
            with open(CFG_FILE, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False)
    except Exception:
        log.exception("save_cfg")


# ---------- защита от циклов ----------
seen: "OrderedDict[int, bool]" = OrderedDict()


@dp.update.outer_middleware()
async def dedup(handler, event, data):
    if event.update_id in seen:
        return
    seen[event.update_id] = True
    if len(seen) > 2000:
        seen.popitem(last=False)
    return await handler(event, data)


_send_lock = asyncio.Lock()
_last_send = 0.0


async def say(chat_id: int, text: str, **kw):
    """Все сообщения в группу идут через лимитер."""
    global _last_send
    async with _send_lock:
        wait = float(cfg["min_gap"]) - (time.monotonic() - _last_send)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_send = time.monotonic()
        return await bot.send_message(chat_id, text, **kw)


# ---------- worker ----------
if ROLE == "worker":
    @dp.message(Command("ping"))  # фильтр сам проверяет @username бота
    async def worker_ping(m: Message):
        u = m.from_user
        if u and u.is_bot and MANAGER_USERNAME and u.username != MANAGER_USERNAME:
            return  # слушаемся только менеджера (и людей)
        await say(m.chat.id, "pong", reply_to_message_id=m.message_id)


# ---------- хелперы manager ----------
mgr = Router()
admin = Router()
mgr.include_router(admin)


class AddBot(StatesGroup):
    username = State()
    url = State()


class SetVal(StatesGroup):
    value = State()


def all_admins():
    return [a for a in [OWNER_ID, *cfg["admins"]] if a]


def is_admin(uid) -> bool:
    return uid in all_admins()


def kb(rows):
    b = InlineKeyboardBuilder()
    for row in rows:
        b.row(*[InlineKeyboardButton(text=t, url=d[4:]) if d.startswith("url:")
                else InlineKeyboardButton(text=t, callback_data=d) for t, d in row])
    return b.as_markup()


BACK = [("◀️ Меню", "menu")]
BACK_ADM = [("◀️ Админка", "adm")]


def main_menu(uid):
    rows = [[("➕ Добавить бота", "add")],
            [("📖 Инструкция", "help"), ("🛟 Поддержка", "support")]]
    if is_admin(uid):
        rows.append([("⚙️ Админка", "adm")])
    return kb(rows)


def admin_kb():
    return kb([
        [("📊 Статус", "a:status"), ("🌐 HTTP", "a:http"), ("📨 TG", "a:tg")],
        [("🤖 Боты", "a:bots"), (f"📥 Заявки ({len(cfg['pending'])})", "a:pend")],
        [("✏️ Инструкция", "s:instruction"), ("✏️ Поддержка", "s:support")],
        [("💬 Группа", "a:group"), ("⏱ Интервал", "s:interval")],
        [("🐢 Пауза", "s:min_gap"), (f"🔁 Авто: {'вкл' if cfg['auto'] else 'выкл'}", "a:auto")],
        [("👥 Админы", "a:admins")],
        BACK,
    ])


async def show(c: CallbackQuery, text: str, markup):
    try:
        await c.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest:
        pass
    try:
        await c.answer()
    except TelegramBadRequest:
        pass


def norm_username(s: str):
    s = s.strip().lstrip("@")
    return s if re.fullmatch(r"(?=.{5,32}$)[A-Za-z][A-Za-z0-9_]*[Bb][Oo][Tt]", s) else None


def norm_url(s: str):
    s = s.strip().rstrip("/")
    return s if re.fullmatch(r"https://[A-Za-z0-9.-]+(:\d+)?", s) else None


def support_link(s: str):
    m = re.match(r"@([A-Za-z0-9_]{4,32})", s.strip())
    if m:
        return "https://t.me/" + m.group(1)
    m = re.search(r"https://\S+", s)
    return m.group(0) if m else None


async def add_target(u: str, url: str):
    cfg["targets"] = [t for t in cfg["targets"] if t["username"].lower() != u.lower()]
    cfg["targets"].append({"username": u, "url": url})
    cfg["pending"].pop(u, None)
    sync_state()
    await save_cfg()


async def notify_admins(text, markup=None):
    for a in all_admins():
        try:
            await bot.send_message(a, text, reply_markup=markup)
        except Exception:
            log.warning("не удалось написать админу %s", a)


# ---------- пинги ----------
async def http_ping(session, t):
    start = time.monotonic()
    try:  # холодный старт Render — до ~60 сек
        async with session.get(t["url"].rstrip("/") + "/health",
                               timeout=aiohttp.ClientTimeout(total=75)) as r:
            ok = r.status == 200
    except Exception as e:
        log.warning("http ping %s: %s", t["username"], e)
        ok = False
    s = state.get(t["username"])
    if s is not None:
        s["http_ok"], s["http_ms"] = ok, int((time.monotonic() - start) * 1000)


async def http_ping_all():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*(http_ping(session, t) for t in list(cfg["targets"])))


async def tg_ping_all(chat_id: int):
    await http_ping_all()  # сначала будим
    for t in list(cfg["targets"]):  # по одному, лимитер держит паузу
        state[t["username"]]["tg_sent"] = time.time()
        await say(chat_id, f"/ping@{t['username']}")
    await asyncio.sleep(15)


def report() -> str:
    lines = []
    for u, s in state.items():
        http = "—" if s["http_ok"] is None else ("✅" if s["http_ok"] else "❌") + f" {s['http_ms']}ms"
        tg = "—" if not s["tg_sent"] else ("✅" if s["tg_pong"] >= s["tg_sent"] else "❌")
        lines.append(f"@{u}: HTTP {http} | TG {tg}")
    return "\n".join(lines) or "Ботов пока нет."


# ---------- пользовательские кнопки ----------
WELCOME = "Привет! Я бот-оркестратор. Выбери действие:"


@mgr.message(CommandStart(), F.chat.type == "private")
async def start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(WELCOME, reply_markup=main_menu(m.from_user.id))


@mgr.callback_query(F.data == "menu")
async def cb_menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await show(c, WELCOME, main_menu(c.from_user.id))


@mgr.callback_query(F.data == "help")
async def cb_help(c: CallbackQuery):
    await show(c, "📖 Инструкция\n\n" + cfg["instruction"], kb([BACK]))


@mgr.callback_query(F.data == "support")
async def cb_support(c: CallbackQuery):
    rows = []
    link = support_link(cfg["support"])
    if link:
        rows.append([("✉️ Написать", "url:" + link)])
    rows.append(BACK)
    await show(c, "🛟 Поддержка\n\n" + cfg["support"], kb(rows))


@mgr.callback_query(F.data == "add")
async def cb_add(c: CallbackQuery, state: FSMContext):
    await state.set_state(AddBot.username)
    await show(c, "Пришли @username бота (должен заканчиваться на bot).", kb([[("Отмена", "menu")]]))


@mgr.message(StateFilter(AddBot.username), F.chat.type == "private")
async def add_username(m: Message, state: FSMContext):
    u = norm_username(m.text or "")
    if not u:
        await m.answer("Не похоже на username бота. Пример: @my_worker_bot")
        return
    await state.update_data(username=u)
    await state.set_state(AddBot.url)
    await m.answer("Теперь URL его сервиса на Render, например https://my-worker.onrender.com")


@mgr.message(StateFilter(AddBot.url), F.chat.type == "private")
async def add_url(m: Message, state: FSMContext):
    url = norm_url(m.text or "")
    if not url:
        await m.answer("Нужен адрес вида https://name.onrender.com (без пути).")
        return
    u = (await state.get_data())["username"]
    await state.clear()
    uid = m.from_user.id
    if is_admin(uid):
        await add_target(u, url)
        await m.answer(f"✅ @{u} добавлен.", reply_markup=main_menu(uid))
        return
    if any(t["username"].lower() == u.lower() for t in cfg["targets"]):
        await m.answer("Этот бот уже в списке.", reply_markup=main_menu(uid))
        return
    cfg["pending"][u] = {"url": url, "from": uid}
    await save_cfg()
    await notify_admins(f"📥 Заявка: @{u}\n{url}\nОт: {m.from_user.full_name} ({uid})",
                        kb([[("✅ Принять", f"ap:{u}"), ("❌ Отклонить", f"rj:{u}")]]))
    await m.answer("Заявка отправлена админу ✅", reply_markup=main_menu(uid))


# pong от воркеров только фиксируем, ответа НЕТ -> цикл невозможен
@mgr.message(F.from_user.is_bot, F.text == "pong")
async def on_pong(m: Message):
    name = (m.from_user.username or "").lower()
    for k in state:
        if k.lower() == name:
            state[k]["tg_pong"] = time.time()


# ---------- админка ----------
admin.callback_query.filter(lambda c: is_admin(c.from_user.id))
admin.message.filter(lambda m: m.from_user is not None and is_admin(m.from_user.id))


@admin.message(Command("admin"), F.chat.type == "private")
async def cmd_admin(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("⚙️ Админка", reply_markup=admin_kb())


@admin.callback_query(F.data == "adm")
async def cb_adm(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await show(c, "⚙️ Админка", admin_kb())


@admin.callback_query(F.data == "a:status")
async def a_status(c: CallbackQuery):
    await show(c, "📊 Статус\n\n" + report(), kb([BACK_ADM]))


@admin.callback_query(F.data == "a:http")
async def a_http(c: CallbackQuery):
    await c.answer("Пингую по HTTP, до минуты…")
    await http_ping_all()
    await show(c, "🌐 HTTP-пинг\n\n" + report(), kb([BACK_ADM]))


@admin.callback_query(F.data == "a:tg")
async def a_tg(c: CallbackQuery):
    if not cfg["group_id"]:
        await show(c, "Группа не задана. Добавь меня в группу и напиши там /setgroup.", kb([BACK_ADM]))
        return
    await c.answer("Бужу и пингую, ~1–2 минуты…")
    await tg_ping_all(cfg["group_id"])
    await show(c, "📨 Telegram-пинг\n\n" + report(), kb([BACK_ADM]))


@admin.callback_query(F.data == "a:bots")
async def a_bots(c: CallbackQuery):
    rows = [[(f"🗑 @{t['username']}", f"d:{t['username']}")] for t in cfg["targets"]]
    rows.append([("➕ Добавить", "add")])
    rows.append(BACK_ADM)
    text = "\n".join(f"@{t['username']} — {t['url']}" for t in cfg["targets"]) or "Ботов пока нет."
    await show(c, "🤖 Боты (нажми, чтобы удалить)\n\n" + text, kb(rows))


@admin.callback_query(F.data.startswith("d:"))
async def a_del(c: CallbackQuery):
    u = c.data[2:]
    cfg["targets"] = [t for t in cfg["targets"] if t["username"] != u]
    sync_state()
    await save_cfg()
    await a_bots(c)


@admin.callback_query(F.data == "a:pend")
async def a_pend(c: CallbackQuery):
    rows = []
    for u in list(cfg["pending"])[:10]:
        rows.append([(f"✅ @{u}", f"ap:{u}"), (f"❌ @{u}", f"rj:{u}")])
    rows.append(BACK_ADM)
    text = "\n".join(f"@{u} — {p['url']}" for u, p in cfg["pending"].items()) or "Заявок нет."
    await show(c, "📥 Заявки\n\n" + text, kb(rows))


@admin.callback_query(F.data.startswith("ap:") | F.data.startswith("rj:"))
async def a_decide(c: CallbackQuery):
    act, u = c.data.split(":", 1)
    p = cfg["pending"].get(u)
    if not p:
        await show(c, "Заявка уже обработана.", kb([BACK_ADM]))
        return
    if act == "ap":
        await add_target(u, p["url"])
    else:
        cfg["pending"].pop(u, None)
        await save_cfg()
    try:
        await bot.send_message(p["from"], f"Заявка на @{u}: {'принята ✅' if act == 'ap' else 'отклонена ❌'}")
    except Exception:
        pass
    await show(c, f"{'✅ Принят' if act == 'ap' else '❌ Отклонён'} @{u}", kb([BACK_ADM]))


@admin.callback_query(F.data == "a:group")
async def a_group(c: CallbackQuery):
    await show(c, f"💬 Группа: {cfg['group_id'] or 'не задана'}\n\n"
                  "Чтобы сменить: добавь меня в группу и напиши там /setgroup.", kb([BACK_ADM]))


@admin.message(Command("setgroup"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_setgroup(m: Message):
    cfg["group_id"] = m.chat.id
    await save_cfg()
    await say(m.chat.id, "✅ Группа сохранена.")


@admin.callback_query(F.data == "a:auto")
async def a_auto(c: CallbackQuery):
    cfg["auto"] = not cfg["auto"]
    await save_cfg()
    await show(c, "⚙️ Админка", admin_kb())


@admin.callback_query(F.data == "a:admins")
async def a_admins(c: CallbackQuery):
    text = "👥 Админы\nВладелец: " + str(OWNER_ID) + "\nДоп.: " + (", ".join(map(str, cfg["admins"])) or "нет")
    await show(c, text, kb([[("➕/➖ Изменить", "s:admins")], BACK_ADM]))


PROMPTS = {
    "instruction": "Пришли новый текст инструкции.",
    "support": "Пришли контакт поддержки: @username или ссылку https://…",
    "interval": "Интервал авто-HTTP-пинга, секунд (минимум 60, рекомендую 600).",
    "min_gap": "Пауза между сообщениями бота в группу, секунд (минимум 2).",
    "admins": "Пришли Telegram ID: добавлю в админы или уберу, если уже там.",
}


@admin.callback_query(F.data.startswith("s:"))
async def a_set(c: CallbackQuery, state: FSMContext):
    key = c.data[2:]
    await state.set_state(SetVal.value)
    await state.update_data(key=key)
    cur = cfg[key] if key != "admins" else (cfg["admins"] or "нет")
    await show(c, f"{PROMPTS[key]}\n\nСейчас: {cur}", kb([[("Отмена", "adm")]]))


@admin.message(StateFilter(SetVal.value), F.chat.type == "private")
async def got_val(m: Message, state: FSMContext):
    key = (await state.get_data())["key"]
    t = (m.text or "").strip()
    try:
        if key in ("instruction", "support"):
            if not t or len(t) > 3500:
                raise ValueError
            cfg[key] = t
        elif key == "interval":
            v = int(t)
            if v < 60:
                raise ValueError
            cfg[key] = v
        elif key == "min_gap":
            v = float(t.replace(",", "."))
            if v < 2:
                raise ValueError
            cfg[key] = v
        elif key == "admins":
            uid = int(t)
            if uid == OWNER_ID:
                raise ValueError
            if uid in cfg["admins"]:
                cfg["admins"].remove(uid)
            else:
                cfg["admins"].append(uid)
    except ValueError:
        await m.answer("Неверное значение, попробуй ещё раз.")
        return
    await save_cfg()
    await state.clear()
    await m.answer("✅ Сохранено", reply_markup=admin_kb())


# команды в группе (для админов)
@admin.message(Command("status"))
async def cmd_status(m: Message):
    await say(m.chat.id, report())


@admin.message(Command("http"))
async def cmd_http(m: Message):
    await say(m.chat.id, "HTTP-пинг (до ~60с)…")
    await http_ping_all()
    await say(m.chat.id, report())


@admin.message(Command("tg"))
async def cmd_tg(m: Message):
    await say(m.chat.id, "Бужу сервисы и пингую…")
    await tg_ping_all(m.chat.id)
    await say(m.chat.id, report())


if ROLE == "manager":
    dp.include_router(mgr)


# ---------- приложение ----------
async def health(_):
    return web.Response(text="ok")


async def background(app: web.Application):
    async def loop():
        while True:
            await asyncio.sleep(max(60, int(cfg["interval"])))
            if cfg["auto"] and cfg["targets"]:
                try:
                    await http_ping_all()
                except Exception:
                    log.exception("bg ping")

    task = asyncio.create_task(loop()) if ROLE == "manager" else None
    yield
    if task:
        task.cancel()


async def on_startup(_):
    if ROLE == "manager":
        await load_cfg()
    await bot.set_webhook(f"{BASE_URL}/webhook", secret_token=SECRET,
                          allowed_updates=["message", "callback_query"],
                          drop_pending_updates=True)


def main():
    app = web.Application()
    app.router.add_get("/health", health)
    SimpleRequestHandler(dp, bot, secret_token=SECRET).register(app, path="/webhook")
    setup_application(app, dp, bot=bot)
    app.cleanup_ctx.append(background)
    app.on_startup.append(on_startup)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
