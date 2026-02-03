import os
import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from aiohttp import web

# ================= CONFIG =================
TZ = ZoneInfo("Asia/Tashkent")

BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_TOKEN_HERE")
MANAGER_ID = int(os.getenv("MANAGER_ID", "123456789"))
CHANNEL = os.getenv("CHANNEL", "zaryco_official")
PHONE = os.getenv("PHONE", "+998771202255")

# Напоминание менеджеру:
REMIND_AFTER_MIN = int(os.getenv("REMIND_AFTER_MIN", "15"))          # через сколько минут напоминать
REMIND_CHECK_EVERY_SEC = int(os.getenv("REMIND_CHECK_EVERY_SEC", "300"))  # как часто проверять (сек)

DB_PATH = os.getenv("DB_PATH", "orders.db")

# Render выдаёт PORT автоматически — используем его
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN or BOT_TOKEN == "PASTE_TOKEN_HERE":
    raise RuntimeError("Укажите BOT_TOKEN (лучше через переменную окружения BOT_TOKEN)")

logging.basicConfig(level=logging.INFO)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# ================= DATABASE =================

def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    conn = db_connect()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            role TEXT,
            product TEXT,
            qty TEXT,
            city TEXT,
            phone TEXT,
            created INTEGER,
            status TEXT DEFAULT 'open',
            notified INTEGER DEFAULT 0
        )
        """
    )

    conn.commit()
    conn.close()

def now_ts() -> int:
    return int(time.time())

def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def is_manager(user_id: int) -> bool:
    return user_id == MANAGER_ID

# ================= STATES =================

class Form(StatesGroup):
    role = State()
    product = State()
    qty = State()
    city = State()
    phone = State()

# ================= KEYBOARDS =================

# Клиентское меню — БЕЗ корзины/истории (как ты попросил)
menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📦 Каталог"), KeyboardButton(text="🧾 Условия")],
        [KeyboardButton(text="⭐ Почему мы"), KeyboardButton(text="📦 Минимальный заказ")],
        [KeyboardButton(text="🤝 Оставить заявку")],
        [KeyboardButton(text="📞 Менеджер"), KeyboardButton(text="📣 Канал")],
    ],
    resize_keyboard=True,
)

# Админ-меню (видит только MANAGER_ID)
admin_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Все заявки"), KeyboardButton(text="📅 Отчёт за день")],
        [KeyboardButton(text="📤 Экспорт Excel"), KeyboardButton(text="✅ Закрыть заявку")],
        [KeyboardButton(text="↩️ В меню")],
    ],
    resize_keyboard=True,
)

cancel_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="↩️ В меню"), KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
)

def subscribe_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📣 Подписаться на канал", url=f"https://t.me/{CHANNEL}")],
        ]
    )

def contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📲 Отправить контакт", request_contact=True)],
            [KeyboardButton(text="↩️ В меню"), KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )

# ================= WEB SERVER (Render 24/7) =================

async def handle_root(request):
    return web.Response(text="OK")

async def handle_health(request):
    return web.Response(text="healthy")

async def start_web_server():
    """
    Важно для Render: Web Service должен слушать PORT.
    Это держит сервис живым и UptimeRobot сможет пинговать URL.
    """
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"Web server started on 0.0.0.0:{PORT}")

# ================= START / MENU =================

@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()

    if is_manager(message.from_user.id):
        await message.answer("👑 Админ панель", reply_markup=admin_menu)
    else:
        await message.answer(
            "🤝 ZARY & CO ОПТ\n"
            "Работаем с магазинами и маркетплейсами.\n"
            "Получите каталог и условия 👇",
            reply_markup=menu,
        )
        await message.answer("Чтобы не пропускать новинки — подпишитесь 👇", reply_markup=subscribe_kb())

@dp.message(Command("menu"))
@dp.message(F.text == "↩️ В меню")
async def go_menu(message: Message, state: FSMContext):
    await state.clear()
    if is_manager(message.from_user.id):
        await message.answer("👑 Админ панель", reply_markup=admin_menu)
    else:
        await message.answer("Главное меню", reply_markup=menu)

@dp.message(Command("cancel"))
@dp.message(F.text == "❌ Отмена")
async def cancel(message: Message, state: FSMContext):
    await state.clear()
    await go_menu(message, state)

# ================= STATIC BUTTONS =================

@dp.message(F.text == "📞 Менеджер")
async def manager(message: Message):
    await message.answer(f"📞 Менеджер: {PHONE}")

@dp.message(F.text == "📣 Канал")
async def channel(message: Message):
    await message.answer(f"📣 Канал: https://t.me/{CHANNEL}")

@dp.message(F.text == "⭐ Почему мы")
async def why(message: Message):
    await message.answer(
        "⭐ Почему выгодно работать с нами:\n"
        "• Национальный бренд\n"
        "• Стабильные поставки\n"
        "• Высокая маржа\n"
        "• Востребованные модели"
    )

@dp.message(F.text == "📦 Минимальный заказ")
async def min_order(message: Message):
    await message.answer("📦 Минимальный заказ уточняет менеджер")

@dp.message(F.text == "📦 Каталог")
async def catalog(message: Message):
    await message.answer(f"📸 Каталог: https://t.me/{CHANNEL}")

@dp.message(F.text == "🧾 Условия")
async def terms(message: Message):
    await message.answer("🧾 Условия: предзаказ • доставка по Узбекистану")

# ================= FORM (LEAD) =================

@dp.message(F.text == "🤝 Оставить заявку")
async def form_start(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Бутик"), KeyboardButton(text="Магазин")],
            [KeyboardButton(text="Маркетплейс"), KeyboardButton(text="Другое")],
            [KeyboardButton(text="↩️ В меню"), KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )
    await state.set_state(Form.role)
    await message.answer("Кто вы?", reply_markup=kb)

@dp.message(Form.role)
async def form_role(message: Message, state: FSMContext):
    if message.text in ("↩️ В меню", "❌ Отмена"):
        return

    await state.update_data(role=message.text)

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Худи"), KeyboardButton(text="Брюки")],
            [KeyboardButton(text="Школьная форма"), KeyboardButton(text="Костюм")],
            [KeyboardButton(text="Пижама"), KeyboardButton(text="Другое")],
            [KeyboardButton(text="↩️ В меню"), KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )

    await state.set_state(Form.product)
    await message.answer("Что хотите заказать? (если нет в списке — напишите текстом)", reply_markup=kb)

@dp.message(Form.product)
async def form_product(message: Message, state: FSMContext):
    if message.text in ("↩️ В меню", "❌ Отмена"):
        return

    await state.update_data(product=message.text)

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="20–50"), KeyboardButton(text="50–100")],
            [KeyboardButton(text="100–300"), KeyboardButton(text="300+")],
            [KeyboardButton(text="↩️ В меню"), KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )

    await state.set_state(Form.qty)
    await message.answer("Сколько штук?", reply_markup=kb)

@dp.message(Form.qty)
async def form_qty(message: Message, state: FSMContext):
    if message.text in ("↩️ В меню", "❌ Отмена"):
        return

    await state.update_data(qty=message.text)
    await state.set_state(Form.city)
    await message.answer("Город доставки?", reply_markup=cancel_kb)

@dp.message(Form.city)
async def form_city(message: Message, state: FSMContext):
    if message.text in ("↩️ В меню", "❌ Отмена"):
        return

    await state.update_data(city=message.text)
    await state.set_state(Form.phone)

    await message.answer("Телефон? (лучше нажмите кнопку «Отправить контакт»)", reply_markup=contact_kb())

@dp.message(Form.phone)
async def form_phone(message: Message, state: FSMContext):
    if message.text in ("↩️ В меню", "❌ Отмена"):
        return

    data = await state.get_data()

    if message.contact and message.contact.phone_number:
        phone = message.contact.phone_number
    else:
        phone = (message.text or "").strip()

    created = now_ts()

    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (user_id, username, role, product, qty, city, phone, created, status, notified)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 0)
        """,
        (
            message.from_user.id,
            message.from_user.username,
            data.get("role"),
            data.get("product"),
            data.get("qty"),
            data.get("city"),
            phone,
            created,
        ),
    )
    order_id = cur.lastrowid
    conn.commit()
    conn.close()

    text = (
        "🛎 Новая заявка\n\n"
        f"ID: #{order_id}\n"
        f"Тип: {data.get('role','-')}\n"
        f"Товар: {data.get('product','-')}\n"
        f"Объём: {data.get('qty','-')}\n"
        f"Город: {data.get('city','-')}\n"
        f"Телефон: {phone or '-'}\n"
        f"От: @{message.from_user.username or 'без username'} (id: {message.from_user.id})\n"
        f"Дата: {now_str()}"
    )

    await bot.send_message(MANAGER_ID, text)

    await message.answer(
        "✅ Спасибо! Менеджер свяжется с вами.\n"
        f"📣 Канал: https://t.me/{CHANNEL}",
        reply_markup=menu,
    )
    await message.answer("Подпишитесь, чтобы не пропускать новинки 👇", reply_markup=subscribe_kb())

    await state.clear()

# ================= ADMIN =================

@dp.message(F.text == "📊 Все заявки")
async def all_orders(message: Message):
    if not is_manager(message.from_user.id):
        return

    conn = db_connect()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, role, product, qty, city, phone, status, created FROM orders ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()

    if not rows:
        await message.answer("Нет заявок")
        return

    lines = ["📊 Последние 20 заявок:\n"]
    for r in rows:
        dt = datetime.fromtimestamp(int(r["created"]), TZ).strftime("%d.%m %H:%M")
        lines.append(
            f"#{r['id']} | {r['role']} | {r['product']} | {r['qty']} | {r['city']} | {r['phone']} | {r['status']} | {dt}"
        )
    await message.answer("\n".join(lines))

@dp.message(F.text == "📅 Отчёт за день")
async def report_day(message: Message):
    if not is_manager(message.from_user.id):
        return

    since = now_ts() - 86400

    conn = db_connect()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM orders WHERE created > ?", (since,)).fetchone()[0]
    open_cnt = cur.execute("SELECT COUNT(*) FROM orders WHERE created > ? AND status='open'", (since,)).fetchone()[0]
    closed_cnt = cur.execute("SELECT COUNT(*) FROM orders WHERE created > ? AND status='closed'", (since,)).fetchone()[0]
    conn.close()

    await message.answer(
        "📅 Отчёт за 24 часа\n"
        f"Всего заявок: {total}\n"
        f"Открытых: {open_cnt}\n"
        f"Закрытых: {closed_cnt}"
    )

@dp.message(F.text == "✅ Закрыть заявку")
async def close_order_prompt(message: Message):
    if not is_manager(message.from_user.id):
        return
    await message.answer("Напишите ID заявки для закрытия (пример: 15)")

@dp.message(F.text.regexp(r"^\d+$"))
async def close_order_by_id(message: Message):
    if not is_manager(message.from_user.id):
        return

    order_id = int(message.text)

    conn = db_connect()
    cur = conn.cursor()
    cur.execute("UPDATE orders SET status='closed' WHERE id = ?", (order_id,))
    changed = cur.rowcount
    conn.commit()
    conn.close()

    if changed:
        await message.answer(f"✅ Заявка #{order_id} закрыта")
    else:
        await message.answer("Не нашёл такую заявку")

# ================= EXPORT EXCEL (admin) =================

def build_excel_bytes(rows):
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter
    import io

    wb = Workbook()
    ws = wb.active
    ws.title = "orders"

    headers = ["id", "created", "status", "user_id", "username", "role", "product", "qty", "city", "phone"]
    ws.append(headers)

    for r in rows:
        dt = datetime.fromtimestamp(int(r["created"]), TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.append([
            r["id"], dt, r["status"], r["user_id"], r["username"], r["role"],
            r["product"], r["qty"], r["city"], r["phone"]
        ])

    for i, h in enumerate(headers, start=1):
        ws.column_dimensions[get_column_letter(i)].width = max(12, len(h) + 2)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

@dp.message(F.text == "📤 Экспорт Excel")
async def export_excel(message: Message):
    if not is_manager(message.from_user.id):
        return

    since_ts = int((datetime.now(TZ) - timedelta(days=30)).timestamp())

    conn = db_connect()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT id, created, status, user_id, username, role, product, qty, city, phone
        FROM orders
        WHERE created >= ?
        ORDER BY id DESC
        """,
        (since_ts,),
    ).fetchall()
    conn.close()

    if not rows:
        await message.answer("За последние 30 дней заявок нет")
        return

    xlsx_bytes = build_excel_bytes(rows)
    filename = f"zaryco_orders_{datetime.now(TZ).strftime('%Y%m%d_%H%M')}.xlsx"

    await message.answer("Готовлю файл…")
    await bot.send_document(
        chat_id=MANAGER_ID,
        document=BufferedInputFile(xlsx_bytes, filename=filename),
        caption="📤 Экспорт заявок (последние 30 дней)",
    )

# ================= AUTO REMINDER =================

async def reminder_loop():
    while True:
        await asyncio.sleep(REMIND_CHECK_EVERY_SEC)

        limit = now_ts() - (REMIND_AFTER_MIN * 60)

        conn = db_connect()
        cur = conn.cursor()

        rows = cur.execute(
            "SELECT id, role, product, city, phone FROM orders WHERE created < ? AND status='open' AND notified = 0",
            (limit,),
        ).fetchall()

        for r in rows:
            await bot.send_message(
                MANAGER_ID,
                "⏰ Напоминание: есть необработанная заявка\n"
                f"#{r['id']} | {r['role']} | {r['product']} | {r['city']} | {r['phone']}"
            )
            cur.execute("UPDATE orders SET notified=1 WHERE id=?", (r["id"],))

        conn.commit()
        conn.close()

# ================= RUN =================

async def main():
    db_init()
    await start_web_server()                  # <-- это делает 24/7 на Render (Web Service)
    asyncio.create_task(reminder_loop())      # <-- напоминания менеджеру
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
