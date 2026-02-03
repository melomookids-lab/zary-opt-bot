import os
import re
import asyncio
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types.input_file import FSInputFile

from openpyxl import Workbook


# =========================
# CONFIG (Render Env Vars)
# =========================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
MANAGER_ID_RAW = (os.getenv("MANAGER_ID") or "").strip()

if not BOT_TOKEN:
    raise RuntimeError("Укажите BOT_TOKEN (Render → Environment Variables)")
if not MANAGER_ID_RAW.isdigit():
    raise RuntimeError("Укажите MANAGER_ID (цифрами) (Render → Environment Variables)")
MANAGER_ID = int(MANAGER_ID_RAW)

CHANNEL = (os.getenv("CHANNEL") or "zaryco_official").strip().lstrip("@")
PHONE = (os.getenv("PHONE") or "+998771202255").strip()
PORT = int((os.getenv("PORT") or "10000").strip())

DB_PATH = "leads.sqlite3"

logging.basicConfig(level=logging.INFO)

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()


# =========================
# DB
# =========================
def db_init():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            lang TEXT NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            lang TEXT NOT NULL,
            role TEXT NOT NULL,
            product TEXT NOT NULL,
            qty TEXT NOT NULL,
            city TEXT NOT NULL,
            phone TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new'
        )
        """)
        con.commit()

def db_get_lang(user_id: int) -> str | None:
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("SELECT lang FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row[0] if row else None

def db_set_lang(user_id: int, lang: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO users(user_id, lang) VALUES(?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET lang=excluded.lang",
            (user_id, lang),
        )
        con.commit()

def db_add_lead(lead: dict):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        INSERT INTO leads (
            created_at, user_id, username, full_name, lang, role, product, qty, city, phone, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead["created_at"], lead["user_id"], lead.get("username"), lead.get("full_name"),
            lead["lang"], lead["role"], lead["product"], lead["qty"], lead["city"], lead["phone"],
            lead.get("status", "new"),
        ))
        con.commit()

def db_last_leads(limit: int = 20):
    with sqlite3.connect(DB_PATH) as con:
        return con.execute("""
            SELECT id, created_at, role, product, qty, city, phone, status, user_id, username, full_name, lang
            FROM leads
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

def db_set_status(lead_id: int, status: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE leads SET status=? WHERE id=?", (status, lead_id))
        con.commit()

def db_all_leads():
    with sqlite3.connect(DB_PATH) as con:
        return con.execute("""
            SELECT id, created_at, role, product, qty, city, phone, status, user_id, username, full_name, lang
            FROM leads
            ORDER BY id DESC
        """).fetchall()


# =========================
# LANGUAGE / TEXT
# =========================
def auto_lang_from_telegram(language_code: str | None) -> str:
    code = (language_code or "").lower()
    if code.startswith("uz"):
        return "uz"
    return "ru"

TXT = {
    "ru": {
        "choose_lang": "Выберите язык / Tilni tanlang:",
        "lang_saved": "✅ Язык установлен: Русский",

        "welcome": "🤝 <b>ZARY & CO ОПТ</b>\nРаботаем с магазинами и маркетплейсами.\nПолучите каталог и условия 👇",

        "menu_hint": "Выберите пункт меню 👇",
        "manager": lambda: f"📞 <b>Менеджер оптового отдела</b>\nТелефон: <b>{PHONE}</b>",
        "channel": lambda: f"📣 <b>Все коллекции в канале</b>:\nhttps://t.me/{CHANNEL}",
        "catalog": lambda: f"📸 <b>Каталог публикуем в канале</b>:\nhttps://t.me/{CHANNEL}",
        "terms": "🧾 <b>Условия опта</b>:\n• Работаем по предзаказу\n• Доставка по Узбекистану\n• Индивидуальные условия для партнёров",
        "why": "⭐ <b>Почему выгодно работать с нами</b>:\n• Национальный бренд\n• Стабильные поставки\n• Высокая маржа\n• Востребованные модели",

        "min_text": "📦 <b>Минимальный заказ</b> уточняется у менеджера.\nХотите оформить заявку сейчас?",
        "min_cta": "✅ Оставить заявку",

        "form_role": "Кто вы?",
        "form_product": "Что хотите заказать?\nЕсли нет в списке — напишите текстом.",
        "form_qty": "Сколько штук?",
        "form_city": "Город доставки?",
        "form_phone": "Телефон:\n(лучше нажмите кнопку «📲 Отправить контакт»)",
        "bad_phone": "❗ Введите корректный номер.\nПример: +998901234567",
        "thanks": lambda: (
            "✅ <b>Спасибо, что выбрали нас!</b>\n"
            "Менеджер свяжется с вами в течение <b>15 минут</b> для уточнения деталей заказа.\n\n"
            f"📣 Канал с коллекциями 👉 https://t.me/{CHANNEL}"
        ),
        "cancelled": "❌ Отменено. Возвращаю в меню.",

        "admin_only": "⛔ Только для администратора.",
        "admin_menu": "🛠 <b>Админ меню</b>",
        "admin_last": "📋 <b>Последние заявки</b>",
        "admin_empty": "Пока заявок нет.",
        "admin_export_ok": "✅ Excel сформирован.",
        "admin_export_fail": "❌ Не смог сформировать Excel.",
        "admin_status_updated": "✅ Статус обновлён.",
        "admin_status_bad": "❗ Неверная команда. Пример: /status 15 work",
        "admin_status_hint": "Статусы: new, work, paid, shipped, closed",
    },

    "uz": {
        "choose_lang": "Tilni tanlang / Выберите язык:",
        "lang_saved": "✅ Til o'rnatildi: O'zbekcha",

        "welcome": "🤝 <b>ZARY & CO ULGURJI</b>\nDo‘konlar va marketplace bilan ishlaymiz.\nKatalog va shartlarni oling 👇",

        "menu_hint": "Menyudan tanlang 👇",
        "manager": lambda: f"📞 <b>Ulgurji bo‘lim menejeri</b>\nTelefon: <b>{PHONE}</b>",
        "channel": lambda: f"📣 <b>Barcha kolleksiyalar kanalimizda</b>:\nhttps://t.me/{CHANNEL}",
        "catalog": lambda: f"📸 <b>Katalog kanalimizda</b>:\nhttps://t.me/{CHANNEL}",
        "terms": "🧾 <b>Ulgurji shartlar</b>:\n• Oldindan buyurtma\n• O‘zbekiston bo‘ylab yetkazib berish\n• Hamkorlar uchun individual shartlar",
        "why": "⭐ <b>Nega biz bilan foydali</b>:\n• Milliy brend\n• Barqaror yetkazib berish\n• Yaxshi marja\n• Talab yuqori modellari",

        "min_text": "📦 <b>Minimal buyurtma</b> miqdorini menejer aytadi.\nHozir ariza qoldirasizmi?",
        "min_cta": "✅ Ariza qoldirish",

        "form_role": "Siz kimsiz?",
        "form_product": "Nima buyurtma qilmoqchisiz?\nRo‘yxatda bo‘lmasa — matn bilan yozing.",
        "form_qty": "Nechta dona?",
        "form_city": "Yetkazib berish shahri?",
        "form_phone": "Telefon:\n(yaxshisi «📲 Kontakt yuborish» tugmasini bosing)",
        "bad_phone": "❗ Telefon raqamini to‘g‘ri kiriting.\nMisol: +998901234567",
        "thanks": lambda: (
            "✅ <b>Rahmat!</b>\n"
            "Menejer <b>15 daqiqa</b> ichida bog‘lanadi va tafsilotlarni aniqlaydi.\n\n"
            f"📣 Kanal 👉 https://t.me/{CHANNEL}"
        ),
        "cancelled": "❌ Bekor qilindi. Menyuga qaytdim.",

        "admin_only": "⛔ Faqat admin uchun.",
        "admin_menu": "🛠 <b>Admin menyu</b>",
        "admin_last": "📋 <b>Oxirgi arizalar</b>",
        "admin_empty": "Hozircha ariza yo‘q.",
        "admin_export_ok": "✅ Excel tayyor.",
        "admin_export_fail": "❌ Excel tayyorlab bo‘lmadi.",
        "admin_status_updated": "✅ Status yangilandi.",
        "admin_status_bad": "❗ Noto‘g‘ri buyruq. Misol: /status 15 work",
        "admin_status_hint": "Statuslar: new, work, paid, shipped, closed",
    }
}

# Buttons RU/UZ (menus)
BTN = {
    "ru": {
        "catalog": "📦 Каталог",
        "terms": "🧾 Условия",
        "why": "⭐ Почему мы",
        "min": "📦 Минимальный заказ",
        "leave": "🤝 Оставить заявку",
        "manager": "📞 Менеджер",
        "channel": "📣 Канал",
        "lang": "🌐 Язык",
        "admin": "🛠 Админ",
        "cancel": "❌ Отмена",
        "contact": "📲 Отправить контакт",
    },
    "uz": {
        "catalog": "📦 Katalog",
        "terms": "🧾 Shartlar",
        "why": "⭐ Nega biz",
        "min": "📦 Minimal buyurtma",
        "leave": "🤝 Ariza qoldirish",
        "manager": "📞 Menejer",
        "channel": "📣 Kanal",
        "lang": "🌐 Til",
        "admin": "🛠 Admin",
        "cancel": "❌ Bekor qilish",
        "contact": "📲 Kontakt yuborish",
    }
}


def kb_lang() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🇷🇺 Русский"), KeyboardButton(text="🇺🇿 O'zbekcha")],
        ],
        resize_keyboard=True
    )

def kb_main(lang: str, is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN[lang]["catalog"]), KeyboardButton(text=BTN[lang]["terms"])],
        [KeyboardButton(text=BTN[lang]["why"]), KeyboardButton(text=BTN[lang]["min"])],
        [KeyboardButton(text=BTN[lang]["leave"])],
        [KeyboardButton(text=BTN[lang]["manager"]), KeyboardButton(text=BTN[lang]["channel"])],
        [KeyboardButton(text=BTN[lang]["lang"])],
    ]
    if is_admin:
        rows.append([KeyboardButton(text=BTN[lang]["admin"])])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def kb_min_cta(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=TXT[lang]["min_cta"])],
            [KeyboardButton(text=BTN[lang]["cancel"])],
        ],
        resize_keyboard=True
    )

def kb_form_role(lang: str) -> ReplyKeyboardMarkup:
    if lang == "uz":
        roles = [["Butik", "Do‘kon"], ["Marketplace", "Boshqa"]]
    else:
        roles = [["Бутик", "Магазин"], ["Маркетплейс", "Другое"]]
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=roles[0][0]), KeyboardButton(text=roles[0][1])],
            [KeyboardButton(text=roles[1][0]), KeyboardButton(text=roles[1][1])],
            [KeyboardButton(text=BTN[lang]["cancel"])],
        ],
        resize_keyboard=True
    )

def kb_form_product(lang: str) -> ReplyKeyboardMarkup:
    if lang == "uz":
        products = [["Xudi", "Shim"], ["Maktab formasi", "Kostyum"], ["Pijoma", "Boshqa"]]
        other = "Boshqa"
    else:
        products = [["Худи", "Брюки"], ["Школьная форма", "Костюм"], ["Пижама", "Другое"]]
        other = "Другое"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=products[0][0]), KeyboardButton(text=products[0][1])],
            [KeyboardButton(text=products[1][0]), KeyboardButton(text=products[1][1])],
            [KeyboardButton(text=products[2][0]), KeyboardButton(text=products[2][1])],
            [KeyboardButton(text=BTN[lang]["cancel"])],
        ],
        resize_keyboard=True
    )

def kb_form_qty(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="20–50"), KeyboardButton(text="50–100")],
            [KeyboardButton(text="100–300"), KeyboardButton(text="300+")],
            [KeyboardButton(text=BTN[lang]["cancel"])],
        ],
        resize_keyboard=True
    )

def kb_form_phone(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]["contact"], request_contact=True)],
            [KeyboardButton(text=BTN[lang]["cancel"])],
        ],
        resize_keyboard=True
    )

def kb_admin(lang: str) -> ReplyKeyboardMarkup:
    # Admin menu minimal: last leads + export
    if lang == "uz":
        last_btn = "📋 Oxirgi arizalar"
        export_btn = "📤 Excel"
        hint_btn = "ℹ️ Statuslar"
        back_btn = "⬅️ Menyu"
    else:
        last_btn = "📋 Последние заявки"
        export_btn = "📤 Excel"
        hint_btn = "ℹ️ Статусы"
        back_btn = "⬅️ Меню"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=last_btn), KeyboardButton(text=export_btn)],
            [KeyboardButton(text=hint_btn)],
            [KeyboardButton(text=back_btn)],
        ],
        resize_keyboard=True
    )


# =========================
# HELPERS
# =========================
def get_user_lang(message: Message) -> str:
    stored = db_get_lang(message.from_user.id)
    if stored in ("ru", "uz"):
        return stored
    # first time: auto detect then store
    lang = auto_lang_from_telegram(message.from_user.language_code)
    db_set_lang(message.from_user.id, lang)
    return lang

def is_admin(message: Message) -> bool:
    return message.from_user.id == MANAGER_ID

def normalize_phone(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"[^\d+]", "", s)
    if s.startswith("998") and not s.startswith("+"):
        s = "+" + s
    return s

def is_valid_phone(phone: str) -> bool:
    p = normalize_phone(phone)
    if p.startswith("+998"):
        # 998 + 9 digits
        digits = re.sub(r"\D", "", p)
        return len(digits) == 12
    digits = re.sub(r"\D", "", p)
    return 9 <= len(digits) <= 15


# =========================
# FSM STATES
# =========================
class Form(StatesGroup):
    role = State()
    product = State()
    qty = State()
    city = State()
    phone = State()


# =========================
# START + LANGUAGE
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["welcome"], reply_markup=kb_main(lang, is_admin(message)))
    await message.answer(TXT[lang]["menu_hint"], reply_markup=kb_main(lang, is_admin(message)))

@dp.message(F.text.in_(["🇷🇺 Русский", "🇺🇿 O'zbekcha"]))
async def set_lang(message: Message, state: FSMContext):
    await state.clear()
    lang = "ru" if "Русский" in message.text else "uz"
    db_set_lang(message.from_user.id, lang)
    await message.answer(TXT[lang]["lang_saved"], reply_markup=kb_main(lang, is_admin(message)))
    await message.answer(TXT[lang]["menu_hint"], reply_markup=kb_main(lang, is_admin(message)))

@dp.message(lambda m: m.text in {"🌐 Язык", "🌐 Til"})
async def change_lang(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["choose_lang"], reply_markup=kb_lang())


# =========================
# MENU HANDLERS (RU/UZ)
# =========================
@dp.message(lambda m: m.text in {"📞 Менеджер", "📞 Menejer"})
async def menu_manager(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["manager"](), reply_markup=kb_main(lang, is_admin(message)))

@dp.message(lambda m: m.text in {"📣 Канал", "📣 Kanal"})
async def menu_channel(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["channel"](), reply_markup=kb_main(lang, is_admin(message)))

@dp.message(lambda m: m.text in {"📦 Каталог", "📦 Katalog"})
async def menu_catalog(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["catalog"](), reply_markup=kb_main(lang, is_admin(message)))

@dp.message(lambda m: m.text in {"🧾 Условия", "🧾 Shartlar"})
async def menu_terms(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["terms"], reply_markup=kb_main(lang, is_admin(message)))

@dp.message(lambda m: m.text in {"⭐ Почему мы", "⭐ Nega biz"})
async def menu_why(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["why"], reply_markup=kb_main(lang, is_admin(message)))

@dp.message(lambda m: m.text in {"📦 Минимальный заказ", "📦 Minimal buyurtma"})
async def menu_min_order(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["min_text"], reply_markup=kb_min_cta(lang))


# =========================
# CANCEL (RU/UZ)
# =========================
@dp.message(lambda m: m.text in {"❌ Отмена", "❌ Bekor qilish"})
async def cancel_any(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["cancelled"], reply_markup=kb_main(lang, is_admin(message)))


# =========================
# FORM START
# =========================
@dp.message(lambda m: m.text in {"🤝 Оставить заявку", "🤝 Ariza qoldirish", "✅ Оставить заявку", "✅ Ariza qoldirish"})
async def form_start(message: Message, state: FSMContext):
    lang = get_user_lang(message)
    await state.set_state(Form.role)
    await message.answer(TXT[lang]["form_role"], reply_markup=kb_form_role(lang))

@dp.message(Form.role)
async def form_role(message: Message, state: FSMContext):
    lang = get_user_lang(message)
    role = (message.text or "").strip()
    if role in {"❌ Отмена", "❌ Bekor qilish"}:
        await cancel_any(message, state)
        return
    await state.update_data(role=role)
    await state.set_state(Form.product)
    await message.answer(TXT[lang]["form_product"], reply_markup=kb_form_product(lang))

@dp.message(Form.product)
async def form_product(message: Message, state: FSMContext):
    lang = get_user_lang(message)
    product = (message.text or "").strip()
    if product in {"❌ Отмена", "❌ Bekor qilish"}:
        await cancel_any(message, state)
        return
    await state.update_data(product=product)
    await state.set_state(Form.qty)
    await message.answer(TXT[lang]["form_qty"], reply_markup=kb_form_qty(lang))

@dp.message(Form.qty)
async def form_qty(message: Message, state: FSMContext):
    lang = get_user_lang(message)
    qty = (message.text or "").strip()
    if qty in {"❌ Отмена", "❌ Bekor qilish"}:
        await cancel_any(message, state)
        return
    await state.update_data(qty=qty)
    await state.set_state(Form.city)
    await message.answer(TXT[lang]["form_city"], reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN[lang]["cancel"])]],
        resize_keyboard=True
    ))

@dp.message(Form.city)
async def form_city(message: Message, state: FSMContext):
    lang = get_user_lang(message)
    city = (message.text or "").strip()
    if city in {"❌ Отмена", "❌ Bekor qilish"}:
        await cancel_any(message, state)
        return
    await state.update_data(city=city)
    await state.set_state(Form.phone)
    await message.answer(TXT[lang]["form_phone"], reply_markup=kb_form_phone(lang))

@dp.message(Form.phone)
async def form_phone(message: Message, state: FSMContext):
    lang = get_user_lang(message)

    # Cancel
    if (message.text or "").strip() in {"❌ Отмена", "❌ Bekor qilish"}:
        await cancel_any(message, state)
        return

    data = await state.get_data()

    # phone from contact or typed
    raw_phone = ""
    if message.contact and message.contact.phone_number:
        raw_phone = message.contact.phone_number
    else:
        raw_phone = (message.text or "").strip()

    phone = normalize_phone(raw_phone)
    if not is_valid_phone(phone):
        await message.answer(TXT[lang]["bad_phone"])
        return

    user = message.from_user

    lead = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
        "lang": lang,
        "role": data.get("role", "-"),
        "product": data.get("product", "-"),
        "qty": data.get("qty", "-"),
        "city": data.get("city", "-"),
        "phone": phone,
        "status": "new",
    }

    # save
    try:
        db_add_lead(lead)
    except Exception:
        logging.exception("DB insert failed")

    # notify manager (bilingual)
    msg_to_manager = (
        "🛎 <b>Новая оптовая заявка / Yangi ulgurji ariza</b>\n\n"
        f"Дата: <b>{lead['created_at']}</b>\n"
        f"Тип/Role: <b>{lead['role']}</b>\n"
        f"Товар/Product: <b>{lead['product']}</b>\n"
        f"Объём/Qty: <b>{lead['qty']}</b>\n"
        f"Город/City: <b>{lead['city']}</b>\n"
        f"Телефон: <b>{lead['phone']}</b>\n\n"
        f"Клиент: <b>{lead['full_name']}</b>"
        + (f" (@{lead['username']})" if lead.get("username") else "")
        + f"\nID: <code>{lead['user_id']}</code>\n"
        f"Статус: <b>new</b>"
    )

    try:
        await bot.send_message(MANAGER_ID, msg_to_manager)
    except Exception:
        # если менеджер не нажал /start или Telegram ограничил — не ломаем клиенту ответ
        logging.exception("Failed to send lead to manager")

    # client thanks + menu
    await message.answer(TXT[lang]["thanks"](), reply_markup=kb_main(lang, is_admin(message)))
    await state.clear()


# =========================
# ADMIN MENU
# =========================
@dp.message(lambda m: m.text in {"🛠 Админ", "🛠 Admin"})
async def admin_menu(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    if not is_admin(message):
        await message.answer(TXT[lang]["admin_only"])
        return
    await message.answer(TXT[lang]["admin_menu"], reply_markup=kb_admin(lang))

@dp.message(lambda m: m.text in {"📋 Последние заявки", "📋 Oxirgi arizalar"})
async def admin_last(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    if not is_admin(message):
        await message.answer(TXT[lang]["admin_only"])
        return

    rows = db_last_leads(20)
    if not rows:
        await message.answer(TXT[lang]["admin_empty"], reply_markup=kb_admin(lang))
        return

    # compact list
    lines = [TXT[lang]["admin_last"]]
    for r in rows:
        lead_id, created_at, role, product, qty, city, phone, status, user_id, username, full_name, llang = r
        uname = f"@{username}" if username else "-"
        lines.append(
            f"\n<b>#{lead_id}</b> | <b>{status}</b> | {created_at}"
            f"\n{role} | {product} | {qty}"
            f"\n{city} | {phone}"
            f"\n{full_name} ({uname}) | id:{user_id} | lang:{llang}"
        )
    lines.append("\n\n/status ID new|work|paid|shipped|closed")
    await message.answer("\n".join(lines), reply_markup=kb_admin(lang))

@dp.message(lambda m: m.text in {"ℹ️ Статусы", "ℹ️ Statuslar"})
async def admin_status_help(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    if not is_admin(message):
        await message.answer(TXT[lang]["admin_only"])
        return
    await message.answer(TXT[lang]["admin_status_hint"], reply_markup=kb_admin(lang))

@dp.message(Command("status"))
async def admin_set_status(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    if not is_admin(message):
        await message.answer(TXT[lang]["admin_only"])
        return

    parts = (message.text or "").split()
    if len(parts) != 3 or not parts[1].isdigit():
        await message.answer(TXT[lang]["admin_status_bad"], reply_markup=kb_admin(lang))
        return

    lead_id = int(parts[1])
    status = parts[2].strip().lower()
    if status not in {"new", "work", "paid", "shipped", "closed"}:
        await message.answer(TXT[lang]["admin_status_bad"], reply_markup=kb_admin(lang))
        return

    try:
        db_set_status(lead_id, status)
        await message.answer(TXT[lang]["admin_status_updated"], reply_markup=kb_admin(lang))
    except Exception:
        logging.exception("Failed to set status")
        await message.answer("❌ Error", reply_markup=kb_admin(lang))

@dp.message(lambda m: m.text == "📤 Excel")
async def admin_export_excel(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    if not is_admin(message):
        await message.answer(TXT[lang]["admin_only"])
        return

    try:
        rows = db_all_leads()
        wb = Workbook()
        ws = wb.active
        ws.title = "Leads"

        ws.append([
            "id", "created_at", "role", "product", "qty", "city", "phone",
            "status", "user_id", "username", "full_name", "lang"
        ])

        for r in rows:
            ws.append(list(r))

        out_dir = Path("exports")
        out_dir.mkdir(exist_ok=True)
        filename = out_dir / f"zary_opt_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        wb.save(filename)

        await message.answer(TXT[lang]["admin_export_ok"], reply_markup=kb_admin(lang))
        await bot.send_document(
            chat_id=MANAGER_ID,
            document=FSInputFile(str(filename)),
            caption="📤 ZARY OPT leads.xlsx"
        )
    except Exception:
        logging.exception("Excel export failed")
        await message.answer(TXT[lang]["admin_export_fail"], reply_markup=kb_admin(lang))

@dp.message(lambda m: m.text in {"⬅️ Меню", "⬅️ Menyu"})
async def admin_back_to_menu(message: Message, state: FSMContext):
    await state.clear()
    lang = get_user_lang(message)
    await message.answer(TXT[lang]["menu_hint"], reply_markup=kb_main(lang, is_admin(message)))


# =========================
# SIMPLE AUTO-ANSWERS (FAQ)
# =========================
@dp.message(F.text)
async def auto_answers(message: Message, state: FSMContext):
    # if user is in FSM, ignore (handled by state handlers)
    if await state.get_state():
        return

    lang = get_user_lang(message)
    text = (message.text or "").lower()

    # simple FAQ triggers (optional, safe)
    if any(k in text for k in ["цена", "price", "narx"]):
        await message.answer("💬 По ценам и опту — уточняет менеджер. Нажмите «📞 Менеджер».", reply_markup=kb_main(lang, is_admin(message)))
        return
    if any(k in text for k in ["доставка", "yetkaz", "delivery"]):
        await message.answer("🚚 Доставка по Узбекистану. Для расчёта — оставьте заявку «🤝».", reply_markup=kb_main(lang, is_admin(message)))
        return

    # default
    await message.answer(TXT[lang]["menu_hint"], reply_markup=kb_main(lang, is_admin(message)))


# =========================
# AIOHTTP HEALTH SERVER (Render)
# =========================
async def root(_request):
    return web.Response(text="ok", content_type="text/plain")

async def health(_request):
    return web.Response(text="ok", content_type="text/plain")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", root)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"Web server started on 0.0.0.0:{PORT}")


# =========================
# RUN
# =========================
async def main():
    db_init()

    # убрать webhook и конфликты (если вдруг когда-то включали webhook)
    await bot.delete_webhook(drop_pending_updates=True)

    await start_web_server()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
