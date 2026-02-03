import os
import re
import asyncio
import logging
import sqlite3
from datetime import datetime
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties

# =========================
# CONFIG (Render Environment Variables)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Укажите BOT_TOKEN (через переменную окружения BOT_TOKEN)")

MANAGER_ID_RAW = os.getenv("MANAGER_ID", "").strip()
if not MANAGER_ID_RAW.isdigit():
    raise RuntimeError("Укажите MANAGER_ID (цифрами) через переменную окружения MANAGER_ID")
MANAGER_ID = int(MANAGER_ID_RAW)

CHANNEL = os.getenv("CHANNEL", "zaryco_official").strip().lstrip("@")
PHONE = os.getenv("PHONE", "+998771202255").strip()

PORT = int(os.getenv("PORT", "10000"))

logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# =========================
# DB (leads)
# =========================
DB_PATH = "leads.sqlite3"

def db_init():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            lang TEXT NOT NULL,
            role TEXT NOT NULL,
            product TEXT NOT NULL,
            qty TEXT NOT NULL,
            city TEXT NOT NULL,
            phone TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT
        )
        """)
        con.commit()

def db_add_lead(data: dict):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        INSERT INTO leads (created_at, lang, role, product, qty, city, phone, user_id, username, full_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(timespec="seconds"),
            data["lang"],
            data["role"],
            data["product"],
            data["qty"],
            data["city"],
            data["phone"],
            data["user_id"],
            data.get("username"),
            data.get("full_name"),
        ))
        con.commit()

def db_last_leads(limit: int = 10):
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("""
        SELECT created_at, role, product, qty, city, phone, user_id, username, full_name
        FROM leads
        ORDER BY id DESC
        LIMIT ?
        """, (limit,))
        return cur.fetchall()

# =========================
# TEXTS (RU / UZ)
# =========================
TXT = {
    "ru": {
        "welcome": "🤝 <b>ZARY & CO ОПТ</b>\nРаботаем с магазинами и маркетплейсами.\nПолучите каталог и условия 👇",
        "menu_hint": "Выберите пункт меню ниже 👇",
        "manager": f"📞 <b>Менеджер оптового отдела</b>\nТелефон: <b>{PHONE}</b>",
        "channel": lambda: f"📣 <b>Все коллекции в канале</b>:\nhttps://t.me/{CHANNEL}",
        "why": "⭐ <b>Почему выгодно работать с нами</b>:\n• Национальный бренд\n• Стабильные поставки\n• Высокая маржа\n• Востребованные модели",
        "terms": "🧾 <b>Условия опта</b>:\n• Работаем по предзаказу\n• Доставка по Узбекистану\n• Индивидуальные условия для партнёров",
        "catalog": lambda: f"📸 <b>Каталог публикуем в канале</b>:\nhttps://t.me/{CHANNEL}",
        "min_order": "📦 <b>Минимальный заказ</b> уточняет менеджер.\n\n👉 Нажмите «🤝 Оставить заявку», чтобы оформить запрос.",
        "min_order_btn": "🤝 Оставить заявку",
        "choose_lang": "Выберите язык / Tilni tanlang:",
        "lang_saved": "✅ Язык сохранён.",
        "who_are_you": "Кто вы?",
        "what_order": "Что хотите заказать?\nЕсли нет в списке — напишите текстом.",
        "how_many": "Сколько штук?",
        "city": "Город доставки?",
        "phone": "Телефон:\n(лучше нажмите кнопку «📲 Отправить контакт»)",
        "bad_phone": "❗ Пожалуйста, введите корректный номер телефона.\nПример: +998901234567",
        "thanks": lambda: f"✅ <b>Спасибо, что выбрали нас!</b>\nМенеджер свяжется с вами в течение <b>15 минут</b> для уточнения деталей заказа.\n\n📣 Канал с коллекциями 👉 https://t.me/{CHANNEL}",
        "sent_manager": "🛎 <b>Новая оптовая заявка</b>",
        "manager_cant_msg": "⚠️ <b>Не смог отправить менеджеру.</b>\nМенеджер должен 1 раз открыть бота и нажать /start, чтобы получать уведомления.",
        "cancelled": "❌ Отменено. Возвращаю в меню.",
        "back_to_menu": "⬅️ Вернуться в меню",
        "history": "📋 Последние заявки (только админ):",
        "no_history": "Пока заявок нет.",
    },
    "uz": {
        "welcome": "🤝 <b>ZARY & CO ULGURJI</b>\nDo‘konlar va marketplace bilan ishlaymiz.\nKatalog va shartlarni oling 👇",
        "menu_hint": "Pastdagi menyudan tanlang 👇",
        "manager": f"📞 <b>Ulgurji bo‘lim menejeri</b>\nTelefon: <b>{PHONE}</b>",
        "channel": lambda: f"📣 <b>Barcha kolleksiyalar kanalimizda</b>:\nhttps://t.me/{CHANNEL}",
        "why": "⭐ <b>Nega biz</b>:\n• Milliy brend\n• Barqaror yetkazib berish\n• Yaxshi marja\n• Talab yuqori modellari",
        "terms": "🧾 <b>Ulgurji shartlar</b>:\n• Oldindan buyurtma\n• O‘zbekiston bo‘ylab yetkazib berish\n• Hamkorlar uchun individual shartlar",
        "catalog": lambda: f"📸 <b>Katalog kanalimizda</b>:\nhttps://t.me/{CHANNEL}",
        "min_order": "📦 <b>Minimal buyurtma</b> miqdorini menejer aytadi.\n\n👉 So‘rov qoldirish uchun «🤝 Ariza qoldirish» ni bosing.",
        "min_order_btn": "🤝 Ariza qoldirish",
        "choose_lang": "Выберите язык / Tilni tanlang:",
        "lang_saved": "✅ Til saqlandi.",
        "who_are_you": "Siz kimsiz?",
        "what_order": "Nima buyurtma qilmoqchisiz?\nAgar ro‘yxatda bo‘lmasa — matn bilan yozing.",
        "how_many": "Nechta dona?",
        "city": "Yetkazib berish shahri?",
        "phone": "Telefon:\n(«📲 Kontakt yuborish» tugmasini bosing)",
        "bad_phone": "❗ Iltimos, telefon raqamini to‘g‘ri kiriting.\nMisol: +998901234567",
        "thanks": lambda: f"✅ <b>Rahmat!</b>\nMenejer <b>15 daqiqa</b> ichida siz bilan bog‘lanadi va tafsilotlarni aniqlaydi.\n\n📣 Kolleksiyalar kanalda 👉 https://t.me/{CHANNEL}",
        "sent_manager": "🛎 <b>Yangi ulgurji ariza</b>",
        "manager_cant_msg": "⚠️ <b>Menejerga yuborib bo‘lmadi.</b>\nMenejer 1 marta botga kirib /start bosishi kerak.",
        "cancelled": "❌ Bekor qilindi. Menyuga qaytaman.",
        "back_to_menu": "⬅️ Menyuga qaytish",
        "history": "📋 Oxirgi arizalar (faqat admin):",
        "no_history": "Hozircha ariza yo‘q.",
    }
}

# =========================
# HELPERS
# =========================
def normalize_phone(s: str) -> str:
    s = s.strip()
    # keep digits and plus
    s = re.sub(r"[^\d+]", "", s)
    # if starts with 998... without plus
    if s.startswith("998") and not s.startswith("+"):
        s = "+" + s
    return s

def is_valid_phone(s: str) -> bool:
    s = normalize_phone(s)
    # Accept +998XXXXXXXXX (12-13 chars) and some general +digits length 9..15
    if s.startswith("+998"):
        return len(re.sub(r"\D", "", s)) == 12  # 998 + 9 digits
    digits = re.sub(r"\D", "", s)
    return 9 <= len(digits) <= 15

def main_menu(lang: str) -> ReplyKeyboardMarkup:
    # same buttons layout, bilingual labels
    if lang == "uz":
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📦 Katalog"), KeyboardButton(text="🧾 Shartlar")],
                [KeyboardButton(text="⭐ Nega biz"), KeyboardButton(text="📦 Minimal buyurtma")],
                [KeyboardButton(text="🤝 Ariza qoldirish")],
                [KeyboardButton(text="📞 Menejer"), KeyboardButton(text="📣 Kanal")],
            ],
            resize_keyboard=True
        )
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Каталог"), KeyboardButton(text="🧾 Условия")],
            [KeyboardButton(text="⭐ Почему мы"), KeyboardButton(text="📦 Минимальный заказ")],
            [KeyboardButton(text="🤝 Оставить заявку")],
            [KeyboardButton(text="📞 Менеджер"), KeyboardButton(text="📣 Канал")],
        ],
        resize_keyboard=True
    )

def lang_from_state(data: dict) -> str:
    return data.get("lang", "ru")

def lang_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru"),
         InlineKeyboardButton(text="🇺🇿 O‘zbekcha", callback_data="lang:uz")]
    ])

def cancel_kb(lang: str) -> ReplyKeyboardMarkup:
    if lang == "uz":
        return ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Bekor qilish")]],
            resize_keyboard=True
        )
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def back_cancel_kb(lang: str) -> ReplyKeyboardMarkup:
    if lang == "uz":
        return ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Orqaga"), KeyboardButton(text="❌ Bekor qilish")]],
            resize_keyboard=True
        )
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def min_order_inline(lang: str) -> InlineKeyboardMarkup:
    txt_btn = TXT[lang]["min_order_btn"]
    cb = "start_form"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=txt_btn, callback_data=cb)]])

# =========================
# STATES
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
async def start(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data(lang="ru")  # default until choose
    await message.answer(TXT["ru"]["choose_lang"], reply_markup=lang_kb())

@dp.callback_query(F.data.startswith("lang:"))
async def set_lang(call: CallbackQuery, state: FSMContext):
    lang = call.data.split(":", 1)[1]
    if lang not in ("ru", "uz"):
        lang = "ru"
    await state.update_data(lang=lang)
    await call.message.answer(TXT[lang]["lang_saved"], reply_markup=main_menu(lang))
    await call.message.answer(TXT[lang]["welcome"])
    await call.answer()

# =========================
# ADMIN COMMAND (optional)
# =========================
@dp.message(Command("leads"))
async def leads_cmd(message: Message, state: FSMContext):
    if message.from_user.id != MANAGER_ID:
        return
    data = await state.get_data()
    lang = lang_from_state(data)
    rows = db_last_leads(10)
    if not rows:
        await message.answer(TXT[lang]["no_history"])
        return
    text = [TXT[lang]["history"]]
    for r in rows:
        created_at, role, product, qty, city, phone, user_id, username, full_name = r
        who = full_name or ""
        if username:
            who += f" (@{username})"
        text.append(
            f"\n• <b>{created_at}</b>\n"
            f"  Тип: {role}\n"
            f"  Товар: {product}\n"
            f"  Объём: {qty}\n"
            f"  Город: {city}\n"
            f"  Тел: {phone}\n"
            f"  Клиент: {who} | id:{user_id}"
        )
    await message.answer("\n".join(text))

# =========================
# MENU BUTTONS (RU / UZ)
# =========================
def is_ru_text(t: str) -> bool:
    return t in {"📦 Каталог", "🧾 Условия", "⭐ Почему мы", "📦 Минимальный заказ", "🤝 Оставить заявку", "📞 Менеджер", "📣 Канал"}

def is_uz_text(t: str) -> bool:
    return t in {"📦 Katalog", "🧾 Shartlar", "⭐ Nega biz", "📦 Minimal buyurtma", "🤝 Ariza qoldirish", "📞 Menejer", "📣 Kanal"}

async def get_lang_for_message(message: Message, state: FSMContext) -> str:
    data = await state.get_data()
    lang = data.get("lang")
    if lang in ("ru", "uz"):
        return lang
    # fallback by button text
    if is_uz_text(message.text or ""):
        return "uz"
    return "ru"

@dp.message(F.text.in_({"📞 Менеджер", "📞 Menejer"}))
async def manager(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await message.answer(TXT[lang]["manager"])

@dp.message(F.text.in_({"📣 Канал", "📣 Kanal"}))
async def channel(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await message.answer(TXT[lang]["channel"]())

@dp.message(F.text.in_({"⭐ Почему мы", "⭐ Nega biz"}))
async def why(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await message.answer(TXT[lang]["why"])

@dp.message(F.text.in_({"📦 Минимальный заказ", "📦 Minimal buyurtma"}))
async def min_order(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await message.answer(TXT[lang]["min_order"], reply_markup=min_order_inline(lang))

@dp.message(F.text.in_({"📦 Каталог", "📦 Katalog"}))
async def catalog(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await message.answer(TXT[lang]["catalog"]())

@dp.message(F.text.in_({"🧾 Условия", "🧾 Shartlar"}))
async def terms(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await message.answer(TXT[lang]["terms"])

# =========================
# FORM START (button + inline)
# =========================
@dp.callback_query(F.data == "start_form")
async def start_form_inline(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)
    await call.answer()
    await form_start_common(call.message, state, lang)

@dp.message(F.text.in_({"🤝 Оставить заявку", "🤝 Ariza qoldirish"}))
async def form_start(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await form_start_common(message, state, lang)

async def form_start_common(message: Message, state: FSMContext, lang: str):
    await state.set_state(Form.role)
    await state.update_data(lang=lang)

    if lang == "uz":
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Butik"), KeyboardButton(text="Do‘kon")],
                [KeyboardButton(text="Marketplace"), KeyboardButton(text="Boshqa")],
                [KeyboardButton(text="❌ Bekor qilish")]
            ],
            resize_keyboard=True
        )
        await message.answer(TXT[lang]["who_are_you"], reply_markup=kb)
    else:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Бутик"), KeyboardButton(text="Магазин")],
                [KeyboardButton(text="Маркетплейс"), KeyboardButton(text="Другое")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
        await message.answer(TXT[lang]["who_are_you"], reply_markup=kb)

# =========================
# CANCEL / BACK (works in all states)
# =========================
@dp.message(F.text.in_({"❌ Отмена", "❌ Bekor qilish"}))
async def cancel_any(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)
    await state.clear()
    await message.answer(TXT[lang]["cancelled"], reply_markup=main_menu(lang))

@dp.message(F.text.in_({"⬅️ Назад", "⬅️ Orqaga"}))
async def back_any(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)
    current = await state.get_state()

    # Back transitions
    if current == Form.product.state:
        await state.set_state(Form.role)
        await form_start_common(message, state, lang)
        return
    if current == Form.qty.state:
        await state.set_state(Form.product)
        await ask_product(message, state, lang)
        return
    if current == Form.city.state:
        await state.set_state(Form.qty)
        await ask_qty(message, state, lang)
        return
    if current == Form.phone.state:
        await state.set_state(Form.city)
        await message.answer(TXT[lang]["city"], reply_markup=back_cancel_kb(lang))
        return

    # If not in form states, just show menu
    await message.answer(TXT[lang]["menu_hint"], reply_markup=main_menu(lang))

# =========================
# FORM HANDLERS
# =========================
@dp.message(Form.role)
async def form_role(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)

    role = (message.text or "").strip()
    if not role or role in {"❌ Отмена", "❌ Bekor qilish", "⬅️ Назад", "⬅️ Orqaga"}:
        return

    await state.update_data(role=role)
    await state.set_state(Form.product)
    await ask_product(message, state, lang)

async def ask_product(message: Message, state: FSMContext, lang: str):
    if lang == "uz":
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Hudi"), KeyboardButton(text="Shim")],
                [KeyboardButton(text="Maktab formasi"), KeyboardButton(text="Kostyum")],
                [KeyboardButton(text="Pijama"), KeyboardButton(text="Boshqa")],
                [KeyboardButton(text="⬅️ Orqaga"), KeyboardButton(text="❌ Bekor qilish")]
            ],
            resize_keyboard=True
        )
    else:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Худи"), KeyboardButton(text="Брюки")],
                [KeyboardButton(text="Школьная форма"), KeyboardButton(text="Костюм")],
                [KeyboardButton(text="Пижама"), KeyboardButton(text="Другое")],
                [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    await message.answer(TXT[lang]["what_order"], reply_markup=kb)

@dp.message(Form.product)
async def form_product(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)

    product = (message.text or "").strip()
    if not product or product in {"⬅️ Назад", "⬅️ Orqaga", "❌ Отмена", "❌ Bekor qilish"}:
        return

    await state.update_data(product=product)
    await state.set_state(Form.qty)
    await ask_qty(message, state, lang)

async def ask_qty(message: Message, state: FSMContext, lang: str):
    if lang == "uz":
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="20–50"), KeyboardButton(text="50–100")],
                [KeyboardButton(text="100–300"), KeyboardButton(text="300+")],
                [KeyboardButton(text="⬅️ Orqaga"), KeyboardButton(text="❌ Bekor qilish")]
            ],
            resize_keyboard=True
        )
    else:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="20–50"), KeyboardButton(text="50–100")],
                [KeyboardButton(text="100–300"), KeyboardButton(text="300+")],
                [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    await message.answer(TXT[lang]["how_many"], reply_markup=kb)

@dp.message(Form.qty)
async def form_qty(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)

    qty = (message.text or "").strip()
    if not qty or qty in {"⬅️ Назад", "⬅️ Orqaga", "❌ Отмена", "❌ Bekor qilish"}:
        return

    await state.update_data(qty=qty)
    await state.set_state(Form.city)
    await message.answer(TXT[lang]["city"], reply_markup=back_cancel_kb(lang))

@dp.message(Form.city)
async def form_city(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)

    city = (message.text or "").strip()
    if not city or city in {"⬅️ Назад", "⬅️ Orqaga", "❌ Отмена", "❌ Bekor qilish"}:
        return

    await state.update_data(city=city)

    if lang == "uz":
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📲 Kontakt yuborish", request_contact=True)],
                [KeyboardButton(text="⬅️ Orqaga"), KeyboardButton(text="❌ Bekor qilish")]
            ],
            resize_keyboard=True
        )
    else:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📲 Отправить контакт", request_contact=True)],
                [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )

    await state.set_state(Form.phone)
    await message.answer(TXT[lang]["phone"], reply_markup=kb)

@dp.message(Form.phone)
async def form_phone(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = lang_from_state(data)

    # Get phone from contact or text
    raw_phone = ""
    if message.contact and message.contact.phone_number:
        raw_phone = message.contact.phone_number
    else:
        raw_phone = (message.text or "").strip()

    phone = normalize_phone(raw_phone)

    if not is_valid_phone(phone):
        await message.answer(TXT[lang]["bad_phone"])
        return

    # Collect lead
    user = message.from_user
    lead = {
        "lang": lang,
        "role": data.get("role", ""),
        "product": data.get("product", ""),
        "qty": data.get("qty", ""),
        "city": data.get("city", ""),
        "phone": phone,
        "user_id": user.id,
        "username": user.username,
        "full_name": (user.full_name or "").strip(),
    }

    # Save to DB
    try:
        db_add_lead(lead)
    except Exception:
        logging.exception("DB insert error")

    # Send to manager
    text = (
        f"{TXT[lang]['sent_manager']}\n\n"
        f"Тип/Role: <b>{lead['role']}</b>\n"
        f"Товар/Product: <b>{lead['product']}</b>\n"
        f"Объём/Qty: <b>{lead['qty']}</b>\n"
        f"Город/City: <b>{lead['city']}</b>\n"
        f"Телефон: <b>{lead['phone']}</b>\n\n"
        f"Клиент: <b>{lead['full_name']}</b>"
        + (f" (@{lead['username']})" if lead.get("username") else "")
        + f"\nID: <code>{lead['user_id']}</code>"
    )

    sent_ok = True
    try:
        await bot.send_message(MANAGER_ID, text)
    except Exception:
        sent_ok = False
        logging.exception("Failed to send to manager")

    # Reply to client (always)
    await message.answer(TXT[lang]["thanks"](), reply_markup=main_menu(lang))
    if not sent_ok:
        # show client nothing about manager; but we can notify manager requirement in logs
        await bot.send_message(
            chat_id=MANAGER_ID,
            text=TXT[lang]["manager_cant_msg"]
        ) if False else None  # disabled to avoid loops

    await state.clear()

# =========================
# FALLBACK: unknown messages -> show menu
# =========================
@dp.message()
async def fallback(message: Message, state: FSMContext):
    lang = await get_lang_for_message(message, state)
    await message.answer(TXT[lang]["menu_hint"], reply_markup=main_menu(lang))

# =========================
# AIOHTTP HEALTH SERVER
# =========================
async def health(request):
    return web.Response(text="ok", content_type="text/plain")

async def root(request):
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
    await start_web_server()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
