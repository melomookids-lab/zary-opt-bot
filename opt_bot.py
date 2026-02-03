import os
import asyncio
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

# =========================
# CONFIG (Render Env Vars)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Укажите BOT_TOKEN через переменную окружения BOT_TOKEN")

MANAGER_ID_RAW = os.getenv("MANAGER_ID", "").strip()
if not MANAGER_ID_RAW.isdigit():
    raise RuntimeError("Укажите MANAGER_ID (цифрами) через переменную окружения MANAGER_ID")
MANAGER_ID = int(MANAGER_ID_RAW)

CHANNEL = os.getenv("CHANNEL", "zaryco_official").strip().lstrip("@")
PHONE = os.getenv("PHONE", "+998771202255").strip()

logging.basicConfig(level=logging.INFO)

bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# =========================
# I18N
# =========================
LANG = {}  # user_id -> "ru" / "uz"

def auto_lang(message: Message) -> str:
    # Telegram language_code can be: "ru", "uz", "en" etc.
    code = (message.from_user.language_code or "").lower()
    if code.startswith("uz"):
        return "uz"
    # Default RU for Uzbekistan users often set "ru"
    return "ru"

T = {
    "ru": {
        "choose_lang": "Выберите язык / Tilni tanlang:",
        "lang_set": "✅ Язык установлен: Русский",
        "lang_set_uz": "✅ Til o'rnatildi: O'zbekcha",

        "welcome": "🤝 <b>ZARY & CO ОПТ</b>\nРаботаем с магазинами и маркетплейсами.\nПолучите каталог и условия 👇",
        "menu_title": "Главное меню 👇",

        "btn_catalog": "📦 Каталог",
        "btn_terms": "🧾 Условия",
        "btn_why": "⭐ Почему мы",
        "btn_min": "📦 Минимальный заказ",
        "btn_leave": "🤝 Оставить заявку",
        "btn_manager": "📞 Менеджер",
        "btn_channel": "📣 Канал",
        "btn_lang": "🌐 Язык",
        "btn_cancel": "❌ Отмена",

        "manager": f"📞 Менеджер оптового отдела\nТелефон: <b>{PHONE}</b>",
        "channel": f"📣 Все коллекции в канале:\nhttps://t.me/{CHANNEL}",
        "catalog": f"📸 Каталог публикуем в канале:\nhttps://t.me/{CHANNEL}",
        "terms": "🧾 <b>Условия опта</b>:\n• Работаем по предзаказу\n• Доставка по Узбекистану\n• Индивидуальные условия для партнёров",
        "why": "⭐ <b>Почему выгодно работать с нами</b>:\n• Национальный бренд\n• Стабильные поставки\n• Высокая маржа\n• Востребованные модели",

        "min_text": "📦 Минимальный заказ уточняется у менеджера.\nХотите оформить заявку сейчас?",
        "min_cta": "✅ Оставить заявку",

        "ask_role": "Кто вы?",
        "role_butik": "Бутик",
        "role_shop": "Магазин",
        "role_market": "Маркетплейс",
        "role_other": "Другое",

        "ask_product": "Что хотите заказать?\nЕсли нет в списке — напишите текстом.",
        "prod_hoodie": "Худи",
        "prod_pants": "Брюки",
        "prod_school": "Школьная форма",
        "prod_suit": "Костюм",
        "prod_pajama": "Пижама",
        "prod_other": "Другое",

        "ask_qty": "Сколько штук?",
        "qty_20_50": "20–50",
        "qty_50_100": "50–100",
        "qty_100_300": "100–300",
        "qty_300p": "300+",

        "ask_city": "Город доставки?",
        "ask_phone": "Телефон: (лучше нажмите кнопку «📲 Отправить контакт»)",
        "send_contact": "📲 Отправить контакт",

        "thanks": f"✅ Спасибо! Вы выбрали ZARY & CO.\nМенеджер свяжется с вами в ближайшие <b>15 минут</b> для уточнения деталей заказа.\n\n📣 Новинки и коллекции 👉 https://t.me/{CHANNEL}",
        "cancelled": "❌ Отменено. Возвращаю в меню.",
        "err_phone": "Пожалуйста, отправьте контакт кнопкой или напишите номер текстом.",
    },

    "uz": {
        "choose_lang": "Tilni tanlang / Выберите язык:",
        "lang_set": "✅ Til o'rnatildi: O'zbekcha",
        "lang_set_uz": "✅ Til o'rnatildi: O'zbekcha",

        "welcome": "🤝 <b>ZARY & CO ULGURJI</b>\nDo'konlar va marketplace bilan ishlaymiz.\nKatalog va shartlarni oling 👇",
        "menu_title": "Asosiy menyu 👇",

        "btn_catalog": "📦 Katalog",
        "btn_terms": "🧾 Shartlar",
        "btn_why": "⭐ Nega biz",
        "btn_min": "📦 Minimal buyurtma",
        "btn_leave": "🤝 Ariza qoldirish",
        "btn_manager": "📞 Menejer",
        "btn_channel": "📣 Kanal",
        "btn_lang": "🌐 Til",
        "btn_cancel": "❌ Bekor qilish",

        "manager": f"📞 Ulgurji bo'lim menejeri\nTelefon: <b>{PHONE}</b>",
        "channel": f"📣 Barcha kolleksiyalar kanalda:\nhttps://t.me/{CHANNEL}",
        "catalog": f"📸 Katalog kanalda:\nhttps://t.me/{CHANNEL}",
        "terms": "🧾 <b>Ulgurji shartlar</b>:\n• Oldindan buyurtma\n• O'zbekiston bo'ylab yetkazib berish\n• Hamkorlar uchun individual shartlar",
        "why": "⭐ <b>Nega biz bilan foydali</b>:\n• Milliy brend\n• Barqaror yetkazib berish\n• Yaxshi marja\n• Talab yuqori modellar",

        "min_text": "📦 Minimal buyurtmani menejer aniqlab beradi.\nHozir ariza qoldirasizmi?",
        "min_cta": "✅ Ariza qoldirish",

        "ask_role": "Siz kimsiz?",
        "role_butik": "Butik",
        "role_shop": "Do'kon",
        "role_market": "Marketplace",
        "role_other": "Boshqa",

        "ask_product": "Nima buyurtma qilmoqchisiz?\nRo'yxatda bo'lmasa — matn bilan yozing.",
        "prod_hoodie": "Xudi",
        "prod_pants": "Shim",
        "prod_school": "Maktab formasi",
        "prod_suit": "Kostyum",
        "prod_pajama": "Pijoma",
        "prod_other": "Boshqa",

        "ask_qty": "Nechta dona?",
        "qty_20_50": "20–50",
        "qty_50_100": "50–100",
        "qty_100_300": "100–300",
        "qty_300p": "300+",

        "ask_city": "Yetkazib berish shahri?",
        "ask_phone": "Telefon: (yaxshisi «📲 Kontakt yuborish» tugmasini bosing)",
        "send_contact": "📲 Kontakt yuborish",

        "thanks": f"✅ Rahmat! ZARY & CO ni tanlaganingiz uchun.\nMenejer <b>15 daqiqa</b> ichida bog'lanib, buyurtma tafsilotlarini aniqlaydi.\n\n📣 Yangiliklar va kolleksiyalar 👉 https://t.me/{CHANNEL}",
        "cancelled": "❌ Bekor qilindi. Menyuga qaytyapman.",
        "err_phone": "Iltimos, kontaktni tugma orqali yuboring yoki raqamni matn bilan yozing.",
    }
}

def get_lang(message: Message) -> str:
    uid = message.from_user.id
    if uid not in LANG:
        LANG[uid] = auto_lang(message)
    return LANG[uid]

def tr(message: Message, key: str) -> str:
    lang = get_lang(message)
    return T[lang][key]

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
# KEYBOARDS
# =========================
def main_menu_kb(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T[lang]["btn_catalog"]), KeyboardButton(text=T[lang]["btn_terms"])],
            [KeyboardButton(text=T[lang]["btn_why"]), KeyboardButton(text=T[lang]["btn_min"])],
            [KeyboardButton(text=T[lang]["btn_leave"])],
            [KeyboardButton(text=T[lang]["btn_manager"]), KeyboardButton(text=T[lang]["btn_channel"])],
            [KeyboardButton(text=T[lang]["btn_lang"])],
        ],
        resize_keyboard=True
    )

def lang_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🇷🇺 Русский")],
            [KeyboardButton(text="🇺🇿 O'zbekcha")],
        ],
        resize_keyboard=True
    )

def cancel_kb(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=T[lang]["btn_cancel"])]],
        resize_keyboard=True
    )

# =========================
# START + LANGUAGE
# =========================
@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()
    # show language choice first time
    if message.from_user.id not in LANG:
        LANG[message.from_user.id] = auto_lang(message)
        await message.answer(T[get_lang(message)]["choose_lang"], reply_markup=lang_kb())
        return

    lang = get_lang(message)
    await message.answer(T[lang]["welcome"], reply_markup=main_menu_kb(lang))

@dp.message(F.text.in_(["🇷🇺 Русский", "🇺🇿 O'zbekcha"]))
async def set_language(message: Message, state: FSMContext):
    await state.clear()
    if "Русский" in message.text:
        LANG[message.from_user.id] = "ru"
        lang = "ru"
        await message.answer(T[lang]["lang_set"], reply_markup=main_menu_kb(lang))
    else:
        LANG[message.from_user.id] = "uz"
        lang = "uz"
        await message.answer(T[lang]["lang_set_uz"], reply_markup=main_menu_kb(lang))

@dp.message(F.text.in_(["🌐 Язык", "🌐 Til"]))
async def change_lang(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(tr(message, "choose_lang"), reply_markup=lang_kb())

# =========================
# CANCEL
# =========================
@dp.message(F.text.in_(["❌ Отмена", "❌ Bekor qilish"]))
async def cancel_any(message: Message, state: FSMContext):
    await state.clear()
    lang = get_lang(message)
    await message.answer(T[lang]["cancelled"], reply_markup=main_menu_kb(lang))

# =========================
# STATIC BUTTONS (RU/UZ)
# =========================
@dp.message(F.text.in_(["📞 Менеджер", "📞 Menejer"]))
async def manager(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(tr(message, "manager"))

@dp.message(F.text.in_(["📣 Канал", "📣 Kanal"]))
async def channel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(tr(message, "channel"))

@dp.message(F.text.in_(["⭐ Почему мы", "⭐ Nega biz"]))
async def why(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(tr(message, "why"))

@dp.message(F.text.in_(["📦 Каталог", "📦 Katalog"]))
async def catalog(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(tr(message, "catalog"))

@dp.message(F.text.in_(["🧾 Условия", "🧾 Shartlar"]))
async def terms(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(tr(message, "terms"))

@dp.message(F.text.in_(["📦 Минимальный заказ", "📦 Minimal buyurtma"]))
async def min_order(message: Message, state: FSMContext):
    await state.clear()
    lang = get_lang(message)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T[lang]["min_cta"])],
            [KeyboardButton(text=T[lang]["btn_cancel"])]
        ],
        resize_keyboard=True
    )
    await message.answer(T[lang]["min_text"], reply_markup=kb)

@dp.message(F.text.in_(["✅ Оставить заявку", "✅ Ariza qoldirish"]))
async def min_cta_to_form(message: Message, state: FSMContext):
    await form_start(message, state)

# =========================
# FORM
# =========================
@dp.message(F.text.in_(["🤝 Оставить заявку", "🤝 Ariza qoldirish"]))
async def form_start(message: Message, state: FSMContext):
    lang = get_lang(message)
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T[lang]["role_butik"]), KeyboardButton(text=T[lang]["role_shop"])],
            [KeyboardButton(text=T[lang]["role_market"]), KeyboardButton(text=T[lang]["role_other"])],
            [KeyboardButton(text=T[lang]["btn_cancel"])]
        ],
        resize_keyboard=True
    )
    await state.set_state(Form.role)
    await message.answer(T[lang]["ask_role"], reply_markup=kb)

@dp.message(Form.role)
async def form_role(message: Message, state: FSMContext):
    lang = get_lang(message)
    if message.text in (T[lang]["btn_cancel"], "❌ Отмена", "❌ Bekor qilish"):
        await cancel_any(message, state)
        return

    await state.update_data(role=message.text)

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T[lang]["prod_hoodie"]), KeyboardButton(text=T[lang]["prod_pants"])],
            [KeyboardButton(text=T[lang]["prod_school"]), KeyboardButton(text=T[lang]["prod_suit"])],
            [KeyboardButton(text=T[lang]["prod_pajama"]), KeyboardButton(text=T[lang]["prod_other"])],
            [KeyboardButton(text=T[lang]["btn_cancel"])]
        ],
        resize_keyboard=True
    )

    await state.set_state(Form.product)
    await message.answer(T[lang]["ask_product"], reply_markup=kb)

@dp.message(Form.product)
async def form_product(message: Message, state: FSMContext):
    lang = get_lang(message)
    if message.text in (T[lang]["btn_cancel"], "❌ Отмена", "❌ Bekor qilish"):
        await cancel_any(message, state)
        return

    await state.update_data(product=message.text)

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T[lang]["qty_20_50"]), KeyboardButton(text=T[lang]["qty_50_100"])],
            [KeyboardButton(text=T[lang]["qty_100_300"]), KeyboardButton(text=T[lang]["qty_300p"])],
            [KeyboardButton(text=T[lang]["btn_cancel"])]
        ],
        resize_keyboard=True
    )

    await state.set_state(Form.qty)
    await message.answer(T[lang]["ask_qty"], reply_markup=kb)

@dp.message(Form.qty)
async def form_qty(message: Message, state: FSMContext):
    lang = get_lang(message)
    if message.text in (T[lang]["btn_cancel"], "❌ Отмена", "❌ Bekor qilish"):
        await cancel_any(message, state)
        return

    await state.update_data(qty=message.text)
    await state.set_state(Form.city)
    await message.answer(T[lang]["ask_city"], reply_markup=cancel_kb(lang))

@dp.message(Form.city)
async def form_city(message: Message, state: FSMContext):
    lang = get_lang(message)
    if message.text in (T[lang]["btn_cancel"], "❌ Отмена", "❌ Bekor qilish"):
        await cancel_any(message, state)
        return

    await state.update_data(city=message.text)

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=T[lang]["send_contact"], request_contact=True)],
            [KeyboardButton(text=T[lang]["btn_cancel"])]
        ],
        resize_keyboard=True
    )

    await state.set_state(Form.phone)
    await message.answer(T[lang]["ask_phone"], reply_markup=kb)

@dp.message(Form.phone)
async def form_phone(message: Message, state: FSMContext):
    lang = get_lang(message)
    if message.text in (T[lang]["btn_cancel"], "❌ Отмена", "❌ Bekor qilish"):
        await cancel_any(message, state)
        return

    data = await state.get_data()

    phone = None
    if message.contact and message.contact.phone_number:
        phone = message.contact.phone_number
    else:
        # accept typed phone too
        txt = (message.text or "").strip()
        if len(txt) < 6:
            await message.answer(T[lang]["err_phone"])
            return
        phone = txt

    # Message to manager (bilingual)
    user = message.from_user
    text_to_manager = (
        "🛎 <b>Новая оптовая заявка / Yangi ulgurji ariza</b>\n\n"
        f"👤 Клиент: {user.full_name} (@{user.username or 'no_username'})\n"
        f"🌐 Lang: {lang}\n\n"
        f"Тип / Turi: <b>{data.get('role','-')}</b>\n"
        f"Товар / Mahsulot: <b>{data.get('product','-')}</b>\n"
        f"Объём / Miqdor: <b>{data.get('qty','-')}</b>\n"
        f"Город / Shahar: <b>{data.get('city','-')}</b>\n"
        f"Телефон / Telefon: <b>{phone}</b>"
    )

    try:
        await bot.send_message(MANAGER_ID, text_to_manager)
    except Exception as e:
        logging.exception("Failed to send message to manager: %s", e)

    await message.answer(T[lang]["thanks"], reply_markup=main_menu_kb(lang))
    await state.clear()

# =========================
# RUN
# =========================
async def main():
    # IMPORTANT: avoid webhook conflicts if previously set
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
