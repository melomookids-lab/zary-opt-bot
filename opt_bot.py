"""
ZARY & CO OPT Bot — Render Production (Fixed)
- Работает с Render (health server + polling)
- Async SQLite через aiosqlite
- Месячный авто-отчет в последний день месяца 23:00
- Экспорт Excel по кнопке
- Исправлены отсутствующие тексты/ключи и админ-статистика
- Админ определяется по MANAGER_ID или ADMIN_ID_1/2/3
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
from calendar import monthrange

import aiosqlite
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types.input_file import FSInputFile
from aiogram.exceptions import TelegramAPIError

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment


# =========================
# CONFIG
# =========================
class Config:
    BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()

    # старое имя переменной (как у тебя на Render)
    MANAGER_ID_RAW = (os.getenv("MANAGER_ID") or "").strip()

    # новые варианты (если когда-то решишь перейти на ADMIN_ID_1)
    ADMIN_IDS: List[int] = []
    for i in range(1, 4):
        v = (os.getenv(f"ADMIN_ID_{i}") or "").strip()
        if v.isdigit():
            ADMIN_IDS.append(int(v))

    # если ADMIN_IDS пуст, используем MANAGER_ID
    if not ADMIN_IDS and MANAGER_ID_RAW.isdigit():
        ADMIN_IDS = [int(MANAGER_ID_RAW)]

    CHANNEL = (os.getenv("CHANNEL") or "zaryco_official").strip().lstrip("@")
    PHONE = (os.getenv("PHONE") or "+998771202255").strip()
    PORT = int((os.getenv("PORT") or "10000").strip())

    DB_PATH = (os.getenv("DB_PATH") or "leads.sqlite3").strip()

    EXPORTS_DIR = Path("exports")
    BACKUP_DIR = Path("backups")
    REPORTS_DIR = Path("reports")

    MAX_EXPORT_AGE_DAYS = 7
    BACKUP_KEEP_COUNT = 5

    # validation
    if not BOT_TOKEN:
        raise RuntimeError("❌ BOT_TOKEN не указан в Environment Variables!")
    if not ADMIN_IDS:
        raise RuntimeError("❌ Нужен админ: добавь MANAGER_ID или ADMIN_ID_1 (число) в Render Environment Variables!")

    PRIMARY_ADMIN = ADMIN_IDS[0]


# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("zary-opt-bot")


# =========================
# DATABASE
# =========================
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA foreign_keys = ON")
        await self.conn.execute("PRAGMA journal_mode = WAL")
        await self.init_tables()
        logger.info("DB connected")

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def init_tables(self):
        assert self.conn is not None
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                lang TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_activity TEXT DEFAULT CURRENT_TIMESTAMP
            );

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
                status TEXT NOT NULL DEFAULT 'new',
                manager_notified INTEGER DEFAULT 0,
                notes TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_leads_user_id ON leads(user_id);
            CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
            CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);

            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                user_id INTEGER,
                action TEXT,
                details TEXT
            );

            CREATE TABLE IF NOT EXISTS monthly_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                sent_at TEXT NOT NULL,
                filename TEXT NOT NULL,
                total_leads INTEGER NOT NULL,
                status TEXT DEFAULT 'sent'
            );
            """
        )
        await self.conn.commit()

    async def get_lang(self, user_id: int) -> Optional[str]:
        assert self.conn is not None
        async with self.conn.execute("SELECT lang FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

    async def set_lang(self, user_id: int, lang: str):
        assert self.conn is not None
        await self.conn.execute(
            """
            INSERT INTO users(user_id, lang, last_activity)
            VALUES(?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                lang=excluded.lang,
                last_activity=CURRENT_TIMESTAMP
            """,
            (user_id, lang),
        )
        await self.conn.commit()

    async def add_lead(self, lead: Dict[str, Any]) -> int:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            INSERT INTO leads(created_at, user_id, username, full_name, lang,
                             role, product, qty, city, phone, status)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                lead["created_at"],
                lead["user_id"],
                lead.get("username"),
                lead.get("full_name"),
                lead["lang"],
                lead["role"],
                lead["product"],
                lead["qty"],
                lead["city"],
                lead["phone"],
                "new",
            ),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def get_last_leads(self, limit: int = 20) -> List[aiosqlite.Row]:
        assert self.conn is not None
        async with self.conn.execute("SELECT * FROM leads ORDER BY id DESC LIMIT ?", (limit,)) as cur:
            return await cur.fetchall()

    async def get_all_leads(self) -> List[aiosqlite.Row]:
        assert self.conn is not None
        async with self.conn.execute("SELECT * FROM leads ORDER BY id DESC") as cur:
            return await cur.fetchall()

    async def get_leads_by_date_range(self, start: str, end: str) -> List[aiosqlite.Row]:
        assert self.conn is not None
        async with self.conn.execute(
            """
            SELECT * FROM leads
            WHERE created_at >= ? AND created_at <= ?
            ORDER BY id DESC
            """,
            (start, end),
        ) as cur:
            return await cur.fetchall()

    async def update_status(self, lead_id: int, status: str) -> bool:
        assert self.conn is not None
        cur = await self.conn.execute("UPDATE leads SET status=? WHERE id=?", (status, lead_id))
        await self.conn.commit()
        return cur.rowcount > 0

    async def update_notification_status(self, lead_id: int, notified: bool):
        assert self.conn is not None
        await self.conn.execute(
            "UPDATE leads SET manager_notified=? WHERE id=?",
            (1 if notified else 0, lead_id),
        )
        await self.conn.commit()

    async def log_activity(self, user_id: int, action: str, details: str = ""):
        assert self.conn is not None
        await self.conn.execute(
            "INSERT INTO activity_log (user_id, action, details) VALUES(?,?,?)",
            (user_id, action, details),
        )
        await self.conn.commit()

    async def get_stats(self) -> Dict[str, int]:
        assert self.conn is not None
        async with self.conn.execute(
            """
            SELECT
                COUNT(*) as total_leads,
                SUM(CASE WHEN status='new' THEN 1 ELSE 0 END) as new_leads,
                SUM(CASE WHEN status='work' THEN 1 ELSE 0 END) as work_leads,
                SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END) as paid_leads,
                SUM(CASE WHEN status='shipped' THEN 1 ELSE 0 END) as shipped_leads,
                SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END) as closed_leads,
                COUNT(DISTINCT user_id) as unique_users
            FROM leads
            """
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else {}

    async def get_monthly_stats(self, year: int, month: int) -> Dict[str, Any]:
        start = f"{year}-{month:02d}-01"
        last_day = monthrange(year, month)[1]
        end = f"{year}-{month:02d}-{last_day} 23:59:59"

        assert self.conn is not None
        async with self.conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='new' THEN 1 ELSE 0 END) as new_count,
                SUM(CASE WHEN status='work' THEN 1 ELSE 0 END) as work_count,
                SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END) as paid_count,
                SUM(CASE WHEN status='shipped' THEN 1 ELSE 0 END) as shipped_count,
                SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END) as closed_count,
                COUNT(DISTINCT user_id) as unique_clients
            FROM leads
            WHERE created_at >= ? AND created_at <= ?
            """,
            (start, end),
        ) as cur:
            row = await cur.fetchone()
            base = dict(row) if row else {}
            return {
                "period": f"{month:02d}.{year}",
                "start": start,
                "end": end[:10],
                **base,
            }

    async def mark_report_sent(self, year: int, month: int, filename: str, total_leads: int):
        assert self.conn is not None
        await self.conn.execute(
            """
            INSERT INTO monthly_reports (year, month, sent_at, filename, total_leads)
            VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?)
            """,
            (year, month, filename, total_leads),
        )
        await self.conn.commit()

    async def is_report_sent(self, year: int, month: int) -> bool:
        assert self.conn is not None
        async with self.conn.execute(
            "SELECT 1 FROM monthly_reports WHERE year=? AND month=? AND status='sent'",
            (year, month),
        ) as cur:
            return await cur.fetchone() is not None


db = Database(Config.DB_PATH)


# =========================
# BOT
# =========================
bot = Bot(Config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())


# =========================
# TEXTS
# =========================
TEXT = {
    "ru": {
        "welcome": (
            "🤝 <b>ZARY & CO — ОПТОВЫЙ ОТДЕЛ</b>\n\n"
            "Работаем с магазинами, бутиками и маркетплейсами.\n"
            "• Национальный бренд\n"
            "• Стабильные поставки\n"
            "• Высокая маржинальность\n\n"
            "👇 Выберите действие:"
        ),
        "menu": "📍 Главное меню",
        "choose_lang": "🌐 Выберите язык:",
        "manager": (
            "📞 <b>Менеджер оптового отдела</b>\n\n"
            f"Телефон: <code>{Config.PHONE}</code>\n"
            "Режим работы: Пн-Пт 9:00-18:00"
        ),
        "channel": f"📣 <b>Наш канал</b>\n\n👉 https://t.me/{Config.CHANNEL}",
        "catalog": f"📸 <b>Каталог</b>\n\n👉 https://t.me/{Config.CHANNEL}",
        "terms": (
            "🧾 <b>Условия сотрудничества</b>\n\n"
            "✅ Форма: предзаказ / наличие\n"
            "✅ Минимальный заказ: от 20 единиц\n"
            "✅ Доставка: по Узбекистану\n"
            "✅ Оплата: перечисление / наличные"
        ),
        "why": (
            "⭐ <b>Почему выбирают нас</b>\n\n"
            "🏭 Собственное производство\n"
            "📦 Ассортимент: 500+ моделей\n"
            "🚚 Доставка: 2-3 дня по стране"
        ),
        "min_order": (
            "📦 <b>Минимальный заказ</b>\n\n"
            "• Опт: от 20 единиц\n"
            "• Крупный опт: от 100 единиц\n\n"
            "Хотите персональный расчёт?"
        ),
        "form_role": "👤 <b>Кто вы?</b>\n\nВыберите тип бизнеса:",
        "form_product": "👕 <b>Что хотите заказать?</b>\n\nВыберите или напишите свой вариант:",
        "form_qty": "📊 <b>Объём заказа?</b>",
        "form_city": "📍 <b>Город доставки?</b>",
        "form_phone": "📱 <b>Контактный телефон</b>\n\nНажмите «📲 Отправить контакт» или введите вручную:",
        "bad_phone": "❌ <b>Некорректный номер</b>\n\nПример: +998901234567",
        "thanks": (
            "✅ <b>Заявка #{lead_id} принята!</b>\n\n"
            "Менеджер свяжется с вами в течение 15 минут.\n\n"
            f"📣 https://t.me/{Config.CHANNEL}\n"
            f"📞 {Config.PHONE}"
        ),
        "cancelled": "❌ Отменено. Возвращаюсь в меню…",
        "admin_only": "⛔ Только для администратора.",
        "admin_menu": "🛠 <b>Панель управления</b>",
        "admin_empty": "📝 Пока нет заявок.",
        "admin_export_ok": "✅ Excel сформирован.",
        "admin_export_fail": "❌ Ошибка при создании Excel.",
        "admin_status_bad": (
            "❌ Неверная команда.\n\n"
            "Используйте: /status ID статус\n"
            "Статусы: new, work, paid, shipped, closed"
        ),
        "admin_status_updated": "✅ Статус обновлён.",
        "error": "⚠️ Ошибка. Попробуйте позже.",
    },
    "uz": {
        "welcome": (
            "🤝 <b>ZARY & CO — ULGURJI BO'LIMI</b>\n\n"
            "Do'konlar, butiklar va marketplace bilan ishlaymiz.\n"
            "• Milliy brend\n"
            "• Barqaror yetkazib berish\n"
            "• Yuqori marja\n\n"
            "👇 Amalni tanlang:"
        ),
        "menu": "📍 Asosiy menyu",
        "choose_lang": "🌐 Tilni tanlang:",
        "manager": (
            "📞 <b>Ulgurji bo'lim menejeri</b>\n\n"
            f"Telefon: <code>{Config.PHONE}</code>\n"
            "Ish vaqti: Du-Ju 9:00-18:00"
        ),
        "channel": f"📣 <b>Kanal</b>\n\n👉 https://t.me/{Config.CHANNEL}",
        "catalog": f"📸 <b>Katalog</b>\n\n👉 https://t.me/{Config.CHANNEL}",
        "terms": (
            "🧾 <b>Hamkorlik shartlari</b>\n\n"
            "✅ Ish: oldindan buyurtma / mavjud\n"
            "✅ Minimal: 20 donadan\n"
            "✅ Yetkazish: O'zbekiston bo'ylab\n"
            "✅ To'lov: o'tkazma / naqd"
        ),
        "why": (
            "⭐ <b>Nega biz</b>\n\n"
            "🏭 O'z ishlab chiqarish\n"
            "📦 500+ model\n"
            "🚚 2-3 kunda yetkazish"
        ),
        "min_order": (
            "📦 <b>Minimal buyurtma</b>\n\n"
            "• Ulgurji: 20 donadan\n"
            "• Katta ulgurji: 100 donadan\n\n"
            "Shaxsiy hisob-kitob xohlaysizmi?"
        ),
        "form_role": "👤 <b>Siz kimsiz?</b>",
        "form_product": "👕 <b>Nima buyurtma qilmoqchisiz?</b>",
        "form_qty": "📊 <b>Buyurtma hajmi?</b>",
        "form_city": "📍 <b>Yetkazib berish shahri?</b>",
        "form_phone": "📱 <b>Aloqa telefoni</b>\n\n«📲 Kontakt yuborish» tugmasini bosing yoki yozing:",
        "bad_phone": "❌ <b>Noto'g'ri raqam</b>\n\nMisol: +998901234567",
        "thanks": (
            "✅ <b>Ariza #{lead_id} qabul qilindi!</b>\n\n"
            "Menejer 15 daqiqa ichida bog'lanadi.\n\n"
            f"📣 https://t.me/{Config.CHANNEL}\n"
            f"📞 {Config.PHONE}"
        ),
        "cancelled": "❌ Bekor qilindi. Menyuga qaytish…",
        "admin_only": "⛔ Faqat admin uchun.",
        "admin_menu": "🛠 <b>Admin panel</b>",
        "admin_empty": "📝 Hozircha arizalar yo'q.",
        "admin_export_ok": "✅ Excel tayyor.",
        "admin_export_fail": "❌ Excel yaratishda xatolik.",
        "admin_status_bad": (
            "❌ Noto'g'ri buyruq.\n\n"
            "/status ID status\n"
            "Status: new, work, paid, shipped, closed"
        ),
        "admin_status_updated": "✅ Status yangilandi.",
        "error": "⚠️ Xatolik. Keyinroq urinib ko'ring.",
    },
}


BTN = {
    "ru": {
        "catalog": "📦 Каталог",
        "terms": "🧾 Условия",
        "why": "⭐ Почему мы",
        "min": "📦 Мин. заказ",
        "leave": "🤝 Заявка",
        "manager": "📞 Менеджер",
        "channel": "📣 Канал",
        "lang": "🌐 Язык",
        "admin": "🛠 Админ",
        "cancel": "❌ Отмена",
        "contact": "📲 Отправить контакт",
        "back": "⬅️ Назад",
    },
    "uz": {
        "catalog": "📦 Katalog",
        "terms": "🧾 Shartlar",
        "why": "⭐ Nega biz",
        "min": "📦 Min. buyurtma",
        "leave": "🤝 Ariza",
        "manager": "📞 Menejer",
        "channel": "📣 Kanal",
        "lang": "🌐 Til",
        "admin": "🛠 Admin",
        "cancel": "❌ Bekor qilish",
        "contact": "📲 Kontakt yuborish",
        "back": "⬅️ Orqaga",
    },
}


def t(key: str, lang: str, **kwargs) -> str:
    lang = lang if lang in TEXT else "ru"
    base = TEXT[lang].get(key, key)
    return base.format(**kwargs)


# =========================
# HELPERS
# =========================
def auto_lang(code: Optional[str]) -> str:
    return "uz" if (code or "").lower().startswith("uz") else "ru"


async def get_user_lang(user_id: int, telegram_lang_code: Optional[str]) -> str:
    stored = await db.get_lang(user_id)
    if stored:
        return stored
    lang = auto_lang(telegram_lang_code)
    await db.set_lang(user_id, lang)
    return lang


def is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_IDS


def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    s = re.sub(r"[^\d+]", "", raw.strip())
    if s.startswith("998") and not s.startswith("+998"):
        s = "+" + s
    if s.startswith("+998") and len(re.sub(r"\D", "", s)) == 12:
        return s
    # если ввели без +998, но 9 цифр
    digits = re.sub(r"\D", "", s)
    if len(digits) == 9:
        return "+998" + digits
    return s


def is_valid_phone(phone: str) -> bool:
    p = normalize_phone(phone)
    digits = re.sub(r"\D", "", p)
    return digits.startswith("998") and len(digits) == 12


# =========================
# FSM
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
class Keyboards:
    @staticmethod
    def lang() -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🇷🇺 Русский"), KeyboardButton(text="🇺🇿 O'zbekcha")]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )

    @staticmethod
    def main(lang: str, admin: bool) -> ReplyKeyboardMarkup:
        b = BTN[lang]
        rows = [
            [KeyboardButton(text=b["catalog"]), KeyboardButton(text=b["terms"])],
            [KeyboardButton(text=b["why"]), KeyboardButton(text=b["min"])],
            [KeyboardButton(text=b["leave"])],
            [KeyboardButton(text=b["manager"]), KeyboardButton(text=b["channel"])],
            [KeyboardButton(text=b["lang"])],
        ]
        if admin:
            rows.append([KeyboardButton(text=b["admin"])])
        return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

    @staticmethod
    def form_role(lang: str) -> ReplyKeyboardMarkup:
        b = BTN[lang]
        if lang == "uz":
            r = [["🏬 Butik", "🏪 Do'kon"], ["📱 Marketplace", "🌐 Boshqa"]]
        else:
            r = [["🏬 Бутик", "🏪 Магазин"], ["📱 Маркетплейс", "🌐 Другое"]]
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=r[0][0]), KeyboardButton(text=r[0][1])],
                [KeyboardButton(text=r[1][0]), KeyboardButton(text=r[1][1])],
                [KeyboardButton(text=b["cancel"])],
            ],
            resize_keyboard=True,
        )

    @staticmethod
    def form_product(lang: str) -> ReplyKeyboardMarkup:
        b = BTN[lang]
        if lang == "uz":
            p = [["👕 Kiyim", "👖 Shim"], ["🎒 Aksessuar", "👔 Boshqa"]]
        else:
            p = [["👕 Одежда", "👖 Брюки"], ["🎒 Аксессуары", "👔 Другое"]]
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=p[0][0]), KeyboardButton(text=p[0][1])],
                [KeyboardButton(text=p[1][0]), KeyboardButton(text=p[1][1])],
                [KeyboardButton(text=b["cancel"])],
            ],
            resize_keyboard=True,
        )

    @staticmethod
    def form_qty(lang: str) -> ReplyKeyboardMarkup:
        b = BTN[lang]
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="20–50"), KeyboardButton(text="50–100")],
                [KeyboardButton(text="100–300"), KeyboardButton(text="300+")],
                [KeyboardButton(text=b["cancel"])],
            ],
            resize_keyboard=True,
        )

    @staticmethod
    def form_phone(lang: str) -> ReplyKeyboardMarkup:
        b = BTN[lang]
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=b["contact"], request_contact=True)],
                [KeyboardButton(text=b["cancel"])],
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )

    @staticmethod
    def admin(lang: str) -> ReplyKeyboardMarkup:
        b = BTN[lang]
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📋 Последние" if lang == "ru" else "📋 Oxirgi"),
                 KeyboardButton(text="📊 Статистика" if lang == "ru" else "📊 Statistika")],
                [KeyboardButton(text="📤 Excel"), KeyboardButton(text="ℹ️ Status")],
                [KeyboardButton(text=b["back"])],
            ],
            resize_keyboard=True,
        )


# =========================
# HANDLERS
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await db.log_activity(message.from_user.id, "start", f"lang={lang}")
    await message.answer(t("welcome", lang))
    await message.answer(t("menu", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(F.text.in_(["🇷🇺 Русский", "🇺🇿 O'zbekcha"]))
async def set_lang(message: Message, state: FSMContext):
    await state.clear()
    lang = "ru" if "Русский" in (message.text or "") else "uz"
    await db.set_lang(message.from_user.id, lang)
    await db.log_activity(message.from_user.id, "set_lang", lang)
    await message.answer(t("welcome", lang))
    await message.answer(t("menu", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["lang"], BTN["uz"]["lang"]})
async def change_lang(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("choose_lang", lang), reply_markup=Keyboards.lang())


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["manager"], BTN["uz"]["manager"]})
async def menu_manager(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("manager", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["channel"], BTN["uz"]["channel"]})
async def menu_channel(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("channel", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["catalog"], BTN["uz"]["catalog"]})
async def menu_catalog(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("catalog", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["terms"], BTN["uz"]["terms"]})
async def menu_terms(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("terms", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["why"], BTN["uz"]["why"]})
async def menu_why(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("why", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["min"], BTN["uz"]["min"]})
async def menu_min(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("min_order", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


# ---- FORM ----
@dp.message(lambda m: (m.text or "") in {BTN["ru"]["leave"], BTN["uz"]["leave"]})
async def form_start(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await state.set_state(Form.role)
    await message.answer(t("form_role", lang), reply_markup=Keyboards.form_role(lang))


@dp.message(Form.role)
async def form_role(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    text = (message.text or "").strip()
    if text in {BTN["ru"]["cancel"], BTN["uz"]["cancel"]}:
        await cancel_handler(message, state)
        return
    await state.update_data(role=text)
    await state.set_state(Form.product)
    await message.answer(t("form_product", lang), reply_markup=Keyboards.form_product(lang))


@dp.message(Form.product)
async def form_product(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    text = (message.text or "").strip()
    if text in {BTN["ru"]["cancel"], BTN["uz"]["cancel"]}:
        await cancel_handler(message, state)
        return
    await state.update_data(product=text)
    await state.set_state(Form.qty)
    await message.answer(t("form_qty", lang), reply_markup=Keyboards.form_qty(lang))


@dp.message(Form.qty)
async def form_qty(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    text = (message.text or "").strip()
    if text in {BTN["ru"]["cancel"], BTN["uz"]["cancel"]}:
        await cancel_handler(message, state)
        return
    await state.update_data(qty=text)
    await state.set_state(Form.city)
    await message.answer(t("form_city", lang), reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN[lang]["cancel"])]],
        resize_keyboard=True
    ))


@dp.message(Form.city)
async def form_city(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    text = (message.text or "").strip()
    if text in {BTN["ru"]["cancel"], BTN["uz"]["cancel"]}:
        await cancel_handler(message, state)
        return
    await state.update_data(city=text)
    await state.set_state(Form.phone)
    await message.answer(t("form_phone", lang), reply_markup=Keyboards.form_phone(lang))


@dp.message(Form.phone)
async def form_phone(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)

    if (message.text or "").strip() in {BTN["ru"]["cancel"], BTN["uz"]["cancel"]}:
        await cancel_handler(message, state)
        return

    raw = message.contact.phone_number if message.contact else (message.text or "").strip()
    phone = normalize_phone(raw)

    if not is_valid_phone(phone):
        await message.answer(t("bad_phone", lang))
        return

    data = await state.get_data()
    user = message.from_user

    lead = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
        "lang": lang,
        "role": data.get("role", "-"),
        "product": data.get("product", "-"),
        "qty": data.get("qty", "-"),
        "city": data.get("city", "-"),
        "phone": phone,
    }

    try:
        lead_id = await db.add_lead(lead)
        await db.log_activity(user.id, "lead_created", f"id={lead_id}")
        await notify_admins(lead, lead_id, lang)
        await message.answer(t("thanks", lang, lead_id=lead_id),
                             reply_markup=Keyboards.main(lang, is_admin(user.id)))
    except Exception as e:
        logger.exception("save lead failed")
        await message.answer(t("error", lang))

    await state.clear()


async def notify_admins(lead: dict, lead_id: int, client_lang: str):
    lang_label = "🇷🇺 RU" if client_lang == "ru" else "🇺🇿 UZ"
    msg = (
        f"🔔 <b>Новая заявка #{lead_id}</b> {lang_label}\n\n"
        f"👤 {lead['full_name']}\n"
        f"📱 <code>{lead['phone']}</code>\n"
        f"🏢 {lead['role']} | {lead['product']} | {lead['qty']}\n"
        f"📍 {lead['city']}\n"
        f"⏰ {lead['created_at']}\n\n"
        f"👤 @{lead['username'] or 'нет'}\n"
        f"🆔 <code>{lead['user_id']}</code>\n"
        f"📋 /status {lead_id} work"
    )

    for admin_id in Config.ADMIN_IDS:
        try:
            await bot.send_message(admin_id, msg)
            await db.update_notification_status(lead_id, True)
        except TelegramAPIError as e:
            logger.error(f"notify admin failed: {e}")
            await db.log_activity(lead["user_id"], "notify_failed", str(e))


async def cancel_handler(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("cancelled", lang),
                         reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["cancel"], BTN["uz"]["cancel"]})
async def cmd_cancel(message: Message, state: FSMContext):
    await cancel_handler(message, state)


# =========================
# ADMIN
# =========================
@dp.message(lambda m: (m.text or "") in {BTN["ru"]["admin"], BTN["uz"]["admin"]})
async def admin_menu(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    if not is_admin(message.from_user.id):
        await message.answer(t("admin_only", lang))
        return
    await message.answer(t("admin_menu", lang), reply_markup=Keyboards.admin(lang))


@dp.message(lambda m: (m.text or "").startswith("📋"))
async def admin_last(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id):
        return
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    rows = await db.get_last_leads(20)
    if not rows:
        await message.answer(t("admin_empty", lang), reply_markup=Keyboards.admin(lang))
        return

    lines = ["📋 <b>Последние заявки</b>\n" if lang == "ru" else "📋 <b>Oxirgi arizalar</b>\n"]
    for r in rows:
        status_emoji = {"new": "🆕", "work": "🔧", "paid": "💰", "shipped": "🚚", "closed": "✅"}.get(r["status"], "❓")
        lines.append(
            f"\n<b>#{r['id']}</b> {status_emoji} <code>{r['status']}</code>\n"
            f"📅 {str(r['created_at'])[:16]} | {r['role']} | {r['product']}\n"
            f"📍 {r['city']} | ☎️ {r['phone']}\n"
            f"{'✓' if r['manager_notified'] else '✗'} | {r['user_id']}\n"
            f"──────────────"
        )
    await message.answer("\n".join(lines), reply_markup=Keyboards.admin(lang))


@dp.message(lambda m: (m.text or "").startswith("📊"))
async def admin_stats(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id):
        return
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    s = await db.get_stats()
    text_ru = (
        "📊 <b>Статистика</b>\n\n"
        f"• Всего заявок: <b>{s.get('total_leads', 0) or 0}</b>\n"
        f"• Новые: <b>{s.get('new_leads', 0) or 0}</b>\n"
        f"• В работе: <b>{s.get('work_leads', 0) or 0}</b>\n"
        f"• Оплачено: <b>{s.get('paid_leads', 0) or 0}</b>\n"
        f"• Отправлено: <b>{s.get('shipped_leads', 0) or 0}</b>\n"
        f"• Закрыто: <b>{s.get('closed_leads', 0) or 0}</b>\n"
        f"• Уникальных клиентов: <b>{s.get('unique_users', 0) or 0}</b>"
    )
    text_uz = (
        "📊 <b>Statistika</b>\n\n"
        f"• Jami arizalar: <b>{s.get('total_leads', 0) or 0}</b>\n"
        f"• Yangi: <b>{s.get('new_leads', 0) or 0}</b>\n"
        f"• Ishlanmoqda: <b>{s.get('work_leads', 0) or 0}</b>\n"
        f"• To'langan: <b>{s.get('paid_leads', 0) or 0}</b>\n"
        f"• Yuborilgan: <b>{s.get('shipped_leads', 0) or 0}</b>\n"
        f"• Yopilgan: <b>{s.get('closed_leads', 0) or 0}</b>\n"
        f"• Unikal mijozlar: <b>{s.get('unique_users', 0) or 0}</b>"
    )
    await message.answer(text_ru if lang == "ru" else text_uz, reply_markup=Keyboards.admin(lang))


@dp.message(Command("status"))
async def admin_set_status(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    if not is_admin(message.from_user.id):
        await message.answer(t("admin_only", lang))
        return

    parts = (message.text or "").split()
    if len(parts) != 3 or not parts[1].isdigit():
        await message.answer(t("admin_status_bad", lang), reply_markup=Keyboards.admin(lang))
        return

    lead_id = int(parts[1])
    status = parts[2].lower().strip()
    if status not in {"new", "work", "paid", "shipped", "closed"}:
        await message.answer(t("admin_status_bad", lang), reply_markup=Keyboards.admin(lang))
        return

    ok = await db.update_status(lead_id, status)
    if ok:
        await message.answer(t("admin_status_updated", lang), reply_markup=Keyboards.admin(lang))
        await db.log_activity(message.from_user.id, "status_update", f"{lead_id}->{status}")
    else:
        await message.answer(f"❌ Заявка #{lead_id} не найдена", reply_markup=Keyboards.admin(lang))


@dp.message(lambda m: (m.text or "") == "📤 Excel")
async def admin_export(message: Message, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id):
        return
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)

    try:
        rows = await db.get_all_leads()
        if not rows:
            await message.answer(t("admin_empty", lang), reply_markup=Keyboards.admin(lang))
            return

        Config.EXPORTS_DIR.mkdir(exist_ok=True)
        out = Config.EXPORTS_DIR / f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await create_excel(rows, out, "Leads")

        await message.answer(t("admin_export_ok", lang), reply_markup=Keyboards.admin(lang))
        await bot.send_document(
            message.from_user.id,
            FSInputFile(str(out)),
            caption=f"📤 Экспорт от {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        )
        await db.log_activity(message.from_user.id, "export_excel", str(out))
    except Exception:
        logger.exception("export failed")
        await message.answer(t("admin_export_fail", lang), reply_markup=Keyboards.admin(lang))


@dp.message(lambda m: (m.text or "") in {BTN["ru"]["back"], BTN["uz"]["back"]})
async def admin_back(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id, message.from_user.language_code)
    await message.answer(t("menu", lang), reply_markup=Keyboards.main(lang, is_admin(message.from_user.id)))


# =========================
# EXCEL
# =========================
async def create_excel(rows: List[aiosqlite.Row], filepath: Path, title: str = "Leads") -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = title

    headers = ["ID", "Дата", "Клиент", "Username", "Язык", "Тип", "Товар",
               "Кол-во", "Город", "Телефон", "Статус", "Уведомлен"]
    ws.append(headers)

    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        cell.alignment = Alignment(horizontal="center")

    for r in rows:
        ws.append([
            r["id"], r["created_at"], r["full_name"], r["username"], r["lang"],
            r["role"], r["product"], r["qty"], r["city"], r["phone"],
            r["status"], "Да" if r["manager_notified"] else "Нет"
        ])

    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

    wb.save(filepath)
    return filepath


# =========================
# MONTHLY REPORT
# =========================
async def send_monthly_report():
    now = datetime.now()
    year, month = now.year, now.month

    if await db.is_report_sent(year, month):
        return

    stats = await db.get_monthly_stats(year, month)
    if (stats.get("total") or 0) == 0:
        return

    start_date = f"{year}-{month:02d}-01"
    last_day = monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day} 23:59:59"

    rows = await db.get_leads_by_date_range(start_date, end_date)

    Config.REPORTS_DIR.mkdir(exist_ok=True)
    filename = Config.REPORTS_DIR / f"monthly_report_{year}_{month:02d}.xlsx"
    await create_excel(rows, filename, f"Report_{month:02d}_{year}")

    intro = (
        f"<b>📊 МЕСЯЧНЫЙ ОТЧЕТ — {stats['period']}</b>\n\n"
        f"📅 Период: {stats['start']} — {stats['end']}\n"
        f"📋 Всего заявок: <b>{stats.get('total', 0) or 0}</b>\n"
        f"👥 Уникальных клиентов: <b>{stats.get('unique_clients', 0) or 0}</b>\n\n"
        f"<b>Статусы:</b>\n"
        f"🆕 Новые: {stats.get('new_count', 0) or 0}\n"
        f"🔧 В работе: {stats.get('work_count', 0) or 0}\n"
        f"💰 Оплачено: {stats.get('paid_count', 0) or 0}\n"
        f"🚚 Отправлено: {stats.get('shipped_count', 0) or 0}\n"
        f"✅ Закрыто: {stats.get('closed_count', 0) or 0}"
    )

    for admin_id in Config.ADMIN_IDS:
        try:
            await bot.send_message(admin_id, intro)
            await bot.send_document(
                admin_id,
                FSInputFile(str(filename)),
                caption=f"📊 Полный отчет за {stats['period']}\nФайл: {filename.name}",
            )
        except TelegramAPIError as e:
            logger.error(f"monthly report send failed: {e}")

    await db.mark_report_sent(year, month, str(filename), int(stats.get("total", 0) or 0))


# =========================
# CLEANUP & BACKUP
# =========================
async def cleanup_old_files():
    try:
        Config.EXPORTS_DIR.mkdir(exist_ok=True)
        cutoff = datetime.now() - timedelta(days=Config.MAX_EXPORT_AGE_DAYS)
        for file in Config.EXPORTS_DIR.glob("*.xlsx"):
            if datetime.fromtimestamp(file.stat().st_mtime) < cutoff:
                file.unlink()
    except Exception as e:
        logger.error(f"cleanup error: {e}")


async def backup_database():
    try:
        Config.BACKUP_DIR.mkdir(exist_ok=True)
        backup_path = Config.BACKUP_DIR / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        import shutil
        shutil.copy(Config.DB_PATH, backup_path)

        backups = sorted(Config.BACKUP_DIR.glob("*.db"), key=lambda p: p.stat().st_mtime)
        for old in backups[:-Config.BACKUP_KEEP_COUNT]:
            old.unlink()
    except Exception as e:
        logger.error(f"backup error: {e}")


# =========================
# WEB SERVER
# =========================
async def start_web_server():
    app = web.Application()

    async def health(_request):
        return web.Response(text="OK", status=200)

    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
    await site.start()
    logger.info(f"Health server: 0.0.0.0:{Config.PORT}")


# =========================
# MAIN
# =========================
async def main():
    await db.connect()
    await cleanup_old_files()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(cleanup_old_files, "cron", hour=3, minute=0)
    scheduler.add_job(backup_database, "cron", hour=2, minute=0)
    scheduler.add_job(send_monthly_report, "cron", day="last", hour=23, minute=0)
    # страховка: если бот был выключен в последний день
    scheduler.add_job(send_monthly_report, "date", run_date=datetime.now() + timedelta(seconds=30))
    scheduler.start()

    await bot.delete_webhook(drop_pending_updates=True)

    logger.info(f"Bot start. Admins={Config.ADMIN_IDS} Channel=@{Config.CHANNEL}")

    await asyncio.gather(
        start_web_server(),
        dp.start_polling(bot, skip_updates=True)
    )


if __name__ == "__main__":
    asyncio.run(main())
