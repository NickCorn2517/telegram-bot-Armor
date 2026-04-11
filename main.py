import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Any

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
PAYMENTS_TOKEN = (os.getenv("PAYMENTS_TOKEN") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
RENDER_EXTERNAL_URL = (os.getenv("RENDER_EXTERNAL_URL") or "").strip()

CHANNEL_ID = -1003616232121
ADMIN_ID = 583554883

DEFAULT_START_TEXT = (
    "🔥 <b>Добро пожаловать</b>\n\n"
    "Здесь можно купить доступ к закрытому каналу, посмотреть тарифы, получить поддержку и задать вопросы."
)

DEFAULT_TARIFFS = [
    {
        "id": 1,
        "name": "Стандарт",
        "price_kop": 1500000,
        "days": 365,
        "active": True,
        "pseudo_mode": "choice",
    },
    {
        "id": 2,
        "name": "Тариф 2",
        "price_kop": 2500000,
        "days": 365,
        "active": False,
        "pseudo_mode": "choice",
    },
    {
        "id": 3,
        "name": "Тариф 3",
        "price_kop": 3500000,
        "days": 365,
        "active": False,
        "pseudo_mode": "choice",
    },
]

DEFAULT_OFFER = {
    "enabled_before_pay": False,
    "enabled_after_pay": True,
    "require_accept_before_pay": False,
    "text": "Текст оферты пока не настроен.",
    "media_type": None,
    "media_file_id": None,
}

DEFAULT_FUNNEL = {
    "enabled": False,
    "send_time": "20:00",
    "steps": [
        {
            "enabled": True,
            "delay_hours": 3,
            "text": "Напоминаем о доступе.",
            "media_type": None,
            "media_file_id": None,
        },
        {
            "enabled": False,
            "delay_hours": 12,
            "text": "Второе касание.",
            "media_type": None,
            "media_file_id": None,
        },
        {
            "enabled": False,
            "delay_hours": 24,
            "text": "Последнее напоминание.",
            "media_type": None,
            "media_file_id": None,
        },
    ],
}

DEFAULT_SHARE = {
    "text": "Присоединяйся через моего бота:",
    "media_type": None,
    "media_file_id": None,
}

DEFAULT_HOMEWORK = {
    "text": "Отправь своё домашнее задание одним сообщением: текст, фото, видео, голосовое, кружок или документ.",
    "media_type": None,
    "media_file_id": None,
}

DEFAULT_REFERRAL = {
    "enabled": True,
    "reward_days": 30,
    "text": "Поделись ссылкой и получи бонус после первой оплаты приглашённого пользователя.",
}

DEFAULT_REMINDERS = {
    "enabled": True,
    "hours_before_1": 72,
    "hours_before_2": 24,
    "hours_before_3": 1,
}

DEFAULT_START_BUTTONS = {
    "buy": True,
    "tariffs": True,
    "share": True,
    "support": True,
    "offer": False,
    "promo": False,
    "my_sub_after_buy": True,
    "custom_root": True,
}

DEFAULT_CUSTOM_NODE = {
    "id": 1,
    "parent_id": None,
    "title": "Новая кнопка",
    "enabled": False,
    "message_text": "Текст не настроен.",
    "media_type": None,
    "media_file_id": None,
    "sort_order": 1,
}

DEFAULT_GUIDES = {
    "start": (
        "🚀 <b>Гайд: старт и меню</b>\n\n"
        "1. Пользователь нажимает /start\n"
        "2. Видит кнопку «Начать»\n"
        "3. После нажатия открывается стартовое сообщение\n"
        "4. Снизу всегда есть «🏠 Меню»\n"
        "5. У админа дополнительно есть «⚙️ Админка»\n\n"
        "Настройки:\n"
        "• стартовый текст — «Контент бота»\n"
        "• стартовое фото/видео — «Контент бота»\n"
        "• видимые кнопки — «Кнопки стартового сообщения»"
    ),
    "start_buttons": (
        "🔘 <b>Гайд: кнопки стартового сообщения</b>\n\n"
        "Можно отдельно включать и выключать:\n"
        "• Купить доступ\n"
        "• Тарифы\n"
        "• Поделиться\n"
        "• Поддержка\n"
        "• Оферта\n"
        "• Промокод\n"
        "• Моя подписка после покупки\n"
        "• Пользовательские кнопки"
    ),
    "tariffs": (
        "💳 <b>Гайд: тарифы</b>\n\n"
        "Формат редактирования:\n"
        "<code>Название | цена_в_рублях | дни | active(1/0) | pseudo(off/choice/default_on)</code>\n\n"
        "pseudo:\n"
        "• off — без напоминания\n"
        "• choice — пользователь выбирает сам\n"
        "• default_on — напоминание включено по умолчанию"
    ),
    "offer": (
        "🛡 <b>Гайд: оферта</b>\n\n"
        "Настройки:\n"
        "• До оплаты — показывать до выбора тарифа\n"
        "• После оплаты — отправлять после оплаты\n"
        "• Требовать согласие — без подтверждения не пускать к оплате"
    ),
    "funnel": (
        "🪄 <b>Гайд: воронка</b>\n\n"
        "Воронка — это автосообщения тем, кто зашёл, но не купил.\n"
        "У каждого шага есть:\n"
        "• включение\n"
        "• задержка в часах\n"
        "• текст\n"
        "• фото/видео"
    ),
    "promo": (
        "🎁 <b>Гайд: промокоды</b>\n\n"
        "Форматы:\n"
        "<code>CODE | percent | 10 | 0 | 0 | 30</code>\n"
        "<code>CODE | amount | 500 | 1 | 100 | 10</code>"
    ),
    "referral": (
        "👥 <b>Гайд: рефералка</b>\n\n"
        "Пользователь получает ссылку через «Поделиться».\n"
        "Если приглашённый оплачивает доступ — пригласившему начисляются бонусные дни."
    ),
    "custom": (
        "🧩 <b>Гайд: пользовательские кнопки и подменю</b>\n\n"
        "У кнопки есть:\n"
        "• ID\n"
        "• название\n"
        "• текст сообщения\n"
        "• фото/видео\n"
        "• родительская кнопка\n"
        "• порядок\n"
        "• включение"
    ),
    "broadcast": (
        "📨 <b>Гайд: рассылки</b>\n\n"
        "Есть 2 вида:\n"
        "• текст\n"
        "• фото/видео с подписью"
    ),
    "manual": (
        "⚙️ <b>Гайд: ручное управление</b>\n\n"
        "Можно вручную:\n"
        "• выдать подписку\n"
        "• снять подписку\n"
        "• отправить ссылку\n"
        "Во всех полях можно использовать user_id или @username"
    ),
    "logs": (
        "🧾 <b>Гайд: логи и статистика</b>\n\n"
        "Статистика нужна для понимания продаж.\n"
        "Логи — для проверки действий пользователя."
    ),
    "troubleshoot": (
        "🛠 <b>Гайд: если что-то не работает</b>\n\n"
        "1. Проверь /start\n"
        "2. Проверь токены\n"
        "3. Проверь базу\n"
        "4. Проверь права бота в канале\n"
        "5. Если действие зависло — жми «⬅️ Назад», «🏠 Меню» или «♻️ Сбросить текущее действие»"
    ),
    "faq": (
        "❓ <b>FAQ для админа/менеджера</b>\n\n"
        "Q: Почему не приходит счёт?\nA: Проверь PAYMENTS_TOKEN.\n\n"
        "Q: Почему не заходит в канал?\nA: Проверь права бота и CHANNEL_ID.\n\n"
        "Q: Почему не ищет @username?\nA: Пользователь должен хотя бы раз написать боту."
    ),
    "recommended": (
        "⭐ <b>Рекомендуемые настройки</b>\n\n"
        "Быстрый старт:\n"
        "• 1 тариф\n"
        "• кнопки: купить, тарифы, поделиться, поддержка\n"
        "• 1 шаг воронки\n\n"
        "Жёсткая продажа:\n"
        "• оферта до оплаты\n"
        "• 2–3 шага воронки\n"
        "• активные промокоды\n\n"
        "Мягкая воронка:\n"
        "• без обязательной оферты до оплаты\n"
        "• 1–2 шага воронки\n"
        "• упор на поддержку"
    ),
    "one_or_three": (
        "🧭 <b>Один тариф / три тарифа</b>\n\n"
        "Один тариф:\n"
        "• включить только тариф 1\n"
        "• можно оставить кнопку «Купить доступ»\n\n"
        "Три тарифа:\n"
        "• включить кнопку «Тарифы»\n"
        "• показать различия по цене/сроку"
    ),
    "ads_checklist": (
        "📋 <b>Чек-лист перед запуском рекламы</b>\n\n"
        "1. Проверить /start\n"
        "2. Проверить покупку\n"
        "3. Проверить ссылку в канал\n"
        "4. Проверить кнопки\n"
        "5. Проверить оферту\n"
        "6. Проверить воронку\n"
        "7. Проверить промокод\n"
        "8. Проверить рефералку\n"
        "9. Проверить диагностику\n"
        "10. Проверить статистику"
    ),
}

BTN_BEGIN = "🚀 Начать"
BTN_MENU = "🏠 Меню"
BTN_ADMIN = "⚙️ Админка"
BTN_BACK = "⬅️ Назад"
BTN_EXIT_YES = "✅ Да, выйти"
BTN_EXIT_NO = "↩️ Нет, остаться"

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
db_pool: Optional[asyncpg.Pool] = None


class AdminStates(StatesGroup):
    set_start_text = State()
    set_start_media = State()

    edit_tariff_data = State()

    set_offer_text = State()
    set_offer_media = State()

    set_share_text = State()
    set_share_media = State()

    set_homework_text = State()
    set_homework_media = State()

    set_funnel_time = State()
    set_funnel_step_text = State()
    set_funnel_step_media = State()
    set_funnel_step_delay = State()

    promo_create = State()
    promo_delete = State()

    answer_pick_user = State()
    answer_message = State()

    manual_add_sub_user = State()
    manual_add_sub_days = State()
    manual_remove_sub_user = State()
    manual_invite_user = State()

    find_user = State()
    logs_user = State()

    broadcast_text = State()
    broadcast_media_caption = State()
    broadcast_media_file = State()

    set_reminders = State()

    add_admin_user = State()
    remove_admin_user = State()

    refcfg = State()

    custom_node_title = State()
    custom_node_text = State()
    custom_node_media = State()
    custom_node_parent = State()
    custom_node_sort = State()
    custom_node_toggle = State()

    guide_edit_pick_key = State()
    guide_edit_text = State()

    diagnostic_user = State()
    diagnostic_kick_user = State()
    diagnostic_invite_user = State()


class UserStates(StatesGroup):
    support_message = State()
    question_content = State()
    homework_message = State()
    enter_promo = State()


class ExitConfirmStates(StatesGroup):
    waiting = State()


def now() -> datetime:
    return datetime.now()


def get_pool() -> asyncpg.Pool:
    if db_pool is None:
        raise RuntimeError("db_pool не инициализирован")
    return db_pool


def dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def loads(data: Optional[str], default: Any):
    if not data:
        return default
    try:
        return json.loads(data)
    except Exception:
        return default


def format_rub_from_kop(kop: int) -> str:
    rub = kop / 100
    return f"{int(rub)}₽" if float(rub).is_integer() else f"{rub:.2f}₽"


def reply_begin_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_BEGIN)]],
        resize_keyboard=True,
        is_persistent=True,
    )


def reply_menu_kb(is_admin_user: bool):
    rows = [[KeyboardButton(text=BTN_MENU)]]
    if is_admin_user:
        rows.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        is_persistent=True,
    )


def reply_back_kb(is_admin_user: bool):
    rows = [[KeyboardButton(text=BTN_BACK)]]
    rows.append([KeyboardButton(text=BTN_MENU)])
    if is_admin_user:
        rows.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        is_persistent=True,
    )


def exit_confirm_kb(is_admin_user: bool):
    rows = [
        [KeyboardButton(text=BTN_EXIT_YES), KeyboardButton(text=BTN_EXIT_NO)],
        [KeyboardButton(text=BTN_MENU)],
    ]
    if is_admin_user:
        rows.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        is_persistent=True,
    )


async def log_action(user_id: Optional[int], action: str, details: str = ""):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(
                """
                INSERT INTO action_logs (user_id, action, details, created_at)
                VALUES ($1, $2, $3, NOW())
                """,
                user_id,
                action,
                details[:3000],
            )
    except Exception:
        logger.exception("LOG ACTION ERROR")


async def init_db():
    global db_pool

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан")

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

    async with db_pool.acquire() as conn:
        logger.info("NEON OK: %s", await conn.fetchval("SELECT version()"))

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                expire_date TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)
        await conn.execute("""ALTER TABLE IF EXISTS users ALTER COLUMN expire_date DROP NOT NULL;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS first_name TEXT;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS username TEXT;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS started BOOLEAN NOT NULL DEFAULT FALSE;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS started_at TIMESTAMP;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS has_purchased BOOLEAN NOT NULL DEFAULT FALSE;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS active_tariff_id INT;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS last_paid_tariff_id INT;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS pseudo_autorenew_enabled BOOLEAN NOT NULL DEFAULT FALSE;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS referrer_id BIGINT;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS referral_rewarded BOOLEAN NOT NULL DEFAULT FALSE;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS accepted_offer_at TIMESTAMP;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS applied_promo_code TEXT;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS pending_tariff_id INT;""")
        await conn.execute("""ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS pending_pseudo_autorenew BOOLEAN NOT NULL DEFAULT FALSE;""")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                added_at TIMESTAMP NOT NULL DEFAULT NOW(),
                added_by BIGINT NOT NULL
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS questions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                type TEXT NOT NULL,
                text TEXT,
                content_type TEXT NOT NULL DEFAULT 'text',
                file_id TEXT,
                caption TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL DEFAULT 'open'
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                discount_percent INT,
                discount_kop INT,
                tariff_id INT,
                max_uses INT,
                used_count INT NOT NULL DEFAULT 0,
                expires_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS action_logs (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                action TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id BIGSERIAL PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                referred_user_id BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE(referrer_id, referred_user_id)
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS funnel_sends (
                user_id BIGINT NOT NULL,
                step_idx INT NOT NULL,
                sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY(user_id, step_idx)
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reminder_sends (
                user_id BIGINT NOT NULL,
                reminder_key TEXT NOT NULL,
                sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY(user_id, reminder_key)
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS custom_nodes (
                id BIGINT PRIMARY KEY,
                parent_id BIGINT,
                title TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT FALSE,
                message_text TEXT,
                media_type TEXT,
                media_file_id TEXT,
                sort_order INT NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        defaults = {
            "start_text": DEFAULT_START_TEXT,
            "start_media_type": None,
            "start_media_file_id": None,
            "tariffs_json": dumps(DEFAULT_TARIFFS),
            "offer_json": dumps(DEFAULT_OFFER),
            "funnel_json": dumps(DEFAULT_FUNNEL),
            "share_json": dumps(DEFAULT_SHARE),
            "homework_json": dumps(DEFAULT_HOMEWORK),
            "referral_json": dumps(DEFAULT_REFERRAL),
            "reminders_json": dumps(DEFAULT_REMINDERS),
            "start_buttons_json": dumps(DEFAULT_START_BUTTONS),
            "guides_json": dumps(DEFAULT_GUIDES),
        }

        for key, value in defaults.items():
            await conn.execute(
                """
                INSERT INTO settings (key, value)
                VALUES ($1, $2)
                ON CONFLICT (key) DO NOTHING
                """,
                key,
                value,
            )

        custom_exists = await conn.fetchval("SELECT 1 FROM custom_nodes LIMIT 1")
        if not custom_exists:
            node = DEFAULT_CUSTOM_NODE
            await conn.execute(
                """
                INSERT INTO custom_nodes (id, parent_id, title, enabled, message_text, media_type, media_file_id, sort_order)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT DO NOTHING
                """,
                node["id"],
                node["parent_id"],
                node["title"],
                node["enabled"],
                node["message_text"],
                node["media_type"],
                node["media_file_id"],
                node["sort_order"],
            )


async def get_setting(key: str) -> Optional[str]:
    async with get_pool().acquire() as conn:
        return await conn.fetchval("SELECT value FROM settings WHERE key = $1", key)


async def set_setting(key: str, value: Optional[str]):
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO settings (key, value)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            key,
            value,
        )


async def get_json_setting(key: str, default: Any):
    return loads(await get_setting(key), default)


async def set_json_setting(key: str, value: Any):
    await set_setting(key, dumps(value))


async def get_start_buttons():
    return await get_json_setting("start_buttons_json", DEFAULT_START_BUTTONS)


async def get_guides():
    return await get_json_setting("guides_json", DEFAULT_GUIDES)


async def save_guides(data: dict):
    await set_json_setting("guides_json", data)


async def is_admin(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    async with get_pool().acquire() as conn:
        return bool(await conn.fetchval("SELECT 1 FROM admins WHERE user_id = $1", user_id))


async def is_general_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


async def add_admin(user_id: int, added_by: int):
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO admins (user_id, added_at, added_by)
            VALUES ($1, NOW(), $2)
            ON CONFLICT (user_id) DO NOTHING
            """,
            user_id,
            added_by,
        )


async def remove_admin(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return False
    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)
    return True


async def list_admins():
    async with get_pool().acquire() as conn:
        return await conn.fetch(
            """
            SELECT user_id, added_at, added_by
            FROM admins
            ORDER BY added_at DESC
            """
        )


async def ensure_user_exists(user: Message | CallbackQuery | Any, start_ref: Optional[int] = None):
    tg_user = user.from_user if hasattr(user, "from_user") else user
    user_id = tg_user.id
    first_name = tg_user.first_name
    username = tg_user.username

    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, expire_date, created_at, first_name, username, last_seen_at)
            VALUES ($1, NULL, NOW(), $2, $3, NOW())
            ON CONFLICT (user_id) DO NOTHING
            """,
            user_id,
            first_name,
            username,
        )
        await conn.execute(
            """
            UPDATE users
            SET first_name = $2,
                username = $3,
                last_seen_at = NOW()
            WHERE user_id = $1
            """,
            user_id,
            first_name,
            username,
        )

        if start_ref and start_ref != user_id:
            current_ref = await conn.fetchval("SELECT referrer_id FROM users WHERE user_id = $1", user_id)
            if not current_ref:
                await conn.execute("UPDATE users SET referrer_id = $2 WHERE user_id = $1", user_id, start_ref)
                await conn.execute(
                    """
                    INSERT INTO referrals (referrer_id, referred_user_id)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    start_ref,
                    user_id,
                )


async def mark_started(user_id: int):
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE users
            SET started = TRUE,
                started_at = COALESCE(started_at, NOW()),
                last_seen_at = NOW()
            WHERE user_id = $1
            """,
            user_id,
        )


async def get_user(user_id: int):
    async with get_pool().acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)


async def resolve_user_input_to_id(raw: str) -> Optional[int]:
    value = (raw or "").strip()
    if not value:
        return None
    if value.startswith("@"):
        username = value[1:].strip().lower()
        async with get_pool().acquire() as conn:
            return await conn.fetchval(
                """
                SELECT user_id
                FROM users
                WHERE LOWER(username) = $1
                ORDER BY last_seen_at DESC NULLS LAST, created_at DESC
                LIMIT 1
                """,
                username,
            )
    try:
        return int(value)
    except Exception:
        return None


async def has_active_sub(user_id: int) -> bool:
    row = await get_user(user_id)
    return bool(row and row["expire_date"] and row["expire_date"] > now())


async def add_sub_days(
    user_id: int,
    days: int,
    tariff_id: Optional[int] = None,
    pseudo_autorenew: Optional[bool] = None,
):
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT expire_date FROM users WHERE user_id = $1", user_id)
        current = now()
        base = row["expire_date"] if row and row["expire_date"] and row["expire_date"] > current else current
        expire = base + timedelta(days=days)
        await conn.execute(
            """
            UPDATE users
            SET expire_date = $2,
                has_purchased = TRUE,
                active_tariff_id = COALESCE($3, active_tariff_id),
                last_paid_tariff_id = COALESCE($3, last_paid_tariff_id),
                pseudo_autorenew_enabled = COALESCE($4, pseudo_autorenew_enabled)
            WHERE user_id = $1
            """,
            user_id,
            expire,
            tariff_id,
            pseudo_autorenew,
        )


async def remove_sub(user_id: int):
    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE users SET expire_date = NULL WHERE user_id = $1", user_id)


async def kick_user_from_channel(user_id: int) -> tuple[bool, str]:
    try:
        await bot.ban_chat_member(CHANNEL_ID, user_id)
        await bot.unban_chat_member(CHANNEL_ID, user_id)
        return True, "Пользователь исключён из канала."
    except Exception as e:
        logger.exception("KICK USER ERROR: %s", e)
        return False, str(e)


async def get_tariffs():
    return sorted(await get_json_setting("tariffs_json", DEFAULT_TARIFFS), key=lambda x: x["id"])


async def save_tariffs(tariffs: list[dict]):
    await set_json_setting("tariffs_json", tariffs)


async def get_tariff_by_id(tariff_id: int):
    for t in await get_tariffs():
        if t["id"] == tariff_id:
            return t
    return None


async def get_offer():
    return await get_json_setting("offer_json", DEFAULT_OFFER)


async def get_funnel():
    return await get_json_setting("funnel_json", DEFAULT_FUNNEL)


async def get_share():
    return await get_json_setting("share_json", DEFAULT_SHARE)


async def get_homework():
    return await get_json_setting("homework_json", DEFAULT_HOMEWORK)


async def get_referral_settings():
    return await get_json_setting("referral_json", DEFAULT_REFERRAL)


async def get_reminders():
    return await get_json_setting("reminders_json", DEFAULT_REMINDERS)


async def get_custom_nodes(parent_id: Optional[int] = None):
    async with get_pool().acquire() as conn:
        if parent_id is None:
            rows = await conn.fetch(
                """
                SELECT *
                FROM custom_nodes
                WHERE parent_id IS NULL
                ORDER BY sort_order ASC, id ASC
                """
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM custom_nodes
                WHERE parent_id = $1
                ORDER BY sort_order ASC, id ASC
                """,
                parent_id,
            )
        return rows


async def get_custom_node(node_id: int):
    async with get_pool().acquire() as conn:
        return await conn.fetchrow("SELECT * FROM custom_nodes WHERE id = $1", node_id)


async def next_custom_node_id() -> int:
    async with get_pool().acquire() as conn:
        val = await conn.fetchval("SELECT COALESCE(MAX(id), 0) + 1 FROM custom_nodes")
        return int(val)


def build_start_inline_kb(
    has_purchase: bool,
    is_admin_user: bool,
    start_buttons: dict,
    custom_nodes: list,
):
    rows = []

    if start_buttons.get("buy", True):
        rows.append([InlineKeyboardButton(text="💳 Купить доступ", callback_data="buy_entry")])
    if start_buttons.get("tariffs", True):
        rows.append([InlineKeyboardButton(text="📦 Тарифы", callback_data="show_tariffs")])
    if start_buttons.get("share", True):
        rows.append([InlineKeyboardButton(text="📣 Поделиться", callback_data="show_share")])
    if start_buttons.get("support", True):
        rows.append([InlineKeyboardButton(text="🛠 Поддержка", callback_data="support_open")])
    if start_buttons.get("offer", False):
        rows.append([InlineKeyboardButton(text="🛡 Оферта", callback_data="show_offer")])
    if start_buttons.get("promo", False):
        rows.append([InlineKeyboardButton(text="🎁 Промокод", callback_data="enter_promo_inline")])
    if has_purchase and start_buttons.get("my_sub_after_buy", True):
        rows.append([InlineKeyboardButton(text="📅 Моя подписка", callback_data="my_sub")])

    if start_buttons.get("custom_root", True):
        for node in custom_nodes:
            if node["enabled"]:
                rows.append([InlineKeyboardButton(text=node["title"], callback_data=f"cnode:{node['id']}")])

    if is_admin_user:
        rows.append([InlineKeyboardButton(text="⚙️ Админка", callback_data="open_admin_panel")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def tariffs_kb(tariffs: list[dict]):
    rows = []
    for t in tariffs:
        if t.get("active"):
            rows.append([
                InlineKeyboardButton(
                    text=f"{t['name']} — {format_rub_from_kop(int(t['price_kop']))} / {t['days']} дн.",
                    callback_data=f"tariff:{t['id']}",
                )
            ])
    rows.append([InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="enter_promo_inline")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="go_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tariff_buy_kb(tariff: dict):
    pseudo = tariff.get("pseudo_mode", "choice")
    rows = []
    if pseudo == "off":
        rows.append([InlineKeyboardButton(text="💳 Оплатить", callback_data=f"pay:{tariff['id']}:0")])
    elif pseudo == "default_on":
        rows.append([InlineKeyboardButton(text="💳 Оплатить + напоминание о продлении", callback_data=f"pay:{tariff['id']}:1")])
        rows.append([InlineKeyboardButton(text="💳 Оплатить без напоминания", callback_data=f"pay:{tariff['id']}:0")])
    else:
        rows.append([InlineKeyboardButton(text="💳 Оплатить", callback_data=f"pay:{tariff['id']}:0")])
        rows.append([InlineKeyboardButton(text="🔔 Оплатить + напоминание о продлении", callback_data=f"pay:{tariff['id']}:1")])
    rows.append([InlineKeyboardButton(text="⬅️ К тарифам", callback_data="show_tariffs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def my_sub_kb(active: bool, reminder_enabled: bool):
    rows = []
    if active:
        rows.append([InlineKeyboardButton(text="🔗 Войти в канал", callback_data="sub_enter_channel")])
        rows.append([InlineKeyboardButton(text="💳 Продлить сейчас", callback_data="show_tariffs")])
        rows.append([
            InlineKeyboardButton(
                text="🔔 Отключить напоминание о продлении" if reminder_enabled else "🔔 Включить напоминание о продлении",
                callback_data="toggle_pseudo",
            )
        ])
        rows.append([
            InlineKeyboardButton(text="❓ Вопрос по контенту", callback_data="ask_content"),
            InlineKeyboardButton(text="📝 Домашнее задание", callback_data="ask_homework"),
        ])
        rows.append([InlineKeyboardButton(text="🛠 Поддержка", callback_data="support_open")])
        rows.append([InlineKeyboardButton(text="🛡 Оферта", callback_data="show_offer")])
    else:
        rows.append([InlineKeyboardButton(text="💳 Купить доступ", callback_data="buy_entry")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="go_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats_menu")],
            [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users_menu")],
            [InlineKeyboardButton(text="❓ Очередь вопросов", callback_data="admin_questions_menu")],
            [InlineKeyboardButton(text="💬 Ответы", callback_data="admin_answers_menu")],
            [InlineKeyboardButton(text="💳 Тарифы", callback_data="admin_tariffs")],
            [InlineKeyboardButton(text="🎁 Промокоды", callback_data="admin_promo_menu")],
            [InlineKeyboardButton(text="👥 Рефералка", callback_data="admin_referral_menu")],
            [InlineKeyboardButton(text="🛡 Оферта", callback_data="admin_offer_menu")],
            [InlineKeyboardButton(text="🪄 Воронка", callback_data="admin_funnel_menu")],
            [InlineKeyboardButton(text="🔔 Напоминания", callback_data="admin_reminders")],
            [InlineKeyboardButton(text="📣 Контент бота", callback_data="admin_content_menu")],
            [InlineKeyboardButton(text="🔘 Кнопки стартового сообщения", callback_data="admin_start_buttons")],
            [InlineKeyboardButton(text="🧩 Пользовательские кнопки и подменю", callback_data="admin_custom_menu")],
            [InlineKeyboardButton(text="📨 Рассылки", callback_data="admin_broadcast_menu")],
            [InlineKeyboardButton(text="👮 Админы", callback_data="admin_admins_menu")],
            [InlineKeyboardButton(text="🧾 Логи", callback_data="admin_logs_menu")],
            [InlineKeyboardButton(text="🩺 Диагностика", callback_data="admin_diagnostics_menu")],
            [InlineKeyboardButton(text="⚙️ Ручное управление", callback_data="admin_manual_menu")],
            [InlineKeyboardButton(text="📚 Гайды", callback_data="admin_guides_menu")],
            [InlineKeyboardButton(text="♻️ Сбросить текущее действие", callback_data="admin_force_reset")],
        ]
    )


def admin_question_reply_kb(question_id: int, user_id: int, q_type: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Ответить", callback_data=f"replyq:{question_id}:{user_id}:{q_type}")]
        ]
    )


def admin_guides_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Старт и меню", callback_data="guide:start")],
            [InlineKeyboardButton(text="🔘 Кнопки стартового сообщения", callback_data="guide:start_buttons")],
            [InlineKeyboardButton(text="💳 Тарифы", callback_data="guide:tariffs")],
            [InlineKeyboardButton(text="🛡 Оферта", callback_data="guide:offer")],
            [InlineKeyboardButton(text="🪄 Воронка", callback_data="guide:funnel")],
            [InlineKeyboardButton(text="🎁 Промокоды", callback_data="guide:promo")],
            [InlineKeyboardButton(text="👥 Рефералка", callback_data="guide:referral")],
            [InlineKeyboardButton(text="🧩 Пользовательские кнопки", callback_data="guide:custom")],
            [InlineKeyboardButton(text="📨 Рассылки", callback_data="guide:broadcast")],
            [InlineKeyboardButton(text="⚙️ Ручное управление", callback_data="guide:manual")],
            [InlineKeyboardButton(text="🧾 Логи и статистика", callback_data="guide:logs")],
            [InlineKeyboardButton(text="❓ FAQ", callback_data="guide:faq")],
            [InlineKeyboardButton(text="⭐ Рекомендуемые настройки", callback_data="guide:recommended")],
            [InlineKeyboardButton(text="🧭 Один тариф / три тарифа", callback_data="guide:one_or_three")],
            [InlineKeyboardButton(text="📋 Чек-лист рекламы", callback_data="guide:ads_checklist")],
            [InlineKeyboardButton(text="🛠 Если что-то не работает", callback_data="guide:troubleshoot")],
            [InlineKeyboardButton(text="✏️ Редактировать гайд", callback_data="guide_edit_menu")],
        ]
    )


async def extract_payload_from_message(message: Message):
    if message.text:
        return {"content_type": "text", "text": message.html_text or message.text, "file_id": None, "caption": None}
    if message.photo:
        return {"content_type": "photo", "text": None, "file_id": message.photo[-1].file_id, "caption": message.html_text or message.caption}
    if message.video:
        return {"content_type": "video", "text": None, "file_id": message.video.file_id, "caption": message.html_text or message.caption}
    if message.voice:
        return {"content_type": "voice", "text": None, "file_id": message.voice.file_id, "caption": message.html_text or message.caption}
    if message.video_note:
        return {"content_type": "video_note", "text": None, "file_id": message.video_note.file_id, "caption": None}
    if message.document:
        return {"content_type": "document", "text": None, "file_id": message.document.file_id, "caption": message.html_text or message.caption}
    return None


async def send_payload(chat_id: int, payload: dict, prefix: Optional[str] = None, reply_markup=None):
    content_type = payload.get("content_type")
    text = payload.get("text")
    file_id = payload.get("file_id")
    caption = payload.get("caption")

    if content_type == "text":
        body = text or ""
        if prefix:
            body = f"{prefix}\n\n{body}" if body else prefix
        await bot.send_message(chat_id, body or "—", reply_markup=reply_markup)
        return

    if content_type == "photo":
        cap = caption or ""
        if prefix:
            cap = f"{prefix}\n\n{cap}" if cap else prefix
        await bot.send_photo(chat_id, photo=file_id, caption=cap or None, reply_markup=reply_markup)
        return

    if content_type == "video":
        cap = caption or ""
        if prefix:
            cap = f"{prefix}\n\n{cap}" if cap else prefix
        await bot.send_video(chat_id, video=file_id, caption=cap or None, reply_markup=reply_markup)
        return

    if content_type == "voice":
        cap = caption or ""
        if prefix:
            await bot.send_message(chat_id, prefix)
        await bot.send_voice(chat_id, voice=file_id, caption=cap or None, reply_markup=reply_markup)
        return

    if content_type == "video_note":
        if prefix:
            await bot.send_message(chat_id, prefix)
        await bot.send_video_note(chat_id, video_note=file_id)
        if reply_markup:
            await bot.send_message(chat_id, "Действия:", reply_markup=reply_markup)
        return

    if content_type == "document":
        cap = caption or ""
        if prefix:
            cap = f"{prefix}\n\n{cap}" if cap else prefix
        await bot.send_document(chat_id, document=file_id, caption=cap or None, reply_markup=reply_markup)
        return

    await bot.send_message(chat_id, prefix or "Сообщение")


async def save_question(user_id: int, username: Optional[str], full_name: Optional[str], q_type: str, payload: dict):
    async with get_pool().acquire() as conn:
        return await conn.fetchval(
            """
            INSERT INTO questions (user_id, username, full_name, type, text, content_type, file_id, caption)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            """,
            user_id,
            username,
            full_name,
            q_type,
            payload.get("text"),
            payload.get("content_type", "text"),
            payload.get("file_id"),
            payload.get("caption"),
        )


async def close_user_questions(user_id: int):
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE questions SET status = 'answered' WHERE user_id = $1 AND status = 'open'",
            user_id,
        )


async def get_open_questions_by_type(q_type: str, limit: int = 20):
    async with get_pool().acquire() as conn:
        return await conn.fetch(
            """
            SELECT *
            FROM questions
            WHERE type = $1 AND status = 'open'
            ORDER BY created_at DESC
            LIMIT $2
            """,
            q_type,
            limit,
        )


async def send_start_screen(chat_id: int, user_id: int):
    row = await get_user(user_id)
    start_text = await get_setting("start_text") or DEFAULT_START_TEXT
    media_type = await get_setting("start_media_type")
    media_file_id = await get_setting("start_media_file_id")
    start_buttons = await get_start_buttons()
    admin_user = await is_admin(user_id)
    custom_nodes = await get_custom_nodes(None)

    inline_markup = build_start_inline_kb(
        has_purchase=bool(row and row["has_purchased"]),
        is_admin_user=admin_user,
        start_buttons=start_buttons,
        custom_nodes=custom_nodes,
    )

    if media_type == "photo" and media_file_id:
        await bot.send_photo(chat_id, media_file_id, caption=start_text, reply_markup=inline_markup)
    elif media_type == "video" and media_file_id:
        await bot.send_video(chat_id, media_file_id, caption=start_text, reply_markup=inline_markup)
    else:
        await bot.send_message(chat_id, start_text, reply_markup=inline_markup)

    await bot.send_message(chat_id, "Нижнее меню активно.", reply_markup=reply_menu_kb(admin_user))


async def send_offer(chat_id: int, with_agree_button: bool = False):
    offer = await get_offer()
    kb = None
    if with_agree_button:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Согласен с офертой", callback_data="offer_accept_and_pay")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="go_main")],
            ]
        )
    payload = {
        "content_type": offer.get("media_type") or "text",
        "text": offer.get("text"),
        "file_id": offer.get("media_file_id"),
        "caption": offer.get("text"),
    }
    await send_payload(chat_id, payload, reply_markup=kb)


async def send_share_message(chat_id: int, user_id: int):
    share = await get_share()
    ref = await get_referral_settings()
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{user_id}"
    prefix = f"{share['text']}\n\n{ref['text']}\n\n<b>Твоя ссылка:</b>\n{link}"
    payload = {
        "content_type": share.get("media_type") or "text",
        "text": share.get("text"),
        "file_id": share.get("media_file_id"),
        "caption": share.get("text"),
    }
    await send_payload(chat_id, payload, prefix=prefix)


async def send_homework_intro(chat_id: int):
    hw = await get_homework()
    payload = {
        "content_type": hw.get("media_type") or "text",
        "text": hw.get("text"),
        "file_id": hw.get("media_file_id"),
        "caption": hw.get("text"),
    }
    await send_payload(chat_id, payload)


async def build_user_stats(user_id: int):
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not row:
            return None
        logs_cnt = await conn.fetchval("SELECT COUNT(*) FROM action_logs WHERE user_id = $1", user_id)
        q_cnt = await conn.fetchval("SELECT COUNT(*) FROM questions WHERE user_id = $1", user_id)
        return row, logs_cnt, q_cnt


async def get_stats():
    async with get_pool().acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        started = await conn.fetchval("SELECT COUNT(*) FROM users WHERE started = TRUE")
        purchased = await conn.fetchval("SELECT COUNT(*) FROM users WHERE has_purchased = TRUE")
        not_purchased = await conn.fetchval("SELECT COUNT(*) FROM users WHERE started = TRUE AND has_purchased = FALSE")
        active = await conn.fetchval("SELECT COUNT(*) FROM users WHERE expire_date IS NOT NULL AND expire_date > NOW()")
        expired = await conn.fetchval("SELECT COUNT(*) FROM users WHERE expire_date IS NOT NULL AND expire_date <= NOW()")
        pseudo = await conn.fetchval("SELECT COUNT(*) FROM users WHERE pseudo_autorenew_enabled = TRUE")
        offer_opened = await conn.fetchval("SELECT COUNT(*) FROM action_logs WHERE action = 'offer_open'")
        offer_accepted = await conn.fetchval("SELECT COUNT(*) FROM users WHERE accepted_offer_at IS NOT NULL")
        tariffs = await conn.fetch(
            """
            SELECT last_paid_tariff_id, COUNT(*) cnt
            FROM users
            WHERE last_paid_tariff_id IS NOT NULL
            GROUP BY last_paid_tariff_id
            ORDER BY last_paid_tariff_id
            """
        )
        return {
            "total": total,
            "started": started,
            "purchased": purchased,
            "not_purchased": not_purchased,
            "active": active,
            "expired": expired,
            "pseudo": pseudo,
            "offer_opened": offer_opened,
            "offer_accepted": offer_accepted,
            "tariffs": tariffs,
        }


async def apply_promo_price(user_id: int, tariff: dict):
    row = await get_user(user_id)
    base = int(tariff["price_kop"])
    code = row["applied_promo_code"] if row else None
    if not code:
        return base, None

    async with get_pool().acquire() as conn:
        promo = await conn.fetchrow("SELECT * FROM promo_codes WHERE code = $1 AND active = TRUE", code)

    if not promo:
        return base, None
    if promo["expires_at"] and promo["expires_at"] < now():
        return base, None
    if promo["max_uses"] is not None and promo["used_count"] >= promo["max_uses"]:
        return base, None
    if promo["tariff_id"] is not None and promo["tariff_id"] != tariff["id"]:
        return base, None

    final_price = base
    if promo["discount_percent"]:
        final_price = int(base * (100 - promo["discount_percent"]) / 100)
    if promo["discount_kop"]:
        final_price = max(100, base - int(promo["discount_kop"]))
    return final_price, promo["code"]


async def send_invoice_for_tariff(user_id: int, tariff_id: int, pseudo_autorenew: bool):
    tariff = await get_tariff_by_id(tariff_id)
    if not tariff or not tariff.get("active"):
        await bot.send_message(user_id, "Тариф недоступен.")
        return

    final_price, promo_code = await apply_promo_price(user_id, tariff)

    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE users
            SET pending_tariff_id = $2,
                pending_pseudo_autorenew = $3
            WHERE user_id = $1
            """,
            user_id,
            tariff_id,
            pseudo_autorenew,
        )

    label = f"{tariff['name']} / {tariff['days']} дней"
    if promo_code:
        label += f" / promo {promo_code}"

    await bot.send_invoice(
        chat_id=user_id,
        title=f"Доступ: {tariff['name']}",
        description=f"Тариф {tariff['name']} на {tariff['days']} дней",
        payload=f"tariff:{tariff_id}:{1 if pseudo_autorenew else 0}",
        provider_token=PAYMENTS_TOKEN,
        currency="RUB",
        prices=[LabeledPrice(label=label, amount=final_price)],
        start_parameter=f"tariff_{tariff_id}",
    )
    await log_action(user_id, "invoice_sent", f"tariff={tariff_id};reminder={pseudo_autorenew};price={final_price}")


async def send_invite_to_user(user_id: int):
    link = await bot.create_chat_invite_link(
        chat_id=CHANNEL_ID,
        member_limit=1,
        expire_date=now() + timedelta(minutes=15),
    )
    await bot.send_message(
        user_id,
        f"🔗 Ссылка для входа в канал:\n{link.invite_link}\n\nСсылка действует 15 минут.",
    )


async def ask_exit_confirmation(message: Message, state: FSMContext, destination: str):
    data = await state.get_data()
    data["exit_destination"] = destination
    data["return_state"] = await state.get_state()
    await state.set_data(data)
    await state.set_state(ExitConfirmStates.waiting)
    is_admin_user = await is_admin(message.from_user.id)
    await message.answer(
        "Ты сейчас в процессе действия.\nВыйти без сохранения?",
        reply_markup=exit_confirm_kb(is_admin_user),
    )


@dp.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    await state.clear()
    ref = None
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1 and parts[1].startswith("ref_"):
            try:
                ref = int(parts[1].replace("ref_", "").strip())
            except Exception:
                ref = None

    await ensure_user_exists(message, ref)
    await message.answer(
        "Нажми кнопку ниже, чтобы открыть меню.",
        reply_markup=reply_begin_kb(),
    )
    await log_action(message.from_user.id, "start", f"ref={ref}")


@dp.message(F.text == BTN_BEGIN)
async def begin_btn(message: Message):
    await ensure_user_exists(message)
    await mark_started(message.from_user.id)
    await send_start_screen(message.chat.id, message.from_user.id)
    await log_action(message.from_user.id, "begin")


@dp.message(StateFilter("*"), F.text == BTN_MENU)
async def universal_menu_btn(message: Message, state: FSMContext):
    current = await state.get_state()
    if current and current != ExitConfirmStates.waiting.state:
        await ask_exit_confirmation(message, state, "menu")
        return
    await state.clear()
    await ensure_user_exists(message)
    await send_start_screen(message.chat.id, message.from_user.id)
    await log_action(message.from_user.id, "menu_open")


@dp.message(StateFilter("*"), F.text == BTN_ADMIN)
async def universal_admin_btn(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return
    current = await state.get_state()
    if current and current != ExitConfirmStates.waiting.state:
        await ask_exit_confirmation(message, state, "admin")
        return
    await state.clear()
    await message.answer("⚙️ <b>Админка</b>", reply_markup=admin_kb())
    await message.answer("Нижнее меню обновлено.", reply_markup=reply_menu_kb(True))


@dp.message(ExitConfirmStates.waiting, F.text == BTN_EXIT_YES)
async def exit_confirm_yes(message: Message, state: FSMContext):
    data = await state.get_data()
    destination = data.get("exit_destination")
    await state.clear()
    if destination == "admin" and await is_admin(message.from_user.id):
        await message.answer("⚙️ <b>Админка</b>", reply_markup=admin_kb())
        await message.answer("Нижнее меню обновлено.", reply_markup=reply_menu_kb(True))
        return
    await send_start_screen(message.chat.id, message.from_user.id)


@dp.message(ExitConfirmStates.waiting, F.text == BTN_EXIT_NO)
async def exit_confirm_no(message: Message, state: FSMContext):
    data = await state.get_data()
    old_state = data.get("return_state")
    if old_state:
        await state.set_state(old_state)
    else:
        await state.clear()
    is_admin_user = await is_admin(message.from_user.id)
    await message.answer("Продолжаем текущее действие.", reply_markup=reply_back_kb(is_admin_user))


@dp.message(StateFilter("*"), F.text == BTN_BACK)
async def universal_back_btn(message: Message, state: FSMContext):
    current = await state.get_state()
    if current in {
        UserStates.enter_promo.state,
        UserStates.support_message.state,
        UserStates.question_content.state,
        UserStates.homework_message.state,
        AdminStates.answer_pick_user.state,
        AdminStates.answer_message.state,
        AdminStates.find_user.state,
        AdminStates.logs_user.state,
        AdminStates.broadcast_text.state,
        AdminStates.broadcast_media_caption.state,
        AdminStates.broadcast_media_file.state,
        AdminStates.manual_add_sub_user.state,
        AdminStates.manual_add_sub_days.state,
        AdminStates.manual_remove_sub_user.state,
        AdminStates.manual_invite_user.state,
        AdminStates.add_admin_user.state,
        AdminStates.remove_admin_user.state,
        AdminStates.set_offer_text.state,
        AdminStates.set_offer_media.state,
        AdminStates.set_share_text.state,
        AdminStates.set_share_media.state,
        AdminStates.set_homework_text.state,
        AdminStates.set_homework_media.state,
        AdminStates.set_start_text.state,
        AdminStates.set_start_media.state,
        AdminStates.edit_tariff_data.state,
        AdminStates.refcfg.state,
        AdminStates.promo_create.state,
        AdminStates.promo_delete.state,
        AdminStates.set_reminders.state,
        AdminStates.set_funnel_time.state,
        AdminStates.set_funnel_step_delay.state,
        AdminStates.set_funnel_step_text.state,
        AdminStates.set_funnel_step_media.state,
        AdminStates.custom_node_title.state,
        AdminStates.custom_node_text.state,
        AdminStates.custom_node_media.state,
        AdminStates.custom_node_parent.state,
        AdminStates.custom_node_sort.state,
        AdminStates.custom_node_toggle.state,
        AdminStates.guide_edit_pick_key.state,
        AdminStates.guide_edit_text.state,
        AdminStates.diagnostic_user.state,
        AdminStates.diagnostic_kick_user.state,
        AdminStates.diagnostic_invite_user.state,
    }:
        await state.clear()
        await send_start_screen(message.chat.id, message.from_user.id)
        return

    await send_start_screen(message.chat.id, message.from_user.id)


@dp.callback_query(F.data == "open_admin_panel")
async def open_admin_panel(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await callback.message.answer("⚙️ <b>Админка</b>", reply_markup=admin_kb())
    await bot.send_message(callback.from_user.id, "Нижнее меню обновлено.", reply_markup=reply_menu_kb(True))
    await callback.answer()


@dp.callback_query(F.data == "go_main")
async def go_main(callback: CallbackQuery):
    await send_start_screen(callback.from_user.id, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "show_tariffs")
async def show_tariffs(callback: CallbackQuery):
    tariffs = await get_tariffs()
    await callback.message.answer("📦 <b>Доступные тарифы</b>", reply_markup=tariffs_kb(tariffs))
    await callback.answer()


@dp.callback_query(F.data == "buy_entry")
async def buy_entry(callback: CallbackQuery):
    offer = await get_offer()
    if offer.get("enabled_before_pay"):
        await log_action(callback.from_user.id, "offer_open", "before_pay")
        if offer.get("require_accept_before_pay"):
            await send_offer(callback.from_user.id, with_agree_button=True)
            await callback.answer()
            return
        await send_offer(callback.from_user.id)

    tariffs = await get_tariffs()
    await callback.message.answer("Выбери тариф:", reply_markup=tariffs_kb(tariffs))
    await callback.answer()


@dp.callback_query(F.data == "offer_accept_and_pay")
async def offer_accept_and_pay(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE users SET accepted_offer_at = NOW() WHERE user_id = $1", callback.from_user.id)
    tariffs = await get_tariffs()
    await callback.message.answer("✅ Оферта подтверждена.\nТеперь выбери тариф:", reply_markup=tariffs_kb(tariffs))
    await callback.answer("Подтверждено")


@dp.callback_query(F.data == "show_offer")
async def show_offer_cb(callback: CallbackQuery):
    await send_offer(callback.from_user.id, with_agree_button=False)
    await log_action(callback.from_user.id, "offer_open", "manual")
    await callback.answer()


@dp.callback_query(F.data == "show_share")
async def show_share_cb(callback: CallbackQuery):
    await send_share_message(callback.from_user.id, callback.from_user.id)
    await log_action(callback.from_user.id, "share_open")
    await callback.answer()


@dp.callback_query(F.data == "enter_promo_inline")
async def enter_promo_inline(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.enter_promo)
    is_admin_user = await is_admin(callback.from_user.id)
    await callback.message.answer(
        "Введи промокод одним сообщением.\nЧтобы выйти, нажми «⬅️ Назад» или «🏠 Меню».",
        reply_markup=reply_back_kb(is_admin_user),
    )
    await callback.answer()


@dp.callback_query(F.data == "support_open")
async def support_open(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.support_message)
    is_admin_user = await is_admin(callback.from_user.id)
    await callback.message.answer(
        "Отправь сообщение в поддержку: текст, фото, видео, voice, кружок или документ.\n\nНажми «⬅️ Назад» для выхода.",
        reply_markup=reply_back_kb(is_admin_user),
    )
    await callback.answer()


@dp.callback_query(F.data == "ask_content")
async def ask_content(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.question_content)
    is_admin_user = await is_admin(callback.from_user.id)
    await callback.message.answer(
        "Отправь вопрос по контенту: текст, фото, видео, voice, кружок или документ.\n\nНажми «⬅️ Назад» для выхода.",
        reply_markup=reply_back_kb(is_admin_user),
    )
    await callback.answer()


@dp.callback_query(F.data == "ask_homework")
async def ask_homework(callback: CallbackQuery, state: FSMContext):
    await send_homework_intro(callback.from_user.id)
    await state.set_state(UserStates.homework_message)
    is_admin_user = await is_admin(callback.from_user.id)
    await bot.send_message(
        callback.from_user.id,
        "Теперь отправь домашнее задание.\nНажми «⬅️ Назад» для выхода.",
        reply_markup=reply_back_kb(is_admin_user),
    )
    await callback.answer()


async def process_user_message_to_admin(message: Message, state: FSMContext, q_type: str, title: str):
    payload = await extract_payload_from_message(message)
    if not payload:
        await message.answer("Этот тип сообщения пока не поддерживается.")
        return

    qid = await save_question(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
        q_type=q_type,
        payload=payload,
    )

    prefix = (
        f"{title}\n\n"
        f"ID: <code>{message.from_user.id}</code>\n"
        f"Имя: {message.from_user.full_name}\n"
        f"Username: @{message.from_user.username if message.from_user.username else 'нет'}\n"
        f"Тип: <b>{q_type}</b>\n"
        f"Question ID: <b>{qid}</b>"
    )

    await send_payload(
        ADMIN_ID,
        payload,
        prefix=prefix,
        reply_markup=admin_question_reply_kb(qid, message.from_user.id, q_type),
    )
    await message.answer("✅ Сообщение отправлено администратору.", reply_markup=reply_menu_kb(await is_admin(message.from_user.id)))
    await state.clear()
    await log_action(message.from_user.id, f"question_{q_type}", f"qid={qid}")


@dp.message(UserStates.support_message)
async def support_message_state(message: Message, state: FSMContext):
    await process_user_message_to_admin(message, state, "support", "🛠 <b>Новое сообщение в поддержку</b>")


@dp.message(UserStates.question_content)
async def content_question_state(message: Message, state: FSMContext):
    await process_user_message_to_admin(message, state, "content", "❓ <b>Новый вопрос по контенту</b>")


@dp.message(UserStates.homework_message)
async def homework_state(message: Message, state: FSMContext):
    await process_user_message_to_admin(message, state, "homework", "📝 <b>Новое домашнее задание</b>")


@dp.message(UserStates.enter_promo)
async def apply_promo(message: Message, state: FSMContext):
    code = (message.text or "").strip().upper()
    if not code:
        await message.answer("Промокод пустой.")
        return

    async with get_pool().acquire() as conn:
        promo = await conn.fetchrow("SELECT * FROM promo_codes WHERE code = $1 AND active = TRUE", code)
        if not promo:
            await message.answer("Промокод не найден или отключён.\nНажми «⬅️ Назад» или «🏠 Меню» для выхода.")
            return
        if promo["expires_at"] and promo["expires_at"] < now():
            await message.answer("Срок действия промокода истёк.")
            return
        if promo["max_uses"] is not None and promo["used_count"] >= promo["max_uses"]:
            await message.answer("Лимит использований промокода исчерпан.")
            return
        await conn.execute("UPDATE users SET applied_promo_code = $2 WHERE user_id = $1", message.from_user.id, code)

    await message.answer(
        f"✅ Промокод <b>{code}</b> сохранён. Теперь выбери тариф.",
        reply_markup=reply_menu_kb(await is_admin(message.from_user.id)),
    )
    await state.clear()


@dp.callback_query(F.data == "my_sub")
async def my_sub(callback: CallbackQuery):
    row = await get_user(callback.from_user.id)
    active = await has_active_sub(callback.from_user.id)
    reminder_enabled = bool(row and row["pseudo_autorenew_enabled"])
    expire_text = row["expire_date"].strftime("%d.%m.%Y %H:%M") if row and row["expire_date"] else "—"
    tariff = await get_tariff_by_id(row["active_tariff_id"]) if row and row["active_tariff_id"] else None

    await callback.message.answer(
        "📅 <b>Моя подписка</b>\n\n"
        f"Статус: {'✅ Активна' if active else '❌ Нет активной'}\n"
        f"Тариф: <b>{tariff['name'] if tariff else '—'}</b>\n"
        f"До: <b>{expire_text}</b>\n"
        f"Напоминание о продлении: <b>{'включено' if reminder_enabled else 'выключено'}</b>",
        reply_markup=my_sub_kb(active, reminder_enabled),
    )
    await callback.answer()


@dp.callback_query(F.data == "toggle_pseudo")
async def toggle_pseudo(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        current = await conn.fetchval("SELECT pseudo_autorenew_enabled FROM users WHERE user_id = $1", callback.from_user.id)
        new_val = not bool(current)
        await conn.execute("UPDATE users SET pseudo_autorenew_enabled = $2 WHERE user_id = $1", callback.from_user.id, new_val)
    await callback.message.answer(
        "✅ Напоминание о продлении включено." if new_val else "✅ Напоминание о продлении отключено."
    )
    await callback.answer()


@dp.callback_query(F.data == "sub_enter_channel")
async def sub_enter_channel(callback: CallbackQuery):
    if not await has_active_sub(callback.from_user.id):
        await callback.message.answer("Нет активной подписки.")
        await callback.answer()
        return
    await send_invite_to_user(callback.from_user.id)
    await callback.answer("Ссылка отправлена")


@dp.callback_query(F.data.startswith("tariff:"))
async def tariff_view(callback: CallbackQuery):
    tariff_id = int(callback.data.split(":")[1])
    tariff = await get_tariff_by_id(tariff_id)
    if not tariff:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    pseudo_text = {
        "off": "Без напоминания о продлении",
        "choice": "Пользователь сам выбирает",
        "default_on": "Напоминание включено по умолчанию",
    }.get(tariff.get("pseudo_mode", "choice"), tariff.get("pseudo_mode", "choice"))

    await callback.message.answer(
        f"💳 <b>{tariff['name']}</b>\n\n"
        f"Цена: <b>{format_rub_from_kop(int(tariff['price_kop']))}</b>\n"
        f"Срок: <b>{tariff['days']} дней</b>\n"
        f"Напоминание о продлении: <b>{pseudo_text}</b>",
        reply_markup=tariff_buy_kb(tariff),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("pay:"))
async def pay_tariff(callback: CallbackQuery):
    _, tariff_id, pseudo = callback.data.split(":")
    await send_invoice_for_tariff(callback.from_user.id, int(tariff_id), pseudo == "1")
    await callback.answer("Счёт отправлен")


@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    await q.answer(ok=True)


@dp.message(F.successful_payment)
async def success_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    try:
        _, tariff_id_s, pseudo_s = payload.split(":")
        tariff_id = int(tariff_id_s)
        pseudo = pseudo_s == "1"
    except Exception:
        await message.answer("Оплата прошла, но payload не распознан.")
        return

    tariff = await get_tariff_by_id(tariff_id)
    if not tariff:
        await message.answer("Оплата прошла, но тариф не найден.")
        return

    await add_sub_days(message.from_user.id, int(tariff["days"]), tariff_id=tariff_id, pseudo_autorenew=pseudo)
    await send_invite_to_user(message.from_user.id)

    async with get_pool().acquire() as conn:
        code = await conn.fetchval("SELECT applied_promo_code FROM users WHERE user_id = $1", message.from_user.id)
        if code:
            await conn.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code = $1", code)
            await conn.execute("UPDATE users SET applied_promo_code = NULL WHERE user_id = $1", message.from_user.id)

        ref_row = await conn.fetchrow(
            "SELECT referrer_id, referral_rewarded FROM users WHERE user_id = $1",
            message.from_user.id,
        )
        referral_settings = await get_referral_settings()
        if ref_row and ref_row["referrer_id"] and not ref_row["referral_rewarded"] and referral_settings.get("enabled"):
            reward_days = int(referral_settings.get("reward_days", 30))
            referrer_id = ref_row["referrer_id"]

            referrer = await conn.fetchrow("SELECT expire_date FROM users WHERE user_id = $1", referrer_id)
            base = referrer["expire_date"] if referrer and referrer["expire_date"] and referrer["expire_date"] > now() else now()
            new_expire = base + timedelta(days=reward_days)

            await conn.execute("UPDATE users SET expire_date = $2 WHERE user_id = $1", referrer_id, new_expire)
            await conn.execute("UPDATE users SET referral_rewarded = TRUE WHERE user_id = $1", message.from_user.id)

            try:
                await bot.send_message(
                    referrer_id,
                    f"🎉 Твой реферал оплатил подписку. Тебе начислено <b>{reward_days} дней</b>.",
                )
            except Exception:
                logger.exception("referral reward notify error")

    offer = await get_offer()
    if offer.get("enabled_after_pay"):
        await send_offer(message.from_user.id)

    await message.answer("✅ Оплата прошла успешно. Ссылка уже отправлена.")
    await log_action(message.from_user.id, "payment_success", f"tariff={tariff_id};reminder={pseudo}")

    try:
        await bot.send_message(
            ADMIN_ID,
            f"💰 <b>Новая оплата</b>\n\n"
            f"User ID: <code>{message.from_user.id}</code>\n"
            f"Тариф: <b>{tariff['name']}</b>\n"
            f"Сумма: <b>{format_rub_from_kop(int(tariff['price_kop']))}</b>\n"
            f"Напоминание о продлении: <b>{'да' if pseudo else 'нет'}</b>",
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("cnode:"))
async def user_custom_node(callback: CallbackQuery):
    node_id = int(callback.data.split(":")[1])
    node = await get_custom_node(node_id)
    if not node or not node["enabled"]:
        await callback.answer("Кнопка недоступна", show_alert=True)
        return

    children = await get_custom_nodes(node_id)
    child_rows = []
    for child in children:
        if child["enabled"]:
            child_rows.append([InlineKeyboardButton(text=child["title"], callback_data=f"cnode:{child['id']}")])
    child_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="go_main")])

    payload = {
        "content_type": node["media_type"] or "text",
        "text": node["message_text"],
        "file_id": node["media_file_id"],
        "caption": node["message_text"],
    }
    await send_payload(
        callback.from_user.id,
        payload,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=child_rows),
    )
    await callback.answer()


@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    await message.answer("⚙️ <b>Админка</b>", reply_markup=admin_kb())
    await message.answer("Нижнее меню обновлено.", reply_markup=reply_menu_kb(True))


@dp.callback_query(F.data == "admin_force_reset")
async def admin_force_reset(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.answer("♻️ Текущее действие сброшено.")
    await callback.answer()


@dp.callback_query(F.data == "admin_diagnostics_menu")
async def admin_diagnostics_menu(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧪 Общая проверка бота", callback_data="diag_full_check")],
            [InlineKeyboardButton(text="🔗 Проверить выдачу ссылки", callback_data="diag_invite_check")],
            [InlineKeyboardButton(text="🚫 Проверить кик из канала", callback_data="diag_kick_check")],
            [InlineKeyboardButton(text="👤 Диагностика пользователя", callback_data="diag_user_check")],
        ]
    )

    await callback.message.answer(
        "🩺 <b>Диагностика</b>\n\n"
        "Здесь можно проверить, работает ли бот, база, выдача ссылок и исключение из канала.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "diag_full_check")
async def diag_full_check(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    results = []

    try:
        me = await bot.get_me()
        results.append(f"✅ Bot API: <b>@{me.username}</b> ({me.id})")
    except Exception as e:
        results.append(f"❌ Bot API: <code>{e}</code>")

    try:
        async with get_pool().acquire() as conn:
            db_now = await conn.fetchval("SELECT NOW()")
        results.append(f"✅ База данных: <b>{db_now}</b>")
    except Exception as e:
        results.append(f"❌ База данных: <code>{e}</code>")

    try:
        chat = await bot.get_chat(CHANNEL_ID)
        results.append(f"✅ Канал доступен: <b>{chat.title}</b>")
    except Exception as e:
        results.append(f"❌ Канал недоступен: <code>{e}</code>")

    try:
        member = await bot.get_chat_member(CHANNEL_ID, callback.from_user.id)
        results.append(f"✅ Проверка статуса в канале: <b>{member.status}</b>")
    except Exception as e:
        results.append(f"⚠️ Проверка статуса в канале: <code>{e}</code>")

    await callback.message.answer(
        "🧪 <b>Общая диагностика</b>\n\n" + "\n".join(results)
    )
    await callback.answer()


@dp.callback_query(F.data == "diag_invite_check")
async def diag_invite_check(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.diagnostic_invite_user)
    await callback.message.answer(
        "Введи <b>user_id</b> или <b>@username</b> пользователя, которому нужно тестово отправить ссылку.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.diagnostic_invite_user)
async def diagnostic_invite_user_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return

    try:
        await send_invite_to_user(user_id)
        await message.answer(f"✅ Тестовая ссылка отправлена пользователю <code>{user_id}</code>.")
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки ссылки: <code>{e}</code>")

    await state.clear()


@dp.callback_query(F.data == "diag_kick_check")
async def diag_kick_check(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.diagnostic_kick_user)
    await callback.message.answer(
        "Введи <b>user_id</b> или <b>@username</b> пользователя, которого нужно тестово исключить из канала.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.diagnostic_kick_user)
async def diagnostic_kick_user_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return

    ok, text = await kick_user_from_channel(user_id)
    await message.answer(
        f"{'✅' if ok else '❌'} Проверка кика для <code>{user_id}</code>\n{text}"
    )
    await state.clear()


@dp.callback_query(F.data == "diag_user_check")
async def diag_user_check(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.diagnostic_user)
    await callback.message.answer(
        "Введи <b>user_id</b> или <b>@username</b> пользователя для полной диагностики.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.diagnostic_user)
async def diagnostic_user_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return

    row = await get_user(user_id)
    if not row:
        await message.answer("Пользователь не найден в базе.")
        await state.clear()
        return

    lines = [
        "👤 <b>Диагностика пользователя</b>",
        f"ID: <code>{user_id}</code>",
        f"Имя: {row['first_name'] or '-'}",
        f"Username: @{row['username'] or '-'}",
        f"Нажал начать: <b>{'да' if row['started'] else 'нет'}</b>",
        f"Покупал: <b>{'да' if row['has_purchased'] else 'нет'}</b>",
        f"Подписка до: <b>{row['expire_date'].strftime('%d.%m.%Y %H:%M') if row['expire_date'] else '—'}</b>",
        f"Тариф: <b>{row['active_tariff_id'] or '—'}</b>",
        f"Напоминание о продлении: <b>{'вкл' if row['pseudo_autorenew_enabled'] else 'выкл'}</b>",
    ]

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        lines.append(f"Статус в канале: <b>{member.status}</b>")
    except Exception as e:
        lines.append(f"Статус в канале: <code>{e}</code>")

    await message.answer("\n".join(lines))
    await state.clear()


@dp.callback_query(F.data == "admin_stats_menu")
async def admin_stats_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Общая статистика", callback_data="admin_stats_overall")],
            [InlineKeyboardButton(text="💳 По тарифам", callback_data="admin_stats_tariffs")],
            [InlineKeyboardButton(text="🎁 По промокодам", callback_data="admin_stats_promos")],
            [InlineKeyboardButton(text="👥 По рефералам", callback_data="admin_stats_referrals")],
        ]
    )
    await callback.message.answer(
        "📊 <b>Раздел статистики</b>\n\n"
        "Здесь можно посмотреть общие показатели, разбивку по тарифам, промокодам и рефералам.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_overall")
async def admin_stats_overall(callback: CallbackQuery):
    s = await get_stats()
    tariffs = {t["id"]: t["name"] for t in await get_tariffs()}
    tariff_lines = []
    for row in s["tariffs"]:
        tariff_lines.append(f"{tariffs.get(row['last_paid_tariff_id'], row['last_paid_tariff_id'])}: <b>{row['cnt']}</b>")

    await callback.message.answer(
        "📊 <b>Общая статистика</b>\n\n"
        f"Всего пользователей: <b>{s['total']}</b>\n"
        f"Нажали начать: <b>{s['started']}</b>\n"
        f"Купили: <b>{s['purchased']}</b>\n"
        f"Зашли и не купили: <b>{s['not_purchased']}</b>\n"
        f"Активных подписок: <b>{s['active']}</b>\n"
        f"Истёкших: <b>{s['expired']}</b>\n"
        f"Напоминание о продлении включено: <b>{s['pseudo']}</b>\n"
        f"Открыли оферту: <b>{s['offer_opened']}</b>\n"
        f"Согласились с офертой: <b>{s['offer_accepted']}</b>\n\n"
        "Покупки по тарифам:\n"
        + ("\n".join(tariff_lines) if tariff_lines else "—")
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_tariffs")
async def admin_stats_tariffs(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT last_paid_tariff_id, COUNT(*) cnt
            FROM users
            WHERE last_paid_tariff_id IS NOT NULL
            GROUP BY last_paid_tariff_id
            ORDER BY last_paid_tariff_id
            """
        )
    tariffs = {t["id"]: t for t in await get_tariffs()}
    text = "💳 <b>Статистика по тарифам</b>\n\n"
    if not rows:
        text += "Покупок пока нет."
    else:
        for row in rows:
            t = tariffs.get(row["last_paid_tariff_id"])
            name = t["name"] if t else f"ID {row['last_paid_tariff_id']}"
            text += f"{name}: <b>{row['cnt']}</b>\n"
    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_promos")
async def admin_stats_promos(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT code, active, used_count, max_uses, expires_at, discount_percent, discount_kop
            FROM promo_codes
            ORDER BY created_at DESC
            LIMIT 50
            """
        )
    text = "🎁 <b>Статистика по промокодам</b>\n\n"
    if not rows:
        text += "Промокодов нет."
    else:
        for r in rows:
            discount = f"{r['discount_percent']}%" if r["discount_percent"] else format_rub_from_kop(int(r["discount_kop"] or 0))
            exp = r["expires_at"].strftime("%d.%m.%Y") if r["expires_at"] else "—"
            max_uses = r["max_uses"] if r["max_uses"] is not None else "∞"
            text += (
                f"<b>{r['code']}</b>\n"
                f"Активен: {'да' if r['active'] else 'нет'}\n"
                f"Скидка: {discount}\n"
                f"Использован: {r['used_count']} / {max_uses}\n"
                f"До: {exp}\n\n"
            )
    await callback.message.answer(text[:4000])
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_referrals")
async def admin_stats_referrals(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        total_refs = await conn.fetchval("SELECT COUNT(*) FROM referrals")
        rewarded = await conn.fetchval("SELECT COUNT(*) FROM users WHERE referral_rewarded = TRUE")
        tops = await conn.fetch(
            """
            SELECT referrer_id, COUNT(*) cnt
            FROM referrals
            GROUP BY referrer_id
            ORDER BY cnt DESC
            LIMIT 20
            """
        )
    text = (
        "👥 <b>Статистика по рефералам</b>\n\n"
        f"Всего реферальных связок: <b>{total_refs}</b>\n"
        f"Начислено бонусов: <b>{rewarded}</b>\n\n"
        "Топ по приглашениям:\n"
    )
    if not tops:
        text += "—"
    else:
        for row in tops:
            text += f"<code>{row['referrer_id']}</code> — <b>{row['cnt']}</b>\n"
    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "admin_users_menu")
async def admin_users_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Найти пользователя", callback_data="admin_find_user")],
            [InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users_list")],
        ]
    )
    await callback.message.answer(
        "👥 <b>Раздел пользователей</b>\n\n"
        "Здесь можно найти пользователя по ID или @username, а также посмотреть список последних пользователей.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_find_user")
async def admin_find_user(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.find_user)
    await callback.message.answer(
        "Введи <b>user_id</b> или <b>@username</b> пользователя.\n\n"
        "Примеры:\n"
        "<code>123456789</code>\n"
        "<code>@nickname</code>",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.find_user)
async def admin_find_user_finish(message: Message, state: FSMContext):
    raw = message.text or ""
    user_id = await resolve_user_input_to_id(raw)
    if not user_id:
        await message.answer("Пользователь не найден. Введи корректный user_id или @username.")
        return

    data = await build_user_stats(user_id)
    if not data:
        await message.answer("Пользователь не найден в базе.")
        await state.clear()
        return

    row, logs_cnt, q_cnt = data
    await message.answer(
        f"👤 <code>{user_id}</code>\n"
        f"Имя: {row['first_name'] or '-'}\n"
        f"Username: @{row['username'] or '-'}\n"
        f"Нажал начать: <b>{'да' if row['started'] else 'нет'}</b>\n"
        f"Покупал: <b>{'да' if row['has_purchased'] else 'нет'}</b>\n"
        f"Подписка до: <b>{row['expire_date'].strftime('%d.%m.%Y %H:%M') if row['expire_date'] else '—'}</b>\n"
        f"Тариф: <b>{row['active_tariff_id'] or '—'}</b>\n"
        f"Напоминание о продлении: <b>{'вкл' if row['pseudo_autorenew_enabled'] else 'выкл'}</b>\n"
        f"Реферер: <b>{row['referrer_id'] or '—'}</b>\n"
        f"Логов: <b>{logs_cnt}</b>\n"
        f"Всего обращений: <b>{q_cnt}</b>"
    )
    await state.clear()


@dp.callback_query(F.data == "admin_users_list")
async def admin_users_list(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id, first_name, username, has_purchased, expire_date, last_paid_tariff_id, pseudo_autorenew_enabled
            FROM users
            ORDER BY created_at DESC
            LIMIT 50
            """
        )
    text = "👥 <b>Последние пользователи</b>\n\n"
    if not rows:
        text += "Нет пользователей."
    else:
        for row in rows:
            exp = row["expire_date"].strftime("%d.%m.%Y") if row["expire_date"] else "—"
            text += (
                f"<code>{row['user_id']}</code> | "
                f"{row['first_name'] or '-'} | "
                f"@{row['username'] or '-'} | "
                f"{'купил' if row['has_purchased'] else 'не купил'} | "
                f"до {exp} | "
                f"tariff {row['last_paid_tariff_id'] or '-'} | "
                f"напоминание {'on' if row['pseudo_autorenew_enabled'] else 'off'}\n"
            )
    await callback.message.answer(text[:4000])
    await callback.answer()


@dp.callback_query(F.data == "admin_questions_menu")
async def admin_questions_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❓ Вопросы по контенту", callback_data="admin_q_content")],
            [InlineKeyboardButton(text="🛠 Поддержка", callback_data="admin_q_support")],
            [InlineKeyboardButton(text="📝 Домашние задания", callback_data="admin_q_homework")],
        ]
    )
    await callback.message.answer(
        "❓ <b>Очередь вопросов</b>\n\n"
        "Выбери раздел, чтобы посмотреть неотвеченные сообщения пользователей.",
        reply_markup=kb,
    )
    await callback.answer()


async def show_questions(callback: CallbackQuery, q_type: str, title: str):
    rows = await get_open_questions_by_type(q_type)
    if not rows:
        await callback.message.answer(f"Открытых элементов в разделе «{title}» нет.")
        await callback.answer()
        return

    for row in rows[:10]:
        prefix = (
            f"{title}\n\n"
            f"Question ID: <b>{row['id']}</b>\n"
            f"User ID: <code>{row['user_id']}</code>\n"
            f"Имя: {row['full_name'] or '-'}\n"
            f"Username: @{row['username'] if row['username'] else 'нет'}\n"
            f"Дата: {row['created_at'].strftime('%d.%m.%Y %H:%M')}"
        )
        payload = {
            "content_type": row["content_type"],
            "text": row["text"],
            "file_id": row["file_id"],
            "caption": row["caption"],
        }
        await send_payload(
            callback.from_user.id,
            payload,
            prefix=prefix,
            reply_markup=admin_question_reply_kb(row["id"], row["user_id"], row["type"]),
        )
    await callback.answer()


@dp.callback_query(F.data == "admin_q_content")
async def admin_q_content(callback: CallbackQuery):
    await show_questions(callback, "content", "❓ <b>Вопрос по контенту</b>")


@dp.callback_query(F.data == "admin_q_support")
async def admin_q_support(callback: CallbackQuery):
    await show_questions(callback, "support", "🛠 <b>Обращение в поддержку</b>")


@dp.callback_query(F.data == "admin_q_homework")
async def admin_q_homework(callback: CallbackQuery):
    await show_questions(callback, "homework", "📝 <b>Домашнее задание</b>")


@dp.callback_query(F.data == "admin_answers_menu")
async def admin_answers_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Ответить по user_id / @username", callback_data="admin_answer_by_id")],
            [InlineKeyboardButton(text="❓ Очередь вопросов", callback_data="admin_questions_menu")],
        ]
    )
    await callback.message.answer(
        "💬 <b>Ответы пользователям</b>\n\n"
        "Можно ответить прямо из очереди вопросов кнопкой «Ответить» или вручную по user_id / @username.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_answer_by_id")
async def admin_answer_by_id(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.answer_pick_user)
    await callback.message.answer(
        "Введи <b>user_id</b> или <b>@username</b> пользователя.\n\n"
        "Потом бот попросит отправить ответ.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("replyq:"))
async def replyq_callback(callback: CallbackQuery, state: FSMContext):
    _, qid, user_id, q_type = callback.data.split(":")
    await state.update_data(answer_user_id=int(user_id), answer_question_id=int(qid), answer_q_type=q_type)
    await state.set_state(AdminStates.answer_message)
    await callback.message.answer(
        f"Ответ пользователю <code>{user_id}</code>.\n"
        "Отправь текст, фото, видео, voice, кружок или документ.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.answer_pick_user)
async def admin_answer_pick_user_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден. Введи user_id или @username.")
        return
    await state.update_data(answer_user_id=user_id)
    await state.set_state(AdminStates.answer_message)
    await message.answer("Отправь ответ: текст, фото, видео, voice, кружок или документ.", reply_markup=reply_back_kb(True))


@dp.message(AdminStates.answer_message)
async def admin_answer_message_finish(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    payload = await extract_payload_from_message(message)
    if not payload:
        await message.answer("Этот тип не поддерживается.")
        return
    data = await state.get_data()
    user_id = int(data["answer_user_id"])
    await send_payload(user_id, payload, prefix="👨‍💼 <b>Администратор</b>")
    await close_user_questions(user_id)
    await message.answer(f"✅ Ответ отправлен пользователю <code>{user_id}</code>.")
    await log_action(message.from_user.id, "admin_answer_sent", f"user={user_id}")
    await state.clear()


@dp.callback_query(F.data == "admin_tariffs")
async def admin_tariffs(callback: CallbackQuery):
    tariffs = await get_tariffs()
    txt = "💳 <b>Тарифы</b>\n\n"
    txt += "Формат настройки:\n"
    txt += "<code>Название | цена_в_рублях | дни | active(1/0) | pseudo(off/choice/default_on)</code>\n\n"
    txt += "Пояснения:\n"
    txt += "active: 1 = тариф виден пользователю, 0 = скрыт\n"
    txt += "pseudo:\n"
    txt += "off = без напоминания\n"
    txt += "choice = пользователь сам выбирает\n"
    txt += "default_on = напоминание включено по умолчанию\n\n"

    for t in tariffs:
        txt += (
            f"ID {t['id']}: <b>{t['name']}</b>\n"
            f"Цена: {format_rub_from_kop(int(t['price_kop']))}\n"
            f"Срок: {t['days']} дн.\n"
            f"Активен: {'да' if t['active'] else 'нет'}\n"
            f"Режим напоминания: {t.get('pseudo_mode', 'choice')}\n\n"
        )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Тариф 1", callback_data="edit_tariff:1")],
            [InlineKeyboardButton(text="Тариф 2", callback_data="edit_tariff:2")],
            [InlineKeyboardButton(text="Тариф 3", callback_data="edit_tariff:3")],
        ]
    )
    await callback.message.answer(txt[:4000], reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("edit_tariff:"))
async def edit_tariff_start(callback: CallbackQuery, state: FSMContext):
    tariff_id = int(callback.data.split(":")[1])
    await state.update_data(tariff_id=tariff_id)
    await state.set_state(AdminStates.edit_tariff_data)
    await callback.message.answer(
        "Отправь строку в формате:\n"
        "<code>Название | цена_в_рублях | дни | active(1/0) | pseudo(off/choice/default_on)</code>\n\n"
        "Пример:\n"
        "<code>Стандарт | 15000 | 365 | 1 | choice</code>",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.edit_tariff_data)
async def edit_tariff_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    tariff_id = data["tariff_id"]
    try:
        parts = [p.strip() for p in (message.text or "").split("|")]
        name = parts[0]
        price_kop = int(round(float(parts[1].replace(",", ".")) * 100))
        days = int(parts[2])
        active = parts[3] == "1"
        pseudo = parts[4]
        if pseudo not in {"off", "choice", "default_on"}:
            raise ValueError
    except Exception:
        await message.answer("Неверный формат. Проверь порядок значений.")
        return

    tariffs = await get_tariffs()
    updated = False
    for t in tariffs:
        if t["id"] == tariff_id:
            t["name"] = name
            t["price_kop"] = price_kop
            t["days"] = days
            t["active"] = active
            t["pseudo_mode"] = pseudo
            updated = True
            break
    if not updated:
        tariffs.append(
            {
                "id": tariff_id,
                "name": name,
                "price_kop": price_kop,
                "days": days,
                "active": active,
                "pseudo_mode": pseudo,
            }
        )
    tariffs = sorted(tariffs, key=lambda x: x["id"])[:3]
    await save_tariffs(tariffs)
    await message.answer("✅ Тариф сохранён.")
    await state.clear()


@dp.callback_query(F.data == "admin_offer_menu")
async def admin_offer_menu(callback: CallbackQuery):
    offer = await get_offer()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"До оплаты: {'ВКЛ' if offer['enabled_before_pay'] else 'ВЫКЛ'}", callback_data="offer_toggle_before")],
            [InlineKeyboardButton(text=f"После оплаты: {'ВКЛ' if offer['enabled_after_pay'] else 'ВЫКЛ'}", callback_data="offer_toggle_after")],
            [InlineKeyboardButton(text=f"Требовать согласие: {'ВКЛ' if offer['require_accept_before_pay'] else 'ВЫКЛ'}", callback_data="offer_toggle_accept")],
            [InlineKeyboardButton(text="📝 Изменить текст", callback_data="offer_set_text")],
            [InlineKeyboardButton(text="🖼 Изменить фото/видео", callback_data="offer_set_media")],
            [InlineKeyboardButton(text="👁 Предпросмотр", callback_data="offer_preview")],
        ]
    )
    await callback.message.answer(
        "🛡 <b>Настройка оферты</b>\n\n"
        "Пояснения:\n"
        "До оплаты — показывать оферту перед выбором тарифа.\n"
        "После оплаты — отправлять оферту после успешной оплаты.\n"
        "Требовать согласие — без подтверждения пользователь не сможет перейти к оплате.\n",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "offer_preview")
async def offer_preview(callback: CallbackQuery):
    await send_offer(callback.from_user.id, with_agree_button=False)
    await callback.answer()


@dp.callback_query(F.data.in_(["offer_toggle_before", "offer_toggle_after", "offer_toggle_accept"]))
async def offer_toggle(callback: CallbackQuery):
    offer = await get_offer()
    if callback.data == "offer_toggle_before":
        offer["enabled_before_pay"] = not offer["enabled_before_pay"]
    elif callback.data == "offer_toggle_after":
        offer["enabled_after_pay"] = not offer["enabled_after_pay"]
    else:
        offer["require_accept_before_pay"] = not offer["require_accept_before_pay"]
    await set_json_setting("offer_json", offer)
    await callback.message.answer("✅ Настройка оферты обновлена.")
    await callback.answer()


@dp.callback_query(F.data == "offer_set_text")
async def offer_set_text(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_offer_text)
    await callback.message.answer("Отправь новый текст оферты.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_offer_text)
async def offer_set_text_finish(message: Message, state: FSMContext):
    offer = await get_offer()
    offer["text"] = message.html_text or message.text or offer["text"]
    await set_json_setting("offer_json", offer)
    await message.answer("✅ Текст оферты сохранён.")
    await state.clear()


@dp.callback_query(F.data == "offer_set_media")
async def offer_set_media(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_offer_media)
    await callback.message.answer("Отправь фото/видео или <code>remove</code>.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_offer_media)
async def offer_set_media_finish(message: Message, state: FSMContext):
    offer = await get_offer()
    if message.text and message.text.strip().lower() == "remove":
        offer["media_type"] = None
        offer["media_file_id"] = None
    elif message.photo:
        offer["media_type"] = "photo"
        offer["media_file_id"] = message.photo[-1].file_id
    elif message.video:
        offer["media_type"] = "video"
        offer["media_file_id"] = message.video.file_id
    else:
        await message.answer("Нужны фото/видео/remove.")
        return
    await set_json_setting("offer_json", offer)
    await message.answer("✅ Медиа оферты сохранено.")
    await state.clear()


@dp.callback_query(F.data == "admin_funnel_menu")
async def admin_funnel_menu(callback: CallbackQuery):
    funnel = await get_funnel()
    rows = [
        [InlineKeyboardButton(text=f"Воронка: {'ВКЛ' if funnel['enabled'] else 'ВЫКЛ'}", callback_data="funnel_toggle")],
        [InlineKeyboardButton(text=f"Время отправки: {funnel['send_time']}", callback_data="funnel_set_time")],
        [InlineKeyboardButton(text=f"Шаг 1 {'ВКЛ' if funnel['steps'][0]['enabled'] else 'ВЫКЛ'}", callback_data="funnel_step:1")],
        [InlineKeyboardButton(text=f"Шаг 2 {'ВКЛ' if funnel['steps'][1]['enabled'] else 'ВЫКЛ'}", callback_data="funnel_step:2")],
        [InlineKeyboardButton(text=f"Шаг 3 {'ВКЛ' if funnel['steps'][2]['enabled'] else 'ВЫКЛ'}", callback_data="funnel_step:3")],
    ]
    await callback.message.answer(
        "🪄 <b>Настройка воронки</b>\n\n"
        "Пояснения:\n"
        "Воронка — серия автоматических сообщений тем, кто зашёл, но не купил.\n"
        "Время отправки — в какое время дня бот может отправлять шаги.\n"
        "У каждого шага есть включение, задержка в часах, текст и медиа.\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@dp.callback_query(F.data == "funnel_toggle")
async def funnel_toggle(callback: CallbackQuery):
    funnel = await get_funnel()
    funnel["enabled"] = not funnel["enabled"]
    await set_json_setting("funnel_json", funnel)
    await callback.message.answer("✅ Воронка обновлена.")
    await callback.answer()


@dp.callback_query(F.data == "funnel_set_time")
async def funnel_set_time(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_funnel_time)
    await callback.message.answer(
        "Введи время отправки в формате <code>20:00</code>.\n"
        "Пример: все шаги будут отправляться не раньше этого времени.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.set_funnel_time)
async def funnel_set_time_finish(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    try:
        hh, mm = text.split(":")
        int(hh), int(mm)
    except Exception:
        await message.answer("Неверный формат.")
        return
    funnel = await get_funnel()
    funnel["send_time"] = text
    await set_json_setting("funnel_json", funnel)
    await message.answer("✅ Время воронки сохранено.")
    await state.clear()


@dp.callback_query(F.data.startswith("funnel_step:"))
async def funnel_step(callback: CallbackQuery):
    idx = int(callback.data.split(":")[1]) - 1
    funnel = await get_funnel()
    step = funnel["steps"][idx]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Вкл/выкл: {'ВКЛ' if step['enabled'] else 'ВЫКЛ'}", callback_data=f"funnel_step_toggle:{idx+1}")],
            [InlineKeyboardButton(text=f"Задержка: {step['delay_hours']} ч", callback_data=f"funnel_step_delay:{idx+1}")],
            [InlineKeyboardButton(text="📝 Изменить текст", callback_data=f"funnel_step_text:{idx+1}")],
            [InlineKeyboardButton(text="🖼 Изменить фото/видео", callback_data=f"funnel_step_media:{idx+1}")],
            [InlineKeyboardButton(text="👁 Предпросмотр", callback_data=f"funnel_step_preview:{idx+1}")],
        ]
    )
    await callback.message.answer(
        f"Шаг {idx+1}\n\n"
        "Пояснения:\n"
        "Задержка — через сколько часов после начала знакомства пользователя с ботом можно отправлять это сообщение.\n"
        "Текст — основное сообщение шага.\n"
        "Фото/видео — медиа шага.\n\n"
        f"Включён: <b>{step['enabled']}</b>\n"
        f"Задержка: <b>{step['delay_hours']} часов</b>\n"
        f"Текст: {step['text']}",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("funnel_step_preview:"))
async def funnel_step_preview(callback: CallbackQuery):
    idx = int(callback.data.split(":")[1]) - 1
    funnel = await get_funnel()
    step = funnel["steps"][idx]
    payload = {
        "content_type": step.get("media_type") or "text",
        "text": step.get("text"),
        "file_id": step.get("media_file_id"),
        "caption": step.get("text"),
    }
    await send_payload(callback.from_user.id, payload)
    await callback.answer()


@dp.callback_query(F.data.startswith("funnel_step_toggle:"))
async def funnel_step_toggle(callback: CallbackQuery):
    idx = int(callback.data.split(":")[1]) - 1
    funnel = await get_funnel()
    funnel["steps"][idx]["enabled"] = not funnel["steps"][idx]["enabled"]
    await set_json_setting("funnel_json", funnel)
    await callback.message.answer("✅ Шаг обновлён.")
    await callback.answer()


@dp.callback_query(F.data.startswith("funnel_step_delay:"))
async def funnel_step_delay_start(callback: CallbackQuery, state: FSMContext):
    idx = int(callback.data.split(":")[1]) - 1
    await state.update_data(funnel_step_idx=idx)
    await state.set_state(AdminStates.set_funnel_step_delay)
    await callback.message.answer(
        "Введи задержку в часах.\n"
        "Например: <code>12</code> — отправить шаг примерно через 12 часов.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.set_funnel_step_delay)
async def funnel_step_delay_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    idx = data["funnel_step_idx"]
    try:
        delay = int((message.text or "").strip())
    except Exception:
        await message.answer("Нужно число.")
        return
    funnel = await get_funnel()
    funnel["steps"][idx]["delay_hours"] = delay
    await set_json_setting("funnel_json", funnel)
    await message.answer("✅ Задержка шага сохранена.")
    await state.clear()


@dp.callback_query(F.data.startswith("funnel_step_text:"))
async def funnel_step_text_start(callback: CallbackQuery, state: FSMContext):
    idx = int(callback.data.split(":")[1]) - 1
    await state.update_data(funnel_step_idx=idx)
    await state.set_state(AdminStates.set_funnel_step_text)
    await callback.message.answer("Отправь новый текст шага.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_funnel_step_text)
async def funnel_step_text_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    idx = data["funnel_step_idx"]
    funnel = await get_funnel()
    funnel["steps"][idx]["text"] = message.html_text or message.text or funnel["steps"][idx]["text"]
    await set_json_setting("funnel_json", funnel)
    await message.answer("✅ Текст шага сохранён.")
    await state.clear()


@dp.callback_query(F.data.startswith("funnel_step_media:"))
async def funnel_step_media_start(callback: CallbackQuery, state: FSMContext):
    idx = int(callback.data.split(":")[1]) - 1
    await state.update_data(funnel_step_idx=idx)
    await state.set_state(AdminStates.set_funnel_step_media)
    await callback.message.answer("Отправь фото/видео или <code>remove</code>.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_funnel_step_media)
async def funnel_step_media_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    idx = data["funnel_step_idx"]
    funnel = await get_funnel()
    if message.text and message.text.strip().lower() == "remove":
        funnel["steps"][idx]["media_type"] = None
        funnel["steps"][idx]["media_file_id"] = None
    elif message.photo:
        funnel["steps"][idx]["media_type"] = "photo"
        funnel["steps"][idx]["media_file_id"] = message.photo[-1].file_id
    elif message.video:
        funnel["steps"][idx]["media_type"] = "video"
        funnel["steps"][idx]["media_file_id"] = message.video.file_id
    else:
        await message.answer("Нужны фото/видео/remove.")
        return
    await set_json_setting("funnel_json", funnel)
    await message.answer("✅ Медиа шага сохранено.")
    await state.clear()


@dp.callback_query(F.data == "admin_content_menu")
async def admin_content_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧠 Стартовый текст", callback_data="admin_start_text")],
            [InlineKeyboardButton(text="🖼 Стартовое фото/видео", callback_data="admin_start_media")],
            [InlineKeyboardButton(text="📣 Share", callback_data="admin_share")],
            [InlineKeyboardButton(text="📝 Домашнее задание", callback_data="admin_homework")],
        ]
    )
    await callback.message.answer(
        "📣 <b>Контент бота</b>\n\n"
        "Здесь настраиваются тексты и медиа основных пользовательских сообщений.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_start_text")
async def admin_start_text(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_start_text)
    current = await get_setting("start_text") or DEFAULT_START_TEXT
    await callback.message.answer(
        f"Текущий стартовый текст:\n\n{current}\n\n"
        "Отправь новый текст стартового сообщения.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.set_start_text)
async def admin_start_text_finish(message: Message, state: FSMContext):
    await set_setting("start_text", message.html_text or message.text or DEFAULT_START_TEXT)
    await message.answer("✅ Стартовый текст сохранён.")
    await state.clear()


@dp.callback_query(F.data == "admin_start_media")
async def admin_start_media(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_start_media)
    await callback.message.answer(
        "Отправь фото/видео для стартового сообщения.\n"
        "Или отправь <code>remove</code>, чтобы убрать медиа.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.set_start_media)
async def admin_start_media_finish(message: Message, state: FSMContext):
    if message.text and message.text.strip().lower() == "remove":
        await set_setting("start_media_type", None)
        await set_setting("start_media_file_id", None)
        await message.answer("✅ Стартовое медиа удалено.")
        await state.clear()
        return
    if message.photo:
        await set_setting("start_media_type", "photo")
        await set_setting("start_media_file_id", message.photo[-1].file_id)
        await message.answer("✅ Стартовое фото сохранено.")
        await state.clear()
        return
    if message.video:
        await set_setting("start_media_type", "video")
        await set_setting("start_media_file_id", message.video.file_id)
        await message.answer("✅ Стартовое видео сохранено.")
        await state.clear()
        return
    await message.answer("Нужны фото, видео или remove.")


@dp.callback_query(F.data == "admin_share")
async def admin_share(callback: CallbackQuery):
    share = await get_share()
    await callback.message.answer(
        f"📣 <b>Настройка блока «Поделиться»</b>\n\n"
        f"Текущий текст:\n{share['text']}\n\n"
        "Пояснение:\n"
        "Этот блок показывает пользователю его реферальную ссылку и дополнительный текст приглашения.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Изменить текст", callback_data="share_set_text")],
                [InlineKeyboardButton(text="Изменить фото/видео", callback_data="share_set_media")],
                [InlineKeyboardButton(text="👁 Предпросмотр", callback_data="share_preview")],
            ]
        ),
    )
    await callback.answer()


@dp.callback_query(F.data == "share_preview")
async def share_preview(callback: CallbackQuery):
    await send_share_message(callback.from_user.id, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "share_set_text")
async def share_set_text(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_share_text)
    await callback.message.answer("Отправь новый текст блока «Поделиться».", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_share_text)
async def share_set_text_finish(message: Message, state: FSMContext):
    share = await get_share()
    share["text"] = message.html_text or message.text or share["text"]
    await set_json_setting("share_json", share)
    await message.answer("✅ Текст блока «Поделиться» сохранён.")
    await state.clear()


@dp.callback_query(F.data == "share_set_media")
async def share_set_media(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_share_media)
    await callback.message.answer("Отправь фото/видео или <code>remove</code>.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_share_media)
async def share_set_media_finish(message: Message, state: FSMContext):
    share = await get_share()
    if message.text and message.text.strip().lower() == "remove":
        share["media_type"] = None
        share["media_file_id"] = None
    elif message.photo:
        share["media_type"] = "photo"
        share["media_file_id"] = message.photo[-1].file_id
    elif message.video:
        share["media_type"] = "video"
        share["media_file_id"] = message.video.file_id
    else:
        await message.answer("Нужны фото/видео/remove.")
        return
    await set_json_setting("share_json", share)
    await message.answer("✅ Медиа блока «Поделиться» сохранено.")
    await state.clear()


@dp.callback_query(F.data == "admin_homework")
async def admin_homework(callback: CallbackQuery):
    hw = await get_homework()
    await callback.message.answer(
        f"📝 <b>Настройка домашнего задания</b>\n\n"
        f"Текущий текст:\n{hw['text']}\n\n"
        "Пояснение:\n"
        "Это вводное сообщение, которое показывается пользователю перед отправкой домашнего задания.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Изменить текст", callback_data="hw_set_text")],
                [InlineKeyboardButton(text="Изменить фото/видео", callback_data="hw_set_media")],
                [InlineKeyboardButton(text="👁 Предпросмотр", callback_data="hw_preview")],
            ]
        ),
    )
    await callback.answer()


@dp.callback_query(F.data == "hw_preview")
async def hw_preview(callback: CallbackQuery):
    await send_homework_intro(callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "hw_set_text")
async def hw_set_text(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_homework_text)
    await callback.message.answer("Отправь новый текст блока домашнего задания.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_homework_text)
async def hw_set_text_finish(message: Message, state: FSMContext):
    hw = await get_homework()
    hw["text"] = message.html_text or message.text or hw["text"]
    await set_json_setting("homework_json", hw)
    await message.answer("✅ Текст домашнего задания сохранён.")
    await state.clear()


@dp.callback_query(F.data == "hw_set_media")
async def hw_set_media(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_homework_media)
    await callback.message.answer("Отправь фото/видео или <code>remove</code>.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.set_homework_media)
async def hw_set_media_finish(message: Message, state: FSMContext):
    hw = await get_homework()
    if message.text and message.text.strip().lower() == "remove":
        hw["media_type"] = None
        hw["media_file_id"] = None
    elif message.photo:
        hw["media_type"] = "photo"
        hw["media_file_id"] = message.photo[-1].file_id
    elif message.video:
        hw["media_type"] = "video"
        hw["media_file_id"] = message.video.file_id
    else:
        await message.answer("Нужны фото/видео/remove.")
        return
    await set_json_setting("homework_json", hw)
    await message.answer("✅ Медиа домашнего задания сохранено.")
    await state.clear()


@dp.callback_query(F.data == "admin_start_buttons")
async def admin_start_buttons(callback: CallbackQuery):
    data = await get_start_buttons()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Купить доступ: {'ВКЛ' if data['buy'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:buy")],
            [InlineKeyboardButton(text=f"Тарифы: {'ВКЛ' if data['tariffs'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:tariffs")],
            [InlineKeyboardButton(text=f"Поделиться: {'ВКЛ' if data['share'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:share")],
            [InlineKeyboardButton(text=f"Поддержка: {'ВКЛ' if data['support'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:support")],
            [InlineKeyboardButton(text=f"Оферта: {'ВКЛ' if data['offer'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:offer")],
            [InlineKeyboardButton(text=f"Промокод: {'ВКЛ' if data['promo'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:promo")],
            [InlineKeyboardButton(text=f"Моя подписка после покупки: {'ВКЛ' if data['my_sub_after_buy'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:my_sub_after_buy")],
            [InlineKeyboardButton(text=f"Пользовательские кнопки: {'ВКЛ' if data['custom_root'] else 'ВЫКЛ'}", callback_data="toggle_start_btn:custom_root")],
        ]
    )
    await callback.message.answer(
        "🔘 <b>Настройка кнопок стартового сообщения</b>\n\n"
        "Здесь можно по отдельности включать и выключать кнопки, которые видит пользователь в стартовом сообщении.\n\n"
        "Кнопка «Админка» здесь не настраивается: у генерального админа она работает всегда.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("toggle_start_btn:"))
async def toggle_start_btn(callback: CallbackQuery):
    key = callback.data.split(":")[1]
    data = await get_start_buttons()
    if key in data:
        data[key] = not data[key]
        await set_json_setting("start_buttons_json", data)
        await callback.message.answer("✅ Настройка кнопки обновлена.")
    await callback.answer()


@dp.callback_query(F.data == "admin_custom_menu")
async def admin_custom_menu(callback: CallbackQuery):
    rows = await get_custom_nodes(None)
    text = "🧩 <b>Пользовательские кнопки и подменю</b>\n\n"
    text += "Здесь можно создавать свои кнопки, подменю и сообщения.\n"
    text += "Каждая кнопка может иметь:\n"
    text += "— название\n"
    text += "— текст сообщения\n"
    text += "— фото/видео\n"
    text += "— родительскую кнопку (чтобы стать пунктом подменю)\n"
    text += "— порядок отображения\n\n"
    if rows:
        text += "Корневые кнопки:\n"
        for r in rows:
            text += f"ID {r['id']} | {'ВКЛ' if r['enabled'] else 'ВЫКЛ'} | {r['title']}\n"
    else:
        text += "Пока нет ни одной кнопки.\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать кнопку", callback_data="custom_create")],
            [InlineKeyboardButton(text="📝 Изменить название", callback_data="custom_set_title")],
            [InlineKeyboardButton(text="💬 Изменить сообщение", callback_data="custom_set_text")],
            [InlineKeyboardButton(text="🖼 Изменить фото/видео", callback_data="custom_set_media")],
            [InlineKeyboardButton(text="📂 Назначить родителя", callback_data="custom_set_parent")],
            [InlineKeyboardButton(text="↕️ Порядок", callback_data="custom_set_sort")],
            [InlineKeyboardButton(text="🔘 Вкл/выкл кнопку", callback_data="custom_toggle")],
            [InlineKeyboardButton(text="👁 Список всех кнопок", callback_data="custom_list_all")],
        ]
    )
    await callback.message.answer(text, reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data == "custom_create")
async def custom_create(callback: CallbackQuery):
    node_id = await next_custom_node_id()
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO custom_nodes (id, parent_id, title, enabled, message_text, media_type, media_file_id, sort_order)
            VALUES ($1, NULL, $2, FALSE, $3, NULL, NULL, $4)
            """,
            node_id,
            f"Новая кнопка {node_id}",
            "Текст не настроен.",
            node_id,
        )
    await callback.message.answer(
        f"✅ Создана кнопка с ID <b>{node_id}</b>.\n"
        "Теперь настрой её название, сообщение и при необходимости родителя."
    )
    await callback.answer()


@dp.callback_query(F.data == "custom_list_all")
async def custom_list_all(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, parent_id, title, enabled, sort_order
            FROM custom_nodes
            ORDER BY COALESCE(parent_id, 0), sort_order, id
            """
        )
    text = "🧩 <b>Все пользовательские кнопки</b>\n\n"
    if not rows:
        text += "Нет кнопок."
    else:
        for r in rows:
            text += (
                f"ID {r['id']} | parent={r['parent_id'] or 'root'} | "
                f"{'ВКЛ' if r['enabled'] else 'ВЫКЛ'} | "
                f"sort={r['sort_order']} | {r['title']}\n"
            )
    await callback.message.answer(text[:4000])
    await callback.answer()


@dp.callback_query(F.data == "custom_set_title")
async def custom_set_title_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.custom_node_title)
    await callback.message.answer(
        "Отправь строку:\n"
        "<code>ID_кнопки | Новое название</code>\n\n"
        "Пример:\n"
        "<code>2 | Отзывы</code>",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.custom_node_title)
async def custom_set_title_finish(message: Message, state: FSMContext):
    try:
        node_id_s, title = [x.strip() for x in (message.text or "").split("|", maxsplit=1)]
        node_id = int(node_id_s)
    except Exception:
        await message.answer("Неверный формат.")
        return
    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE custom_nodes SET title = $2 WHERE id = $1", node_id, title)
    await message.answer("✅ Название кнопки сохранено.")
    await state.clear()


@dp.callback_query(F.data == "custom_set_text")
async def custom_set_text_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.custom_node_text)
    await callback.message.answer(
        "Отправь строку:\n"
        "<code>ID_кнопки | Новый текст сообщения</code>",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.custom_node_text)
async def custom_set_text_finish(message: Message, state: FSMContext):
    try:
        node_id_s, text = [x.strip() for x in (message.text or "").split("|", maxsplit=1)]
        node_id = int(node_id_s)
    except Exception:
        await message.answer("Неверный формат.")
        return
    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE custom_nodes SET message_text = $2 WHERE id = $1", node_id, text)
    await message.answer("✅ Текст кнопки сохранён.")
    await state.clear()


@dp.callback_query(F.data == "custom_set_media")
async def custom_set_media_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.custom_node_media)
    await callback.message.answer(
        "Сначала отправь строку с ID:\n"
        "<code>ID_кнопки</code>\n"
        "потом следующим сообщением отправь фото/видео.\n\n"
        "Чтобы убрать медиа: <code>ID_кнопки | remove</code>",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.custom_node_media, F.text)
async def custom_set_media_text_stage(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if "|" in text and text.lower().endswith("remove"):
        try:
            node_id_s, _ = [x.strip() for x in text.split("|", maxsplit=1)]
            node_id = int(node_id_s)
        except Exception:
            await message.answer("Неверный формат.")
            return
        async with get_pool().acquire() as conn:
            await conn.execute(
                "UPDATE custom_nodes SET media_type = NULL, media_file_id = NULL WHERE id = $1",
                node_id,
            )
        await message.answer("✅ Медиа удалено.")
        await state.clear()
        return

    try:
        node_id = int(text)
    except Exception:
        await message.answer("Нужно отправить только ID кнопки.")
        return
    await state.update_data(custom_media_node_id=node_id)
    await message.answer("Теперь отправь фото или видео для этой кнопки.")


@dp.message(AdminStates.custom_node_media, F.photo)
async def custom_set_media_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    node_id = data.get("custom_media_node_id")
    if not node_id:
        await message.answer("Сначала отправь ID кнопки.")
        return
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE custom_nodes SET media_type = 'photo', media_file_id = $2 WHERE id = $1",
            node_id,
            message.photo[-1].file_id,
        )
    await message.answer("✅ Фото сохранено.")
    await state.clear()


@dp.message(AdminStates.custom_node_media, F.video)
async def custom_set_media_video(message: Message, state: FSMContext):
    data = await state.get_data()
    node_id = data.get("custom_media_node_id")
    if not node_id:
        await message.answer("Сначала отправь ID кнопки.")
        return
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE custom_nodes SET media_type = 'video', media_file_id = $2 WHERE id = $1",
            node_id,
            message.video.file_id,
        )
    await message.answer("✅ Видео сохранено.")
    await state.clear()


@dp.callback_query(F.data == "custom_set_parent")
async def custom_set_parent_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.custom_node_parent)
    await callback.message.answer(
        "Отправь строку:\n"
        "<code>ID_кнопки | parent_id</code>\n\n"
        "Если нужно сделать корневой кнопкой:\n"
        "<code>ID_кнопки | 0</code>",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.custom_node_parent)
async def custom_set_parent_finish(message: Message, state: FSMContext):
    try:
        node_id_s, parent_id_s = [x.strip() for x in (message.text or "").split("|", maxsplit=1)]
        node_id = int(node_id_s)
        parent_id = int(parent_id_s)
    except Exception:
        await message.answer("Неверный формат.")
        return
    if node_id == parent_id and parent_id != 0:
        await message.answer("Кнопка не может быть родителем самой себе.")
        return
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE custom_nodes SET parent_id = $2 WHERE id = $1",
            node_id,
            None if parent_id == 0 else parent_id,
        )
    await message.answer("✅ Родитель кнопки обновлён.")
    await state.clear()


@dp.callback_query(F.data == "custom_set_sort")
async def custom_set_sort_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.custom_node_sort)
    await callback.message.answer(
        "Отправь строку:\n"
        "<code>ID_кнопки | порядок</code>\n\n"
        "Чем меньше число, тем выше кнопка будет в списке.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.custom_node_sort)
async def custom_set_sort_finish(message: Message, state: FSMContext):
    try:
        node_id_s, sort_s = [x.strip() for x in (message.text or "").split("|", maxsplit=1)]
        node_id = int(node_id_s)
        sort = int(sort_s)
    except Exception:
        await message.answer("Неверный формат.")
        return
    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE custom_nodes SET sort_order = $2 WHERE id = $1", node_id, sort)
    await message.answer("✅ Порядок кнопки сохранён.")
    await state.clear()


@dp.callback_query(F.data == "custom_toggle")
async def custom_toggle_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.custom_node_toggle)
    await callback.message.answer(
        "Отправь ID кнопки, которую нужно включить или выключить.\n"
        "Бот сам переключит её состояние.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.custom_node_toggle)
async def custom_toggle_finish(message: Message, state: FSMContext):
    try:
        node_id = int((message.text or "").strip())
    except Exception:
        await message.answer("Нужно указать ID кнопки.")
        return
    async with get_pool().acquire() as conn:
        current = await conn.fetchval("SELECT enabled FROM custom_nodes WHERE id = $1", node_id)
        if current is None:
            await message.answer("Кнопка не найдена.")
            return
        await conn.execute("UPDATE custom_nodes SET enabled = $2 WHERE id = $1", node_id, not current)
    await message.answer("✅ Состояние кнопки переключено.")
    await state.clear()


@dp.callback_query(F.data == "admin_promo_menu")
async def admin_promo_menu(callback: CallbackQuery):
    await callback.message.answer(
        "🎁 <b>Промокоды</b>\n\n"
        "Создание:\n"
        "<code>CODE | percent | значение | tariff_id(или 0) | max_uses(или 0) | days_valid</code>\n"
        "или\n"
        "<code>CODE | amount | значение_в_рублях | tariff_id(или 0) | max_uses(или 0) | days_valid</code>\n\n"
        "Пояснения:\n"
        "percent — скидка в процентах\n"
        "amount — фиксированная скидка в рублях\n"
        "tariff_id = 0 — промокод для всех тарифов\n"
        "max_uses = 0 — без лимита\n"
        "days_valid — сколько дней действует промокод\n",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Создать промокод", callback_data="promo_create")],
                [InlineKeyboardButton(text="Отключить промокод", callback_data="promo_delete")],
                [InlineKeyboardButton(text="📊 Статистика промокодов", callback_data="admin_stats_promos")],
            ]
        ),
    )
    await callback.answer()


@dp.callback_query(F.data == "promo_create")
async def promo_create_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.promo_create)
    await callback.message.answer("Отправь строку создания промокода.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.promo_create)
async def promo_create_finish(message: Message, state: FSMContext):
    try:
        code, mode, value, tariff_id, max_uses, days_valid = [x.strip() for x in (message.text or "").split("|")]
        code = code.upper()
        tariff_id_i = int(tariff_id)
        max_uses_i = int(max_uses)
        days_valid_i = int(days_valid)
        expires = now() + timedelta(days=days_valid_i)
        discount_percent = int(value) if mode == "percent" else None
        discount_kop = int(round(float(value.replace(",", ".")) * 100)) if mode == "amount" else None
        if mode not in {"percent", "amount"}:
            raise ValueError
    except Exception:
        await message.answer("Неверный формат.")
        return

    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO promo_codes (code, active, discount_percent, discount_kop, tariff_id, max_uses, used_count, expires_at)
            VALUES ($1, TRUE, $2, $3, $4, $5, 0, $6)
            ON CONFLICT (code) DO UPDATE SET
                active = TRUE,
                discount_percent = EXCLUDED.discount_percent,
                discount_kop = EXCLUDED.discount_kop,
                tariff_id = EXCLUDED.tariff_id,
                max_uses = EXCLUDED.max_uses,
                expires_at = EXCLUDED.expires_at
            """,
            code,
            discount_percent,
            discount_kop,
            None if tariff_id_i == 0 else tariff_id_i,
            None if max_uses_i == 0 else max_uses_i,
            expires,
        )
    await message.answer(f"✅ Промокод <b>{code}</b> сохранён.")
    await state.clear()


@dp.callback_query(F.data == "promo_delete")
async def promo_delete_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.promo_delete)
    await callback.message.answer("Введи код промокода для деактивации.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.promo_delete)
async def promo_delete_finish(message: Message, state: FSMContext):
    code = (message.text or "").strip().upper()
    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE promo_codes SET active = FALSE WHERE code = $1", code)
    await message.answer("✅ Промокод отключён.")
    await state.clear()


@dp.callback_query(F.data == "admin_referral_menu")
async def admin_referral_menu(callback: CallbackQuery):
    s = await get_referral_settings()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика рефералов", callback_data="admin_stats_referrals")],
            [InlineKeyboardButton(text="✏️ Изменить настройки", callback_data="refcfg_edit")],
        ]
    )
    await callback.message.answer(
        "👥 <b>Рефералка</b>\n\n"
        f"Включена: <b>{s['enabled']}</b>\n"
        f"Бонусных дней: <b>{s['reward_days']}</b>\n"
        f"Текст: {s['text']}\n\n"
        "Пояснение:\n"
        "После первой оплаты приглашённого пользователя пригласившему начисляются бонусные дни.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "refcfg_edit")
async def refcfg_edit(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.refcfg)
    await callback.message.answer(
        "Отправь строку:\n"
        "<code>1 | 30 | Новый текст рефералки</code>\n\n"
        "Пояснения:\n"
        "1/0 — включить или выключить реферальную систему\n"
        "30 — сколько дней начислять за успешное приглашение",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.refcfg)
async def refcfg_finish(message: Message, state: FSMContext):
    try:
        enabled_s, days_s, text = [x.strip() for x in (message.text or "").split("|", maxsplit=2)]
        data = await get_referral_settings()
        data["enabled"] = enabled_s == "1"
        data["reward_days"] = int(days_s)
        data["text"] = text
        await set_json_setting("referral_json", data)
        await message.answer("✅ Рефералка обновлена.")
        await state.clear()
    except Exception:
        await message.answer("Неверный формат.")


@dp.callback_query(F.data == "admin_reminders")
async def admin_reminders(callback: CallbackQuery, state: FSMContext):
    rem = await get_reminders()
    await callback.message.answer(
        "🔔 <b>Напоминания о продлении</b>\n\n"
        f"Включены: <b>{rem['enabled']}</b>\n"
        f"Первое за: <b>{rem['hours_before_1']}</b> ч\n"
        f"Второе за: <b>{rem['hours_before_2']}</b> ч\n"
        f"Третье за: <b>{rem['hours_before_3']}</b> ч\n\n"
        "Отправь строку:\n"
        "<code>1 | 72 | 24 | 1</code>\n\n"
        "Пояснения:\n"
        "1/0 — включить или выключить все напоминания\n"
        "72 | 24 | 1 — за сколько часов до конца подписки отправлять 3 напоминания",
        reply_markup=reply_back_kb(True),
    )
    await state.set_state(AdminStates.set_reminders)
    await callback.answer()


@dp.message(AdminStates.set_reminders)
async def admin_reminders_finish(message: Message, state: FSMContext):
    try:
        enabled_s, h1, h2, h3 = [x.strip() for x in (message.text or "").split("|")]
        rem = {
            "enabled": enabled_s == "1",
            "hours_before_1": int(h1),
            "hours_before_2": int(h2),
            "hours_before_3": int(h3),
        }
    except Exception:
        await message.answer("Неверный формат.")
        return
    await set_json_setting("reminders_json", rem)
    await message.answer("✅ Напоминания обновлены.")
    await state.clear()


@dp.callback_query(F.data == "admin_broadcast_menu")
async def admin_broadcast_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📨 Рассылка текстом", callback_data="admin_broadcast_text")],
            [InlineKeyboardButton(text="🎬 Рассылка фото/видео", callback_data="admin_broadcast_media")],
        ]
    )
    await callback.message.answer(
        "📨 <b>Раздел рассылок</b>\n\n"
        "Можно отправить массовое сообщение всем пользователям из базы.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_broadcast_text")
async def admin_broadcast_text(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.broadcast_text)
    await callback.message.answer("Отправь текст рассылки.", reply_markup=reply_back_kb(True))
    await callback.answer()


@dp.message(AdminStates.broadcast_text)
async def admin_broadcast_text_finish(message: Message, state: FSMContext):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
    ok = 0
    bad = 0
    for row in rows:
        try:
            await bot.send_message(row["user_id"], message.html_text or message.text or "")
            ok += 1
            await asyncio.sleep(0.03)
        except Exception:
            bad += 1
    await message.answer(f"✅ Рассылка завершена.\nУспешно: {ok}\nОшибок: {bad}")
    await state.clear()


@dp.callback_query(F.data == "admin_broadcast_media")
async def admin_broadcast_media(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.broadcast_media_caption)
    await callback.message.answer(
        "Сначала отправь подпись, либо <code>-</code> без подписи.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.broadcast_media_caption)
async def admin_broadcast_media_caption_finish(message: Message, state: FSMContext):
    caption = "" if (message.text or "").strip() == "-" else (message.html_text or message.text or "")
    await state.update_data(caption=caption)
    await state.set_state(AdminStates.broadcast_media_file)
    await message.answer("Теперь отправь фото или видео.", reply_markup=reply_back_kb(True))


@dp.message(AdminStates.broadcast_media_file)
async def admin_broadcast_media_file_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    caption = data.get("caption", "")
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
    ok = 0
    bad = 0
    for row in rows:
        try:
            if message.photo:
                await bot.send_photo(row["user_id"], message.photo[-1].file_id, caption=caption or None)
            elif message.video:
                await bot.send_video(row["user_id"], message.video.file_id, caption=caption or None)
            else:
                await message.answer("Нужно фото или видео.")
                return
            ok += 1
            await asyncio.sleep(0.03)
        except Exception:
            bad += 1
    await message.answer(f"✅ Медиа-рассылка завершена.\nУспешно: {ok}\nОшибок: {bad}")
    await state.clear()


@dp.callback_query(F.data == "admin_admins_menu")
async def admin_admins_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👮 Список админов", callback_data="admin_admins_list")],
            [InlineKeyboardButton(text="➕ Выдать админку", callback_data="admin_admins_add")],
            [InlineKeyboardButton(text="➖ Снять админку", callback_data="admin_admins_remove")],
        ]
    )
    await callback.message.answer(
        "👮 <b>Раздел админов</b>\n\n"
        "Только генеральный админ может выдавать и забирать админку.\n"
        "Кнопка «Админка» у генерального админа работает всегда.\n\n"
        "Во всех действиях можно использовать user_id и @username.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_admins_list")
async def admin_admins_list(callback: CallbackQuery):
    rows = await list_admins()
    text = f"👮 <b>Админы</b>\n\nГлавный админ: <code>{ADMIN_ID}</code>\n\n"
    if rows:
        for row in rows:
            text += (
                f"<code>{row['user_id']}</code> | "
                f"добавлен: {row['added_at'].strftime('%d.%m.%Y %H:%M')} | "
                f"кем: <code>{row['added_by']}</code>\n"
            )
    else:
        text += "Дополнительных админов нет."
    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "admin_admins_add")
async def admin_admins_add(callback: CallbackQuery, state: FSMContext):
    if not await is_general_admin(callback.from_user.id):
        await callback.answer("Только главный админ", show_alert=True)
        return
    await state.set_state(AdminStates.add_admin_user)
    await callback.message.answer(
        "Введи user_id или @username пользователя, которому выдать админку.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.add_admin_user)
async def admin_admins_add_finish(message: Message, state: FSMContext):
    if not await is_general_admin(message.from_user.id):
        return
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return
    await add_admin(user_id, message.from_user.id)
    await message.answer(f"✅ Пользователь <code>{user_id}</code> теперь админ.")
    await state.clear()


@dp.callback_query(F.data == "admin_admins_remove")
async def admin_admins_remove(callback: CallbackQuery, state: FSMContext):
    if not await is_general_admin(callback.from_user.id):
        await callback.answer("Только главный админ", show_alert=True)
        return
    await state.set_state(AdminStates.remove_admin_user)
    await callback.message.answer(
        "Введи user_id или @username пользователя, у которого забрать админку.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.remove_admin_user)
async def admin_admins_remove_finish(message: Message, state: FSMContext):
    if not await is_general_admin(message.from_user.id):
        return
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return
    ok = await remove_admin(user_id)
    if not ok:
        await message.answer("Главного админа снять нельзя.")
    else:
        await message.answer(f"✅ Админка у <code>{user_id}</code> снята.")
    await state.clear()


@dp.callback_query(F.data == "admin_logs_menu")
async def admin_logs_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Логи по user_id / @username", callback_data="admin_logs_by_user")],
            [InlineKeyboardButton(text="🕘 Последние действия", callback_data="admin_logs_recent")],
            [InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users_list")],
        ]
    )
    await callback.message.answer(
        "🧾 <b>Раздел логов</b>\n\n"
        "Можно посмотреть логи конкретного пользователя или последние действия по системе.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_logs_by_user")
async def admin_logs_by_user(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.logs_user)
    await callback.message.answer(
        "Введи user_id или @username для просмотра логов.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.logs_user)
async def admin_logs_by_user_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT action, details, created_at
            FROM action_logs
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT 20
            """,
            user_id,
        )
    if not rows:
        await message.answer("Логов нет.")
    else:
        text = f"🧾 <b>Логи пользователя {user_id}</b>\n\n"
        for r in rows:
            text += f"{r['created_at'].strftime('%d.%m %H:%M')} | <b>{r['action']}</b>\n{r['details'] or '-'}\n\n"
        await message.answer(text[:4000])
    await state.clear()


@dp.callback_query(F.data == "admin_logs_recent")
async def admin_logs_recent(callback: CallbackQuery):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id, action, details, created_at
            FROM action_logs
            ORDER BY created_at DESC
            LIMIT 30
            """
        )
    text = "🕘 <b>Последние действия</b>\n\n"
    if not rows:
        text += "Логов пока нет."
    else:
        for row in rows:
            text += (
                f"{row['created_at'].strftime('%d.%m %H:%M')} | "
                f"<code>{row['user_id'] or 0}</code> | "
                f"<b>{row['action']}</b>\n"
                f"{row['details'] or '-'}\n\n"
            )
    await callback.message.answer(text[:4000])
    await callback.answer()


@dp.callback_query(F.data == "admin_manual_menu")
async def admin_manual_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_manual_add_sub")],
            [InlineKeyboardButton(text="❌ Снять подписку", callback_data="admin_manual_remove_sub")],
            [InlineKeyboardButton(text="🔗 Выдать ссылку вручную", callback_data="admin_manual_invite")],
        ]
    )
    await callback.message.answer(
        "⚙️ <b>Ручное управление</b>\n\n"
        "Здесь можно вручную выдать подписку, снять её или отправить ссылку в канал.\n"
        "Во всех действиях можно использовать user_id и @username.",
        reply_markup=kb,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_manual_add_sub")
async def admin_manual_add_sub(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.manual_add_sub_user)
    await callback.message.answer(
        "Введи user_id или @username пользователя.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.manual_add_sub_user)
async def admin_manual_add_sub_user_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return
    await state.update_data(manual_user_id=user_id)
    await state.set_state(AdminStates.manual_add_sub_days)
    await message.answer("На сколько дней выдать подписку?", reply_markup=reply_back_kb(True))


@dp.message(AdminStates.manual_add_sub_days)
async def admin_manual_add_sub_days_finish(message: Message, state: FSMContext):
    try:
        days = int((message.text or "").strip())
    except Exception:
        await message.answer("Нужно число дней.")
        return
    data = await state.get_data()
    user_id = data["manual_user_id"]
    await add_sub_days(user_id, days)
    await message.answer(f"✅ Подписка выдана пользователю <code>{user_id}</code> на {days} дней.")
    await state.clear()


@dp.callback_query(F.data == "admin_manual_remove_sub")
async def admin_manual_remove_sub(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.manual_remove_sub_user)
    await callback.message.answer(
        "Введи user_id или @username пользователя.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.manual_remove_sub_user)
async def admin_manual_remove_sub_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return

    await remove_sub(user_id)
    kicked, kick_text = await kick_user_from_channel(user_id)

    await message.answer(
        f"✅ Подписка у <code>{user_id}</code> снята.\n"
        f"{'✅' if kicked else '⚠️'} Кик из канала: {kick_text}"
    )
    await state.clear()


@dp.callback_query(F.data == "admin_manual_invite")
async def admin_manual_invite(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.manual_invite_user)
    await callback.message.answer(
        "Введи user_id или @username пользователя, которому выдать ссылку.",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.manual_invite_user)
async def admin_manual_invite_finish(message: Message, state: FSMContext):
    user_id = await resolve_user_input_to_id(message.text or "")
    if not user_id:
        await message.answer("Пользователь не найден.")
        return
    try:
        await send_invite_to_user(user_id)
        await message.answer(f"✅ Ссылка отправлена пользователю <code>{user_id}</code>.")
    except Exception as e:
        await message.answer(f"Ошибка отправки: <code>{e}</code>")
    await state.clear()


@dp.callback_query(F.data == "admin_guides_menu")
async def admin_guides_menu(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await callback.message.answer(
        "📚 <b>Гайды и документация</b>\n\n"
        "Здесь собраны инструкции по настройке бота.\n"
        "Выбери нужный раздел.",
        reply_markup=admin_guides_kb(),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("guide:"))
async def admin_guide_open(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    key = callback.data.split(":", maxsplit=1)[1]
    guides = await get_guides()
    text = guides.get(key)

    if not text:
        await callback.answer("Раздел не найден", show_alert=True)
        return

    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "guide_edit_menu")
async def guide_edit_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.guide_edit_pick_key)
    await callback.message.answer(
        "Введи ключ гайда, который хочешь отредактировать.\n\n"
        "Доступные ключи:\n"
        "<code>start</code>\n"
        "<code>start_buttons</code>\n"
        "<code>tariffs</code>\n"
        "<code>offer</code>\n"
        "<code>funnel</code>\n"
        "<code>promo</code>\n"
        "<code>referral</code>\n"
        "<code>custom</code>\n"
        "<code>broadcast</code>\n"
        "<code>manual</code>\n"
        "<code>logs</code>\n"
        "<code>troubleshoot</code>\n"
        "<code>faq</code>\n"
        "<code>recommended</code>\n"
        "<code>one_or_three</code>\n"
        "<code>ads_checklist</code>",
        reply_markup=reply_back_kb(True),
    )
    await callback.answer()


@dp.message(AdminStates.guide_edit_pick_key)
async def guide_edit_pick_key_finish(message: Message, state: FSMContext):
    key = (message.text or "").strip()
    guides = await get_guides()
    if key not in guides:
        await message.answer("Неизвестный ключ гайда.")
        return
    await state.update_data(guide_key=key)
    await state.set_state(AdminStates.guide_edit_text)
    await message.answer(
        f"Теперь отправь новый текст для гайда <code>{key}</code>.",
        reply_markup=reply_back_kb(True),
    )


@dp.message(AdminStates.guide_edit_text)
async def guide_edit_text_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    key = data.get("guide_key")
    if not key:
        await message.answer("Ключ гайда не найден.")
        return
    guides = await get_guides()
    guides[key] = message.html_text or message.text or guides.get(key, "")
    await save_guides(guides)
    await message.answer(f"✅ Гайд <code>{key}</code> обновлён.")
    await state.clear()


@dp.message(Command("dbtest"))
async def dbtest(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    async with get_pool().acquire() as conn:
        now_db = await conn.fetchval("SELECT NOW()")
        users_cnt = await conn.fetchval("SELECT COUNT(*) FROM users")
        q_cnt = await conn.fetchval("SELECT COUNT(*) FROM questions")
        p_cnt = await conn.fetchval("SELECT COUNT(*) FROM promo_codes")
        a_cnt = await conn.fetchval("SELECT COUNT(*) FROM admins")
        c_cnt = await conn.fetchval("SELECT COUNT(*) FROM custom_nodes")
    await message.answer(
        f"✅ DB OK\n\n"
        f"NOW(): <b>{now_db}</b>\n"
        f"users: <b>{users_cnt}</b>\n"
        f"questions: <b>{q_cnt}</b>\n"
        f"promo: <b>{p_cnt}</b>\n"
        f"extra_admins: <b>{a_cnt}</b>\n"
        f"custom_nodes: <b>{c_cnt}</b>"
    )


async def check_subs():
    while True:
        try:
            async with get_pool().acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT user_id, expire_date
                    FROM users
                    WHERE expire_date IS NOT NULL
                    """
                )

            current = now()
            for row in rows:
                if row["expire_date"] and row["expire_date"] < current:
                    try:
                        await bot.ban_chat_member(CHANNEL_ID, row["user_id"])
                        await bot.unban_chat_member(CHANNEL_ID, row["user_id"])
                        await log_action(row["user_id"], "auto_kick_expired", "Истекла подписка, выполнен кик из канала")
                    except Exception as e:
                        logger.exception("AUTO KICK ERROR: %s", e)
                        await log_action(row["user_id"], "auto_kick_expired_error", str(e))
        except Exception:
            logger.exception("CHECK_SUBS ERROR")

        await asyncio.sleep(60)


async def check_funnel():
    while True:
        try:
            funnel = await get_funnel()
            if funnel.get("enabled"):
                hh, mm = map(int, funnel.get("send_time", "20:00").split(":"))
                current = now()
                async with get_pool().acquire() as conn:
                    users = await conn.fetch(
                        """
                        SELECT user_id, started_at, has_purchased
                        FROM users
                        WHERE started = TRUE AND has_purchased = FALSE AND started_at IS NOT NULL
                        """
                    )
                    for u in users:
                        if u["has_purchased"] or not u["started_at"]:
                            continue
                        for idx, step in enumerate(funnel["steps"], start=1):
                            if not step.get("enabled"):
                                continue
                            exists = await conn.fetchval(
                                "SELECT 1 FROM funnel_sends WHERE user_id = $1 AND step_idx = $2",
                                u["user_id"],
                                idx,
                            )
                            if exists:
                                continue
                            due = u["started_at"] + timedelta(hours=int(step.get("delay_hours", 0)))
                            send_moment = due.replace(hour=hh, minute=mm, second=0, microsecond=0)
                            if current >= send_moment:
                                payload = {
                                    "content_type": step.get("media_type") or "text",
                                    "text": step.get("text"),
                                    "file_id": step.get("media_file_id"),
                                    "caption": step.get("text"),
                                }
                                await send_payload(u["user_id"], payload)
                                await conn.execute(
                                    "INSERT INTO funnel_sends (user_id, step_idx, sent_at) VALUES ($1, $2, NOW()) ON CONFLICT DO NOTHING",
                                    u["user_id"],
                                    idx,
                                )
                                await log_action(u["user_id"], "funnel_sent", f"step={idx}")
        except Exception:
            logger.exception("CHECK_FUNNEL ERROR")
        await asyncio.sleep(300)


async def check_renew_reminders():
    while True:
        try:
            rem = await get_reminders()
            if rem.get("enabled"):
                async with get_pool().acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT user_id, expire_date, pseudo_autorenew_enabled, active_tariff_id
                        FROM users
                        WHERE expire_date IS NOT NULL AND expire_date > NOW()
                        """
                    )
                    thresholds = [
                        ("h1", int(rem["hours_before_1"])),
                        ("h2", int(rem["hours_before_2"])),
                        ("h3", int(rem["hours_before_3"])),
                    ]
                    for row in rows:
                        hours_left = (row["expire_date"] - now()).total_seconds() / 3600
                        for key, hours in thresholds:
                            if hours_left <= hours:
                                exists = await conn.fetchval(
                                    "SELECT 1 FROM reminder_sends WHERE user_id = $1 AND reminder_key = $2",
                                    row["user_id"],
                                    key,
                                )
                                if exists:
                                    continue
                                tariff = await get_tariff_by_id(row["active_tariff_id"]) if row["active_tariff_id"] else None
                                text = (
                                    f"⏰ Подписка скоро закончится.\n"
                                    f"Осталось примерно <b>{max(0, int(hours_left))} ч.</b>\n"
                                )
                                kb = InlineKeyboardMarkup(
                                    inline_keyboard=[
                                        [InlineKeyboardButton(text="💳 Продлить сейчас", callback_data="show_tariffs")]
                                    ]
                                )
                                await bot.send_message(row["user_id"], text, reply_markup=kb)
                                if row["pseudo_autorenew_enabled"] and tariff:
                                    await send_invoice_for_tariff(row["user_id"], tariff["id"], True)
                                await conn.execute(
                                    "INSERT INTO reminder_sends (user_id, reminder_key, sent_at) VALUES ($1, $2, NOW()) ON CONFLICT DO NOTHING",
                                    row["user_id"],
                                    key,
                                )
        except Exception:
            logger.exception("CHECK_REMINDERS ERROR")
        await asyncio.sleep(300)


async def handle(request):
    return web.Response(text="OK")


async def start_web():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("WEB STARTED ON PORT %s", port)


@dp.error()
async def on_error(event):
    logger.exception("GLOBAL ERROR: %s", event.exception)
    return True


async def send_deploy_report():
    tariffs = await get_tariffs()
    tariff_1 = tariffs[0]
    text = (
        "✅ <b>Бот запущен</b>\n\n"
        f"Время: <b>{now().strftime('%d.%m.%Y %H:%M:%S')}</b>\n"
        f"URL: <b>{RENDER_EXTERNAL_URL or 'не задан'}</b>\n"
        f"Тариф 1: <b>{tariff_1['name']}</b> / {format_rub_from_kop(int(tariff_1['price_kop']))} / {tariff_1['days']} дн."
    )
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        logger.exception("DEPLOY REPORT ERROR")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    if not PAYMENTS_TOKEN:
        raise RuntimeError("PAYMENTS_TOKEN не задан")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан")

    logger.info("BOT_TOKEN prefix: %s", BOT_TOKEN[:15] if BOT_TOKEN else "EMPTY")

    await init_db()
    await start_web()

    asyncio.create_task(check_subs())
    asyncio.create_task(check_funnel())
    asyncio.create_task(check_renew_reminders())

    await bot.delete_webhook(drop_pending_updates=True)

    me = await bot.get_me()
    logger.info("BOT = @%s (%s)", me.username, me.id)

    await send_deploy_report()

    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types(),
    )


if __name__ == "__main__":
    asyncio.run(main())
