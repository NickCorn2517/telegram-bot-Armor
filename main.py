import asyncio
import logging
import os
from datetime import datetime, timedelta

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
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
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_СЮДА_BOT_TOKEN")
PAYMENTS_TOKEN = os.getenv("PAYMENTS_TOKEN", "ВСТАВЬ_СЮДА_PAYMENTS_TOKEN")

CHANNEL_ID = -1003616232121
ADMIN_ID = 583554883

DB_PATH = "subs.db"

DEFAULT_PRICE = 1500000  # 15000 RUB в копейках
DEFAULT_DAYS = 365

DEFAULT_START_TEXT = (
    "🔥 <b>Доступ к обучению обклейки полиуретановой пленкой</b>\n\n"
    "💰 15000₽ / 365 дней\n"
    "📈 Материалы, полный цикл обучения, все аспекты бизнеса"
)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

BTN_MENU = "🏠 Меню"
BTN_BUY = "💳 Купить доступ"
BTN_SUB = "📅 Моя подписка"
BTN_ADMIN = "⚙️ Админка"


# ================= FSM =================
class AdminStates(StatesGroup):
    add_user_id = State()
    add_days = State()

    extend_user_id = State()
    extend_days = State()

    delete_user_id = State()
    find_user_id = State()

    broadcast_text = State()
    broadcast_media_caption = State()
    broadcast_media_file = State()

    invite_user_id = State()
    answer_user_id = State()
    answer_text = State()

    add_admin_user_id = State()
    remove_admin_user_id = State()

    set_start_text = State()
    set_start_media = State()

    set_price = State()
    set_sub_days = State()


class UserStates(StatesGroup):
    ask_content = State()
    ask_support = State()


# ================= БАЗА =================
async def init_db():
    logger.info("INIT_DB: start")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                expire_date TEXT NULL,
                created_at TEXT NOT NULL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                type TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open'
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                added_at TEXT NOT NULL,
                added_by INTEGER NOT NULL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value)
            VALUES ('start_text', ?)
        """, (DEFAULT_START_TEXT,))

        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value)
            VALUES ('start_media_type', NULL)
        """)

        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value)
            VALUES ('start_media_file_id', NULL)
        """)

        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value)
            VALUES ('sub_price_kop', ?)
        """, (str(DEFAULT_PRICE),))

        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value)
            VALUES ('sub_days', ?)
        """, (str(DEFAULT_DAYS),))

        await db.commit()

    logger.info("INIT_DB: done")


# ================= ХЕЛПЕРЫ БД =================
async def get_setting(key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row[0] if row else None


async def set_setting(key: str, value: str | None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))
        await db.commit()


async def get_sub_price() -> int:
    value = await get_setting("sub_price_kop")
    try:
        return int(value)
    except Exception:
        return DEFAULT_PRICE


async def get_sub_days() -> int:
    value = await get_setting("sub_days")
    try:
        return int(value)
    except Exception:
        return DEFAULT_DAYS


def format_rub_from_kop(kop: int) -> str:
    rub = kop / 100
    if float(rub).is_integer():
        return f"{int(rub)}₽"
    return f"{rub:.2f}₽"


async def is_admin(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM admins WHERE user_id = ?",
            (user_id,)
        )
        row = await cur.fetchone()
        return bool(row)


async def add_admin(user_id: int, added_by: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO admins (user_id, added_at, added_by)
            VALUES (?, ?, ?)
        """, (user_id, datetime.now().isoformat(), added_by))
        await db.commit()


async def remove_admin(user_id: int):
    if user_id == ADMIN_ID:
        return False

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        await db.commit()
    return True


async def list_admins():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT user_id, added_at, added_by
            FROM admins
            ORDER BY added_at DESC
        """)
        return await cur.fetchall()


async def ensure_user_exists(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, expire_date, created_at)
            VALUES (?, NULL, ?)
        """, (user_id, datetime.now().isoformat()))
        await db.commit()


async def get_sub(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT user_id, expire_date, created_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        return await cur.fetchone()


async def has_active_sub(user_id: int) -> bool:
    row = await get_sub(user_id)
    if not row or not row["expire_date"]:
        return False
    return datetime.fromisoformat(row["expire_date"]) > datetime.now()


async def add_sub(user_id: int):
    days = await get_sub_days()
    await add_sub_days(user_id, days)


async def add_sub_days(user_id: int, days: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT expire_date FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cur.fetchone()

        now = datetime.now()

        if row:
            old_expire = row[0]
            if old_expire:
                old_expire_dt = datetime.fromisoformat(old_expire)
                if old_expire_dt > now:
                    expire = old_expire_dt + timedelta(days=days)
                else:
                    expire = now + timedelta(days=days)
            else:
                expire = now + timedelta(days=days)

            await db.execute(
                "UPDATE users SET expire_date = ? WHERE user_id = ?",
                (expire.isoformat(), user_id)
            )
        else:
            expire = now + timedelta(days=days)
            await db.execute(
                "INSERT INTO users (user_id, expire_date, created_at) VALUES (?, ?, ?)",
                (user_id, expire.isoformat(), now.isoformat())
            )

        await db.commit()


async def remove_sub(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET expire_date = NULL WHERE user_id = ?",
            (user_id,)
        )
        await db.commit()


async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT user_id, expire_date, created_at FROM users ORDER BY created_at DESC"
        )
        return await cur.fetchall()


async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        now = datetime.now().isoformat()

        cur = await db.execute("SELECT COUNT(*) FROM users")
        total = (await cur.fetchone())[0]

        cur = await db.execute("""
            SELECT COUNT(*) FROM users
            WHERE expire_date IS NOT NULL AND expire_date > ?
        """, (now,))
        active = (await cur.fetchone())[0]

        cur = await db.execute("""
            SELECT COUNT(*) FROM users
            WHERE expire_date IS NOT NULL AND expire_date <= ?
        """, (now,))
        expired = (await cur.fetchone())[0]

        cur = await db.execute("""
            SELECT COUNT(*) FROM questions
            WHERE type = 'content' AND status = 'open'
        """)
        open_content = (await cur.fetchone())[0]

        cur = await db.execute("""
            SELECT COUNT(*) FROM questions
            WHERE type = 'support' AND status = 'open'
        """)
        open_support = (await cur.fetchone())[0]

        cur = await db.execute("SELECT COUNT(*) FROM admins")
        admins_count = (await cur.fetchone())[0]

        return total, active, expired, open_content, open_support, admins_count + 1


async def save_question(user_id: int, username: str | None, full_name: str | None, q_type: str, text: str):
    await ensure_user_exists(user_id)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO questions (user_id, username, full_name, type, text, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
        """, (
            user_id,
            username,
            full_name,
            q_type,
            text,
            datetime.now().isoformat()
        ))
        await db.commit()


async def get_open_questions(q_type: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT id, user_id, username, full_name, text, created_at
            FROM questions
            WHERE type = ? AND status = 'open'
            ORDER BY id DESC
            LIMIT 20
        """, (q_type,))
        return await cur.fetchall()


async def close_questions_by_user(user_id: int, q_type: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE questions
            SET status = 'answered'
            WHERE user_id = ? AND type = ? AND status = 'open'
        """, (user_id, q_type))
        await db.commit()


async def get_user_question_counts(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM questions WHERE user_id = ?",
            (user_id,)
        )
        total = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(*) FROM questions WHERE user_id = ? AND type = 'content'",
            (user_id,)
        )
        content = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(*) FROM questions WHERE user_id = ? AND type = 'support'",
            (user_id,)
        )
        support = (await cur.fetchone())[0]

        return total, content, support


async def get_user_stats_text(user_id: int):
    user = await get_sub(user_id)
    if not user:
        return None

    created_at = datetime.fromisoformat(user["created_at"])
    expire = datetime.fromisoformat(user["expire_date"]) if user["expire_date"] else None
    days_in_base = (datetime.now() - created_at).days
    total_q, content_q, support_q = await get_user_question_counts(user_id)

    status = "✅ Активна" if expire and expire > datetime.now() else "❌ Нет активной подписки"
    expire_text = expire.strftime('%d.%m.%Y %H:%M') if expire else "—"

    return (
        f"👤 <code>{user_id}</code>\n"
        f"Статус: {status}\n"
        f"Подписка до: <b>{expire_text}</b>\n"
        f"В базе: <b>{days_in_base}</b> дн.\n"
        f"Всего вопросов: <b>{total_q}</b>\n"
        f"По контенту: <b>{content_q}</b>\n"
        f"В поддержку: <b>{support_q}</b>"
    )


# ================= КЛАВИАТУРЫ =================
def reply_main_kb(admin_user: bool):
    rows = [
        [KeyboardButton(text=BTN_MENU), KeyboardButton(text=BTN_SUB)],
        [KeyboardButton(text=BTN_BUY)],
    ]
    if admin_user:
        rows.append([KeyboardButton(text=BTN_ADMIN)])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        is_persistent=True
    )


def start_inline_kb(admin_user: bool):
    rows = [
        [InlineKeyboardButton(text=BTN_BUY, callback_data="buy")],
        [InlineKeyboardButton(text=BTN_SUB, callback_data="my_sub")],
    ]
    if admin_user:
        rows.append([InlineKeyboardButton(text=BTN_ADMIN, callback_data="open_admin")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def sub_manage_kb(active: bool):
    rows = []
    if active:
        rows.append([InlineKeyboardButton(text="🔗 Войти в канал", callback_data="sub_enter_channel")])

    rows.extend([
        [InlineKeyboardButton(text="❓ Вопрос по контенту", callback_data="sub_question_content")],
        [InlineKeyboardButton(text="🛠 Техподдержка / сотрудничество", callback_data="sub_question_support")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Общая статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Подписчики", callback_data="admin_users")],
        [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin_find")],
        [InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_add_sub")],
        [InlineKeyboardButton(text="⏩ Продлить подписку", callback_data="admin_extend_sub")],
        [InlineKeyboardButton(text="❌ Снять подписку", callback_data="admin_delete_sub")],
        [InlineKeyboardButton(text="🔗 Выдать ссылку вручную", callback_data="admin_invite")],
        [InlineKeyboardButton(text="💰 Изменить цену", callback_data="admin_set_price")],
        [InlineKeyboardButton(text="📆 Изменить срок", callback_data="admin_set_days")],
        [InlineKeyboardButton(text="👮 Выдать админку", callback_data="admin_add_admin")],
        [InlineKeyboardButton(text="🚫 Забрать админку", callback_data="admin_remove_admin")],
        [InlineKeyboardButton(text="📋 Список админов", callback_data="admin_list_admins")],
        [InlineKeyboardButton(text="📝 Изменить стартовый текст", callback_data="admin_set_start_text")],
        [InlineKeyboardButton(text="🖼 Изменить стартовое фото/видео", callback_data="admin_set_start_media")],
        [InlineKeyboardButton(text="📣 Рассылка: только текст", callback_data="admin_broadcast_text")],
        [InlineKeyboardButton(text="🎬 Рассылка: фото/видео", callback_data="admin_broadcast_media")],
        [InlineKeyboardButton(text="❓ Вопросы по контенту", callback_data="admin_content_questions")],
        [InlineKeyboardButton(text="🛠 Обращения в поддержку", callback_data="admin_support_questions")],
        [InlineKeyboardButton(text="💬 Ответить пользователю", callback_data="admin_answer_user")],
    ])


# ================= ОБЩИЕ ФУНКЦИИ =================
async def send_start_menu(message: Message):
    await ensure_user_exists(message.from_user.id)

    admin_user = await is_admin(message.from_user.id)
    start_text = await get_setting("start_text") or DEFAULT_START_TEXT
    media_type = await get_setting("start_media_type")
    media_file_id = await get_setting("start_media_file_id")

    reply_markup = reply_main_kb(admin_user)
    inline_markup = start_inline_kb(admin_user)

    if media_type == "photo" and media_file_id:
        await message.answer_photo(
            photo=media_file_id,
            caption=start_text,
            reply_markup=inline_markup
        )
        await message.answer("Меню:", reply_markup=reply_markup)
        return

    if media_type == "video" and media_file_id:
        await message.answer_video(
            video=media_file_id,
            caption=start_text,
            reply_markup=inline_markup
        )
        await message.answer("Меню:", reply_markup=reply_markup)
        return

    await message.answer(start_text, reply_markup=inline_markup)
    await message.answer("Меню:", reply_markup=reply_markup)


async def send_invite_to_user(user_id: int):
    link = await bot.create_chat_invite_link(
        chat_id=CHANNEL_ID,
        member_limit=1,
        expire_date=datetime.now() + timedelta(minutes=10)
    )
    await bot.send_message(
        user_id,
        f"🔗 Ссылка для входа в канал:\n{link.invite_link}\n\nСсылка действует 10 минут."
    )


async def require_admin(event_user_id: int) -> bool:
    return await is_admin(event_user_id)


async def send_startup_report():
    try:
        price_kop = await get_sub_price()
        sub_days = await get_sub_days()

        text = (
            "✅ <b>Бот успешно запущен на VPS</b>\n\n"
            f"🕒 Время запуска: <b>{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</b>\n"
            f"💰 Текущая цена: <b>{format_rub_from_kop(price_kop)}</b>\n"
            f"📆 Срок подписки: <b>{sub_days} дней</b>\n"
            f"💾 База: <b>SQLite (subs.db)</b>"
        )
        await bot.send_message(ADMIN_ID, text)
    except Exception as e:
        logger.exception("STARTUP REPORT ERROR: %s", e)


# ================= СТАРТ / МЕНЮ =================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    await send_start_menu(message)


@dp.message(F.text == BTN_MENU)
async def menu_btn(message: Message):
    await send_start_menu(message)


@dp.message(F.text == BTN_BUY)
async def buy_btn_message(message: Message):
    await buy_invoice(message.from_user.id)
    await message.answer("Счёт отправлен.")


@dp.message(F.text == BTN_SUB)
async def sub_btn_message(message: Message):
    await show_my_sub(message)


@dp.message(F.text == BTN_ADMIN)
async def admin_btn_message(message: Message):
    if not await require_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return
    await message.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb())


@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery):
    await send_start_menu(callback.message)
    await callback.answer()


# ================= ПОКУПКА =================
async def buy_invoice(user_id: int):
    price = await get_sub_price()
    days = await get_sub_days()
    prices = [LabeledPrice(label=f"Подписка на {days} дней", amount=price)]

    await bot.send_invoice(
        chat_id=user_id,
        title="Доступ в канал",
        description=f"{days} дней доступа",
        payload="sub",
        provider_token=PAYMENTS_TOKEN,
        currency="RUB",
        prices=prices,
        start_parameter="buy"
    )


@dp.callback_query(F.data == "buy")
async def buy_callback(callback: CallbackQuery):
    await buy_invoice(callback.from_user.id)
    await callback.answer("Счёт отправлен")


# ================= МОЯ ПОДПИСКА =================
async def show_my_sub(message: Message):
    await ensure_user_exists(message.from_user.id)

    sub = await get_sub(message.from_user.id)
    active = await has_active_sub(message.from_user.id)

    if not sub:
        await message.answer(
            "❌ У тебя нет подписки.\n\nПосле покупки здесь появится управление подпиской.",
            reply_markup=sub_manage_kb(False)
        )
        return

    created_at = datetime.fromisoformat(sub["created_at"])
    expire = datetime.fromisoformat(sub["expire_date"]) if sub["expire_date"] else None
    days_in_base = (datetime.now() - created_at).days
    total_q, content_q, support_q = await get_user_question_counts(message.from_user.id)

    status = "✅ Активна" if active else "❌ Истекла"
    expire_text = expire.strftime('%d.%m.%Y %H:%M') if expire else "—"

    await message.answer(
        f"📅 <b>Твоя подписка</b>\n\n"
        f"Статус: {status}\n"
        f"Действует до: <b>{expire_text}</b>\n"
        f"В базе: <b>{days_in_base}</b> дн.\n"
        f"Всего вопросов: <b>{total_q}</b>\n"
        f"По контенту: <b>{content_q}</b>\n"
        f"В поддержку: <b>{support_q}</b>\n\n"
        f"Ниже доступны инструменты управления.",
        reply_markup=sub_manage_kb(active)
    )


@dp.callback_query(F.data == "my_sub")
async def my_sub_callback(callback: CallbackQuery):
    await show_my_sub(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "sub_enter_channel")
async def sub_enter_channel(callback: CallbackQuery):
    if not await has_active_sub(callback.from_user.id):
        await callback.message.answer("❌ Кнопка доступна только при активной подписке.")
        await callback.answer()
        return

    await send_invite_to_user(callback.from_user.id)
    await callback.message.answer("✅ Новая ссылка отправлена тебе в чат.")
    await callback.answer()


# ================= ВОПРОСЫ =================
@dp.callback_query(F.data == "sub_question_content")
async def sub_question_content(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.ask_content)
    await callback.message.answer("Напиши свой вопрос по контенту одним сообщением.")
    await callback.answer()


@dp.message(UserStates.ask_content)
async def process_content_question(message: Message, state: FSMContext):
    await save_question(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
        q_type="content",
        text=message.text or ""
    )
    await bot.send_message(
        ADMIN_ID,
        f"❓ <b>Новый вопрос по контенту</b>\n\n"
        f"ID: <code>{message.from_user.id}</code>\n"
        f"Имя: {message.from_user.full_name}\n"
        f"Username: @{message.from_user.username if message.from_user.username else 'нет'}\n\n"
        f"{message.text}"
    )
    await message.answer("✅ Вопрос отправлен администратору.")
    await state.clear()


@dp.callback_query(F.data == "sub_question_support")
async def sub_question_support(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.ask_support)
    await callback.message.answer("Напиши обращение в техподдержку / по сотрудничеству одним сообщением.")
    await callback.answer()


@dp.message(UserStates.ask_support)
async def process_support_question(message: Message, state: FSMContext):
    await save_question(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
        q_type="support",
        text=message.text or ""
    )
    await bot.send_message(
        ADMIN_ID,
        f"🛠 <b>Новое обращение в поддержку</b>\n\n"
        f"ID: <code>{message.from_user.id}</code>\n"
        f"Имя: {message.from_user.full_name}\n"
        f"Username: @{message.from_user.username if message.from_user.username else 'нет'}\n\n"
        f"{message.text}"
    )
    await message.answer("✅ Обращение отправлено администратору.")
    await state.clear()


# ================= ПЛАТЕЖ =================
@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    await q.answer(ok=True)


@dp.message(F.successful_payment)
async def success_payment(message: Message):
    user_id = message.from_user.id
    sub_days = await get_sub_days()
    price_kop = await get_sub_price()

    await add_sub(user_id)
    await send_invite_to_user(user_id)

    try:
        await bot.send_message(
            ADMIN_ID,
            f"💰 Новая оплата\n\n"
            f"User ID: <code>{user_id}</code>\n"
            f"Сумма: <b>{format_rub_from_kop(price_kop)}</b>\n"
            f"Срок: <b>{sub_days} дней</b>"
        )
    except Exception:
        pass


# ================= АДМИНКА =================
@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if not await require_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return
    await message.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb())


@dp.callback_query(F.data == "open_admin")
async def open_admin(callback: CallbackQuery):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await callback.message.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb())
    await callback.answer()


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    total, active, expired, open_content, open_support, admins_count = await get_stats()
    price_kop = await get_sub_price()
    sub_days = await get_sub_days()

    await callback.message.answer(
        "📊 <b>Общая статистика</b>\n\n"
        f"Всего пользователей: <b>{total}</b>\n"
        f"Активных подписок: <b>{active}</b>\n"
        f"Истёкших подписок: <b>{expired}</b>\n"
        f"Открытых вопросов по контенту: <b>{open_content}</b>\n"
        f"Открытых обращений в поддержку: <b>{open_support}</b>\n"
        f"Админов всего: <b>{admins_count}</b>\n\n"
        f"Текущая цена: <b>{format_rub_from_kop(price_kop)}</b>\n"
        f"Текущий срок: <b>{sub_days} дней</b>"
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_set_price")
async def admin_set_price_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    current = await get_sub_price()
    await state.set_state(AdminStates.set_price)
    await callback.message.answer(
        f"Текущая цена: <b>{format_rub_from_kop(current)}</b>\n\n"
        f"Отправь новую цену в рублях.\n"
        f"Пример: <code>15000</code>"
    )
    await callback.answer()


@dp.message(AdminStates.set_price)
async def admin_set_price_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    try:
        rub = float(message.text.replace(",", ".").strip())
        if rub <= 0:
            raise ValueError
        kop = int(round(rub * 100))
    except Exception:
        await message.answer("Введи корректную цену. Пример: <code>15000</code>")
        return

    await set_setting("sub_price_kop", str(kop))
    await message.answer(f"✅ Новая цена сохранена: <b>{format_rub_from_kop(kop)}</b>")
    await state.clear()


@dp.callback_query(F.data == "admin_set_days")
async def admin_set_days_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    current = await get_sub_days()
    await state.set_state(AdminStates.set_sub_days)
    await callback.message.answer(
        f"Текущий срок: <b>{current} дней</b>\n\n"
        f"Отправь новое количество дней.\n"
        f"Пример: <code>365</code>"
    )
    await callback.answer()


@dp.message(AdminStates.set_sub_days)
async def admin_set_days_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    try:
        days = int(message.text.strip())
        if days <= 0:
            raise ValueError
    except Exception:
        await message.answer("Введи корректное число дней. Пример: <code>365</code>")
        return

    await set_setting("sub_days", str(days))
    await message.answer(f"✅ Новый срок подписки сохранён: <b>{days} дней</b>")
    await state.clear()


@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await get_all_users()
    if not rows:
        await callback.message.answer("Пользователей в базе нет.")
        await callback.answer()
        return

    text = "👥 <b>Подписчики</b>\n\n"
    now = datetime.now()

    for row in rows[:50]:
        user_id = row["user_id"]
        expire_dt = datetime.fromisoformat(row["expire_date"]) if row["expire_date"] else None
        created_at = datetime.fromisoformat(row["created_at"])
        days_in_base = (now - created_at).days
        status = "✅ активна" if expire_dt and expire_dt > now else "❌ нет активной"
        expire_text = expire_dt.strftime('%d.%m.%Y') if expire_dt else "—"
        text += f"<code>{user_id}</code> | {status} | до {expire_text} | в базе {days_in_base} дн.\n"

    if len(rows) > 50:
        text += f"\nПоказаны первые 50 из {len(rows)}"

    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "admin_find")
async def admin_find_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.find_user_id)
    await callback.message.answer("Введите user_id пользователя:")
    await callback.answer()


@dp.message(AdminStates.find_user_id)
async def admin_find_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    stats_text = await get_user_stats_text(user_id)
    if not stats_text:
        await message.answer("Пользователь не найден в базе.")
        await state.clear()
        return

    await message.answer(stats_text)
    await state.clear()


@dp.callback_query(F.data == "admin_add_sub")
async def admin_add_sub_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.add_user_id)
    await callback.message.answer("Введите user_id пользователя, которому выдать подписку:")
    await callback.answer()


@dp.message(AdminStates.add_user_id)
async def admin_add_sub_user(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    await state.update_data(user_id=user_id)
    await state.set_state(AdminStates.add_days)
    await message.answer("На сколько дней выдать подписку?")


@dp.message(AdminStates.add_days)
async def admin_add_sub_days_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    try:
        days = int(message.text.strip())
    except Exception:
        await message.answer("Нужно указать число дней.")
        return

    data = await state.get_data()
    user_id = data["user_id"]
    await add_sub_days(user_id, days)
    await message.answer(f"✅ Подписка выдана пользователю <code>{user_id}</code> на {days} дней.")
    await state.clear()


@dp.callback_query(F.data == "admin_extend_sub")
async def admin_extend_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.extend_user_id)
    await callback.message.answer("Введите user_id пользователя для продления:")
    await callback.answer()


@dp.message(AdminStates.extend_user_id)
async def admin_extend_user(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    await state.update_data(user_id=user_id)
    await state.set_state(AdminStates.extend_days)
    await message.answer("На сколько дней продлить подписку?")


@dp.message(AdminStates.extend_days)
async def admin_extend_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    try:
        days = int(message.text.strip())
    except Exception:
        await message.answer("Нужно указать число дней.")
        return

    data = await state.get_data()
    user_id = data["user_id"]
    await add_sub_days(user_id, days)
    await message.answer(f"✅ Подписка пользователю <code>{user_id}</code> продлена на {days} дней.")
    await state.clear()


@dp.callback_query(F.data == "admin_delete_sub")
async def admin_delete_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.delete_user_id)
    await callback.message.answer("Введите user_id пользователя, у которого снять подписку:")
    await callback.answer()


@dp.message(AdminStates.delete_user_id)
async def admin_delete_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    await remove_sub(user_id)
    await message.answer(f"✅ Подписка у пользователя <code>{user_id}</code> снята.")
    await state.clear()


@dp.callback_query(F.data == "admin_invite")
async def admin_invite_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.invite_user_id)
    await callback.message.answer("Введите user_id пользователя, которому выдать ссылку:")
    await callback.answer()


@dp.message(AdminStates.invite_user_id)
async def admin_invite_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    try:
        await send_invite_to_user(user_id)
        await message.answer(f"✅ Ссылка отправлена пользователю <code>{user_id}</code>.")
    except Exception as e:
        await message.answer(f"Ошибка отправки: <code>{e}</code>")

    await state.clear()


# ================= АДМИНЫ =================
@dp.callback_query(F.data == "admin_add_admin")
async def admin_add_admin_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Только главный админ", show_alert=True)
        return
    await state.set_state(AdminStates.add_admin_user_id)
    await callback.message.answer("Введите user_id пользователя, которому выдать админку:")
    await callback.answer()


@dp.message(AdminStates.add_admin_user_id)
async def admin_add_admin_finish(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    await add_admin(user_id, message.from_user.id)
    await message.answer(f"✅ Пользователь <code>{user_id}</code> теперь админ.")
    await state.clear()


@dp.callback_query(F.data == "admin_remove_admin")
async def admin_remove_admin_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Только главный админ", show_alert=True)
        return
    await state.set_state(AdminStates.remove_admin_user_id)
    await callback.message.answer("Введите user_id пользователя, у которого забрать админку:")
    await callback.answer()


@dp.message(AdminStates.remove_admin_user_id)
async def admin_remove_admin_finish(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    ok = await remove_admin(user_id)
    if not ok:
        await message.answer("Главного админа снять нельзя.")
    else:
        await message.answer(f"✅ Админка у <code>{user_id}</code> снята.")
    await state.clear()


@dp.callback_query(F.data == "admin_list_admins")
async def admin_list_admins(callback: CallbackQuery):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await list_admins()
    text = f"👮 <b>Админы</b>\n\nГлавный админ: <code>{ADMIN_ID}</code>\n\n"

    if rows:
        for row in rows:
            dt = datetime.fromisoformat(row["added_at"]).strftime('%d.%m.%Y %H:%M')
            text += (
                f"<code>{row['user_id']}</code> | "
                f"добавлен: {dt} | "
                f"кем: <code>{row['added_by']}</code>\n"
            )
    else:
        text += "Дополнительных админов нет."

    await callback.message.answer(text)
    await callback.answer()


# ================= НАСТРОЙКИ СТАРТА =================
@dp.callback_query(F.data == "admin_set_start_text")
async def admin_set_start_text(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.set_start_text)
    current = await get_setting("start_text") or DEFAULT_START_TEXT
    await callback.message.answer(
        f"Текущий текст:\n\n{current}\n\nОтправь новый текст стартового сообщения."
    )
    await callback.answer()


@dp.message(AdminStates.set_start_text)
async def admin_set_start_text_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    await set_setting("start_text", message.html_text or message.text or DEFAULT_START_TEXT)
    await message.answer("✅ Стартовый текст обновлён.")
    await state.clear()


@dp.callback_query(F.data == "admin_set_start_media")
async def admin_set_start_media(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.set_start_media)
    await callback.message.answer(
        "Отправь фото или видео для стартового сообщения.\n"
        "Чтобы убрать медиа, отправь текст: <code>remove</code>"
    )
    await callback.answer()


@dp.message(AdminStates.set_start_media, F.text)
async def admin_remove_start_media(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    if message.text.strip().lower() == "remove":
        await set_setting("start_media_type", None)
        await set_setting("start_media_file_id", None)
        await message.answer("✅ Медиа для стартового сообщения удалено.")
        await state.clear()
        return

    await message.answer("Отправь фото, видео или текст <code>remove</code>.")


@dp.message(AdminStates.set_start_media, F.photo)
async def admin_set_start_photo(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    file_id = message.photo[-1].file_id
    await set_setting("start_media_type", "photo")
    await set_setting("start_media_file_id", file_id)
    await message.answer("✅ Стартовое фото обновлено.")
    await state.clear()


@dp.message(AdminStates.set_start_media, F.video)
async def admin_set_start_video(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    file_id = message.video.file_id
    await set_setting("start_media_type", "video")
    await set_setting("start_media_file_id", file_id)
    await message.answer("✅ Стартовое видео обновлено.")
    await state.clear()


# ================= РАССЫЛКА =================
@dp.callback_query(F.data == "admin_broadcast_text")
async def admin_broadcast_text_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.broadcast_text)
    await callback.message.answer("Отправь текст рассылки.")
    await callback.answer()


@dp.message(AdminStates.broadcast_text)
async def admin_broadcast_text_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    rows = await get_all_users()
    success = 0
    failed = 0

    for row in rows:
        user_id = row["user_id"]
        try:
            await bot.send_message(user_id, message.html_text or message.text or "")
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await message.answer(
        f"📣 Текстовая рассылка завершена.\n\n"
        f"Успешно: <b>{success}</b>\n"
        f"Ошибок: <b>{failed}</b>"
    )
    await state.clear()


@dp.callback_query(F.data == "admin_broadcast_media")
async def admin_broadcast_media_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.broadcast_media_caption)
    await callback.message.answer("Отправь подпись к рассылке. Можно отправить <code>-</code> без подписи.")
    await callback.answer()


@dp.message(AdminStates.broadcast_media_caption)
async def admin_broadcast_media_caption_finish(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    caption = "" if (message.text or "").strip() == "-" else (message.html_text or message.text or "")
    await state.update_data(caption=caption)
    await state.set_state(AdminStates.broadcast_media_file)
    await message.answer("Теперь отправь фото или видео для рассылки.")


@dp.message(AdminStates.broadcast_media_file, F.photo)
async def admin_broadcast_media_photo(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    data = await state.get_data()
    caption = data.get("caption", "")
    file_id = message.photo[-1].file_id

    rows = await get_all_users()
    success = 0
    failed = 0

    for row in rows:
        user_id = row["user_id"]
        try:
            await bot.send_photo(user_id, photo=file_id, caption=caption)
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await message.answer(
        f"📣 Фото-рассылка завершена.\n\n"
        f"Успешно: <b>{success}</b>\n"
        f"Ошибок: <b>{failed}</b>"
    )
    await state.clear()


@dp.message(AdminStates.broadcast_media_file, F.video)
async def admin_broadcast_media_video(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    data = await state.get_data()
    caption = data.get("caption", "")
    file_id = message.video.file_id

    rows = await get_all_users()
    success = 0
    failed = 0

    for row in rows:
        user_id = row["user_id"]
        try:
            await bot.send_video(user_id, video=file_id, caption=caption)
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await message.answer(
        f"📣 Видео-рассылка завершена.\n\n"
        f"Успешно: <b>{success}</b>\n"
        f"Ошибок: <b>{failed}</b>"
    )
    await state.clear()


# ================= ВОПРОСЫ / ОТВЕТЫ =================
@dp.callback_query(F.data == "admin_content_questions")
async def admin_content_questions(callback: CallbackQuery):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await get_open_questions("content")
    if not rows:
        await callback.message.answer("Открытых вопросов по контенту нет.")
        await callback.answer()
        return

    text = "❓ <b>Вопросы по контенту</b>\n\n"
    for row in rows:
        dt = datetime.fromisoformat(row["created_at"]).strftime('%d.%m.%Y %H:%M')
        text += (
            f"#{row['id']} | <code>{row['user_id']}</code>\n"
            f"Имя: {row['full_name'] or '-'}\n"
            f"Username: @{row['username'] if row['username'] else 'нет'}\n"
            f"Дата: {dt}\n"
            f"Текст: {row['text']}\n\n"
        )

    await callback.message.answer(text[:4000])
    await callback.answer()


@dp.callback_query(F.data == "admin_support_questions")
async def admin_support_questions(callback: CallbackQuery):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await get_open_questions("support")
    if not rows:
        await callback.message.answer("Открытых обращений в поддержку нет.")
        await callback.answer()
        return

    text = "🛠 <b>Обращения в поддержку</b>\n\n"
    for row in rows:
        dt = datetime.fromisoformat(row["created_at"]).strftime('%d.%m.%Y %H:%M')
        text += (
            f"#{row['id']} | <code>{row['user_id']}</code>\n"
            f"Имя: {row['full_name'] or '-'}\n"
            f"Username: @{row['username'] if row['username'] else 'нет'}\n"
            f"Дата: {dt}\n"
            f"Текст: {row['text']}\n\n"
        )

    await callback.message.answer(text[:4000])
    await callback.answer()


@dp.callback_query(F.data == "admin_answer_user")
async def admin_answer_user_start(callback: CallbackQuery, state: FSMContext):
    if not await require_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.answer_user_id)
    await callback.message.answer("Введите user_id пользователя, которому хочешь ответить:")
    await callback.answer()


@dp.message(AdminStates.answer_user_id)
async def admin_answer_get_user_id(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return
    try:
        user_id = int(message.text.strip())
    except Exception:
        await message.answer("Нужен числовой user_id.")
        return

    await state.update_data(answer_user_id=user_id)
    await state.set_state(AdminStates.answer_text)
    await message.answer("Теперь отправь текст ответа пользователю:")


@dp.message(AdminStates.answer_text)
async def admin_answer_send(message: Message, state: FSMContext):
    if not await require_admin(message.from_user.id):
        return

    data = await state.get_data()
    user_id = data["answer_user_id"]
    answer_text = message.html_text or message.text or ""

    try:
        await bot.send_message(
            user_id,
            f"👨‍💼 <b>Администратор</b>\n\n{answer_text}"
        )
        await close_questions_by_user(user_id, "content")
        await close_questions_by_user(user_id, "support")
        await message.answer(f"✅ Ответ отправлен пользователю <code>{user_id}</code>.")
    except Exception as e:
        await message.answer(f"Не удалось отправить ответ.\nОшибка: <code>{e}</code>")

    await state.clear()


# ================= ДОП КОМАНДЫ =================
@dp.message(Command("dbtest"))
async def dbtest(message: Message):
    if not await require_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        users_count = (await cur.fetchone())[0]

        cur = await db.execute("SELECT COUNT(*) FROM questions")
        questions_count = (await cur.fetchone())[0]

        cur = await db.execute("SELECT COUNT(*) FROM admins")
        admins_count = (await cur.fetchone())[0]

    await message.answer(
        f"✅ SQLite работает\n\n"
        f"Файл БД: <b>{DB_PATH}</b>\n"
        f"Пользователей: <b>{users_count}</b>\n"
        f"Вопросов: <b>{questions_count}</b>\n"
        f"Доп. админов: <b>{admins_count}</b>"
    )


# ================= ФОНОВАЯ ПРОВЕРКА =================
async def check_subs():
    logger.info("CHECK_SUBS: started")
    while True:
        try:
            rows = await get_all_users()
            now = datetime.now()

            for row in rows:
                expire_raw = row["expire_date"]
                if not expire_raw:
                    continue

                expire = datetime.fromisoformat(expire_raw)
                if expire < now:
                    try:
                        await bot.ban_chat_member(CHANNEL_ID, row["user_id"])
                        await bot.unban_chat_member(CHANNEL_ID, row["user_id"])
                    except Exception:
                        pass
        except Exception as e:
            logger.exception("CHECK_SUBS ERROR: %s", e)

        await asyncio.sleep(60)


# ================= ЗАПУСК =================
async def main():
    logger.info("MAIN: start")

    if not BOT_TOKEN or BOT_TOKEN == "ВСТАВЬ_СЮДА_BOT_TOKEN":
        raise RuntimeError("BOT_TOKEN не задан")
    if not PAYMENTS_TOKEN or PAYMENTS_TOKEN == "ВСТАВЬ_СЮДА_PAYMENTS_TOKEN":
        raise RuntimeError("PAYMENTS_TOKEN не задан")

    await init_db()

    logger.info("MAIN: starting background task check_subs")
    asyncio.create_task(check_subs())

    logger.info("MAIN: deleting webhook")
    await bot.delete_webhook(drop_pending_updates=True)

    await send_startup_report()

    logger.info("MAIN: start polling")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
