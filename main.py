import asyncio
import os
from datetime import datetime, timedelta

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
PAYMENTS_TOKEN = os.getenv("PAYMENTS_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

CHANNEL_ID = -1003616232121
ADMIN_ID = 583554883

PRICE = 1500000  # 15000.00 RUB в копейках
DAYS = 365

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
db_pool: asyncpg.Pool | None = None


# ================= FSM =================
class AdminStates(StatesGroup):
    add_user_id = State()
    add_days = State()
    extend_user_id = State()
    extend_days = State()
    delete_user_id = State()
    find_user_id = State()
    broadcast_text = State()
    invite_user_id = State()
    answer_user_id = State()
    answer_text = State()


class UserStates(StatesGroup):
    ask_content = State()
    ask_support = State()


# ================= БАЗА =================
async def init_db():
    global db_pool

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задана в переменных окружения")

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                expire_date TIMESTAMP NOT NULL
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS questions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                type TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL DEFAULT 'open'
            );
        """)


async def get_sub(user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT expire_date FROM users WHERE user_id = $1",
            user_id
        )


async def has_active_sub(user_id: int) -> bool:
    row = await get_sub(user_id)
    if not row:
        return False
    return row["expire_date"] > datetime.now()


async def add_sub(user_id: int):
    await add_sub_days(user_id, DAYS)


async def add_sub_days(user_id: int, days: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT expire_date FROM users WHERE user_id = $1",
            user_id
        )

        now = datetime.now()

        if row:
            old_expire = row["expire_date"]
            expire = old_expire + timedelta(days=days) if old_expire > now else now + timedelta(days=days)
            await conn.execute(
                "UPDATE users SET expire_date = $1 WHERE user_id = $2",
                expire, user_id
            )
        else:
            expire = now + timedelta(days=days)
            await conn.execute(
                "INSERT INTO users (user_id, expire_date) VALUES ($1, $2)",
                user_id, expire
            )


async def remove_sub(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM users WHERE user_id = $1",
            user_id
        )


async def get_all_users():
    async with db_pool.acquire() as conn:
        return await conn.fetch(
            "SELECT user_id, expire_date FROM users ORDER BY expire_date DESC"
        )


async def get_stats():
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        active = await conn.fetchval("SELECT COUNT(*) FROM users WHERE expire_date > NOW()")
        expired = await conn.fetchval("SELECT COUNT(*) FROM users WHERE expire_date <= NOW()")
        open_content = await conn.fetchval(
            "SELECT COUNT(*) FROM questions WHERE type = 'content' AND status = 'open'"
        )
        open_support = await conn.fetchval(
            "SELECT COUNT(*) FROM questions WHERE type = 'support' AND status = 'open'"
        )
        return total, active, expired, open_content, open_support


async def save_question(user_id: int, username: str | None, full_name: str | None, q_type: str, text: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO questions (user_id, username, full_name, type, text)
            VALUES ($1, $2, $3, $4, $5)
            """,
            user_id, username, full_name, q_type, text
        )


async def get_open_questions(q_type: str):
    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, user_id, username, full_name, text, created_at
            FROM questions
            WHERE type = $1 AND status = 'open'
            ORDER BY id DESC
            LIMIT 20
            """,
            q_type
        )


async def close_questions_by_user(user_id: int, q_type: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE questions
            SET status = 'answered'
            WHERE user_id = $1 AND type = $2 AND status = 'open'
            """,
            user_id, q_type
        )


# ================= ХЕЛПЕРЫ =================
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def start_kb(user_id: int):
    rows = [
        [InlineKeyboardButton(text="💳 Купить доступ", callback_data="buy")],
        [InlineKeyboardButton(text="📅 Моя подписка", callback_data="my_sub")],
    ]
    if is_admin(user_id):
        rows.append([InlineKeyboardButton(text="⚙️ Админка", callback_data="open_admin")])
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
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Активные подписки", callback_data="admin_users")],
        [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin_find")],
        [InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_add_sub")],
        [InlineKeyboardButton(text="⏩ Продлить подписку", callback_data="admin_extend_sub")],
        [InlineKeyboardButton(text="❌ Удалить подписку", callback_data="admin_delete_sub")],
        [InlineKeyboardButton(text="🔗 Выдать ссылку вручную", callback_data="admin_invite")],
        [InlineKeyboardButton(text="📣 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="❓ Вопросы по контенту", callback_data="admin_content_questions")],
        [InlineKeyboardButton(text="🛠 Обращения в поддержку", callback_data="admin_support_questions")],
        [InlineKeyboardButton(text="💬 Ответить пользователю", callback_data="admin_answer_user")],
    ])


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


# ================= СТАРТ =================
@dp.message(CommandStart())
async def start(message: Message):
    await message.answer(
        "🔥 <b>Доступ к обучению обклейки полиуретановой пленкой</b>\n\n"
        "💰 15000₽ / 365 дней\n"
        "📈 Материалы, полный цикл обучения, все аспекты бизнеса",
        reply_markup=start_kb(message.from_user.id)
    )


@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery):
    await callback.message.answer("Главное меню:", reply_markup=start_kb(callback.from_user.id))
    await callback.answer()


# ================= ПОКУПКА =================
@dp.callback_query(F.data == "buy")
async def buy(callback: CallbackQuery):
    prices = [LabeledPrice(label="Подписка на 365 дней", amount=PRICE)]
    await callback.answer()

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="Доступ в канал",
        description="365 дней доступа",
        payload="sub",
        provider_token=PAYMENTS_TOKEN,
        currency="RUB",
        prices=prices,
        start_parameter="buy"
    )


# ================= МОЯ ПОДПИСКА =================
@dp.callback_query(F.data == "my_sub")
async def my_sub(callback: CallbackQuery):
    sub = await get_sub(callback.from_user.id)
    active = await has_active_sub(callback.from_user.id)

    if not sub:
        await callback.message.answer(
            "❌ У тебя нет подписки.\n\nПосле покупки здесь появится управление подпиской.",
            reply_markup=sub_manage_kb(False)
        )
        await callback.answer()
        return

    expire = sub["expire_date"]
    status = "✅ Активна" if active else "❌ Истекла"

    await callback.message.answer(
        f"📅 <b>Твоя подписка</b>\n\n"
        f"Статус: {status}\n"
        f"Действует до: <b>{expire.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
        f"Ниже доступны инструменты управления.",
        reply_markup=sub_manage_kb(active)
    )
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
        text=message.text,
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
        text=message.text,
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
    await add_sub(user_id)
    await send_invite_to_user(user_id)

    try:
        await bot.send_message(
            ADMIN_ID,
            f"💰 Новая оплата\n\nUser ID: <code>{user_id}</code>\nСумма: <b>15000₽</b>\nСрок: <b>{DAYS} дней</b>"
        )
    except Exception:
        pass


# ================= АДМИНКА =================
@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return
    await message.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb())


@dp.callback_query(F.data == "open_admin")
async def open_admin(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await callback.message.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb())
    await callback.answer()


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    total, active, expired, open_content, open_support = await get_stats()
    await callback.message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"Всего пользователей: <b>{total}</b>\n"
        f"Активных подписок: <b>{active}</b>\n"
        f"Истёкших подписок: <b>{expired}</b>\n"
        f"Открытых вопросов по контенту: <b>{open_content}</b>\n"
        f"Открытых обращений в поддержку: <b>{open_support}</b>"
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await get_all_users()
    if not rows:
        await callback.message.answer("Пользователей в базе нет.")
        await callback.answer()
        return

    text = "👥 <b>Подписки</b>\n\n"
    now = datetime.now()

    for row in rows[:50]:
        user_id = row["user_id"]
        expire_dt = row["expire_date"]
        status = "✅ активна" if expire_dt > now else "❌ истекла"
        text += f"<code>{user_id}</code> — {expire_dt.strftime('%d.%m.%Y %H:%M')} — {status}\n"

    if len(rows) > 50:
        text += f"\nПоказаны первые 50 из {len(rows)}"

    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "admin_find")
async def admin_find_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.find_user_id)
    await callback.message.answer("Введите user_id пользователя:")
    await callback.answer()


@dp.message(AdminStates.find_user_id)
async def admin_find_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой user_id.")
        return

    sub = await get_sub(user_id)
    if not sub:
        await message.answer(f"Пользователь <code>{user_id}</code> не найден в базе.")
        await state.clear()
        return

    expire = sub["expire_date"]
    now = datetime.now()
    status = "✅ активна" if expire > now else "❌ истекла"

    await message.answer(
        f"👤 Пользователь: <code>{user_id}</code>\n"
        f"📅 Подписка до: <b>{expire.strftime('%d.%m.%Y %H:%M')}</b>\n"
        f"Статус: {status}"
    )
    await state.clear()


@dp.callback_query(F.data == "admin_add_sub")
async def admin_add_sub_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.add_user_id)
    await callback.message.answer("Введите user_id пользователя, которому выдать подписку:")
    await callback.answer()


@dp.message(AdminStates.add_user_id)
async def admin_add_sub_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой user_id.")
        return

    await state.update_data(user_id=user_id)
    await state.set_state(AdminStates.add_days)
    await message.answer("На сколько дней выдать подписку?")


@dp.message(AdminStates.add_days)
async def admin_add_sub_days_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        days = int(message.text.strip())
    except ValueError:
        await message.answer("Нужно указать число дней.")
        return

    data = await state.get_data()
    user_id = data["user_id"]

    await add_sub_days(user_id, days)
    await message.answer(f"✅ Подписка выдана пользователю <code>{user_id}</code> на {days} дней.")
    await state.clear()


@dp.callback_query(F.data == "admin_extend_sub")
async def admin_extend_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.extend_user_id)
    await callback.message.answer("Введите user_id пользователя для продления:")
    await callback.answer()


@dp.message(AdminStates.extend_user_id)
async def admin_extend_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой user_id.")
        return

    await state.update_data(user_id=user_id)
    await state.set_state(AdminStates.extend_days)
    await message.answer("На сколько дней продлить подписку?")


@dp.message(AdminStates.extend_days)
async def admin_extend_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        days = int(message.text.strip())
    except ValueError:
        await message.answer("Нужно указать число дней.")
        return

    data = await state.get_data()
    user_id = data["user_id"]

    await add_sub_days(user_id, days)
    await message.answer(f"✅ Подписка пользователю <code>{user_id}</code> продлена на {days} дней.")
    await state.clear()


@dp.callback_query(F.data == "admin_delete_sub")
async def admin_delete_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.delete_user_id)
    await callback.message.answer("Введите user_id пользователя, у которого удалить подписку:")
    await callback.answer()


@dp.message(AdminStates.delete_user_id)
async def admin_delete_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой user_id.")
        return

    await remove_sub(user_id)
    await message.answer(f"✅ Подписка пользователя <code>{user_id}</code> удалена.")
    await state.clear()


@dp.callback_query(F.data == "admin_invite")
async def admin_invite_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.invite_user_id)
    await callback.message.answer("Введите user_id пользователя, которому выдать ссылку:")
    await callback.answer()


@dp.message(AdminStates.invite_user_id)
async def admin_invite_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой user_id.")
        return

    try:
        await send_invite_to_user(user_id)
        await message.answer(f"✅ Ссылка отправлена пользователю <code>{user_id}</code>.")
    except Exception as e:
        await message.answer(f"Не удалось отправить сообщение пользователю.\nОшибка: <code>{e}</code>")

    await state.clear()


@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.broadcast_text)
    await callback.message.answer("Отправь текст рассылки:")
    await callback.answer()


@dp.message(AdminStates.broadcast_text)
async def admin_broadcast_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    rows = await get_all_users()
    success = 0
    failed = 0

    for row in rows:
        user_id = row["user_id"]
        try:
            await bot.send_message(user_id, message.text)
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await message.answer(
        f"📣 Рассылка завершена.\n\n"
        f"Успешно: <b>{success}</b>\n"
        f"Ошибок: <b>{failed}</b>"
    )
    await state.clear()


@dp.callback_query(F.data == "admin_content_questions")
async def admin_content_questions(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await get_open_questions("content")
    if not rows:
        await callback.message.answer("Открытых вопросов по контенту нет.")
        await callback.answer()
        return

    text = "❓ <b>Вопросы по контенту</b>\n\n"
    for row in rows:
        dt = row["created_at"].strftime('%d.%m.%Y %H:%M')
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
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await get_open_questions("support")
    if not rows:
        await callback.message.answer("Открытых обращений в поддержку нет.")
        await callback.answer()
        return

    text = "🛠 <b>Обращения в поддержку</b>\n\n"
    for row in rows:
        dt = row["created_at"].strftime('%d.%m.%Y %H:%M')
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
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(AdminStates.answer_user_id)
    await callback.message.answer("Введите user_id пользователя, которому хочешь ответить:")
    await callback.answer()


@dp.message(AdminStates.answer_user_id)
async def admin_answer_get_user_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой user_id.")
        return

    await state.update_data(answer_user_id=user_id)
    await state.set_state(AdminStates.answer_text)
    await message.answer("Теперь отправь текст ответа пользователю:")


@dp.message(AdminStates.answer_text)
async def admin_answer_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    data = await state.get_data()
    user_id = data["answer_user_id"]
    answer_text = message.text

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


# ================= ФОНОВАЯ ПРОВЕРКА =================
async def check_subs():
    while True:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, expire_date FROM users")

        now = datetime.now()
        for row in rows:
            if row["expire_date"] < now:
                try:
                    await bot.ban_chat_member(CHANNEL_ID, row["user_id"])
                    await bot.unban_chat_member(CHANNEL_ID, row["user_id"])
                except Exception:
                    pass

        await asyncio.sleep(60)


# ================= АНТИ-СОН =================
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


# ================= ЗАПУСК =================
async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    if not PAYMENTS_TOKEN:
        raise RuntimeError("PAYMENTS_TOKEN не задан")

    await init_db()
    await start_web()
    asyncio.create_task(check_subs())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
