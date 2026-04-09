import asyncio
import aiosqlite
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, LabeledPrice, PreCheckoutQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = "8791147608:AAFgE6MkWMT423RURwYut4YQC6N6N0dR2Us"
PAYMENTS_TOKEN = "381764678:TEST:174936"

CHANNEL_ID = -1003616232121
ADMIN_ID = 583554883

PRICE = 1500000  # 15000.00 RUB в копейках
DAYS = 365

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()


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


# ================= БАЗА =================
async def init_db():
    async with aiosqlite.connect("subs.db") as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            expire_date TEXT
        )
        """)
        await db.commit()


async def get_sub(user_id: int):
    async with aiosqlite.connect("subs.db") as db:
        cur = await db.execute(
            "SELECT expire_date FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cur.fetchone()
        return row


async def add_sub(user_id: int):
    await add_sub_days(user_id, DAYS)


async def add_sub_days(user_id: int, days: int):
    async with aiosqlite.connect("subs.db") as db:
        cur = await db.execute(
            "SELECT expire_date FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cur.fetchone()

        now = datetime.now()

        if row:
            old_expire = datetime.fromisoformat(row[0])
            if old_expire > now:
                expire = old_expire + timedelta(days=days)
            else:
                expire = now + timedelta(days=days)
        else:
            expire = now + timedelta(days=days)

        await db.execute(
            "INSERT OR REPLACE INTO users VALUES (?, ?)",
            (user_id, expire.isoformat())
        )
        await db.commit()


async def remove_sub(user_id: int):
    async with aiosqlite.connect("subs.db") as db:
        await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        await db.commit()


async def get_all_users():
    async with aiosqlite.connect("subs.db") as db:
        cur = await db.execute(
            "SELECT user_id, expire_date FROM users ORDER BY expire_date DESC"
        )
        return await cur.fetchall()


async def get_stats():
    async with aiosqlite.connect("subs.db") as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        total = (await cur.fetchone())[0]

        now = datetime.now().isoformat()

        cur = await db.execute(
            "SELECT COUNT(*) FROM users WHERE expire_date > ?",
            (now,)
        )
        active = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(*) FROM users WHERE expire_date <= ?",
            (now,)
        )
        expired = (await cur.fetchone())[0]

        return total, active, expired


# ================= ХЕЛПЕРЫ =================
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Купить доступ", callback_data="buy")],
        [InlineKeyboardButton(text="📅 Моя подписка", callback_data="my_sub")]
    ])


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Активные подписки", callback_data="admin_users")],
        [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin_find")],
        [InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_add_sub")],
        [InlineKeyboardButton(text="⏩ Продлить подписку", callback_data="admin_extend_sub")],
        [InlineKeyboardButton(text="❌ Удалить подписку", callback_data="admin_delete_sub")],
        [InlineKeyboardButton(text="🔗 Выдать ссылку вручную", callback_data="admin_invite")],
        [InlineKeyboardButton(text="📣 Рассылка", callback_data="admin_broadcast")]
    ])


# ================= СТАРТ =================
@dp.message(CommandStart())
async def start(message: Message):
    await message.answer_photo(
        # ВАЖНО: тут должна быть ПРЯМАЯ ссылка на картинку, не страница ibb.co
        photo="https://i.ibb.co/jpPL9Kk/photo-2026-03-28-9-05-07-PM.jpg",
        caption=(
            "🔥 <b>Доступ к обучению обклейки полиуретановой пленкой</b>\n\n"
            "💰 15000₽ / 365 дней\n"
            "📈 Материалы, полный цикл обучения, все аспекты бизнеса"
        ),
        reply_markup=main_kb()
    )


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

    if not sub:
        await callback.message.answer("❌ У тебя нет подписки")
        await callback.answer()
        return

    expire = datetime.fromisoformat(sub[0])

    await callback.message.answer(
        f"📅 Подписка до:\n<b>{expire.strftime('%d.%m.%Y %H:%M')}</b>"
    )
    await callback.answer()


# ================= ПЛАТЕЖ =================
@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    await q.answer(ok=True)


@dp.message(F.successful_payment)
async def success_payment(message: Message):
    user_id = message.from_user.id

    await add_sub(user_id)

    expire_date = datetime.now() + timedelta(minutes=10)

    link = await bot.create_chat_invite_link(
        chat_id=CHANNEL_ID,
        member_limit=1,
        expire_date=expire_date
    )

    await message.answer(
        f"✅ Оплата прошла!\n\n"
        f"🔗 Ссылка (действует 10 минут):\n{link.invite_link}"
    )

    try:
        await bot.send_message(
            ADMIN_ID,
            f"💰 Новая оплата\n\n"
            f"User ID: <code>{user_id}</code>\n"
            f"Сумма: <b>15000₽</b>\n"
            f"Срок: <b>{DAYS} дней</b>"
        )
    except Exception:
        pass


# ================= АДМИНКА =================
@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return

    await message.answer(
        "⚙️ <b>Админ-панель</b>\n\nВыбери действие:",
        reply_markup=admin_kb()
    )


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    total, active, expired = await get_stats()

    await callback.message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"Всего пользователей в БД: <b>{total}</b>\n"
        f"Активных подписок: <b>{active}</b>\n"
        f"Истёкших подписок: <b>{expired}</b>"
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

    for user_id, expire in rows[:50]:
        expire_dt = datetime.fromisoformat(expire)
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

    expire = datetime.fromisoformat(sub[0])
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

    expire_date = datetime.now() + timedelta(minutes=10)

    link = await bot.create_chat_invite_link(
        chat_id=CHANNEL_ID,
        member_limit=1,
        expire_date=expire_date
    )

    try:
        await bot.send_message(
            user_id,
            f"🔗 Вот ваша ссылка для входа в канал:\n{link.invite_link}\n\n"
            f"Ссылка действует 10 минут."
        )
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

    for user_id, _ in rows:
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


# ================= АВТО-КИК =================
async def check_subs():
    while True:
        async with aiosqlite.connect("subs.db") as db:
            cur = await db.execute("SELECT user_id, expire_date FROM users")
            rows = await cur.fetchall()

            now = datetime.now()

            for user_id, expire in rows:
                expire_dt = datetime.fromisoformat(expire)

                if expire_dt < now:
                    try:
                        await bot.ban_chat_member(CHANNEL_ID, user_id)
                        await bot.unban_chat_member(CHANNEL_ID, user_id)
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
    await init_db()
    await start_web()

    asyncio.create_task(check_subs())

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
