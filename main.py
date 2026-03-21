import asyncio
import aiosqlite
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.types import LabeledPrice

BOT_TOKEN = "8791147608:AAFgE6MkWMT423RURwYut4YQC6N6N0dR2Us"
PAYMENTS_TOKEN = "ВСТАВЬ_СЮДА"
CHANNEL_ID = "@твой_канал"
ADMIN_ID = 123456789

PRICE = 1999
CURRENCY = "RUB"
DAYS = 30

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# --- БАЗА ---
async def init_db():
    async with aiosqlite.connect("subs.db") as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            expire_date TEXT,
            total_paid INTEGER,
            payments_count INTEGER,
            last_notified TEXT,
            stage TEXT,
            last_action TEXT
        )
        """)
        await db.commit()

async def create_user_if_not_exists(user_id):
    async with aiosqlite.connect("subs.db") as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        if not await cursor.fetchone():
            await db.execute(
                "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_id, None, 0, 0, None, "new", datetime.now().isoformat())
            )
            await db.commit()

async def update_user(user_id):
    async with aiosqlite.connect("subs.db") as db:
        cursor = await db.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        row = await cursor.fetchone()

        if row:
            expire = datetime.fromisoformat(row[1]) if row[1] else datetime.now()
            new_expire = max(expire, datetime.now()) + timedelta(days=DAYS)
            total_paid = row[2] + PRICE
            payments_count = row[3] + 1
        else:
            new_expire = datetime.now() + timedelta(days=DAYS)
            total_paid = PRICE
            payments_count = 1

        await db.execute(
            "REPLACE INTO users VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, new_expire.isoformat(), total_paid, payments_count, None, "paid", datetime.now().isoformat())
        )
        await db.commit()

async def get_users():
    async with aiosqlite.connect("subs.db") as db:
        async with db.execute("SELECT * FROM users") as cursor:
            return await cursor.fetchall()

async def remove_user(user_id):
    async with aiosqlite.connect("subs.db") as db:
        await db.execute("DELETE FROM users WHERE user_id=?", (user_id,))
        await db.commit()

# --- СТАРТ ---
@dp.message(commands=["start"])
async def start(message: types.Message):
    user_id = message.from_user.id
    await create_user_if_not_exists(user_id)

    await message.answer(
        "🔥 Закрытый канал с ценным контентом\n\nХочешь посмотреть, что внутри? Напиши 'да'"
    )

# --- ВОРОНКА ---
@dp.message(lambda m: "да" in m.text.lower())
async def warm(message: types.Message):
    await message.answer(
        f"📊 Ты получишь:\n"
        f"— инсайды\n— кейсы\n— инструменты\n\n"
        f"Доступ: {PRICE} {CURRENCY}\n\nНапиши 'купить'"
    )

# --- ПОКУПКА ---
@dp.message(lambda m: "купить" in m.text.lower())
async def buy(message: types.Message):
    prices = [LabeledPrice(label="Доступ", amount=PRICE * 100)]

    await bot.send_invoice(
        chat_id=message.chat.id,
        title="Подписка",
        description=f"{DAYS} дней доступа",
        payload="sub",
        provider_token=PAYMENTS_TOKEN,
        currency=CURRENCY,
        prices=prices
    )

@dp.pre_checkout_query()
async def checkout(q: types.PreCheckoutQuery):
    await bot.answer_pre_checkout_query(q.id, ok=True)

# --- ОПЛАТА ---
@dp.message(lambda m: m.successful_payment)
async def success(message: types.Message):
    user_id = message.from_user.id

    await update_user(user_id)

    invite = await bot.create_chat_invite_link(
        chat_id=CHANNEL_ID,
        member_limit=1
    )

    await message.answer(f"✅ Оплата прошла\n{invite.invite_link}")

# --- АДМИН ---
@dp.message(commands=["admin"])
async def admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    users = await get_users()
    total_users = len(users)
    total_income = sum(u[2] for u in users)
    total_payments = sum(u[3] for u in users)

    await message.answer(
        f"📊 Статистика\n👥 {total_users}\n💰 {total_income} {CURRENCY}\n💳 {total_payments}"
    )

# --- МОНИТОР ---
async def monitor():
    while True:
        users = await get_users()
        now = datetime.now()

        for u in users:
            user_id, expire, _, _, last_notified, stage, last_action = u

            if expire:
                expire_date = datetime.fromisoformat(expire)

                if now > expire_date:
                    try:
                        await bot.ban_chat_member(CHANNEL_ID, user_id)
                        await bot.unban_chat_member(CHANNEL_ID, user_id)
                    except:
                        pass
                    await remove_user(user_id)

            # автоворонка
            if stage != "paid":
                last = datetime.fromisoformat(last_action)
                diff = (now - last).total_seconds()

                if 600 < diff < 900:
                    await bot.send_message(user_id, "⏳ Ты ещё думаешь?")

                if 3600 < diff < 4000:
                    await bot.send_message(user_id, "🔥 Уже есть результаты у участников")

                if 86400 < diff < 90000:
                    await bot.send_message(user_id, "⚠️ Последний шанс зайти")

        await asyncio.sleep(1800)

# --- СТАРТ ---
async def main():
    await init_db()
    asyncio.create_task(monitor())
    await dp.start_polling(bot)

asyncio.run(main())
