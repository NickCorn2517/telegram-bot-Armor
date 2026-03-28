import asyncio
import aiosqlite
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, LabeledPrice, PreCheckoutQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from aiohttp import web

BOT_TOKEN = "8791147608:AAFgE6MkWMT423RURwYut4YQC6N6N0dR2Us"
PAYMENTS_TOKEN = "PAYMENT_TOKEN"

CHANNEL_ID = -1003616232121

PRICE = 15000
DAYS = 365

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()


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


async def get_sub(user_id):
    async with aiosqlite.connect("subs.db") as db:
        cur = await db.execute(
            "SELECT expire_date FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cur.fetchone()
        return row


async def add_sub(user_id):
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
                expire = old_expire + timedelta(days=DAYS)
            else:
                expire = now + timedelta(days=DAYS)
        else:
            expire = now + timedelta(days=DAYS)

        await db.execute(
            "INSERT OR REPLACE INTO users VALUES (?, ?)",
            (user_id, expire.isoformat())
        )
        await db.commit()


# ================= КНОПКИ =================
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Купить доступ", callback_data="buy")],
        [InlineKeyboardButton(text="📅 Моя подписка", callback_data="my_sub")]
    ])


# ================= СТАРТ =================
@dp.message(CommandStart())
async def start(message: Message):
    await message.answer_photo(
        photo="<a href="https://ibb.co/6d74ch0"><img src="https://i.ibb.co/jpPL9Kk/photo-2026-03-28-9-05-07-PM.jpg" alt="photo 2026 03 28 9 05 07 PM" border="0"></a>",
        caption=(
            "🔥 <b>Доступ к курсу обклейки</b>\n\n"
            "💰 15000₽ / 365 дней\n"
            "📈 Инсайды,

# ================= КНОПКА КУПИТЬ =================
@dp.callback_query(F.data == "buy")
async def buy(callback):
    prices = [LabeledPrice(label="Подписка", amount=PRICE)]

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="Доступ в канал",
        description="30 дней доступа",
        payload="sub",
        provider_token=PAYMENTS_TOKEN,
        currency="USD",
        prices=prices
    )


# ================= МОЯ ПОДПИСКА =================
@dp.callback_query(F.data == "my_sub")
async def my_sub(callback):
    sub = await get_sub(callback.from_user.id)

    if not sub:
        await callback.message.answer("❌ У тебя нет подписки")
        return

    expire = datetime.fromisoformat(sub[0])

    await callback.message.answer(
        f"📅 Подписка до:\n<b>{expire}</b>"
    )


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
        f"🔗 Ссылка (10 минут):\n{link.invite_link}"
    )


# ================= АВТО-КИК =================
async def check_subs():
    while True:
        async with aiosqlite.connect("subs.db") as db:
            cur = await db.execute("SELECT user_id, expire_date FROM users")
            rows = await cur.fetchall()

            now = datetime.now()

            for user_id, expire in rows:
                expire = datetime.fromisoformat(expire)

                if expire < now:
                    try:
                        await bot.ban_chat_member(CHANNEL_ID, user_id)
                        await bot.unban_chat_member(CHANNEL_ID, user_id)
                    except:
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

    site = web.TCPSite(runner, "0.0.0.0", 10000)
    await site.start()


# ================= ЗАПУСК =================
async def main():
    await init_db()
    await start_web()

    asyncio.create_task(check_subs())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
