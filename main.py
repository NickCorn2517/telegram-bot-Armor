import asyncio
import aiosqlite
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, LabeledPrice, PreCheckoutQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from aiohttp import web
import os

BOT_TOKEN = "8791147608:AAFgE6MkWMT423RURwYut4YQC6N6N0dR2Us"
PAYMENTS_TOKEN = "381764678:TEST:174936"

CHANNEL_ID = -1003616232121

PRICE = 1500000  # 15000.00 RUB в копейках
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


async def get_sub(user_id: int):
    async with aiosqlite.connect("subs.db") as db:
        cur = await db.execute(
            "SELECT expire_date FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cur.fetchone()
        return row


async def add_sub(user_id: int):
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
        # Нужна ПРЯМАЯ ссылка на изображение, а не страница ImgBB
        photo="https://i.ibb.co/jpPL9Kk/photo-2026-03-28-9-05-07-PM.jpg",
        caption=(
            "🔥 <b>Доступ к обучению обклейки полиуретановой пленкой</b>\n\n"
            "💰 15000₽ / 365 дней\n"
            "📈 Материалы, полный цикл обучения, все аспекты бизнеса"
        ),
        reply_markup=main_kb()
    )


# ================= КНОПКА КУПИТЬ =================
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
