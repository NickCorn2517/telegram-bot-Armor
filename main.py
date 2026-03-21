import asyncio
import aiosqlite
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, LabeledPrice, PreCheckoutQuery
from aiogram.filters import Command
from aiogram.enums import ParseMode

BOT_TOKEN = "8791147608:AAFgE6MkWMT423RURwYut4YQC6N6N0dR2Us"
PAYMENTS_TOKEN = "ВСТАВЬ_СЮДА"

CHANNEL_ID = -1003620487067
ADMIN_ID = 583554883

PRICE = 1999  # в центах (19.99$)
DAYS = 30

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()


# --- БАЗА ---
async def init_db():
    async with aiosqlite.connect("subs.db") as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            expire_date TEXT
        )
        """)
        await db.commit()


async def add_sub(user_id):
    expire = datetime.now() + timedelta(days=DAYS)
    async with aiosqlite.connect("subs.db") as db:
        await db.execute("INSERT OR REPLACE INTO users VALUES (?, ?)", (user_id, expire.isoformat()))
        await db.commit()


# --- СТАРТ ---
@dp.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "🔥 Доступ к приватному каналу\n\n"
        "💰 Цена: 19.99$\n"
        "⏳ Доступ: 30 дней\n\n"
        "Нажми: /buy"
    )


# --- ПОКУПКА ---
@dp.message(Command("buy"))
async def buy(message: Message):
    prices = [LabeledPrice(label="Подписка", amount=PRICE)]

    await bot.send_invoice(
        chat_id=message.chat.id,
        title="Доступ в канал",
        description="30 дней доступа",
        payload="sub",
        provider_token=PAYMENTS_TOKEN,
        currency="USD",
        prices=prices,
        start_parameter="buy"
    )


# --- ПОДТВЕРЖДЕНИЕ ПЛАТЕЖА ---
@dp.pre_checkout_query()
async def pre_checkout(pre_checkout_q: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)


# --- УСПЕШНАЯ ОПЛАТА ---
@dp.message(F.successful_payment)
async def success_payment(message: Message):
    user_id = message.from_user.id

    await add_sub(user_id)

    link = await bot.create_chat_invite_link(
        chat_id=CHANNEL_ID,
        member_limit=1
    )

    await message.answer(
        f"✅ Оплата прошла!\n\n"
        f"Вот твоя ссылка:\n{link.invite_link}"
    )


# --- ЗАПУСК ---
async def main():
    await init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
