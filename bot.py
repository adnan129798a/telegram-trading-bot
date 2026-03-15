import os
import sqlite3
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, Header, HTTPException
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes
import uvicorn

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@your_channel")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change_me_secret")
DB_PATH = os.getenv("DB_PATH", "bot.db")
PORT = int(os.getenv("PORT", "8000"))
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

telegram_app: Optional[Application] = None
telegram_bot: Optional[Bot] = None


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()


def save_user(user_id: int, username: Optional[str], first_name: Optional[str]) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (user_id, username, first_name, is_active)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            is_active=1
        """,
        (user_id, username, first_name),
    )
    conn.commit()
    conn.close()


def deactivate_user(user_id: int) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_active=0 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


def get_active_users() -> list[int]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE is_active=1")
    rows = cur.fetchall()
    conn.close()
    return [int(row["user_id"]) for row in rows]


def channel_link() -> str:
    return f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}"


def subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📢 اشترك في القناة", url=channel_link())]]
    )


async def is_user_subscribed(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception as exc:
        logger.exception("Failed to check subscription: %s", exc)
        return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    if not await is_user_subscribed(context, user.id):
        await update.message.reply_text(
            f"⚠️ يجب الاشتراك أولًا في القناة {CHANNEL_USERNAME} لاستخدام البوت.",
            reply_markup=subscribe_keyboard(),
        )
        return

    save_user(user.id, user.username, user.first_name)

    msg = (
        "أهلًا بك في بوت إشارات التداول 📈\n\n"
        "عندما تصل إشارة من TradingView ستصلك هنا مباشرة.\n\n"
        "الأوامر:\n"
        "/start - تفعيل البوت\n"
        "/help - شرح البوت\n"
        "/status - التحقق من تسجيلك\n\n"
        "⚠️ الإشارات تعليمية وليست نصيحة مالية."
    )
    await update.message.reply_text(msg)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "هذا البوت يستقبل إشارات من TradingView عبر Webhook ثم يرسلها لك على تيليغرام."
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    subscribed = await is_user_subscribed(context, user.id)

    if not subscribed:
        await update.message.reply_text(
            "❌ أنت غير مشترك حاليًا في القناة المطلوبة.",
            reply_markup=subscribe_keyboard(),
        )
        return

    save_user(user.id, user.username, user.first_name)
    await update.message.reply_text("✅ أنت مسجل في البوت والاشتراك صحيح.")


def format_signal(data: dict) -> str:
    symbol = data.get("symbol", "Unknown")
    action = str(data.get("action", "NO SIGNAL")).upper()
    timeframe = data.get("timeframe", "-")
    entry = data.get("entry", "-")
    sl = data.get("sl", "-")
    tp = data.get("tp", "-")
    confidence = data.get("confidence", "-")
    strategy = data.get("strategy", "Strategy")
    reason = data.get("reason", "-")

    icon = "🚀" if action == "BUY" else "🔻" if action == "SELL" else "⚪"

    return (
        f"{icon} إشارة جديدة\n\n"
        f"الاستراتيجية: {strategy}\n"
        f"الرمز: {symbol}\n"
        f"الفريم: {timeframe}\n"
        f"الإشارة: {action}\n"
        f"الدخول: {entry}\n"
        f"وقف الخسارة: {sl}\n"
        f"جني الربح: {tp}\n"
        f"الثقة: {confidence}\n"
        f"السبب: {reason}\n\n"
        "⚠️ للتعليم فقط وليست نصيحة مالية."
    )


async def broadcast_signal(data: dict) -> dict:
    global telegram_bot
    users = get_active_users()
    message = format_signal(data)

    sent = 0
    failed = 0

    for user_id in users:
        try:
            await telegram_bot.send_message(chat_id=user_id, text=message)
            sent += 1
        except Exception as exc:
            logger.exception("Failed to send to %s: %s", user_id, exc)
            failed += 1
            err = str(exc).lower()
            if "forbidden" in err or "chat not found" in err or "blocked" in err:
                deactivate_user(user_id)

    return {"ok": True, "sent": sent, "failed": failed, "total": len(users)}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, telegram_bot

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing")
    if not CHANNEL_USERNAME.startswith("@"):
        raise RuntimeError("CHANNEL_USERNAME must start with @")
    if not RAILWAY_PUBLIC_DOMAIN:
        raise RuntimeError("RAILWAY_PUBLIC_DOMAIN is missing")

    init_db()

    telegram_app = Application.builder().token(BOT_TOKEN).updater(None).build()
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("help", help_command))
    telegram_app.add_handler(CommandHandler("status", status_command))

    await telegram_app.initialize()
    await telegram_app.start()

    telegram_bot = telegram_app.bot

    telegram_webhook_url = f"https://{RAILWAY_PUBLIC_DOMAIN}/telegram-webhook"
    await telegram_bot.set_webhook(url=telegram_webhook_url)

    logger.info("Telegram bot started with webhook: %s", telegram_webhook_url)

    yield

    await telegram_bot.delete_webhook()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Telegram bot stopped")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"ok": True, "message": "Telegram Trading Bot is running"}


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data=data, bot=telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}


@app.post("/tv-webhook")
async def tv_webhook(
    request: Request,
    x_webhook_secret: Optional[str] = Header(default=None),
):
    if x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    required = ["symbol", "action"]
    missing = [key for key in required if key not in payload]
    if missing:
        raise HTTPException(
            status_code=400, detail=f"Missing fields: {', '.join(missing)}"
        )

    result = await broadcast_signal(payload)
    return result


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, reload=False)