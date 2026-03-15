import os
import sqlite3
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from telegram import (
    Update,
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
import uvicorn

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@your_channel")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change_me_secret")
DB_PATH = os.getenv("DB_PATH", "bot.db")
PORT = int(os.getenv("PORT", "8080"))
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")

# الأزواج والفريمات المعروضة للمستخدم
SYMBOL_OPTIONS = [
    "ALL",
    "BTCUSDT",
    "ETHUSDT",
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
]

TIMEFRAME_OPTIONS = [
    "1m",
    "5m",
    "15m"
]

# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

telegram_app: Optional[Application] = None
telegram_bot: Optional[Bot] = None


# =========================
# DATABASE
# =========================
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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS preferences (
            user_id INTEGER PRIMARY KEY,
            symbol TEXT DEFAULT 'ALL',
            timeframe TEXT DEFAULT 'ALL',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
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

    cur.execute(
        """
        INSERT INTO preferences (user_id)
        VALUES (?)
        ON CONFLICT(user_id) DO NOTHING
        """,
        (user_id,),
    )

    conn.commit()
    conn.close()


def deactivate_user(user_id: int) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_active=0 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


def get_user_preferences(user_id: int) -> tuple[str, str]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT symbol, timeframe FROM preferences WHERE user_id=?",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return ("ALL", "ALL")
    return (row["symbol"], row["timeframe"])


def update_user_symbol(user_id: int, symbol: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, symbol)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            symbol=excluded.symbol,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, symbol),
    )
    conn.commit()
    conn.close()


def update_user_timeframe(user_id: int, timeframe: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, timeframe)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            timeframe=excluded.timeframe,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, timeframe),
    )
    conn.commit()
    conn.close()


def get_active_users_with_preferences() -> list[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            users.user_id,
            COALESCE(preferences.symbol, 'ALL') AS symbol,
            COALESCE(preferences.timeframe, 'ALL') AS timeframe
        FROM users
        LEFT JOIN preferences ON users.user_id = preferences.user_id
        WHERE users.is_active=1
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# =========================
# TELEGRAM UI HELPERS
# =========================
def channel_link() -> str:
    return f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}"


def subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📢 اشترك في القناة", url=channel_link())]]
    )


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⚙️ اختيار الرمز", callback_data="menu_symbol")],
            [InlineKeyboardButton("⏱ اختيار الفريم", callback_data="menu_timeframe")],
            [InlineKeyboardButton("📋 عرض إعداداتي", callback_data="menu_settings")],
            [InlineKeyboardButton("📢 فتح القناة", url=channel_link())],
        ]
    )


def symbol_keyboard(current_symbol: str) -> InlineKeyboardMarkup:
    rows = []
    row = []

    for i, symbol in enumerate(SYMBOL_OPTIONS, start=1):
        label = f"✅ {symbol}" if symbol == current_symbol else symbol
        row.append(InlineKeyboardButton(label, callback_data=f"symbol:{symbol}"))
        if i % 2 == 0:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def timeframe_keyboard(current_timeframe: str) -> InlineKeyboardMarkup:
    rows = []

    for tf in TIMEFRAME_OPTIONS:
        if tf == "5m":
            label_base = "5m ⭐ الموصى به"
        elif tf == "1m":
            label_base = "1m ⚡ سريع"
        else:
            label_base = "15m 🛡 هادئ"

        label = f"✅ {label_base}" if tf == current_timeframe else label_base
        rows.append([InlineKeyboardButton(label, callback_data=f"timeframe:{tf}")])

    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")])

    return InlineKeyboardMarkup(rows)


def format_user_settings(user_id: int) -> str:
    symbol, timeframe = get_user_preferences(user_id)
    return (
        "📋 إعداداتك الحالية\n\n"
        f"الرمز المفضل: {symbol}\n"
        f"الفريم المفضل: {timeframe}\n\n"
        "سيصلك فقط ما يطابق هذه الإعدادات.\n"
        "إذا اخترت ALL فستصلك كل الإشارات في هذا القسم."
    )


# =========================
# SUBSCRIPTION CHECK
# =========================
async def is_user_subscribed(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception as exc:
        logger.exception("Failed to check subscription: %s", exc)
        return False


# =========================
# TELEGRAM COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    if not await is_user_subscribed(context, user.id):
        await update.message.reply_text(
            f"⚠️ يجب الاشتراك أولًا في القناة {CHANNEL_USERNAME} لاستخدام البوت.",
            reply_markup=subscribe_keyboard(),
        )
        return

    save_user(user.id, user.username, user.first_name)

    text = (
        "📈 أهلًا بك في بوت إشارات التداول\n\n"
        "هذا البوت يرسل إشارات TradingView مباشرة إلى تيليغرام.\n"
        "يمكنك تخصيص الرمز والفريم حتى تصلك الإشارات التي تريدها فقط.\n\n"
        "استخدم الأزرار التالية لإعداد البوت بالطريقة التي تناسبك."
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "ℹ️ شرح البوت\n\n"
        "1) اشترك في القناة أولًا\n"
        "2) اكتب /start\n"
        "3) اختر الرمز والفريم المفضلين\n"
        "4) عندما تصل إشارة من TradingView ستصلك مباشرة\n\n"
        "الأوامر:\n"
        "/start - تشغيل البوت\n"
        "/help - شرح البوت\n"
        "/status - التحقق من تسجيلك\n"
        "/menu - فتح القائمة\n\n"
        "⚠️ الإشارات تعليمية وليست نصيحة مالية."
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    if not await is_user_subscribed(context, user.id):
        await update.message.reply_text(
            "❌ أنت غير مشترك حاليًا في القناة المطلوبة.",
            reply_markup=subscribe_keyboard(),
        )
        return

    save_user(user.id, user.username, user.first_name)
    text = "✅ أنت مسجل في البوت والاشتراك صحيح.\n\n" + format_user_settings(user.id)
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📍 القائمة الرئيسية", reply_markup=main_menu_keyboard())


# =========================
# CALLBACKS
# =========================
async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    save_user(user.id, user.username, user.first_name)

    current_symbol, current_timeframe = get_user_preferences(user.id)

    if query.data == "menu_symbol":
        await query.message.reply_text(
            "⚙️ اختر الرمز الذي تريد استقبال إشاراته:",
            reply_markup=symbol_keyboard(current_symbol),
        )
        return

    if query.data == "menu_timeframe":
        await query.message.reply_text(
            "⏱ اختر الفريم الذي تريد استقبال إشاراته:",
            reply_markup=timeframe_keyboard(current_timeframe),
        )
        return

    if query.data == "menu_settings":
        await query.message.reply_text(
            format_user_settings(user.id),
            reply_markup=main_menu_keyboard(),
        )
        return

    if query.data == "back_main":
        await query.message.reply_text("📍 القائمة الرئيسية", reply_markup=main_menu_keyboard())
        return

    if query.data.startswith("symbol:"):
        symbol = query.data.split(":", 1)[1]
        update_user_symbol(user.id, symbol)
        _, tf = get_user_preferences(user.id)
        await query.message.reply_text(
            f"✅ تم حفظ الرمز: {symbol}\nالفريم الحالي: {tf}",
            reply_markup=main_menu_keyboard(),
        )
        return

    if query.data.startswith("timeframe:"):
        timeframe = query.data.split(":", 1)[1]
        update_user_timeframe(user.id, timeframe)
        sym, _ = get_user_preferences(user.id)
        await query.message.reply_text(
            f"✅ تم حفظ الفريم: {timeframe}\nالرمز الحالي: {sym}",
            reply_markup=main_menu_keyboard(),
        )
        return


# =========================
# SIGNALS
# =========================
def matches_preferences(user_symbol: str, user_timeframe: str, signal_symbol: str, signal_timeframe: str) -> bool:
    symbol_ok = user_symbol == "ALL" or user_symbol.upper() == signal_symbol.upper()
    timeframe_ok = user_timeframe == "ALL" or user_timeframe.lower() == signal_timeframe.lower()
    return symbol_ok and timeframe_ok


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

    signal_symbol = str(data.get("symbol", "")).upper()
    signal_timeframe = str(data.get("timeframe", "ALL"))
    users = get_active_users_with_preferences()
    message = format_signal(data)

    sent = 0
    failed = 0
    skipped = 0

    for row in users:
        user_id = int(row["user_id"])
        user_symbol = str(row["symbol"])
        user_timeframe = str(row["timeframe"])

        if not matches_preferences(user_symbol, user_timeframe, signal_symbol, signal_timeframe):
            skipped += 1
            continue

        try:
            await telegram_bot.send_message(chat_id=user_id, text=message)
            sent += 1
        except Exception as exc:
            logger.exception("Failed to send to %s: %s", user_id, exc)
            failed += 1
            err = str(exc).lower()
            if "forbidden" in err or "chat not found" in err or "blocked" in err:
                deactivate_user(user_id)

    return {
        "ok": True,
        "sent": sent,
        "failed": failed,
        "skipped": skipped,
        "total": len(users),
    }


# =========================
# FASTAPI LIFESPAN
# =========================
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
    telegram_app.add_handler(CommandHandler("menu", menu_command))
    telegram_app.add_handler(CallbackQueryHandler(handle_buttons))

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


# =========================
# FASTAPI APP
# =========================
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
async def tv_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    secret = payload.get("secret")
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    required = ["symbol", "action"]
    missing = [key for key in required if key not in payload]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing fields: {', '.join(missing)}")

    result = await broadcast_signal(payload)
    return result


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, reload=False)