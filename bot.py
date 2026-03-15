import os
import math
import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional, Any

import httpx
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
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
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
DB_PATH = os.getenv("DB_PATH", "bot.db")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
PORT = int(os.getenv("PORT", "8080"))

# نسبة الحد الأدنى لقوة الإشارة حتى تُرسل
MIN_SIGNAL_STRENGTH = int(os.getenv("MIN_SIGNAL_STRENGTH", "70"))

# أصول البداية
SYMBOL_OPTIONS = [
    "BTC/USD",
    "ETH/USD",
    "SOL/USD",
    "XAU/USD",
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
]

TIMEFRAME_OPTIONS = ["ALL", "1m", "5m", "15m"]

# Twelve Data intervals
INTERVAL_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
}

# كم شمعة نجلب
CANDLE_LIMIT = 120

# تشغيل فحص الخلفية
ENABLE_AUTOSCAN = True


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
scanner_task: Optional[asyncio.Task] = None


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
            symbol TEXT DEFAULT 'BTC/USD',
            timeframe TEXT DEFAULT '5m',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS last_signals (
            signal_key TEXT PRIMARY KEY,
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
        return ("BTC/USD", "5m")
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
            COALESCE(preferences.symbol, 'BTC/USD') AS symbol,
            COALESCE(preferences.timeframe, '5m') AS timeframe
        FROM users
        LEFT JOIN preferences ON users.user_id = preferences.user_id
        WHERE users.is_active=1
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def was_signal_sent(signal_key: str) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM last_signals WHERE signal_key=?", (signal_key,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def remember_signal(signal_key: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO last_signals (signal_key)
        VALUES (?)
        """,
        (signal_key,),
    )
    conn.commit()
    conn.close()


# =========================
# TELEGRAM UI
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
    labels = {
        "ALL": "ALL 🔔 كل الفريمات",
        "1m": "1m ⚡ سريع",
        "5m": "5m ⭐ الموصى به",
        "15m": "15m 🛡 هادئ",
    }

    rows = []
    for tf in TIMEFRAME_OPTIONS:
        label_base = labels.get(tf, tf)
        label = f"✅ {label_base}" if tf == current_timeframe else label_base
        rows.append([InlineKeyboardButton(label, callback_data=f"timeframe:{tf}")])

    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def format_user_settings(user_id: int) -> str:
    symbol, timeframe = get_user_preferences(user_id)
    timeframe_text = "كل الفريمات (1m + 5m + 15m)" if timeframe == "ALL" else timeframe
    return (
        "📋 إعداداتك الحالية\n\n"
        f"الرمز المفضل: {symbol}\n"
        f"الفريم المفضل: {timeframe_text}\n\n"
        "سيصلك فقط ما يطابق هذه الإعدادات.\n"
        "إذا اخترت ALL فستصلك إشارات الدقيقة والخمس دقائق والخمس عشرة دقيقة معًا."
    )


# =========================
# TELEGRAM HELPERS
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
        "هذا البوت يحلل السوق تلقائيًا ويرسل فرصًا متوقعة.\n"
        "يمكنك اختيار الرمز والفريم أو استلام كل الفريمات.\n\n"
        "ملاحظة: فريم 5 دقائق هو الأفضل لمعظم المستخدمين لأنه أكثر توازنًا من فريم الدقيقة."
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "ℹ️ شرح البوت\n\n"
        "1) اشترك في القناة أولًا\n"
        "2) اكتب /start\n"
        "3) اختر الرمز والفريم المفضلين أو كل الفريمات\n"
        "4) عندما تظهر فرصة قوية ستصلك مباشرة\n\n"
        "الأوامر:\n"
        "/start - تشغيل البوت\n"
        "/help - شرح البوت\n"
        "/status - عرض إعداداتك\n"
        "/menu - فتح القائمة\n\n"
        "⚠️ الإشارات توقعات مبنية على التحليل الفني وليست نصيحة مالية."
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
# MARKET DATA + INDICATORS
# =========================
async def fetch_candles(symbol: str, timeframe: str) -> list[dict[str, Any]]:
    interval = INTERVAL_MAP[timeframe]
    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": CANDLE_LIMIT,
        "apikey": TWELVEDATA_API_KEY,
        "format": "JSON",
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    if "values" not in data:
        message = data.get("message", "No values in response")
        raise RuntimeError(f"Twelve Data error for {symbol} {timeframe}: {message}")

    values = list(reversed(data["values"]))

    candles = []
    for item in values:
        candles.append(
            {
                "datetime": item["datetime"],
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
            }
        )
    return candles


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []

    multiplier = 2 / (period + 1)
    out = [values[0]]
    for price in values[1:]:
        out.append((price - out[-1]) * multiplier + out[-1])
    return out


def rsi(values: list[float], period: int = 14) -> list[float]:
    if len(values) < period + 1:
        return [50.0] * len(values)

    gains = [0.0]
    losses = [0.0]

    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(abs(min(diff, 0.0)))

    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    out = [50.0] * len(values)

    if avg_loss == 0:
        out[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[period] = 100 - (100 / (1 + rs))

    for i in range(period + 1, len(values)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100 - (100 / (1 + rs))

    return out


def macd(values: list[float]) -> tuple[list[float], list[float], list[float]]:
    ema12 = ema(values, 12)
    ema26 = ema(values, 26)
    macd_line = [a - b for a, b in zip(ema12, ema26)]
    signal_line = ema(macd_line, 9)
    hist = [a - b for a, b in zip(macd_line, signal_line)]
    return macd_line, signal_line, hist


def atr(candles: list[dict[str, Any]], period: int = 14) -> list[float]:
    if len(candles) < 2:
        return [0.0] * len(candles)

    tr_values = [0.0]
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        tr_values.append(tr)

    out = [0.0] * len(candles)
    if len(tr_values) <= period:
        return out

    first_atr = sum(tr_values[1 : period + 1]) / period
    out[period] = first_atr

    for i in range(period + 1, len(tr_values)):
        out[i] = ((out[i - 1] * (period - 1)) + tr_values[i]) / period

    return out


def round_by_symbol(symbol: str, value: float) -> float:
    if "JPY" in symbol:
        return round(value, 3)
    if symbol.startswith("XAU/"):
        return round(value, 2)
    if symbol in {"BTC/USD", "ETH/USD", "SOL/USD"}:
        return round(value, 2)
    return round(value, 5)


def build_signal(symbol: str, timeframe: str, candles: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if len(candles) < 50:
        return None

    closes = [c["close"] for c in candles]
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    rsi14 = rsi(closes, 14)
    macd_line, signal_line, macd_hist = macd(closes)
    atr14 = atr(candles, 14)

    i = len(candles) - 1
    latest = candles[i]
    prev = candles[i - 1]

    price = latest["close"]
    latest_ema9 = ema9[i]
    latest_ema21 = ema21[i]
    latest_rsi = rsi14[i]
    latest_macd = macd_line[i]
    latest_macd_signal = signal_line[i]
    latest_hist = macd_hist[i]
    prev_hist = macd_hist[i - 1]
    latest_atr = atr14[i] if atr14[i] > 0 else max(price * 0.003, 0.1)

    buy_score = 0
    sell_score = 0
    reasons = []

    # Trend
    if latest_ema9 > latest_ema21:
        buy_score += 30
        reasons.append("EMA9 فوق EMA21")
    elif latest_ema9 < latest_ema21:
        sell_score += 30
        reasons.append("EMA9 تحت EMA21")

    # RSI
    if 55 <= latest_rsi <= 70:
        buy_score += 20
        reasons.append("RSI يدعم الصعود")
    elif 30 <= latest_rsi <= 45:
        sell_score += 20
        reasons.append("RSI يدعم الهبوط")

    # MACD
    if latest_macd > latest_macd_signal and latest_hist > prev_hist:
        buy_score += 25
        reasons.append("MACD إيجابي")
    elif latest_macd < latest_macd_signal and latest_hist < prev_hist:
        sell_score += 25
        reasons.append("MACD سلبي")

    # Candle confirmation
    if latest["close"] > prev["high"]:
        buy_score += 20
        reasons.append("اختراق قمة الشمعة السابقة")
    elif latest["close"] < prev["low"]:
        sell_score += 20
        reasons.append("كسر قاع الشمعة السابقة")

    # Volatility sanity
    if latest_atr > 0:
        buy_score += 5
        sell_score += 5

    action = None
    strength = 0

    if buy_score >= MIN_SIGNAL_STRENGTH and buy_score > sell_score:
        action = "BUY"
        strength = min(buy_score, 95)
        entry = price
        sl = price - (latest_atr * 1.5)
        tp = price + (latest_atr * 3.0)
    elif sell_score >= MIN_SIGNAL_STRENGTH and sell_score > buy_score:
        action = "SELL"
        strength = min(sell_score, 95)
        entry = price
        sl = price + (latest_atr * 1.5)
        tp = price - (latest_atr * 3.0)
    else:
        return None

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "action": action,
        "entry": round_by_symbol(symbol, entry),
        "sl": round_by_symbol(symbol, sl),
        "tp": round_by_symbol(symbol, tp),
        "confidence": f"{strength}%",
        "strategy": "EMA + RSI + MACD + ATR",
        "reason": " | ".join(reasons[:4]),
        "bar_time": latest["datetime"],
    }


# =========================
# SIGNAL DELIVERY
# =========================
def matches_preferences(
    user_symbol: str,
    user_timeframe: str,
    signal_symbol: str,
    signal_timeframe: str,
) -> bool:
    symbol_ok = user_symbol.upper() == signal_symbol.upper()
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

    icon = "🚀" if action == "BUY" else "🔻"

    return (
        f"{icon} صفقة متوقعة\n\n"
        f"الاستراتيجية: {strategy}\n"
        f"الرمز: {symbol}\n"
        f"الفريم: {timeframe}\n"
        f"الإشارة: {action}\n"
        f"الدخول المقترح: {entry}\n"
        f"وقف الخسارة: {sl}\n"
        f"الهدف: {tp}\n"
        f"قوة الإشارة: {confidence}\n"
        f"السبب: {reason}\n\n"
        "⚠️ هذه إشارة متوقعة وليست نصيحة مالية."
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
# AUTO SCANNER
# =========================
def should_run_for_second(timeframe: str, second_counter: int) -> bool:
    if timeframe == "1m":
        return second_counter % 60 == 0
    if timeframe == "5m":
        return second_counter % 300 == 0
    if timeframe == "15m":
        return second_counter % 900 == 0
    return False


async def run_scan_once(timeframe: str) -> None:
    for symbol in SYMBOL_OPTIONS:
        try:
            candles = await fetch_candles(symbol, timeframe)
            signal = build_signal(symbol, timeframe, candles)

            if not signal:
                continue

            signal_key = f"{signal['symbol']}|{signal['timeframe']}|{signal['action']}|{signal['bar_time']}"
            if was_signal_sent(signal_key):
                continue

            result = await broadcast_signal(signal)
            remember_signal(signal_key)
            logger.info("Signal sent: %s -> %s", signal_key, result)

        except Exception as exc:
            logger.exception("Scan failed for %s %s: %s", symbol, timeframe, exc)


async def background_scanner() -> None:
    second_counter = 0
    await asyncio.sleep(10)

    while True:
        try:
            second_counter += 5

            for tf in ["1m", "5m", "15m"]:
                if should_run_for_second(tf, second_counter):
                    await run_scan_once(tf)

            if second_counter >= 86400:
                second_counter = 0

        except Exception as exc:
            logger.exception("Background scanner error: %s", exc)

        await asyncio.sleep(5)


# =========================
# FASTAPI LIFESPAN
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, telegram_bot, scanner_task

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing")
    if not CHANNEL_USERNAME.startswith("@"):
        raise RuntimeError("CHANNEL_USERNAME must start with @")
    if not RAILWAY_PUBLIC_DOMAIN:
        raise RuntimeError("RAILWAY_PUBLIC_DOMAIN is missing")
    if not TWELVEDATA_API_KEY:
        raise RuntimeError("TWELVEDATA_API_KEY is missing")

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

    if ENABLE_AUTOSCAN:
        scanner_task = asyncio.create_task(background_scanner())

    logger.info("Telegram bot started with webhook: %s", telegram_webhook_url)

    yield

    if scanner_task:
        scanner_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            pass

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


@app.post("/manual-test-signal")
async def manual_test_signal():
    test_signal = {
        "symbol": "BTC/USD",
        "timeframe": "5m",
        "action": "BUY",
        "entry": 65000,
        "sl": 64500,
        "tp": 66000,
        "confidence": "80%",
        "strategy": "Manual Test",
        "reason": "Test signal endpoint",
        "bar_time": "manual",
    }
    result = await broadcast_signal(test_signal)
    return result


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, reload=False)