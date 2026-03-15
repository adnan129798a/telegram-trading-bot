import os
import logging
from dataclasses import dataclass
from typing import Optional, Literal

import pandas as pd
import MetaTrader5 as mt5
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@your_channel_name")

SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"]
TIMEFRAME_MAP = {
    "1m": mt5.TIMEFRAME_M1,
    "5m": mt5.TIMEFRAME_M5,
    "15m": mt5.TIMEFRAME_M15,
}
TIMEFRAMES = list(TIMEFRAME_MAP.keys())
BARS_TO_FETCH = 300

# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# user temporary state
user_state: dict[int, dict[str, str]] = {}


@dataclass
class SignalResult:
    symbol: str
    timeframe: str
    action: Literal["BUY", "SELL", "NO SIGNAL"]
    entry: Optional[float]
    sl: Optional[float]
    tp: Optional[float]
    confidence: int
    reason: str
    trend: str
    rsi: float
    macd: float
    macd_signal: float
    ema_fast: float
    ema_slow: float
    atr: float


# =========================
# TELEGRAM HELPERS
# =========================
def channel_link() -> str:
    return f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}"


def symbols_keyboard() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, sym in enumerate(SYMBOLS, start=1):
        row.append(InlineKeyboardButton(sym, callback_data=f"symbol:{sym}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔄 تحقق من الاشتراك", callback_data="verify_subscription")])
    return InlineKeyboardMarkup(rows)


def timeframe_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(tf, callback_data=f"tf:{tf}")] for tf in TIMEFRAMES]
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_symbols")])
    return InlineKeyboardMarkup(rows)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📈 إشارة الآن", callback_data="menu_signal")],
            [InlineKeyboardButton("ℹ️ مساعدة", callback_data="menu_help")],
            [InlineKeyboardButton("📢 القناة", url=channel_link())],
        ]
    )


async def is_user_subscribed(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception as exc:
        logger.exception("Subscription check failed: %s", exc)
        return False


async def require_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    subscribed = await is_user_subscribed(context, user_id)
    if subscribed:
        return True

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📢 اشترك في القناة", url=channel_link())],
            [InlineKeyboardButton("✅ تحققت من الاشتراك", callback_data="verify_subscription")],
        ]
    )

    msg = (
        "⚠️ لاستخدام البوت يجب الاشتراك أولًا في القناة.\n\n"
        f"القناة المطلوبة: {CHANNEL_USERNAME}"
    )

    if update.callback_query:
        await update.callback_query.message.reply_text(msg, reply_markup=keyboard)
    else:
        await update.message.reply_text(msg, reply_markup=keyboard)
    return False


# =========================
# MT5 HELPERS
# =========================
def mt5_connect() -> None:
    if mt5.initialize():
        return
    raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")


def mt5_disconnect() -> None:
    try:
        mt5.shutdown()
    except Exception:
        logger.exception("Failed to shutdown MT5")


def ensure_symbol(symbol: str) -> None:
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError(f"الرمز {symbol} غير موجود في منصة MT5")
    if not info.visible:
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"تعذر تفعيل الرمز {symbol} في Market Watch")


def get_rates(symbol: str, timeframe_key: str, bars: int = BARS_TO_FETCH) -> pd.DataFrame:
    timeframe = TIMEFRAME_MAP[timeframe_key]
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
    if rates is None:
        raise RuntimeError(f"فشل جلب البيانات: {mt5.last_error()}")

    df = pd.DataFrame(rates)
    if df.empty:
        raise RuntimeError("لم يتم استلام أي شموع من MT5")

    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    return df


# =========================
# INDICATORS
# =========================
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # EMA
    out["ema_fast"] = out["close"].ewm(span=9, adjust=False).mean()
    out["ema_slow"] = out["close"].ewm(span=21, adjust=False).mean()

    # RSI(14)
    delta = out["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    out["rsi"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = out["close"].ewm(span=12, adjust=False).mean()
    ema26 = out["close"].ewm(span=26, adjust=False).mean()
    out["macd"] = ema12 - ema26
    out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]

    # ATR(14)
    prev_close = out["close"].shift(1)
    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - prev_close).abs()
    tr3 = (out["low"] - prev_close).abs()
    out["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["atr"] = out["tr"].ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()

    # candle body / momentum helper
    out["body"] = (out["close"] - out["open"]).abs()
    out["bullish"] = out["close"] > out["open"]
    out["bearish"] = out["close"] < out["open"]

    return out


# =========================
# SIGNAL ENGINE
# =========================
def build_signal(symbol: str, timeframe_key: str) -> SignalResult:
    df = get_rates(symbol, timeframe_key)
    df = add_indicators(df)
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    price = float(latest["close"])
    atr = float(latest["atr"]) if pd.notna(latest["atr"]) else max(price * 0.001, 0.0001)
    ema_fast = float(latest["ema_fast"])
    ema_slow = float(latest["ema_slow"])
    rsi = float(latest["rsi"]) if pd.notna(latest["rsi"]) else 50.0
    macd = float(latest["macd"])
    macd_signal = float(latest["macd_signal"])

    trend = "صاعد" if ema_fast > ema_slow else "هابط"
    reason_parts = []
    buy_score = 0
    sell_score = 0

    # EMA trend
    if ema_fast > ema_slow:
        buy_score += 30
        reason_parts.append("EMA9 فوق EMA21")
    else:
        sell_score += 30
        reason_parts.append("EMA9 تحت EMA21")

    # RSI filter
    if 55 <= rsi <= 72:
        buy_score += 25
        reason_parts.append("RSI يدعم الصعود")
    elif 28 <= rsi <= 45:
        sell_score += 25
        reason_parts.append("RSI يدعم الهبوط")
    elif rsi > 72:
        sell_score += 10
        reason_parts.append("RSI مرتفع جدًا")
    elif rsi < 28:
        buy_score += 10
        reason_parts.append("RSI منخفض جدًا")

    # MACD confirmation
    if macd > macd_signal and latest["macd_hist"] > prev["macd_hist"]:
        buy_score += 25
        reason_parts.append("MACD إيجابي")
    elif macd < macd_signal and latest["macd_hist"] < prev["macd_hist"]:
        sell_score += 25
        reason_parts.append("MACD سلبي")

    # Candle confirmation
    if latest["bullish"] and latest["close"] > prev["high"]:
        buy_score += 20
        reason_parts.append("اختراق شمعة صاعد")
    elif latest["bearish"] and latest["close"] < prev["low"]:
        sell_score += 20
        reason_parts.append("كسر شمعة هابط")

    action: Literal["BUY", "SELL", "NO SIGNAL"] = "NO SIGNAL"
    confidence = 0
    sl = None
    tp = None
    reason = " | ".join(reason_parts) if reason_parts else "لا توجد شروط كافية"

    if buy_score >= 65 and buy_score > sell_score:
        action = "BUY"
        confidence = min(buy_score, 95)
        sl = price - (atr * 1.5)
        tp = price + (atr * 3.0)
    elif sell_score >= 65 and sell_score > buy_score:
        action = "SELL"
        confidence = min(sell_score, 95)
        sl = price + (atr * 1.5)
        tp = price - (atr * 3.0)
    else:
        confidence = max(buy_score, sell_score)
        reason = f"لا توجد إشارة قوية الآن | buy_score={buy_score} sell_score={sell_score}"

    digits = 5
    info = mt5.symbol_info(symbol)
    if info is not None and hasattr(info, "digits"):
        digits = int(info.digits)

    return SignalResult(
        symbol=symbol,
        timeframe=timeframe_key,
        action=action,
        entry=round(price, digits),
        sl=round(sl, digits) if sl is not None else None,
        tp=round(tp, digits) if tp is not None else None,
        confidence=confidence,
        reason=reason,
        trend=trend,
        rsi=round(rsi, 2),
        macd=round(macd, 5),
        macd_signal=round(macd_signal, 5),
        ema_fast=round(ema_fast, digits),
        ema_slow=round(ema_slow, digits),
        atr=round(atr, digits),
    )


def format_signal(result: SignalResult) -> str:
    if result.action == "NO SIGNAL":
        return (
            f"📊 الرمز: {result.symbol}\n"
            f"⏱ الفريم: {result.timeframe}\n"
            f"📈 الاتجاه: {result.trend}\n"
            f"🚫 الإشارة: NO SIGNAL\n"
            f"💪 القوة: {result.confidence}%\n\n"
            f"RSI: {result.rsi}\n"
            f"EMA9: {result.ema_fast}\n"
            f"EMA21: {result.ema_slow}\n"
            f"MACD: {result.macd}\n"
            f"Signal: {result.macd_signal}\n"
            f"ATR: {result.atr}\n\n"
            f"السبب: {result.reason}\n\n"
            "⚠️ هذه إشارة تعليمية وليست نصيحة مالية."
        )

    return (
        f"📊 الرمز: {result.symbol}\n"
        f"⏱ الفريم: {result.timeframe}\n"
        f"📈 الاتجاه: {result.trend}\n"
        f"🚀 الإشارة: {result.action}\n"
        f"💪 القوة: {result.confidence}%\n\n"
        f"الدخول: {result.entry}\n"
        f"وقف الخسارة: {result.sl}\n"
        f"جني الربح: {result.tp}\n\n"
        f"RSI: {result.rsi}\n"
        f"EMA9: {result.ema_fast}\n"
        f"EMA21: {result.ema_slow}\n"
        f"MACD: {result.macd}\n"
        f"Signal: {result.macd_signal}\n"
        f"ATR: {result.atr}\n\n"
        f"السبب: {result.reason}\n\n"
        "⚠️ هذه إشارة تعليمية وليست نصيحة مالية."
    )


# =========================
# BOT COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ok = await require_subscription(update, context)
    if not ok:
        return

    text = (
        "أهلًا بك في بوت الإشارات 📈\n\n"
        "هذا البوت يعطي إشارة فورية بناءً على مؤشرات فنية من بيانات MT5.\n"
        "ابدأ من الزر أدناه."
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ok = await require_subscription(update, context)
    if not ok:
        return

    text = (
        "الأوامر المتاحة:\n"
        "/start - تشغيل البوت\n"
        "/signal - طلب إشارة جديدة\n"
        "/help - شرح البوت\n\n"
        "آلية الإشارة الحالية:\n"
        "- EMA 9 / EMA 21\n"
        "- RSI 14\n"
        "- MACD\n"
        "- ATR لحساب SL/TP\n\n"
        "⚠️ يجب أن يكون MT5 مفتوحًا على نفس الكمبيوتر."
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ok = await require_subscription(update, context)
    if not ok:
        return

    await update.message.reply_text("اختر الرمز:", reply_markup=symbols_keyboard())


# =========================
# CALLBACKS
# =========================
async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    user_state.setdefault(user_id, {})

    if query.data == "verify_subscription":
        if await is_user_subscribed(context, user_id):
            await query.message.reply_text("✅ تم التحقق من الاشتراك بنجاح.", reply_markup=main_menu_keyboard())
        else:
            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📢 اشترك في القناة", url=channel_link())],
                    [InlineKeyboardButton("✅ أعد التحقق", callback_data="verify_subscription")],
                ]
            )
            await query.message.reply_text("❌ لم يتم العثور على اشتراكك بعد.", reply_markup=keyboard)
        return

    if not await require_subscription(update, context):
        return

    if query.data == "menu_help":
        await query.message.reply_text(
            "اضغط /signal ثم اختر الزوج والفريم للحصول على إشارة فورية.\n"
            "البوت الحالي تعليمي ويعتمد على MT5 ومؤشرات فنية أساسية."
        )
        return

    if query.data == "menu_signal":
        await query.message.reply_text("اختر الرمز:", reply_markup=symbols_keyboard())
        return

    if query.data == "back_symbols":
        await query.message.reply_text("اختر الرمز:", reply_markup=symbols_keyboard())
        return

    if query.data.startswith("symbol:"):
        symbol = query.data.split(":", 1)[1]
        user_state[user_id]["symbol"] = symbol
        await query.message.reply_text(
            f"تم اختيار الرمز: {symbol}\nالآن اختر الفريم:",
            reply_markup=timeframe_keyboard(),
        )
        return

    if query.data.startswith("tf:"):
        timeframe = query.data.split(":", 1)[1]
        symbol = user_state[user_id].get("symbol")

        if not symbol:
            await query.message.reply_text("اختر الرمز أولًا.", reply_markup=symbols_keyboard())
            return

        await query.message.reply_text(f"⏳ جارِ تحليل {symbol} على فريم {timeframe} ...")

        try:
            mt5_connect()
            ensure_symbol(symbol)
            result = build_signal(symbol, timeframe)
        except Exception as exc:
            logger.exception("Signal generation failed")
            await query.message.reply_text(f"حدث خطأ: {exc}")
            return
        finally:
            mt5_disconnect()

        await query.message.reply_text(format_signal(result))
        await query.message.reply_text("هل تريد إشارة أخرى؟", reply_markup=main_menu_keyboard())
        return


# =========================
# MAIN
# =========================
def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN == "PUT_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("ضع BOT_TOKEN أولًا")
    if not CHANNEL_USERNAME.startswith("@"):
        raise RuntimeError("ضع CHANNEL_USERNAME بهذا الشكل: @your_channel")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("signal", signal_command))
    app.add_handler(CallbackQueryHandler(handle_buttons))

    logger.info("Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()