import os
import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional, Any

import httpx
from fastapi import FastAPI, Request
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import uvicorn

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@your_channel")
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
DB_PATH = os.getenv("DB_PATH", "bot.db")
PORT = int(os.getenv("PORT", "8080"))

MIN_SIGNAL_STRENGTH = int(os.getenv("MIN_SIGNAL_STRENGTH", "80"))
SCAN_INTERVAL_SECONDS = 300
CANDLE_LIMIT = 120

SYMBOL_OPTIONS = [
    "ALL",
    "XAU/USD", "BTC/USD", "EUR/USD",
    "ETH/USD", "SOL/USD", "BNB/USD", "XRP/USD", "ADA/USD", "DOGE/USD",
    "AVAX/USD", "LINK/USD", "DOT/USD", "MATIC/USD", "LTC/USD", "BCH/USD",
    "ATOM/USD", "NEAR/USD", "UNI/USD", "TRX/USD", "ETC/USD", "XLM/USD",
    "APT/USD", "ARB/USD", "OP/USD", "INJ/USD",
    "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "NZD/USD", "USD/CAD",
    "EUR/JPY", "GBP/JPY", "EUR/GBP", "EUR/CHF", "AUD/JPY", "CHF/JPY",
    "GBP/CHF", "NZD/JPY", "AUD/CAD", "CAD/JPY", "EUR/CAD", "GBP/CAD",
    "XAG/USD", "XPT/USD",
]

TIMEFRAME_OPTIONS = ["ALL", "5m"]
INTERVAL_MAP = {"5m": "5min"}

FOCUS_MODES = {
    "core": ["XAU/USD", "BTC/USD", "EUR/USD"],
    "wide": [s for s in SYMBOL_OPTIONS if s != "ALL"],
}

TEXTS = {
    "ar": {
        "choose_language": "🌐 اختر اللغة / Choose language",
        "lang_saved": "✅ تم حفظ اللغة: العربية",
        "must_join": "⚠️ يجب الاشتراك أولًا في القناة {channel} لاستخدام البوت.",
        "join_channel": "📢 اشترك في القناة",
        "welcome": "📈 أهلًا بك في بوت إشارات التداول\n\nهذا البوت يراقب السوق تلقائيًا كل 5 دقائق، ويختار أفضل صفقة محتملة فقط.\nيمكنك اختيار رمز معيّن أو استلام أفضل الفرص من كل الرموز.\n\n⭐ فريم 5 دقائق هو الخيار الموصى به لأنه أكثر توازنًا من فريم الدقيقة.",
        "help": "ℹ️ شرح البوت\n\n1) اشترك في القناة أولًا\n2) اكتب /start\n3) اختر الرمز أو كل الرموز\n4) اختر الفريم أو كل الفريمات\n5) اختر وضع الأصول المفضلة أو فحص 50+ أصل\n6) البوت يفحص السوق كل 5 دقائق ويرسل أفضل صفقة فقط\n\nالأوامر:\n/start - تشغيل البوت\n/help - شرح البوت\n/status - عرض إعداداتك\n/menu - فتح القائمة\n/language - تغيير اللغة\n\n⚠️ الإشارات توقعات مبنية على التحليل الفني وليست نصيحة مالية.",
        "status_ok": "✅ أنت مسجل في البوت والاشتراك صحيح.\n\n",
        "not_joined": "❌ أنت غير مشترك حاليًا في القناة المطلوبة.",
        "menu_title": "📍 القائمة الرئيسية",
        "btn_best": "🔥 أفضل صفقة الآن",
        "btn_scan": "📊 تحليل السوق الآن",
        "btn_core": "🎯 وضع الأصول المفضلة",
        "btn_wide": "🌐 فحص 50+ أصل",
        "btn_symbol": "⚙️ اختيار الرمز",
        "btn_timeframe": "⏱ اختيار الفريم",
        "btn_settings": "📋 عرض إعداداتي",
        "btn_channel": "📢 فتح القناة",
        "btn_language": "🌐 اللغة",
        "btn_back": "⬅️ رجوع",
        "pick_symbol": "⚙️ اختر الرمز الذي تريد استقبال إشاراته:",
        "pick_timeframe": "⏱ اختر الفريم الذي تريد استقبال إشاراته:",
        "saved_symbol": "✅ تم حفظ الرمز: {symbol}\nالفريم الحالي: {tf}",
        "saved_timeframe": "✅ تم حفظ الفريم: {tf}\nالرمز الحالي: {symbol}",
        "core_saved": "✅ تم تفعيل وضع الأصول المفضلة: الذهب + البيتكوين + اليورو/دولار",
        "wide_saved": "✅ تم تفعيل وضع فحص 50+ أصل. سيتم إرسال أقوى صفقة فقط.",
        "searching_best": "⏳ جارِ البحث عن أفضل صفقة الآن...",
        "searching_market": "📊 جارِ تحليل السوق الآن...",
        "no_signal": "❌ لا توجد صفقة قوية الآن وفق الشروط الحالية.",
        "no_market_signal": "📊 لا توجد إشارات قوية الآن في السوق.",
        "top_opps": "📊 أفضل الفرص الحالية:\n",
        "settings_header": "📋 إعداداتك الحالية\n\n",
        "pref_symbol": "الرمز المفضل: {symbol}",
        "pref_tf": "الفريم المفضل: {tf}",
        "pref_mode": "وضع الفحص: {mode}",
        "pref_note": "سيصلك فقط ما يطابق هذه الإعدادات.\nالبوت يفحص السوق كل 5 دقائق ويختار أقوى صفقة فقط.",
        "all_symbols": "كل الرموز",
        "all_timeframes": "كل الفريمات",
        "mode_core_name": "الأصول المفضلة",
        "mode_wide_name": "فحص 50+ أصل",
        "symbol_all": "ALL 🔔 كل الرموز",
        "tf_all": "ALL 🔔 كل الفريمات",
        "tf_5m": "5m ⭐ الموصى به",
        "signal_title_buy": "🚀 صفقة قوية محتملة",
        "signal_title_sell": "🔻 صفقة قوية محتملة",
        "label_symbol": "📊 الرمز",
        "label_tf": "⏱ الفريم",
        "label_action": "📈 الإشارة",
        "label_entry": "💰 الدخول المقترح",
        "label_tp": "🎯 الهدف",
        "label_sl": "🛑 وقف الخسارة",
        "label_strength": "🔥 قوة الإشارة",
        "label_strategy": "📡 الاستراتيجية",
        "label_reason": "🧠 السبب",
        "signal_disclaimer": "⚠️ هذه إشارة متوقعة وليست نصيحة مالية.",
        "manual_reason": "إشارة تجريبية",
    },
    "en": {
        "choose_language": "🌐 Choose language / اختر اللغة",
        "lang_saved": "✅ Language saved: English",
        "must_join": "⚠️ You must join {channel} first to use the bot.",
        "join_channel": "📢 Join channel",
        "welcome": "📈 Welcome to the trading signals bot\n\nThis bot scans the market automatically every 5 minutes and sends only the strongest setup.\nYou can choose one symbol or receive the best opportunities across all symbols.\n\n⭐ 5m is the recommended timeframe because it is more balanced than 1m.",
        "help": "ℹ️ Bot guide\n\n1) Join the channel first\n2) Send /start\n3) Choose one symbol or all symbols\n4) Choose one timeframe or all timeframes\n5) Choose core assets mode or 50+ assets scan\n6) The bot scans every 5 minutes and sends only the best setup\n\nCommands:\n/start - start the bot\n/help - help\n/status - your settings\n/menu - open menu\n/language - change language\n\n⚠️ Signals are predictions based on technical analysis, not financial advice.",
        "status_ok": "✅ You are registered and your channel subscription is valid.\n\n",
        "not_joined": "❌ You are not subscribed to the required channel right now.",
        "menu_title": "📍 Main menu",
        "btn_best": "🔥 Best setup now",
        "btn_scan": "📊 Scan market now",
        "btn_core": "🎯 Core assets mode",
        "btn_wide": "🌐 Scan 50+ assets",
        "btn_symbol": "⚙️ Choose symbol",
        "btn_timeframe": "⏱ Choose timeframe",
        "btn_settings": "📋 My settings",
        "btn_channel": "📢 Open channel",
        "btn_language": "🌐 Language",
        "btn_back": "⬅️ Back",
        "pick_symbol": "⚙️ Choose the symbol you want to receive signals for:",
        "pick_timeframe": "⏱ Choose the timeframe you want to receive signals for:",
        "saved_symbol": "✅ Symbol saved: {symbol}\nCurrent timeframe: {tf}",
        "saved_timeframe": "✅ Timeframe saved: {tf}\nCurrent symbol: {symbol}",
        "core_saved": "✅ Core assets mode enabled: Gold + Bitcoin + EUR/USD",
        "wide_saved": "✅ 50+ assets scan enabled. Only the strongest setup will be sent.",
        "searching_best": "⏳ Searching for the best setup now...",
        "searching_market": "📊 Scanning market now...",
        "no_signal": "❌ No strong setup right now under current conditions.",
        "no_market_signal": "📊 No strong signals in the market right now.",
        "top_opps": "📊 Top current opportunities:\n",
        "settings_header": "📋 Your current settings\n\n",
        "pref_symbol": "Preferred symbol: {symbol}",
        "pref_tf": "Preferred timeframe: {tf}",
        "pref_mode": "Scan mode: {mode}",
        "pref_note": "You will only receive alerts matching these settings.\nThe bot scans every 5 minutes and selects only the strongest setup.",
        "all_symbols": "All symbols",
        "all_timeframes": "All timeframes",
        "mode_core_name": "Core assets",
        "mode_wide_name": "50+ assets scan",
        "symbol_all": "ALL 🔔 All symbols",
        "tf_all": "ALL 🔔 All timeframes",
        "tf_5m": "5m ⭐ Recommended",
        "signal_title_buy": "🚀 Strong potential setup",
        "signal_title_sell": "🔻 Strong potential setup",
        "label_symbol": "📊 Symbol",
        "label_tf": "⏱ Timeframe",
        "label_action": "📈 Signal",
        "label_entry": "💰 Suggested entry",
        "label_tp": "🎯 Target",
        "label_sl": "🛑 Stop loss",
        "label_strength": "🔥 Signal strength",
        "label_strategy": "📡 Strategy",
        "label_reason": "🧠 Reason",
        "signal_disclaimer": "⚠️ This is a probabilistic setup, not financial advice.",
        "manual_reason": "Manual test signal",
    },
}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

telegram_app: Optional[Application] = None
telegram_bot: Optional[Bot] = None
scanner_task: Optional[asyncio.Task] = None


def detect_default_lang(language_code: Optional[str]) -> str:
    if language_code and language_code.lower().startswith("ar"):
        return "ar"
    return "en"


def t(lang: str, key: str, **kwargs) -> str:
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)


def channel_link() -> str:
    return f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}"


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("العربية 🇸🇦", callback_data="lang:ar")],
            [InlineKeyboardButton("English 🇬🇧", callback_data="lang:en")],
        ]
    )


def subscribe_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(t(lang, "join_channel"), url=channel_link())]]
    )


def main_menu_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t(lang, "btn_best"), callback_data="best_now")],
            [InlineKeyboardButton(t(lang, "btn_scan"), callback_data="scan_now")],
            [InlineKeyboardButton(t(lang, "btn_core"), callback_data="mode_core")],
            [InlineKeyboardButton(t(lang, "btn_wide"), callback_data="mode_wide")],
            [InlineKeyboardButton(t(lang, "btn_symbol"), callback_data="menu_symbol")],
            [InlineKeyboardButton(t(lang, "btn_timeframe"), callback_data="menu_timeframe")],
            [InlineKeyboardButton(t(lang, "btn_settings"), callback_data="menu_settings")],
            [InlineKeyboardButton(t(lang, "btn_language"), callback_data="menu_language")],
            [InlineKeyboardButton(t(lang, "btn_channel"), url=channel_link())],
        ]
    )


def symbol_keyboard(current_symbol: str, lang: str) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, symbol in enumerate(SYMBOL_OPTIONS, start=1):
        base = t(lang, "symbol_all") if symbol == "ALL" else symbol
        label = f"✅ {base}" if symbol == current_symbol else base
        row.append(InlineKeyboardButton(label, callback_data=f"symbol:{symbol}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def timeframe_keyboard(current_timeframe: str, lang: str) -> InlineKeyboardMarkup:
    labels = {"ALL": t(lang, "tf_all"), "5m": t(lang, "tf_5m")}
    rows = []
    for tf in TIMEFRAME_OPTIONS:
        base = labels.get(tf, tf)
        label = f"✅ {base}" if tf == current_timeframe else base
        rows.append([InlineKeyboardButton(label, callback_data=f"timeframe:{tf}")])
    rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def format_user_settings(user_id: int, lang: str) -> str:
    symbol, timeframe = get_user_preferences(user_id)
    mode = get_user_mode(user_id)
    symbol_text = t(lang, "all_symbols") if symbol == "ALL" else symbol
    timeframe_text = t(lang, "all_timeframes") if timeframe == "ALL" else timeframe
    mode_text = t(lang, "mode_core_name") if mode == "core" else t(lang, "mode_wide_name")
    return (
        t(lang, "settings_header")
        + t(lang, "pref_symbol", symbol=symbol_text)
        + "\n"
        + t(lang, "pref_tf", tf=timeframe_text)
        + "\n"
        + t(lang, "pref_mode", mode=mode_text)
        + "\n\n"
        + t(lang, "pref_note")
    )


async def is_user_subscribed(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception as exc:
        logger.exception("Failed to check subscription: %s", exc)
        return False


async def language_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(TEXTS["en"]["choose_language"], reply_markup=language_keyboard())


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    lang = get_user_lang(user.id, detect_default_lang(getattr(user, "language_code", None)))
    save_user(user.id, user.username, user.first_name, lang)

    if not await is_user_subscribed(context, user.id):
        await update.message.reply_text(
            t(lang, "must_join", channel=CHANNEL_USERNAME),
            reply_markup=subscribe_keyboard(lang),
        )
        return

    await update.message.reply_text(t(lang, "welcome"), reply_markup=main_menu_keyboard(lang))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    lang = get_user_lang(user.id, detect_default_lang(getattr(user, "language_code", None)))
    await update.message.reply_text(t(lang, "help"), reply_markup=main_menu_keyboard(lang))


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    lang = get_user_lang(user.id, detect_default_lang(getattr(user, "language_code", None)))
    save_user(user.id, user.username, user.first_name, lang)

    if not await is_user_subscribed(context, user.id):
        await update.message.reply_text(t(lang, "not_joined"), reply_markup=subscribe_keyboard(lang))
        return

    await update.message.reply_text(
        t(lang, "status_ok") + format_user_settings(user.id, lang),
        reply_markup=main_menu_keyboard(lang),
    )


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    lang = get_user_lang(user.id, detect_default_lang(getattr(user, "language_code", None)))
    await update.message.reply_text(t(lang, "menu_title"), reply_markup=main_menu_keyboard(lang))


async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    fallback_lang = detect_default_lang(getattr(user, "language_code", None))
    lang = get_user_lang(user.id, fallback_lang)
    save_user(user.id, user.username, user.first_name, lang)
    current_symbol, current_timeframe = get_user_preferences(user.id)

    if query.data == "menu_language":
        await query.message.reply_text(TEXTS["en"]["choose_language"], reply_markup=language_keyboard())
        return

    if query.data.startswith("lang:"):
        selected = query.data.split(":", 1)[1]
        if selected not in TEXTS:
            selected = "en"
        update_user_lang(user.id, selected)
        await query.message.reply_text(t(selected, "lang_saved"), reply_markup=main_menu_keyboard(selected))
        return

    lang = get_user_lang(user.id, fallback_lang)

    if query.data == "menu_symbol":
        await query.message.reply_text(t(lang, "pick_symbol"), reply_markup=symbol_keyboard(current_symbol, lang))
        return

    if query.data == "menu_timeframe":
        await query.message.reply_text(t(lang, "pick_timeframe"), reply_markup=timeframe_keyboard(current_timeframe, lang))
        return

    if query.data == "menu_settings":
        await query.message.reply_text(format_user_settings(user.id, lang), reply_markup=main_menu_keyboard(lang))
        return

    if query.data == "back_main":
        await query.message.reply_text(t(lang, "menu_title"), reply_markup=main_menu_keyboard(lang))
        return

    if query.data == "mode_core":
        update_user_mode(user.id, "core")
        await query.message.reply_text(t(lang, "core_saved"), reply_markup=main_menu_keyboard(lang))
        return

    if query.data == "mode_wide":
        update_user_mode(user.id, "wide")
        await query.message.reply_text(t(lang, "wide_saved"), reply_markup=main_menu_keyboard(lang))
        return

    if query.data == "best_now":
        await query.message.reply_text(t(lang, "searching_best"))
        mode = get_user_mode(user.id)
        results = await find_all_current_signals(mode=mode)
        signal = results[0] if results else None
        if not signal:
            await query.message.reply_text(t(lang, "no_signal"), reply_markup=main_menu_keyboard(lang))
            return
        await query.message.reply_text(format_signal(signal, lang), reply_markup=main_menu_keyboard(lang))
        return

    if query.data == "scan_now":
        await query.message.reply_text(t(lang, "searching_market"))
        mode = get_user_mode(user.id)
        results = await find_all_current_signals(mode=mode)
        if not results:
            await query.message.reply_text(t(lang, "no_market_signal"), reply_markup=main_menu_keyboard(lang))
            return
        lines = [t(lang, "top_opps")]
        for i, sig in enumerate(results[:5], start=1):
            lines.append(f"{i}) {sig['symbol']} | {sig['action']} | {sig['timeframe']} | {sig['confidence']}")
        await query.message.reply_text("\n".join(lines), reply_markup=main_menu_keyboard(lang))
        return

    if query.data.startswith("symbol:"):
        symbol = query.data.split(":", 1)[1]
        update_user_symbol(user.id, symbol)
        _, tf = get_user_preferences(user.id)
        await query.message.reply_text(t(lang, "saved_symbol", symbol=symbol, tf=tf), reply_markup=main_menu_keyboard(lang))
        return

    if query.data.startswith("timeframe:"):
        timeframe = query.data.split(":", 1)[1]
        update_user_timeframe(user.id, timeframe)
        sym, _ = get_user_preferences(user.id)
        await query.message.reply_text(t(lang, "saved_timeframe", tf=timeframe, symbol=sym), reply_markup=main_menu_keyboard(lang))
        return


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
    return [
        {
            "datetime": item["datetime"],
            "open": float(item["open"]),
            "high": float(item["high"]),
            "low": float(item["low"]),
            "close": float(item["close"]),
        }
        for item in values
    ]


async def fetch_binance_long_short_ratio(symbol: str, period: str = "5m") -> Optional[float]:
    pair_map = {
        "BTC/USD": "BTCUSD", "ETH/USD": "ETHUSD", "SOL/USD": "SOLUSD", "BNB/USD": "BNBUSD",
        "XRP/USD": "XRPUSD", "ADA/USD": "ADAUSD", "DOGE/USD": "DOGEUSD", "AVAX/USD": "AVAXUSD",
        "LINK/USD": "LINKUSD", "DOT/USD": "DOTUSD",
    }
    pair = pair_map.get(symbol)
    if not pair:
        return None

    url = "https://dapi.binance.com/futures/data/globalLongShortAccountRatio"
    params = {"pair": pair, "period": period, "limit": 1}

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    if not data:
        return None

    try:
        return float(data[0]["longShortRatio"])
    except Exception:
        return None


async def fetch_myfxbook_outlook_hint(symbol: str) -> Optional[str]:
    forex_map = {
        "EUR/USD": "EURUSD", "GBP/USD": "GBPUSD", "USD/JPY": "USDJPY",
        "USD/CHF": "USDCHF", "AUD/USD": "AUDUSD", "NZD/USD": "NZDUSD", "USD/CAD": "USDCAD",
    }
    pair = forex_map.get(symbol)
    if not pair:
        return None

    url = "https://www.myfxbook.com/community/outlook"
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        html = resp.text

    if pair not in html:
        return None
    return "community-data-present"


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

    avg_gain = sum(gains[1: period + 1]) / period
    avg_loss = sum(losses[1: period + 1]) / period
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
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_values.append(tr)

    out = [0.0] * len(candles)
    if len(tr_values) <= period:
        return out

    first_atr = sum(tr_values[1: period + 1]) / period
    out[period] = first_atr

    for i in range(period + 1, len(tr_values)):
        out[i] = ((out[i - 1] * (period - 1)) + tr_values[i]) / period
    return out


def round_by_symbol(symbol: str, value: float) -> float:
    if "JPY" in symbol:
        return round(value, 3)
    if symbol.startswith(("XAU/", "XAG/", "XPT/")):
        return round(value, 2)

    crypto_prefixes = {
        "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "AVAX", "LINK",
        "DOT", "MATIC", "LTC", "BCH", "ATOM", "NEAR", "UNI", "TRX",
        "ETC", "XLM", "APT", "ARB", "OP", "INJ"
    }
    if symbol.endswith("/USD") and symbol.split("/")[0] in crypto_prefixes:
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

    if latest_ema9 > latest_ema21:
        buy_score += 30
        reasons.append("EMA9 فوق EMA21")
    elif latest_ema9 < latest_ema21:
        sell_score += 30
        reasons.append("EMA9 تحت EMA21")

    if 55 <= latest_rsi <= 70:
        buy_score += 20
        reasons.append("RSI يدعم الصعود")
    elif 30 <= latest_rsi <= 45:
        sell_score += 20
        reasons.append("RSI يدعم الهبوط")

    if latest_macd > latest_macd_signal and latest_hist > prev_hist:
        buy_score += 25
        reasons.append("MACD إيجابي")
    elif latest_macd < latest_macd_signal and latest_hist < prev_hist:
        sell_score += 25
        reasons.append("MACD سلبي")

    if latest["close"] > prev["high"]:
        buy_score += 20
        reasons.append("اختراق قمة الشمعة السابقة")
    elif latest["close"] < prev["low"]:
        sell_score += 20
        reasons.append("كسر قاع الشمعة السابقة")

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
        "strength_value": strength,
        "strategy": "EMA + RSI + MACD + ATR",
        "reason": " | ".join(reasons[:4]),
        "bar_time": latest["datetime"],
    }


async def enrich_signal_with_sentiment(signal: dict) -> dict:
    symbol = signal["symbol"]
    action = signal["action"]
    strength = int(str(signal["confidence"]).replace("%", ""))

    ratio = await fetch_binance_long_short_ratio(symbol, "5m")
    if ratio is not None:
        if action == "BUY" and ratio > 1.1:
            strength += 10
            signal["reason"] += " | Binance longs supportive"
        elif action == "SELL" and ratio < 0.9:
            strength += 10
            signal["reason"] += " | Binance shorts supportive"
        else:
            strength -= 5
            signal["reason"] += " | Binance sentiment neutral"

    community_hint = await fetch_myfxbook_outlook_hint(symbol)
    if community_hint:
        strength += 3
        signal["reason"] += " | Myfxbook available"

    strength = max(0, min(strength, 95))
    signal["confidence"] = f"{strength}%"
    signal["strength_value"] = strength
    return signal


def format_signal(data: dict, lang: str) -> str:
    symbol = data.get("symbol", "Unknown")
    action = str(data.get("action", "NO SIGNAL")).upper()
    timeframe = data.get("timeframe", "-")
    entry = data.get("entry", "-")
    sl = data.get("sl", "-")
    tp = data.get("tp", "-")
    confidence = data.get("confidence", "-")
    strategy = data.get("strategy", "Strategy")
    reason = data.get("reason", "-")

    title = t(lang, "signal_title_buy") if action == "BUY" else t(lang, "signal_title_sell")
    return (
        f"{title}\n\n"
        f"{t(lang, 'label_symbol')}: {symbol}\n"
        f"{t(lang, 'label_tf')}: {timeframe}\n"
        f"{t(lang, 'label_action')}: {action}\n\n"
        f"{t(lang, 'label_entry')}: {entry}\n"
        f"{t(lang, 'label_tp')}: {tp}\n"
        f"{t(lang, 'label_sl')}: {sl}\n"
        f"{t(lang, 'label_strength')}: {confidence}\n\n"
        f"{t(lang, 'label_strategy')}: {strategy}\n"
        f"{t(lang, 'label_reason')}: {reason}\n\n"
        f"{t(lang, 'signal_disclaimer')}"
    )


def matches_preferences(user_symbol: str, user_timeframe: str, signal_symbol: str, signal_timeframe: str) -> bool:
    symbol_ok = user_symbol == "ALL" or user_symbol.upper() == signal_symbol.upper()
    timeframe_ok = user_timeframe == "ALL" or user_timeframe.lower() == signal_timeframe.lower()
    return symbol_ok and timeframe_ok


async def broadcast_signal(data: dict) -> dict:
    global telegram_bot

    signal_symbol = str(data.get("symbol", "")).upper()
    signal_timeframe = str(data.get("timeframe", "ALL"))
    users = get_active_users_with_preferences()

    sent = 0
    failed = 0
    skipped = 0

    for row in users:
        user_id = int(row["user_id"])
        user_lang = row["language"] if row["language"] in TEXTS else "en"
        user_symbol = str(row["symbol"])
        user_timeframe = str(row["timeframe"])

        if not matches_preferences(user_symbol, user_timeframe, signal_symbol, signal_timeframe):
            skipped += 1
            continue

        try:
            await telegram_bot.send_message(chat_id=user_id, text=format_signal(data, user_lang))
            sent += 1
        except Exception as exc:
            logger.exception("Failed to send to %s: %s", user_id, exc)
            failed += 1
            err = str(exc).lower()
            if "forbidden" in err or "chat not found" in err or "blocked" in err:
                deactivate_user(user_id)

    return {"ok": True, "sent": sent, "failed": failed, "skipped": skipped, "total": len(users)}


async def find_all_current_signals(mode: str = "core") -> list[dict[str, Any]]:
    results = []
    symbols_to_scan = FOCUS_MODES.get(mode, FOCUS_MODES["core"])

    for symbol in symbols_to_scan:
        try:
            candles = await fetch_candles(symbol, "5m")
            signal = build_signal(symbol, "5m", candles)
            if signal:
                signal = await enrich_signal_with_sentiment(signal)
                if signal["strength_value"] >= MIN_SIGNAL_STRENGTH:
                    results.append(signal)
        except Exception as exc:
            logger.exception("Scan failed for %s: %s", symbol, exc)

    results.sort(key=lambda x: x.get("strength_value", 0), reverse=True)
    return results


async def background_scanner() -> None:
    await asyncio.sleep(15)

    while True:
        try:
            results = await find_all_current_signals(mode="wide")
            best_signal = results[0] if results else None

            if best_signal:
                signal_key = (
                    f"{best_signal['symbol']}|"
                    f"{best_signal['timeframe']}|"
                    f"{best_signal['action']}|"
                    f"{best_signal['bar_time']}"
                )

                if not was_signal_sent(signal_key):
                    result = await broadcast_signal(best_signal)
                    remember_signal(signal_key)
                    logger.info("Best signal sent: %s -> %s", signal_key, result)
                else:
                    logger.info("Duplicate best signal skipped: %s", signal_key)
            else:
                logger.info("No strong signal found in this 5m cycle.")

        except Exception as exc:
            logger.exception("Background scanner error: %s", exc)

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


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
    telegram_app.add_handler(CommandHandler("language", language_command))
    telegram_app.add_handler(CallbackQueryHandler(handle_buttons))

    await telegram_app.initialize()
    await telegram_app.start()

    telegram_bot = telegram_app.bot
    telegram_webhook_url = f"https://{RAILWAY_PUBLIC_DOMAIN}/telegram-webhook"
    await telegram_bot.set_webhook(url=telegram_webhook_url)

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


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"ok": True, "message": "بوت التداول على تيليجرام قيد التشغيل"}


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data=data, bot=telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}


@app.get("/manual-test-signal")
async def manual_test_signal():
    test_signal = {
        "symbol": "BTC/USD",
        "timeframe": "5m",
        "action": "BUY",
        "entry": 65000,
        "sl": 64500,
        "tp": 66000,
        "confidence": "80%",
        "strength_value": 80,
        "strategy": "Manual Test",
        "reason": TEXTS["en"]["manual_reason"],
        "bar_time": "manual",
    }
    result = await broadcast_signal(test_signal)
    return result


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, reload=False)