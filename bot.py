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

# =========================
# CONFIG
# =========================
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

# =========================
# I18N
# =========================
TEXTS = {
    "ar": {
        "choose_language": "🌐 اختر اللغة / Choose language",
        "lang_saved": "✅ تم حفظ اللغة: العربية",
        "must_join": "⚠️ يجب الاشتراك أولًا في القناة {channel} لاستخدام البوت.",
        "join_channel": "📢 اشترك في القناة",
        "welcome": (
            "📈 أهلًا بك في بوت إشارات التداول\n\n"
            "هذا البوت يراقب السوق تلقائيًا كل 5 دقائق، ويختار أفضل صفقة محتملة فقط.\n"
            "يمكنك اختيار رمز معيّن أو استلام أفضل الفرص من كل الرموز.\n\n"
            "⭐ فريم 5 دقائق هو الخيار الموصى به لأنه أكثر توازنًا من فريم الدقيقة."
        ),
        "help": (
            "ℹ️ شرح البوت\n\n"
            "1) اشترك في القناة أولًا\n"
            "2) اكتب /start\n"
            "3) اختر الرمز أو كل الرموز\n"
            "4) اختر الفريم أو كل الفريمات\n"
            "5) اختر وضع الأصول المفضلة أو فحص 50+ أصل\n"
            "6) البوت يفحص السوق كل 5 دقائق ويرسل أفضل صفقة فقط\n\n"
            "الأوامر:\n"
            "/start - تشغيل البوت\n"
            "/help - شرح البوت\n"
            "/status - عرض إعداداتك\n"
            "/menu - فتح القائمة\n"
            "/language - تغيير اللغة\n\n"
            "⚠️ الإشارات توقعات مبنية على التحليل الفني وليست نصيحة مالية."
        ),
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
        "welcome": (
            "📈 Welcome to the trading signals bot\n\n"
            "This bot scans the market automatically every 5 minutes and sends only the strongest setup.\n"
            "You can choose one symbol or receive the best opportunities across all symbols.\n\n"
            "⭐ 5m is the recommended timeframe because it is more balanced than 1m."
        ),
        "help": (
            "ℹ️ Bot guide\n\n"
            "1) Join the channel first\n"
            "2) Send /start\n"
            "3) Choose one symbol or all symbols\n"
            "4) Choose one timeframe or all timeframes\n"
            "5) Choose core assets mode or 50+ assets scan\n"
            "6) The bot scans every 5 minutes and sends only the best setup\n\n"
            "Commands:\n"
            "/start - start the bot\n"
            "/help - help\n"
            "/status - your settings\n"
            "/menu - open menu\n"
            "/language - change language\n\n"
            "⚠️ Signals are predictions based on technical analysis, not financial advice."
        ),
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


def detect_default_lang(language_code: Optional[str]) -> str:
    if language_code and language_code.lower().startswith("ar"):
        return "ar"
    return "en"


def t(lang: str, key: str, **kwargs) -> str:
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)


# =========================
# DB
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
            language TEXT DEFAULT 'en',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS preferences (
            user_id INTEGER PRIMARY KEY,
            symbol TEXT DEFAULT 'ALL',
            timeframe TEXT DEFAULT '5m',
            mode TEXT DEFAULT 'core',
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


def save_user(user_id: int, username: Optional[str], first_name: Optional[str], language: str) -> None:
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO users (user_id, username, first_name, is_active, language)
        VALUES (?, ?, ?, 1, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            is_active=1
        """,
        (user_id, username, first_name, language),
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


def get_user_lang(user_id: int, fallback: str = "en") -> str:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT language FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row and row["language"] in TEXTS:
        return row["language"]
    return fallback


def update_user_lang(user_id: int, language: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (user_id, language, is_active)
        VALUES (?, ?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            language=excluded.language,
            is_active=1
        """,
        (user_id, language),
    )
    conn.commit()
    conn.close()


def get_user_preferences(user_id: int) -> tuple[str, str]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT symbol, timeframe FROM preferences WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return ("ALL", "5m")
    return (row["symbol"], row["timeframe"])


def get_user_mode(user_id: int) -> str:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT mode FROM preferences WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row or not row["mode"]:
        return "core"
    return row["mode"]


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


def update_user_mode(user_id: int, mode: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, mode)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            mode=excluded.mode,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, mode),
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
            COALESCE(users.language, 'en') AS language,
            COALESCE(preferences.symbol, 'ALL') AS symbol,
            COALESCE(preferences.timeframe, '5m') AS timeframe,
            COALESCE(preferences.mode, 'core') AS mode
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
    cur.execute("INSERT OR IGNORE INTO last_signals (signal_key) VALUES (?)", (signal_key,))
    conn.commit()
    conn.close()


# =========================
# UI
# =========================
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


# =========================
# TELEGRAM
# =========================
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
        await query.message.reply_text(
            t(lang, "saved_symbol", symbol=symbol, tf=tf),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data.startswith("timeframe:"):
        timeframe = query.data.split(":", 1)[1]
        update_user_timeframe(user.id, timeframe)
        sym, _ = get_user_preferences(user.id)
        await query.message.reply_text(
            t(lang, "saved_timeframe", tf=timeframe, symbol=sym),
            reply_markup=main_menu_keyboard(lang),
        )
        return


# =========================
# ANALYSIS
# =========================
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


# =========================
# APP
# =========================
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