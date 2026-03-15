import os
import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional, Any

import httpx
from fastapi import FastAPI, Request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
ALERT_TYPE_OPTIONS = ["both", "live", "pending"]
INTERVAL_MAP = {"5m": "5min"}

FOCUS_MODES = {
    "core": ["XAU/USD", "BTC/USD", "EUR/USD"],
    "wide": [s for s in SYMBOL_OPTIONS if s != "ALL"],
}


# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


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
            "هذا البوت يراقب السوق كل 5 دقائق ويعرض أفضل الفرص.\n"
            "يمكنك استقبال صفقات فورية أو مترقبة أو الاثنين معًا.\n\n"
            "⭐ فريم 5 دقائق هو الخيار الموصى به."
        ),
        "help": (
            "ℹ️ شرح البوت\n\n"
            "1) اشترك في القناة أولًا\n"
            "2) اكتب /start\n"
            "3) اختر الرمز أو كل الرموز\n"
            "4) اختر الفريم\n"
            "5) اختر نوع التنبيهات\n"
            "6) البوت يفحص السوق كل 5 دقائق\n\n"
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
        "btn_best_live": "🔥 أفضل صفقة فورية الآن",
        "btn_best_pending": "📌 أفضل صفقة مترقبة الآن",
        "btn_scan": "📊 تحليل السوق الآن",
        "btn_core": "🎯 وضع الأصول المفضلة",
        "btn_wide": "🌐 فحص 50+ أصل",
        "btn_symbol": "⚙️ اختيار الرمز",
        "btn_timeframe": "⏱ اختيار الفريم",
        "btn_alert_type": "🔔 نوع التنبيهات",
        "btn_settings": "📋 عرض إعداداتي",
        "btn_channel": "📢 فتح القناة",
        "btn_language": "🌐 اللغة",
        "btn_back": "⬅️ رجوع",
        "pick_symbol": "⚙️ اختر الرمز الذي تريد استقبال إشاراته:",
        "pick_timeframe": "⏱ اختر الفريم الذي تريد استقبال إشاراته:",
        "pick_alert_type": "🔔 اختر نوع التنبيهات التي تريدها:",
        "saved_symbol": "✅ تم حفظ الرمز: {symbol}\nالفريم الحالي: {tf}",
        "saved_timeframe": "✅ تم حفظ الفريم: {tf}\nالرمز الحالي: {symbol}",
        "saved_alert_type": "✅ تم حفظ نوع التنبيهات: {alert_type}",
        "core_saved": "✅ تم تفعيل وضع الأصول المفضلة: الذهب + البيتكوين + اليورو/دولار",
        "wide_saved": "✅ تم تفعيل وضع فحص 50+ أصل.",
        "searching_best_live": "⏳ جارِ البحث عن أفضل صفقة فورية...",
        "searching_best_pending": "⏳ جارِ البحث عن أفضل صفقة مترقبة...",
        "searching_market": "📊 جارِ تحليل السوق الآن...",
        "no_live_signal": "❌ لا توجد صفقة فورية قوية الآن.",
        "no_pending_signal": "❌ لا توجد صفقة مترقبة قوية الآن.",
        "no_market_signal": "📊 لا توجد إشارات قوية الآن في السوق.",
        "top_opps": "📊 أفضل الفرص الحالية:\n",
        "settings_header": "📋 إعداداتك الحالية\n\n",
        "pref_symbol": "الرمز المفضل: {symbol}",
        "pref_tf": "الفريم المفضل: {tf}",
        "pref_mode": "وضع الفحص: {mode}",
        "pref_alert_type": "نوع التنبيهات: {alert_type}",
        "pref_note": "سيصلك فقط ما يطابق هذه الإعدادات.\nالبوت يفحص السوق كل 5 دقائق.",
        "all_symbols": "كل الرموز",
        "all_timeframes": "كل الفريمات",
        "mode_core_name": "الأصول المفضلة",
        "mode_wide_name": "فحص 50+ أصل",
        "alert_type_both": "فورية + مترقبة",
        "alert_type_live": "فورية فقط",
        "alert_type_pending": "مترقبة فقط",
        "symbol_all": "ALL 🔔 كل الرموز",
        "tf_all": "ALL 🔔 كل الفريمات",
        "tf_5m": "5m ⭐ الموصى به",
        "signal_title_buy": "🚀 صفقة فورية صعود",
        "signal_title_sell": "🔻 صفقة فورية هبوط",
        "pending_title_buy": "📌 صفقة مترقبة صعود",
        "pending_title_sell": "📌 صفقة مترقبة هبوط",
        "label_symbol": "📊 الرمز",
        "label_tf": "⏱ الفريم",
        "label_action": "📈 الإشارة",
        "label_entry": "💰 الدخول المقترح",
        "label_tp": "🎯 الهدف",
        "label_sl": "🛑 وقف الخسارة",
        "label_strength": "🔥 قوة الإشارة",
        "label_strategy": "📡 الاستراتيجية",
        "label_reason": "🧠 السبب",
        "label_order_type": "📍 نوع الأمر",
        "label_current_price": "💹 السعر الحالي",
        "label_trigger_price": "🎯 سعر التفعيل",
        "signal_disclaimer": "⚠️ هذه إشارة متوقعة وليست نصيحة مالية.",
        "pending_note": "⚠️ لا تدخل إلا عند وصول السعر إلى سعر التفعيل.",
        "manual_reason": "إشارة تجريبية",
    },
    "en": {
        "choose_language": "🌐 Choose language / اختر اللغة",
        "lang_saved": "✅ Language saved: English",
        "must_join": "⚠️ You must join {channel} first to use the bot.",
        "join_channel": "📢 Join channel",
        "welcome": (
            "📈 Welcome to the trading signals bot\n\n"
            "This bot scans the market every 5 minutes and shows the best opportunities.\n"
            "You can receive live setups, pending setups, or both.\n\n"
            "⭐ 5m is the recommended timeframe."
        ),
        "help": (
            "ℹ️ Bot guide\n\n"
            "1) Join the channel first\n"
            "2) Send /start\n"
            "3) Choose one symbol or all symbols\n"
            "4) Choose timeframe\n"
            "5) Choose alert type\n"
            "6) The bot scans every 5 minutes\n\n"
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
        "btn_best_live": "🔥 Best live setup now",
        "btn_best_pending": "📌 Best pending setup now",
        "btn_scan": "📊 Scan market now",
        "btn_core": "🎯 Core assets mode",
        "btn_wide": "🌐 Scan 50+ assets",
        "btn_symbol": "⚙️ Choose symbol",
        "btn_timeframe": "⏱ Choose timeframe",
        "btn_alert_type": "🔔 Alert type",
        "btn_settings": "📋 My settings",
        "btn_channel": "📢 Open channel",
        "btn_language": "🌐 Language",
        "btn_back": "⬅️ Back",
        "pick_symbol": "⚙️ Choose the symbol you want to receive signals for:",
        "pick_timeframe": "⏱ Choose the timeframe you want to receive signals for:",
        "pick_alert_type": "🔔 Choose the alert type you want:",
        "saved_symbol": "✅ Symbol saved: {symbol}\nCurrent timeframe: {tf}",
        "saved_timeframe": "✅ Timeframe saved: {tf}\nCurrent symbol: {symbol}",
        "saved_alert_type": "✅ Alert type saved: {alert_type}",
        "core_saved": "✅ Core assets mode enabled: Gold + Bitcoin + EUR/USD",
        "wide_saved": "✅ 50+ assets scan enabled.",
        "searching_best_live": "⏳ Searching for the best live setup...",
        "searching_best_pending": "⏳ Searching for the best pending setup...",
        "searching_market": "📊 Scanning market now...",
        "no_live_signal": "❌ No strong live setup right now.",
        "no_pending_signal": "❌ No strong pending setup right now.",
        "no_market_signal": "📊 No strong signals right now.",
        "top_opps": "📊 Top current opportunities:\n",
        "settings_header": "📋 Your current settings\n\n",
        "pref_symbol": "Preferred symbol: {symbol}",
        "pref_tf": "Preferred timeframe: {tf}",
        "pref_mode": "Scan mode: {mode}",
        "pref_alert_type": "Alert type: {alert_type}",
        "pref_note": "You will only receive alerts matching these settings.\nThe bot scans every 5 minutes.",
        "all_symbols": "All symbols",
        "all_timeframes": "All timeframes",
        "mode_core_name": "Core assets",
        "mode_wide_name": "50+ assets scan",
        "alert_type_both": "Live + Pending",
        "alert_type_live": "Live only",
        "alert_type_pending": "Pending only",
        "symbol_all": "ALL 🔔 All symbols",
        "tf_all": "ALL 🔔 All timeframes",
        "tf_5m": "5m ⭐ Recommended",
        "signal_title_buy": "🚀 Live bullish setup",
        "signal_title_sell": "🔻 Live bearish setup",
        "pending_title_buy": "📌 Pending bullish setup",
        "pending_title_sell": "📌 Pending bearish setup",
        "label_symbol": "📊 Symbol",
        "label_tf": "⏱ Timeframe",
        "label_action": "📈 Signal",
        "label_entry": "💰 Suggested entry",
        "label_tp": "🎯 Target",
        "label_sl": "🛑 Stop loss",
        "label_strength": "🔥 Signal strength",
        "label_strategy": "📡 Strategy",
        "label_reason": "🧠 Reason",
        "label_order_type": "📍 Order type",
        "label_current_price": "💹 Current price",
        "label_trigger_price": "🎯 Trigger price",
        "signal_disclaimer": "⚠️ This is a probabilistic setup, not financial advice.",
        "pending_note": "⚠️ Do not enter unless price reaches the trigger level.",
        "manual_reason": "Manual test signal",
    },
}


# =========================
# GLOBALS
# =========================
telegram_app: Optional[Application] = None
scanner_task: Optional[asyncio.Task] = None


# =========================
# HELPERS
# =========================
def channel_link() -> str:
    return f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}"


def detect_default_lang(language_code: Optional[str]) -> str:
    if language_code and language_code.lower().startswith("ar"):
        return "ar"
    return "en"


def t(lang: str, key: str, **kwargs) -> str:
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)


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
            alert_type TEXT DEFAULT 'both',
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


def get_user_alert_type(user_id: int) -> str:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT alert_type FROM preferences WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row or not row["alert_type"]:
        return "both"
    return row["alert_type"]


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


def update_user_alert_type(user_id: int, alert_type: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, alert_type)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            alert_type=excluded.alert_type,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, alert_type),
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
            COALESCE(preferences.mode, 'core') AS mode,
            COALESCE(preferences.alert_type, 'both') AS alert_type
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
# UI BUILDERS
# =========================
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
            [InlineKeyboardButton(t(lang, "btn_best_live"), callback_data="best_live_now")],
            [InlineKeyboardButton(t(lang, "btn_best_pending"), callback_data="best_pending_now")],
            [InlineKeyboardButton(t(lang, "btn_scan"), callback_data="scan_now")],
            [InlineKeyboardButton(t(lang, "btn_core"), callback_data="def main_menu_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t(lang, "btn_best_live"), callback_data="best_live_now")],
            [InlineKeyboardButton(t(lang, "btn_best_pending"), callback_data="best_pending_now")],
            [InlineKeyboardButton(t(lang, "btn_scan"), callback_data="scan_now")],
            [InlineKeyboardButton(t(lang, "btn_core"), callback_data="mode_core")],
            [InlineKeyboardButton(t(lang, "btn_wide"), callback_data="mode_wide")],
            [InlineKeyboardButton(t(lang, "btn_symbol"), callback_data="menu_symbol")],
            [InlineKeyboardButton(t(lang, "btn_timeframe"), callback_data="menu_timeframe")],
            [InlineKeyboardButton(t(lang, "btn_alert_type"), callback_data="menu_alert_type")],
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
    labels = {
        "ALL": t(lang, "tf_all"),
        "5m": t(lang, "tf_5m"),
    }
    rows = []
    for tf in TIMEFRAME_OPTIONS:
        base = labels.get(tf, tf)
        label = f"✅ {base}" if tf == current_timeframe else base
        rows.append([InlineKeyboardButton(label, callback_data=f"timeframe:{tf}")])
    rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def alert_type_keyboard(current_alert_type: str, lang: str) -> InlineKeyboardMarkup:
    mapping = {
        "both": t(lang, "alert_type_both"),
        "live": t(lang, "alert_type_live"),
        "pending": t(lang, "alert_type_pending"),
    }
    rows = []
    for alert_type in ALERT_TYPE_OPTIONS:
        base = mapping[alert_type]
        label = f"✅ {base}" if alert_type == current_alert_type else base
        rows.append([InlineKeyboardButton(label, callback_data=f"alert_type:{alert_type}")])
    rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def format_user_settings(user_id: int, lang: str) -> str:
    symbol, timeframe = get_user_preferences(user_id)
    mode = get_user_mode(user_id)
    alert_type = get_user_alert_type(user_id)

    symbol_text = t(lang, "all_symbols") if symbol == "ALL" else symbol
    timeframe_text = t(lang, "all_timeframes") if timeframe == "ALL" else timeframe
    mode_text = t(lang, "mode_core_name") if mode == "core" else t(lang, "mode_wide_name")

    if alert_type == "live":
        alert_text = t(lang, "alert_type_live")
    elif alert_type == "pending":
        alert_text = t(lang, "alert_type_pending")
    else:
        alert_text = t(lang, "alert_type_both")

    return (
        t(lang, "settings_header")
        + t(lang, "pref_symbol", symbol=symbol_text)
        + "\n"
        + t(lang, "pref_tf", tf=timeframe_text)
        + "\n"
        + t(lang, "pref_mode", mode=mode_text)
        + "\n"
        + t(lang, "pref_alert_type", alert_type=alert_text)
        + "\n\n"
        + t(lang, "pref_note")
    )


# =========================
# MARKET DATA
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


async def fetch_binance_long_short_ratio(symbol: str, period: str = "5m") -> Optional[float]:
    pair_map = {
        "BTC/USD": "BTCUSD",
        "ETH/USD": "ETHUSD",
        "SOL/USD": "SOLUSD",
        "BNB/USD": "BNBUSD",
        "XRP/USD": "XRPUSD",
        "ADA/USD": "ADAUSD",
        "DOGE/USD": "DOGEUSD",
        "AVAX/USD": "AVAXUSD",
        "LINK/USD": "LINKUSD",
        "DOT/USD": "DOTUSD",
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


# =========================
# INDICATORS
# =========================
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
        "ETC", "XLM", "APT", "ARB", "OP", "INJ",
    }
    if symbol.endswith("/USD") and symbol.split("/")[0] in crypto_prefixes:
        return round(value, 2)
    return round(value, 5)


# =========================
# SIGNAL BUILDERS
# =========================
def build_live_signal(symbol: str, timeframe: str, candles: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
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
    latest_atr = atr14[i] if atr14[i] > 0 else max(price * 0.003, 0.1)

    buy_score = 0
    sell_score = 0
    reasons = []

    if ema9[i] > ema21[i]:
        buy_score += 30
        reasons.append("EMA9 above EMA21")
    elif ema9[i] < ema21[i]:
        sell_score += 30
        reasons.append("EMA9 below EMA21")

    if 55 <= rsi14[i] <= 70:
        buy_score += 20
        reasons.append("RSI supports upside")
    elif 30 <= rsi14[i] <= 45:
        sell_score += 20
        reasons.append("RSI supports downside")

    if macd_line[i] > signal_line[i] and macd_hist[i] > macd_hist[i - 1]:
        buy_score += 25
        reasons.append("MACD bullish")
    elif macd_line[i] < signal_line[i] and macd_hist[i] < macd_hist[i - 1]:
        sell_score += 25
        reasons.append("MACD bearish")

    if latest["close"] > prev["high"]:
        buy_score += 20
        reasons.append("Break above previous high")
    elif latest["close"] < prev["low"]:
        sell_score += 20
        reasons.append("Break below previous low")

    buy_score += 5
    sell_score += 5

    if buy_score >= MIN_SIGNAL_STRENGTH and buy_score > sell_score:
        return {
            "kind": "live",
            "symbol": symbol,
            "timeframe": timeframe,
            "action": "BUY",
            "entry": round_by_symbol(symbol, price),
            "sl": round_by_symbol(symbol, price - latest_atr * 1.5),
            "tp": round_by_symbol(symbol, price + latest_atr * 3.0),
            "confidence": f"{min(buy_score, 95)}%",
            "strength_value": min(buy_score, 95),
            "strategy": "EMA + RSI + MACD + ATR",
            "reason": " | ".join(reasons[:4]),
            "bar_time": latest["datetime"],
        }

    if sell_score >= MIN_SIGNAL_STRENGTH and sell_score > buy_score:
        return {
            "kind": "live",
            "symbol": symbol,
            "timeframe": timeframe,
            "action": "SELL",
            "entry": round_by_symbol(symbol, price),
            "sl": round_by_symbol(symbol, price + latest_atr * 1.5),
            "tp": round_by_symbol(symbol, price - latest_atr * 3.0),
            "confidence": f"{min(sell_score, 95)}%",
            "strength_value": min(sell_score, 95),
            "strategy": "EMA + RSI + MACD + ATR",
            "reason": " | ".join(reasons[:4]),
            "bar_time": latest["datetime"],
        }

    return None


def build_pending_signal(symbol: str, timeframe: str, candles: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if len(candles) < 50:
        return None

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    rsi14 = rsi(closes, 14)
    macd_line, signal_line, _ = macd(closes)
    atr14 = atr(candles, 14)

    i = len(candles) - 1
    price = closes[i]
    latest_atr = atr14[i] if atr14[i] > 0 else max(price * 0.003, 0.1)

    recent_high = max(highs[-10:])
    recent_low = min(lows[-10:])

    buy_score = 0
    sell_score = 0
    buy_reasons = []
    sell_reasons = []

    if ema9[i] > ema21[i]:
        buy_score += 25
        buy_reasons.append("EMA bullish trend")
    if 45 <= rsi14[i] <= 60:
        buy_score += 15
        buy_reasons.append("RSI ready for bullish continuation")
    if macd_line[i] >= signal_line[i]:
        buy_score += 20
        buy_reasons.append("MACD supports upside")

    if ema9[i] < ema21[i]:
        sell_score += 25
        sell_reasons.append("EMA bearish trend")
    if 40 <= rsi14[i] <= 55:
        sell_score += 15
        sell_reasons.append("RSI ready for bearish continuation")
    if macd_line[i] <= signal_line[i]:
        sell_score += 20
        sell_reasons.append("MACD supports downside")

    buy_trigger = price - (latest_atr * 0.6)
    buy_tp = buy_trigger + (latest_atr * 2.5)
    buy_sl = buy_trigger - (latest_atr * 1.2)

    sell_trigger = price + (latest_atr * 0.6)
    sell_tp = sell_trigger - (latest_atr * 2.5)
    sell_sl = sell_trigger + (latest_atr * 1.2)

    buy_distance_pct = abs((buy_trigger - price) / price) * 100
    sell_distance_pct = abs((sell_trigger - price) / price) * 100

    if buy_score >= 60 and buy_distance_pct <= 1.0 and buy_trigger > recent_low:
        return {
            "kind": "pending",
            "symbol": symbol,
            "timeframe": timeframe,
            "action": "BUY",
            "order_type": "BUY LIMIT",
            "current_price": round_by_symbol(symbol, price),
            "entry": round_by_symbol(symbol, buy_trigger),
            "sl": round_by_symbol(symbol, buy_sl),
            "tp": round_by_symbol(symbol, buy_tp),
            "confidence": f"{min(buy_score + 15, 95)}%",
            "strength_value": min(buy_score + 15, 95),
            "strategy": "Pending Pullback Setup",
            "reason": " | ".join(buy_reasons[:4]),
            "bar_time": candles[i]["datetime"],
        }

    if sell_score >= 60 and sell_distance_pct <= 1.0 and sell_trigger < recent_high:
        return {
            "kind": "pending",
            "symbol": symbol,
            "timeframe": timeframe,
            "action": "SELL",
            "order_type": "SELL LIMIT",
            "current_price": round_by_symbol(symbol, price),
            "entry": round_by_symbol(symbol, sell_trigger),
            "sl": round_by_symbol(symbol, sell_sl),
            "tp": round_by_symbol(symbol, sell_tp),
            "confidence": f"{min(sell_score + 15, 95)}%",
            "strength_value": min(sell_score + 15, 95),
            "strategy": "Pending Pullback Setup",
            "reason": " | ".join(sell_reasons[:4]),
            "bar_time": candles[i]["datetime"],
        }

    return None


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

    strength = max(0, min(strength, 95))
    signal["confidence"] = f"{strength}%"
    signal["strength_value"] = strength
    return signal


# =========================
# FORMATTERS
# =========================
def format_live_signal(data: dict, lang: str) -> str:
    title = t(lang, "signal_title_buy") if data["action"] == "BUY" else t(lang, "signal_title_sell")
    return (
        f"{title}\n\n"
        f"{t(lang, 'label_symbol')}: {data['symbol']}\n"
        f"{t(lang, 'label_tf')}: {data['timeframe']}\n"
        f"{t(lang, 'label_action')}: {data['action']}\n\n"
        f"{t(lang, 'label_entry')}: {data['entry']}\n"
        f"{t(lang, 'label_tp')}: {data['tp']}\n"
        f"{t(lang, 'label_sl')}: {data['sl']}\n"
        f"{t(lang, 'label_strength')}: {data['confidence']}\n\n"
        f"{t(lang, 'label_strategy')}: {data['strategy']}\n"
        f"{t(lang, 'label_reason')}: {data['reason']}\n\n"
        f"{t(lang, 'signal_disclaimer')}"
    )


def format_pending_signal(data: dict, lang: str) -> str:
    title = t(lang, "pending_title_buy") if data["action"] == "BUY" else t(lang, "pending_title_sell")
    return (
        f"{title}\n\n"
        f"{t(lang, 'label_symbol')}: {data['symbol']}\n"
        f"{t(lang, 'label_tf')}: {data['timeframe']}\n"
        f"{t(lang, 'label_order_type')}: {data['order_type']}\n\n"
        f"{t(lang, 'label_current_price')}: {data['current_price']}\n"
        f"{t(lang, 'label_trigger_price')}: {data['entry']}\n"
        f"{t(lang, 'label_tp')}: {data['tp']}\n"
        f"{t(lang, 'label_sl')}: {data['sl']}\n"
        f"{t(lang, 'label_strength')}: {data['confidence']}\n\n"
        f"{t(lang, 'label_strategy')}: {data['strategy']}\n"
        f"{t(lang, 'label_reason')}: {data['reason']}\n\n"
        f"{t(lang, 'pending_note')}\n"
        f"{t(lang, 'signal_disclaimer')}"
    )


# =========================
# MATCHING
# =========================
def matches_preferences(user_symbol: str, user_timeframe: str, signal_symbol: str, signal_timeframe: str) -> bool:
    symbol_ok = user_symbol == "ALL" or user_symbol.upper() == signal_symbol.upper()
    timeframe_ok = user_timeframe == "ALL" or user_timeframe.lower() == signal_timeframe.lower()
    return symbol_ok and timeframe_ok


def alert_type_matches(user_alert_type: str, signal_kind: str) -> bool:
    if user_alert_type == "both":
        return True
    if user_alert_type == "live" and signal_kind == "live":
        return True
    if user_alert_type == "pending" and signal_kind == "pending":
        return True
    return False


# =========================
# SEARCH ENGINE
# =========================
async def find_all_live_signals(mode: str = "core") -> list[dict[str, Any]]:
    results = []
    symbols_to_scan = FOCUS_MODES.get(mode, FOCUS_MODES["core"])

    for symbol in symbols_to_scan:
        try:
            candles = await fetch_candles(symbol, "5m")
            signal = build_live_signal(symbol, "5m", candles)
            if signal:
                signal = await enrich_signal_with_sentiment(signal)
                if signal["strength_value"] >= MIN_SIGNAL_STRENGTH:
                    results.append(signal)
        except Exception as exc:
            logger.exception("Live scan failed for %s: %s", symbol, exc)

    results.sort(key=lambda x: x.get("strength_value", 0), reverse=True)
    return results


async def find_all_pending_signals(mode: str = "core") -> list[dict[str, Any]]:
    results = []
    symbols_to_scan = FOCUS_MODES.get(mode, FOCUS_MODES["core"])

    for symbol in symbols_to_scan:
        try:
            candles = await fetch_candles(symbol, "5m")
            signal = build_pending_signal(symbol, "5m", candles)
            if signal:
                signal = await enrich_signal_with_sentiment(signal)
                if signal["strength_value"] >= 70:
                    results.append(signal)
        except Exception as exc:
            logger.exception("Pending scan failed for %s: %s", symbol, exc)

    results.sort(key=lambda x: x.get("strength_value", 0), reverse=True)
    return results


# =========================
# DELIVERY
# =========================
async def broadcast_signal(data: dict) -> dict:
    signal_symbol = str(data.get("symbol", "")).upper()
    signal_timeframe = str(data.get("timeframe", "ALL"))
    signal_kind = str(data.get("kind", "live"))

    users = get_active_users_with_preferences()
    sent = 0
    failed = 0
    skipped = 0

    for row in users:
        user_id = int(row["user_id"])
        user_lang = row["language"] if row["language"] in TEXTS else "en"
        user_symbol = str(row["symbol"])
        user_timeframe = str(row["timeframe"])
        user_alert_type = str(row["alert_type"])

        if not matches_preferences(user_symbol, user_timeframe, signal_symbol, signal_timeframe):
            skipped += 1
            continue

        if not alert_type_matches(user_alert_type, signal_kind):
            skipped += 1
            continue

        try:
            text = format_pending_signal(data, user_lang) if signal_kind == "pending" else format_live_signal(data, user_lang)
            await telegram_app.bot.send_message(chat_id=user_id, text=text)
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
            live_results = await find_all_live_signals(mode="wide")
            best_live = live_results[0] if live_results else None

            if best_live:
                signal_key = f"live|{best_live['symbol']}|{best_live['timeframe']}|{best_live['action']}|{best_live['bar_time']}"
                if not was_signal_sent(signal_key):
                    result = await broadcast_signal(best_live)
                    remember_signal(signal_key)
                    logger.info("Best live signal sent: %s -> %s", signal_key, result)
            else:
                pending_results = await find_all_pending_signals(mode="wide")
                best_pending = pending_results[0] if pending_results else None
                if best_pending:
                    signal_key = f"pending|{best_pending['symbol']}|{best_pending['timeframe']}|{best_pending['action']}|{best_pending['bar_time']}"
                    if not was_signal_sent(signal_key):
                        result = await broadcast_signal(best_pending)
                        remember_signal(signal_key)
                        logger.info("Best pending signal sent: %s -> %s", signal_key, result)
                else:
                    logger.info("No strong live or pending signal found in this cycle.")
        except Exception as exc:
            logger.exception("Background scanner error: %s", exc)

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


# =========================
# TELEGRAM COMMANDS
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
    current_alert_type = get_user_alert_type(user.id)

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

    if query.data == "menu_alert_type":
        await query.message.reply_text(t(lang, "pick_alert_type"), reply_markup=alert_type_keyboard(current_alert_type, lang))
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

    if query.data == "best_live_now":
        await query.message.reply_text(t(lang, "searching_best_live"))
        mode = get_user_mode(user.id)
        results = await find_all_live_signals(mode=mode)
        signal = results[0] if results else None
        if not signal:
            await query.message.reply_text(t(lang, "no_live_signal"), reply_markup=main_menu_keyboard(lang))
            return
        await query.message.reply_text(format_live_signal(signal, lang), reply_markup=main_menu_keyboard(lang))
        return

    if query.data == "best_pending_now":
        await query.message.reply_text(t(lang, "searching_best_pending"))
        mode = get_user_mode(user.id)
        results = await find_all_pending_signals(mode=mode)
        signal = results[0] if results else None
        if not signal:
            await query.message.reply_text(t(lang, "no_pending_signal"), reply_markup=main_menu_keyboard(lang))
            return
        await query.message.reply_text(format_pending_signal(signal, lang), reply_markup=main_menu_keyboard(lang))
        return

    if query.data == "scan_now":
        await query.message.reply_text(t(lang, "searching_market"))
        mode = get_user_mode(user.id)

        live_results = await find_all_live_signals(mode=mode)
        pending_results = await find_all_pending_signals(mode=mode)

        if not live_results and not pending_results:
            await query.message.reply_text(t(lang, "no_market_signal"), reply_markup=main_menu_keyboard(lang))
            return

        lines = [t(lang, "top_opps")]
        count = 1

        for sig in live_results[:3]:
            lines.append(f"{count}) LIVE | {sig['symbol']} | {sig['action']} | {sig['confidence']}")
            count += 1

        for sig in pending_results[:2]:
            lines.append(f"{count}) PENDING | {sig['symbol']} | {sig['action']} | {sig['confidence']}")
            count += 1

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

    if query.data.startswith("alert_type:"):
        alert_type = query.data.split(":", 1)[1]
        if alert_type not in ALERT_TYPE_OPTIONS:
            alert_type = "both"
        update_user_alert_type(user.id, alert_type)

        if alert_type == "live":
            alert_text = t(lang, "alert_type_live")
        elif alert_type == "pending":
            alert_text = t(lang, "alert_type_pending")
        else:
            alert_text = t(lang, "alert_type_both")

        await query.message.reply_text(
            t(lang, "saved_alert_type", alert_type=alert_text),
            reply_markup=main_menu_keyboard(lang),
        )
        return


# =========================
# FASTAPI LIFESPAN
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, scanner_task

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

    telegram_webhook_url = f"https://{RAILWAY_PUBLIC_DOMAIN}/telegram-webhook"
    await telegram_app.bot.set_webhook(url=telegram_webhook_url)

    scanner_task = asyncio.create_task(background_scanner())
    logger.info("Telegram bot started with webhook: %s", telegram_webhook_url)

    yield

    if scanner_task:
        scanner_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            pass

    await telegram_app.bot.delete_webhook()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Telegram bot stopped")


# =========================
# FASTAPI APP
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


@app.get("/manual-test-live")
async def manual_test_live():
    test_signal = {
        "kind": "live",
        "symbol": "BTC/USD",
        "timeframe": "5m",
        "action": "BUY",
        "entry": 65000,
        "sl": 64500,
        "tp": 66000,
        "confidence": "80%",
        "strength_value": 80,
        "strategy": "Manual Test Live",
        "reason": TEXTS["en"]["manual_reason"],
        "bar_time": "manual_live",
    }
    result = await broadcast_signal(test_signal)
    return result


@app.get("/manual-test-pending")
async def manual_test_pending():
    test_signal = {
        "kind": "pending",
        "symbol": "XAU/USD",
        "timeframe": "5m",
        "action": "BUY",
        "order_type": "BUY LIMIT",
        "current_price": 3350.20,
        "entry": 3347.80,
        "sl": 3342.90,
        "tp": 3358.60,
        "confidence": "83%",
        "strength_value": 83,
        "strategy": "Manual Test Pending",
        "reason": TEXTS["en"]["manual_reason"],
        "bar_time": "manual_pending",
    }
    result = await broadcast_signal(test_signal)
    return result


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, reload=False)