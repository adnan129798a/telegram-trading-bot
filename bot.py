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

MIN_SIGNAL_STRENGTH = int(os.getenv("MIN_SIGNAL_STRENGTH", "60"))
SCAN_INTERVAL_SECONDS = 300
CANDLE_LIMIT = 120

SYMBOL_OPTIONS = [
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

GB_SYMBOLS = ["XAU/USD", "BTC/USD"]
WIDE_SYMBOLS = [s for s in SYMBOL_OPTIONS if s not in GB_SYMBOLS]
ALL_SYMBOLS = list(SYMBOL_OPTIONS)

INTERVAL_MAP = {"5m": "5min"}


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
            "📈 أهلًا بك في بوت الإشارات\n\n"
            "هذا البوت يفحص السوق تلقائيًا كل 5 دقائق.\n"
            "أنت تختار مرة واحدة:\n"
            "• نوع التنبيهات\n"
            "• مجموعة الأصول\n"
            "• مستوى قوة الإشارة\n\n"
            "وبعدها سيصلك التنبيه تلقائيًا فور ظهور صفقة قوية."
        ),
        "help": (
            "ℹ️ شرح البوت\n\n"
            "1) اشترك في القناة أولًا\n"
            "2) اكتب /start\n"
            "3) اختر نوع التنبيهات\n"
            "4) اختر مجموعة الأصول\n"
            "5) اختر مستوى قوة الإشارة\n"
            "6) فعّل التنبيهات مرة واحدة\n\n"
            "بعدها سيقوم البوت بفحص السوق كل 5 دقائق\n"
            "ويرسل لك الصفقة تلقائيًا عند توفرها.\n\n"
            "الأوامر:\n"
            "/start - تشغيل البوت\n"
            "/menu - فتح القائمة\n"
            "/status - عرض إعداداتك\n"
            "/language - تغيير اللغة\n"
            "/help - شرح البوت\n\n"
            "⚠️ الإشارات توقعات مبنية على التحليل الفني وليست نصيحة مالية."
        ),
        "menu_title": "📍 القائمة الرئيسية",
        "status_ok": "✅ إعداداتك الحالية:\n\n",
        "not_joined": "❌ أنت غير مشترك حاليًا في القناة المطلوبة.",
        "btn_live_only": "🔥 تفعيل الفورية فقط",
        "btn_pending_only": "📌 تفعيل المترقبة فقط",
        "btn_both": "🚀 تفعيل الفورية + المترقبة",
        "btn_assets": "🌍 اختيار مجموعة الأصول",
        "btn_strength": "⚡ قوة الإشارة",
        "btn_settings": "📋 عرض إعداداتي",
        "btn_disable": "🛑 إيقاف التنبيهات",
        "btn_language": "🌐 اللغة",
        "btn_channel": "📢 فتح القناة",
        "btn_back": "⬅️ رجوع",
        "pick_assets": "🌍 اختر مجموعة الأصول التي تريد متابعتها:",
        "pick_strength": "⚡ اختر مستوى قوة الإشارة:",
        "asset_gb": "🥇 الذهب + البيتكوين",
        "asset_wide": "🌐 باقي 50+ أصل",
        "asset_all": "🚀 جميع الأصول",
        "saved_assets": "✅ تم حفظ مجموعة الأصول: {group}",
        "strength_saved": "✅ تم حفظ مستوى الإشارة: {level}",
        "strength_conservative": "🟢 محافظ 80%",
        "strength_balanced": "🟡 متوازن 70%",
        "strength_aggressive": "🔴 هجومي 60%",
        "enabled_live": (
            "✅ تم تفعيل تنبيهات الصفقات الفورية.\n\n"
            "🌍 مجموعة الأصول: {group}\n"
            "⚡ مستوى الإشارة: {strength}\n"
            "⏱ سيتم فحص السوق تلقائيًا كل 5 دقائق\n"
            "وسيتم إخطارك فور ظهور صفقة فورية قوية."
        ),
        "enabled_pending": (
            "✅ تم تفعيل تنبيهات الصفقات المترقبة.\n\n"
            "🌍 مجموعة الأصول: {group}\n"
            "⚡ مستوى الإشارة: {strength}\n"
            "⏱ سيتم فحص السوق تلقائيًا كل 5 دقائق\n"
            "وسيتم إخطارك فور ظهور أقرب صفقة مترقبة قوية."
        ),
        "enabled_both": (
            "✅ تم تفعيل تنبيهات الصفقات الفورية + المترقبة.\n\n"
            "🌍 مجموعة الأصول: {group}\n"
            "⚡ مستوى الإشارة: {strength}\n"
            "⏱ سيتم فحص السوق تلقائيًا كل 5 دقائق\n"
            "وسيتم إخطارك فور ظهور صفقة قوية."
        ),
        "disabled_alerts": "🛑 تم إيقاف جميع التنبيهات التلقائية.",
        "settings_header": "📋 إعداداتك الحالية\n\n",
        "pref_alerts": "🔔 نوع التنبيهات: {value}",
        "pref_assets": "🌍 مجموعة الأصول: {value}",
        "pref_strength": "⚡ مستوى الإشارة: {value}",
        "pref_enabled": "⚙️ الحالة: {value}",
        "enabled_yes": "مفعلة",
        "enabled_no": "متوقفة",
        "alerts_live": "فورية فقط",
        "alerts_pending": "مترقبة فقط",
        "alerts_both": "فورية + مترقبة",
        "signal_title_buy": "🚀 صفقة فورية صعود",
        "signal_title_sell": "🔻 صفقة فورية هبوط",
        "pending_title_buy": "📌 صفقة مترقبة صعود",
        "pending_title_sell": "📌 صفقة مترقبة هبوط",
        "label_symbol": "📊 الرمز",
        "label_action": "📈 الإشارة",
        "label_tf": "⏱ الفريم",
        "label_entry": "💰 الدخول",
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
    },
    "en": {
        "choose_language": "🌐 Choose language / اختر اللغة",
        "lang_saved": "✅ Language saved: English",
        "must_join": "⚠️ You must join {channel} first to use the bot.",
        "join_channel": "📢 Join channel",
        "welcome": (
            "📈 Welcome to the signals bot\n\n"
            "This bot scans the market automatically every 5 minutes.\n"
            "You choose once:\n"
            "• alert type\n"
            "• asset group\n"
            "• signal strength\n\n"
            "Then you will receive signals automatically when a strong setup appears."
        ),
        "help": (
            "ℹ️ Bot guide\n\n"
            "1) Join the channel first\n"
            "2) Send /start\n"
            "3) Choose alert type\n"
            "4) Choose asset group\n"
            "5) Choose signal strength\n"
            "6) Enable alerts once\n\n"
            "Then the bot scans the market every 5 minutes\n"
            "and sends you setups automatically.\n\n"
            "Commands:\n"
            "/start - start the bot\n"
            "/menu - open menu\n"
            "/status - your settings\n"
            "/language - change language\n"
            "/help - help\n\n"
            "⚠️ Signals are probabilistic technical setups, not financial advice."
        ),
        "menu_title": "📍 Main menu",
        "status_ok": "✅ Your current settings:\n\n",
        "not_joined": "❌ You are not subscribed to the required channel right now.",
        "btn_live_only": "🔥 Enable live only",
        "btn_pending_only": "📌 Enable pending only",
        "btn_both": "🚀 Enable live + pending",
        "btn_assets": "🌍 Choose asset group",
        "btn_strength": "⚡ Signal strength",
        "btn_settings": "📋 My settings",
        "btn_disable": "🛑 Disable alerts",
        "btn_language": "🌐 Language",
        "btn_channel": "📢 Open channel",
        "btn_back": "⬅️ Back",
        "pick_assets": "🌍 Choose the asset group you want to monitor:",
        "pick_strength": "⚡ Choose signal strength level:",
        "asset_gb": "🥇 Gold + Bitcoin",
        "asset_wide": "🌐 Other 50+ assets",
        "asset_all": "🚀 All assets",
        "saved_assets": "✅ Asset group saved: {group}",
        "strength_saved": "✅ Signal strength saved: {level}",
        "strength_conservative": "🟢 Conservative 80%",
        "strength_balanced": "🟡 Balanced 70%",
        "strength_aggressive": "🔴 Aggressive 60%",
        "enabled_live": (
            "✅ Live alerts enabled.\n\n"
            "🌍 Asset group: {group}\n"
            "⚡ Signal strength: {strength}\n"
            "⏱ The market will be scanned automatically every 5 minutes\n"
            "and you will be notified when a strong live setup appears."
        ),
        "enabled_pending": (
            "✅ Pending alerts enabled.\n\n"
            "🌍 Asset group: {group}\n"
            "⚡ Signal strength: {strength}\n"
            "⏱ The market will be scanned automatically every 5 minutes\n"
            "and you will be notified when a strong pending setup appears."
        ),
        "enabled_both": (
            "✅ Live + pending alerts enabled.\n\n"
            "🌍 Asset group: {group}\n"
            "⚡ Signal strength: {strength}\n"
            "⏱ The market will be scanned automatically every 5 minutes\n"
            "and you will be notified when a strong setup appears."
        ),
        "disabled_alerts": "🛑 All automatic alerts have been disabled.",
        "settings_header": "📋 Your current settings\n\n",
        "pref_alerts": "🔔 Alert type: {value}",
        "pref_assets": "🌍 Asset group: {value}",
        "pref_strength": "⚡ Signal strength: {value}",
        "pref_enabled": "⚙️ Status: {value}",
        "enabled_yes": "Enabled",
        "enabled_no": "Disabled",
        "alerts_live": "Live only",
        "alerts_pending": "Pending only",
        "alerts_both": "Live + pending",
        "signal_title_buy": "🚀 Live bullish setup",
        "signal_title_sell": "🔻 Live bearish setup",
        "pending_title_buy": "📌 Pending bullish setup",
        "pending_title_sell": "📌 Pending bearish setup",
        "label_symbol": "📊 Symbol",
        "label_action": "📈 Signal",
        "label_tf": "⏱ Timeframe",
        "label_entry": "💰 Entry",
        "label_tp": "🎯 Target",
        "label_sl": "🛑 Stop loss",
        "label_strength": "🔥 Strength",
        "label_strategy": "📡 Strategy",
        "label_reason": "🧠 Reason",
        "label_order_type": "📍 Order type",
        "label_current_price": "💹 Current price",
        "label_trigger_price": "🎯 Trigger price",
        "signal_disclaimer": "⚠️ This is a probabilistic setup, not financial advice.",
        "pending_note": "⚠️ Do not enter unless price reaches the trigger level.",
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


def asset_group_name(group: str, lang: str) -> str:
    if group == "gb":
        return t(lang, "asset_gb")
    if group == "wide":
        return t(lang, "asset_wide")
    return t(lang, "asset_all")


def alert_type_name(alert_type: str, lang: str) -> str:
    if alert_type == "live":
        return t(lang, "alerts_live")
    if alert_type == "pending":
        return t(lang, "alerts_pending")
    return t(lang, "alerts_both")


def strength_name(value: int, lang: str) -> str:
    if value >= 80:
        return t(lang, "strength_conservative")
    if value >= 70:
        return t(lang, "strength_balanced")
    return t(lang, "strength_aggressive")


def symbols_for_group(group: str) -> list[str]:
    if group == "gb":
        return GB_SYMBOLS
    if group == "wide":
        return WIDE_SYMBOLS
    return ALL_SYMBOLS


# =========================
# DATABASE
# =========================
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cur.fetchall()]
    if column not in columns:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.commit()


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
            alert_type TEXT DEFAULT 'both',
            asset_group TEXT DEFAULT 'all',
            alerts_enabled INTEGER DEFAULT 0,
            min_strength INTEGER DEFAULT 70,
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

    ensure_column(conn, "preferences", "alert_type", "TEXT DEFAULT 'both'")
    ensure_column(conn, "preferences", "asset_group", "TEXT DEFAULT 'all'")
    ensure_column(conn, "preferences", "alerts_enabled", "INTEGER DEFAULT 0")
    ensure_column(conn, "preferences", "min_strength", "INTEGER DEFAULT 70")

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


def get_user_preferences(user_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COALESCE(alert_type, 'both') AS alert_type,
            COALESCE(asset_group, 'all') AS asset_group,
            COALESCE(alerts_enabled, 0) AS alerts_enabled,
            COALESCE(min_strength, 70) AS min_strength
        FROM preferences
        WHERE user_id=?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def update_user_asset_group(user_id: int, asset_group: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, asset_group)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            asset_group=excluded.asset_group,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, asset_group),
    )
    conn.commit()
    conn.close()


def update_user_min_strength(user_id: int, min_strength: int) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, min_strength)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            min_strength=excluded.min_strength,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, min_strength),
    )
    conn.commit()
    conn.close()


def get_user_min_strength(user_id: int) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(min_strength, 70) AS min_strength FROM preferences WHERE user_id=?",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return 70
    return int(row["min_strength"])


def update_user_alerts(user_id: int, alert_type: str, enabled: bool) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, alert_type, alerts_enabled)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            alert_type=excluded.alert_type,
            alerts_enabled=excluded.alerts_enabled,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, alert_type, 1 if enabled else 0),
    )
    conn.commit()
    conn.close()


def disable_user_alerts(user_id: int) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO preferences (user_id, alerts_enabled)
        VALUES (?, 0)
        ON CONFLICT(user_id) DO UPDATE SET
            alerts_enabled=0,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id,),
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
            COALESCE(preferences.alert_type, 'both') AS alert_type,
            COALESCE(preferences.asset_group, 'all') AS asset_group,
            COALESCE(preferences.alerts_enabled, 0) AS alerts_enabled,
            COALESCE(preferences.min_strength, 70) AS min_strength
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
            [InlineKeyboardButton(t(lang, "btn_live_only"), callback_data="enable_live")],
            [InlineKeyboardButton(t(lang, "btn_pending_only"), callback_data="enable_pending")],
            [InlineKeyboardButton(t(lang, "btn_both"), callback_data="enable_both")],
            [InlineKeyboardButton(t(lang, "btn_assets"), callback_data="menu_assets")],
            [InlineKeyboardButton(t(lang, "btn_strength"), callback_data="menu_strength")],
            [InlineKeyboardButton(t(lang, "btn_settings"), callback_data="menu_settings")],
            [InlineKeyboardButton(t(lang, "btn_disable"), callback_data="disable_alerts")],
            [InlineKeyboardButton(t(lang, "btn_language"), callback_data="menu_language")],
            [InlineKeyboardButton(t(lang, "btn_channel"), url=channel_link())],
        ]
    )


def asset_group_keyboard(current_group: str, lang: str) -> InlineKeyboardMarkup:
    options = [
        ("gb", t(lang, "asset_gb")),
        ("wide", t(lang, "asset_wide")),
        ("all", t(lang, "asset_all")),
    ]
    rows = []
    for group_value, label in options:
        shown = f"✅ {label}" if current_group == group_value else label
        rows.append([InlineKeyboardButton(shown, callback_data=f"asset_group:{group_value}")])
    rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def strength_keyboard(current_strength: int, lang: str) -> InlineKeyboardMarkup:
    options = [
        (80, t(lang, "strength_conservative")),
        (70, t(lang, "strength_balanced")),
        (60, t(lang, "strength_aggressive")),
    ]
    rows = []
    for value, label in options:
        shown = f"✅ {label}" if current_strength == value else label
        rows.append([InlineKeyboardButton(shown, callback_data=f"strength:{value}")])
    rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


def format_user_settings(user_id: int, lang: str) -> str:
    prefs = get_user_preferences(user_id)
    alert_type = prefs["alert_type"] if prefs else "both"
    asset_group = prefs["asset_group"] if prefs else "all"
    enabled = int(prefs["alerts_enabled"]) if prefs else 0
    min_strength = int(prefs["min_strength"]) if prefs else 70

    return (
        t(lang, "settings_header")
        + t(lang, "pref_alerts", value=alert_type_name(alert_type, lang))
        + "\n"
        + t(lang, "pref_assets", value=asset_group_name(asset_group, lang))
        + "\n"
        + t(lang, "pref_strength", value=strength_name(min_strength, lang))
        + "\n"
        + t(lang, "pref_enabled", value=t(lang, "enabled_yes") if enabled else t(lang, "enabled_no"))
    )


# =========================
# MARKET DATA
# =========================
async def fetch_candles(symbol: str, timeframe: str = "5m") -> list[dict[str, Any]]:
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
        raise RuntimeError(f"Twelve Data error for {symbol}: {message}")

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
def build_live_signal(symbol: str, candles: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
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
            "timeframe": "5m",
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
            "timeframe": "5m",
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


def build_pending_signal(symbol: str, candles: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
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
            "timeframe": "5m",
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
            "timeframe": "5m",
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
    ratio = await fetch_binance_long_short_ratio(signal["symbol"], "5m")
    strength = int(str(signal["confidence"]).replace("%", ""))

    if ratio is not None:
        if signal["action"] == "BUY" and ratio > 1.1:
            strength += 10
            signal["reason"] += " | Binance longs supportive"
        elif signal["action"] == "SELL" and ratio < 0.9:
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
# DELIVERY
# =========================
def signal_matches_group(signal_symbol: str, asset_group: str) -> bool:
    return signal_symbol in symbols_for_group(asset_group)


async def send_signal_to_user(user_id: int, lang: str, signal: dict) -> bool:
    try:
        text = format_pending_signal(signal, lang) if signal["kind"] == "pending" else format_live_signal(signal, lang)
        await telegram_app.bot.send_message(chat_id=user_id, text=text)
        return True
    except Exception as exc:
        logger.exception("Failed sending to %s: %s", user_id, exc)
        err = str(exc).lower()
        if "forbidden" in err or "chat not found" in err or "blocked" in err:
            deactivate_user(user_id)
        return False

async def background_scanner() -> None:
    await asyncio.sleep(15)

    while True:
        try:
            live_results: list[dict[str, Any]] = []
            pending_results: list[dict[str, Any]] = []

            for symbol in ALL_SYMBOLS:
                try:
                    candles = await fetch_candles(symbol, "5m")

                    live_signal = build_live_signal(symbol, candles)
                    if live_signal:
                        live_signal = await enrich_signal_with_sentiment(live_signal)
                        if live_signal["strength_value"] >= MIN_SIGNAL_STRENGTH:
                            live_results.append(live_signal)

                    pending_signal = build_pending_signal(symbol, candles)
                    if pending_signal:
                        pending_signal = await enrich_signal_with_sentiment(pending_signal)
                        if pending_signal["strength_value"] >= 60:
                            pending_results.append(pending_signal)

                except Exception as exc:
                    logger.exception("Scan failed for %s: %s", symbol, exc)

            live_results.sort(key=lambda x: x["strength_value"], reverse=True)
            pending_results.sort(key=lambda x: x["strength_value"], reverse=True)

            users = get_active_users_with_preferences()

            for row in users:
                user_id = int(row["user_id"])
                lang = row["language"] if row["language"] in TEXTS else "en"
                alert_type = str(row["alert_type"])
                asset_group = str(row["asset_group"])
                alerts_enabled = int(row["alerts_enabled"])
                min_strength = int(row["min_strength"])

                if not alerts_enabled:
                    continue

                if alert_type in ("live", "both"):
                    for signal in live_results:
                        if signal_matches_group(signal["symbol"], asset_group) and signal["strength_value"] >= min_strength:
                            signal_key = f"user:{user_id}|live|{signal['symbol']}|{signal['action']}|{signal['bar_time']}"
                            if not was_signal_sent(signal_key):
                                ok = await send_signal_to_user(user_id, lang, signal)
                                if ok:
                                    remember_signal(signal_key)
                            break

                if alert_type in ("pending", "both"):
                    for signal in pending_results:
                        if signal_matches_group(signal["symbol"], asset_group) and signal["strength_value"] >= min_strength:
                            signal_key = f"user:{user_id}|pending|{signal['symbol']}|{signal['action']}|{signal['bar_time']}"
                            if not was_signal_sent(signal_key):
                                ok = await send_signal_to_user(user_id, lang, signal)
                                if ok:
                                    remember_signal(signal_key)
                            break

            logger.info(
                "Scan cycle done. live=%s pending=%s users=%s",
                len(live_results),
                len(pending_results),
                len(users),
            )

        except Exception as exc:
            logger.exception("Background scanner error: %s", exc)

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


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

    if not await is_user_subscribed(context, user.id):
        await query.message.reply_text(
            t(lang, "must_join", channel=CHANNEL_USERNAME),
            reply_markup=subscribe_keyboard(lang),
        )
        return

    prefs = get_user_preferences(user.id)
    current_group = prefs["asset_group"] if prefs else "all"
    current_strength = int(prefs["min_strength"]) if prefs else 70

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

    if query.data == "menu_assets":
        await query.message.reply_text(
            t(lang, "pick_assets"),
            reply_markup=asset_group_keyboard(current_group, lang),
        )
        return

    if query.data == "menu_strength":
        await query.message.reply_text(
            t(lang, "pick_strength"),
            reply_markup=strength_keyboard(current_strength, lang),
        )
        return

    if query.data.startswith("asset_group:"):
        group_value = query.data.split(":", 1)[1]
        if group_value not in {"gb", "wide", "all"}:
            group_value = "all"
        update_user_asset_group(user.id, group_value)
        await query.message.reply_text(
            t(lang, "saved_assets", group=asset_group_name(group_value, lang)),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data.startswith("strength:"):
        try:
            value = int(query.data.split(":", 1)[1])
        except Exception:
            value = 70

        if value not in (60, 70, 80):
            value = 70

        update_user_min_strength(user.id, value)
        await query.message.reply_text(
            t(lang, "strength_saved", level=strength_name(value, lang)),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data == "enable_live":
        update_user_alerts(user.id, "live", True)
        prefs = get_user_preferences(user.id)
        group = prefs["asset_group"] if prefs else "all"
        strength = int(prefs["min_strength"]) if prefs else 70
        await query.message.reply_text(
            t(
                lang,
                "enabled_live",
                group=asset_group_name(group, lang),
                strength=strength_name(strength, lang),
            ),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data == "enable_pending":
        update_user_alerts(user.id, "pending", True)
        prefs = get_user_preferences(user.id)
        group = prefs["asset_group"] if prefs else "all"
        strength = int(prefs["min_strength"]) if prefs else 70
        await query.message.reply_text(
            t(
                lang,
                "enabled_pending",
                group=asset_group_name(group, lang),
                strength=strength_name(strength, lang),
            ),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data == "enable_both":
        update_user_alerts(user.id, "both", True)
        prefs = get_user_preferences(user.id)
        group = prefs["asset_group"] if prefs else "all"
        strength = int(prefs["min_strength"]) if prefs else 70
        await query.message.reply_text(
            t(
                lang,
                "enabled_both",
                group=asset_group_name(group, lang),
                strength=strength_name(strength, lang),
            ),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data == "disable_alerts":
        disable_user_alerts(user.id)
        await query.message.reply_text(
            t(lang, "disabled_alerts"),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data == "menu_settings":
        await query.message.reply_text(
            format_user_settings(user.id, lang),
            reply_markup=main_menu_keyboard(lang),
        )
        return

    if query.data == "back_main":
        await query.message.reply_text(
            t(lang, "menu_title"),
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
    logger.info("Telegram bot started with webhook: %s", telegram_webhook_url)

    scanner_task = asyncio.create_task(background_scanner())

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
    return {"ok": True, "message": "Trading bot is running"}


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data=data, bot=telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT, reload=False)