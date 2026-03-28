
import os
import sqlite3
import json
import logging
import uuid
import asyncio
import warnings
import threading
import re
import time
from collections import defaultdict, deque
from flask import Flask, request as flask_request
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.Application").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
warnings.filterwarnings("ignore", category=UserWarning, module="telegram")

# ─── Configuration ──────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN",   "8601517042:AAHQumWfRybDByIVR6Ju-F2ebfd-t0qysdQ")
ADMIN_ID    = int(os.environ.get("ADMIN_ID", "8373846582"))
PORT        = int(os.environ.get("PORT",    5000))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
DB_PATH     = os.environ.get("DB_PATH",     "premium_store.db")

# ─── Global Telegram Application (webhook mode) ─────────────────────────────
tg_app: "Application" = None
_bg_loop: asyncio.AbstractEventLoop = None

# ─── Rate Limiting ──────────────────────────────────────────────────────────
# 3 messages per second = 10 second ban
RATE_LIMIT_MESSAGES = 3
RATE_LIMIT_WINDOW   = 1    # seconds
RATE_LIMIT_BAN_SEC  = 10   # ban duration in seconds

_rate_tracker: dict[int, deque] = defaultdict(deque)
_rate_banned: dict[int, float]  = {}
_rate_lock = threading.Lock()


def rate_check(uid: int) -> bool:
    """Returns True if user is allowed, False if rate limited/banned."""
    if uid == ADMIN_ID:
        return True
    now = time.monotonic()
    with _rate_lock:
        # Check if currently banned
        if uid in _rate_banned:
            if now < _rate_banned[uid]:
                return False
            else:
                del _rate_banned[uid]
                _rate_tracker[uid].clear()

        dq = _rate_tracker[uid]
        # Remove timestamps older than window
        while dq and now - dq[0] > RATE_LIMIT_WINDOW:
            dq.popleft()

        dq.append(now)

        if len(dq) > RATE_LIMIT_MESSAGES:
            _rate_banned[uid] = now + RATE_LIMIT_BAN_SEC
            _rate_tracker[uid].clear()
            logger.info(f"Rate limit ban: user {uid} for {RATE_LIMIT_BAN_SEC}s")
            return False

    return True


def is_rate_banned(uid: int) -> bool:
    """Check if user is in rate-limit ban."""
    if uid == ADMIN_ID:
        return False
    now = time.monotonic()
    with _rate_lock:
        if uid in _rate_banned:
            if now < _rate_banned[uid]:
                return True
            del _rate_banned[uid]
    return False


# ─── Conversation States ─────────────────────────────────────────────────────
(
    AP_NAME, AP_DESC, AP_PRICE, AP_CAT, AP_FILE, AP_PREVIEW, AP_STOCK,
    EP_VALUE,
    CP_CODE, CP_DISC, CP_LIMIT,
    BS_VALUE,
    PM_NAME, PM_DETAIL,
    PM_EDIT_NAME, PM_EDIT_DETAIL,
    PAY_PROOF,
    BC_MSG, BC_OK,
    AR_MSG,
    SEARCH_Q,
    REFILE_FILE, REPREVIEW_FILE,
    SET_VALUE,
    CAT_NAME, CAT_ICON,
    CAT_EDIT_NAME, CAT_EDIT_ICON,
    FORCE_CHANNEL,
    COUPON_EDIT_CODE, COUPON_EDIT_DISC, COUPON_EDIT_LIMIT,
    ORDER_NOTE,
    USER_MSG,
) = range(34)


# ════════════════════════════════════════════════════════════════════════════
#  DATABASE LAYER
# ════════════════════════════════════════════════════════════════════════════

def ensure_tables(db_path: str = DB_PATH):
    con = sqlite3.connect(db_path)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS users (
            user_id   INTEGER PRIMARY KEY,
            username  TEXT    DEFAULT '',
            full_name TEXT    DEFAULT '',
            balance   REAL    DEFAULT 0,
            banned    INTEGER DEFAULT 0,
            joined_at TEXT    DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS categories (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT UNIQUE NOT NULL,
            icon       TEXT DEFAULT '📦',
            sort_order INTEGER DEFAULT 0,
            active     INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS products (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT    NOT NULL,
            description  TEXT    DEFAULT '',
            price        REAL    NOT NULL DEFAULT 0,
            category     TEXT    DEFAULT 'General',
            file_id      TEXT,
            file_type    TEXT    DEFAULT 'text',
            file_content TEXT    DEFAULT '',
            preview_id   TEXT,
            preview_type TEXT,
            stock        INTEGER DEFAULT -1,
            active       INTEGER DEFAULT 1,
            sales        INTEGER DEFAULT 0,
            created_at   TEXT    DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS cart (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity   INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS coupons (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            code      TEXT    UNIQUE NOT NULL,
            discount  REAL    NOT NULL,
            type      TEXT    DEFAULT 'fixed',
            uses_left INTEGER DEFAULT 1,
            active    INTEGER DEFAULT 1,
            created_at TEXT   DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS orders (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            order_code      TEXT    UNIQUE NOT NULL,
            user_id         INTEGER NOT NULL,
            items_json      TEXT    NOT NULL,
            subtotal        REAL    DEFAULT 0,
            discount        REAL    DEFAULT 0,
            total           REAL    DEFAULT 0,
            coupon_code     TEXT,
            payment_method  TEXT,
            payment_proof   TEXT,
            admin_note      TEXT,
            status          TEXT    DEFAULT 'pending',
            delivered       INTEGER DEFAULT 0,
            created_at      TEXT    DEFAULT (datetime('now')),
            updated_at      TEXT    DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS payment_methods (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT NOT NULL,
            details TEXT NOT NULL,
            active  INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS broadcast_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            message    TEXT,
            sent_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)

    defaults = {
        "shop_name":          "💎 Premium Digital Store",
        "welcome_msg":        "🛍 Premium digital products: scripts, PDFs, software keys, templates & more.\n\nBrowse our collection and get instant delivery!",
        "support_msg":        "📞 Contact admin: @admin\n\nWe respond within 24 hours.",
        "footer_msg":         "✨ Thank you for your purchase! Come back for more.",
        "currency":           "$",
        "auto_deliver":       "0",
        "require_proof":      "1",
        "force_join_channel": "",
        "force_join_label":   "📢 Join Our Channel",
        "maintenance_mode":   "0",
        "min_order_amount":   "0",
        "tax_percent":        "0",
        "order_timeout_hrs":  "48",
    }
    for k, v in defaults.items():
        con.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))

    default_cats = [
        ("Python Scripts", "🐍"),
        ("Web Scripts",    "🌐"),
        ("Bot Scripts",    "🤖"),
        ("PDF & eBooks",   "📚"),
        ("WordPress",      "🎨"),
        ("Software",       "💾"),
        ("License Keys",   "🔑"),
        ("Courses",        "🎓"),
        ("Templates",      "🗂"),
        ("General",        "📦"),
    ]
    for i, (name, icon) in enumerate(default_cats):
        con.execute(
            "INSERT OR IGNORE INTO categories(name, icon, sort_order) VALUES(?,?,?)",
            (name, icon, i)
        )

    if not con.execute("SELECT COUNT(*) FROM payment_methods").fetchone()[0]:
        con.executemany("INSERT INTO payment_methods(name,details) VALUES(?,?)", [
            ("💳 PayPal",  "Send to: shop@example.com\nPlease note your Order ID in memo."),
            ("₿ Crypto",   "BTC: bc1qxxxxxxxxxxxxxxxxxxxxxx\nETH: 0xxxxxxxxxxxxxxxxxxx\nSend exact amount."),
            ("🏦 Bank",    "Bank: Example Bank\nAcc: 1234567890\nRef: Your Order ID"),
        ])

    if not con.execute("SELECT COUNT(*) FROM products").fetchone()[0]:
        con.executemany(
            "INSERT INTO products(name,description,price,category,file_type,file_content,stock) VALUES(?,?,?,?,?,?,?)",
            [
                ("Python Web Scraper",
                 "✅ Full scraper with BeautifulSoup & Selenium\n✅ Proxy support\n✅ CSV/JSON export\n✅ Anti-bot bypass",
                 29.99, "Python Scripts", "text",
                 "🔗 Download: https://example.com/scraper\n🔑 License: XXXX-XXXX-XXXX", -1),
                ("Telegram Bot Template",
                 "✅ Python bot starter pack\n✅ SQLite database\n✅ Inline keyboards\n✅ Payment integration ready",
                 49.99, "Bot Scripts", "text",
                 "🔗 Repo: https://github.com/example/tgbot\n🔑 License: YYYY-YYYY-YYYY", -1),
                ("Freelancing Guide PDF",
                 "✅ 200+ pages on Upwork & Fiverr\n✅ Client handling\n✅ Portfolio tips\n✅ Proposal templates",
                 14.99, "PDF & eBooks", "text",
                 "📥 PDF: https://example.com/guide.pdf", -1),
            ]
        )

    con.commit()
    con.close()
    logger.info(f"✅ Database ready: {db_path}")


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=10000")
    return con


# ─── Config helpers ──────────────────────────────────────────────────────────
def cfg(key: str, default: str = "") -> str:
    try:
        con = db()
        row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        con.close()
        return row["value"] if row else default
    except Exception:
        return default

def set_cfg(key: str, val: str):
    try:
        con = db()
        con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, val))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"set_cfg error: {e}")

def all_cfg() -> dict:
    try:
        con = db()
        rows = con.execute("SELECT key,value FROM settings").fetchall()
        con.close()
        return {r["key"]: r["value"] for r in rows}
    except Exception:
        return {}


# ─── User helpers ──────────────────────────────────────────────────────────
def reg_user(uid: int, uname: str, fname: str):
    try:
        con = db()
        con.execute(
            "INSERT OR IGNORE INTO users(user_id,username,full_name) VALUES(?,?,?)",
            (uid, uname or "", fname or "")
        )
        con.execute(
            "UPDATE users SET username=?, full_name=? WHERE user_id=?",
            (uname or "", fname or "", uid)
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"reg_user error: {e}")

def is_banned(uid: int) -> bool:
    try:
        con = db()
        row = con.execute("SELECT banned FROM users WHERE user_id=?", (uid,)).fetchone()
        con.close()
        return bool(row and row["banned"])
    except Exception:
        return False

def get_user(uid: int):
    try:
        con = db()
        row = con.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        con.close()
        return row
    except Exception:
        return None

def all_users():
    try:
        con = db()
        rows = con.execute("SELECT * FROM users ORDER BY joined_at DESC").fetchall()
        con.close()
        return rows
    except Exception:
        return []

def search_users(q: str):
    try:
        con = db()
        rows = con.execute(
            "SELECT * FROM users WHERE LOWER(username) LIKE ? OR LOWER(full_name) LIKE ? OR CAST(user_id AS TEXT) LIKE ?",
            (f"%{q}%", f"%{q}%", f"%{q}%")
        ).fetchall()
        con.close()
        return rows
    except Exception:
        return []


# ─── Category helpers ──────────────────────────────────────────────────────
def get_active_categories():
    try:
        con = db()
        rows = con.execute(
            "SELECT * FROM categories WHERE active=1 ORDER BY sort_order, name"
        ).fetchall()
        con.close()
        return rows
    except Exception:
        return []

def get_all_categories():
    try:
        con = db()
        rows = con.execute("SELECT * FROM categories ORDER BY sort_order, name").fetchall()
        con.close()
        return rows
    except Exception:
        return []

def get_category_by_id(cid: int):
    try:
        con = db()
        row = con.execute("SELECT * FROM categories WHERE id=?", (cid,)).fetchone()
        con.close()
        return row
    except Exception:
        return None

def add_category(name: str, icon: str = "📦"):
    try:
        con = db()
        con.execute("INSERT INTO categories(name, icon) VALUES(?,?)", (name, icon))
        con.commit()
        con.close()
        return True
    except sqlite3.IntegrityError:
        con.close()
        return False
    except Exception as e:
        logger.error(f"add_category error: {e}")
        return False

def update_category(cid: int, name: str = None, icon: str = None, active: int = None):
    try:
        con = db()
        if name is not None:
            con.execute("UPDATE categories SET name=? WHERE id=?", (name, cid))
        if icon is not None:
            con.execute("UPDATE categories SET icon=? WHERE id=?", (icon, cid))
        if active is not None:
            con.execute("UPDATE categories SET active=? WHERE id=?", (active, cid))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"update_category error: {e}")

def delete_category(cid: int):
    try:
        con = db()
        con.execute("DELETE FROM categories WHERE id=?", (cid,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"delete_category error: {e}")


# ─── Product helpers ──────────────────────────────────────────────────────
def get_products(cat: str = None):
    try:
        con = db()
        if cat and cat != "all":
            rows = con.execute(
                "SELECT * FROM products WHERE category=? AND active=1 ORDER BY id", (cat,)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM products WHERE active=1 ORDER BY id"
            ).fetchall()
        con.close()
        return rows
    except Exception:
        return []

def get_product(pid: int):
    try:
        con = db()
        row = con.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
        con.close()
        return row
    except Exception:
        return None

def get_cats():
    try:
        con = db()
        rows = con.execute(
            "SELECT DISTINCT category FROM products WHERE active=1 ORDER BY category"
        ).fetchall()
        con.close()
        return [r["category"] for r in rows]
    except Exception:
        return []

def search_products(q: str):
    try:
        con = db()
        rows = con.execute(
            """SELECT * FROM products WHERE active=1
               AND (LOWER(name) LIKE ? OR LOWER(category) LIKE ? OR LOWER(description) LIKE ?)
               ORDER BY name""",
            (f"%{q}%", f"%{q}%", f"%{q}%")
        ).fetchall()
        con.close()
        return rows
    except Exception:
        return []

def toggle_product(pid: int):
    try:
        con = db()
        con.execute("UPDATE products SET active = 1 - active WHERE id=?", (pid,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"toggle_product error: {e}")

def delete_product(pid: int):
    try:
        con = db()
        con.execute("DELETE FROM products WHERE id=?", (pid,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"delete_product error: {e}")

def update_product_field(pid: int, field: str, value):
    allowed = {"name", "description", "price", "category", "stock",
               "file_id", "file_type", "file_content", "preview_id", "preview_type"}
    if field not in allowed:
        return
    try:
        con = db()
        con.execute(f"UPDATE products SET {field}=? WHERE id=?", (value, pid))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"update_product_field error: {e}")


# ─── Cart helpers ──────────────────────────────────────────────────────────
def get_cart(uid: int):
    try:
        con = db()
        rows = con.execute(
            """SELECT c.id, c.quantity,
                      p.id as product_id, p.name, p.price,
                      p.stock, p.file_id, p.file_type, p.file_content
               FROM cart c
               JOIN products p ON c.product_id = p.id
               WHERE c.user_id = ? AND p.active = 1""",
            (uid,)
        ).fetchall()
        con.close()
        return rows
    except Exception:
        return []

def cart_add(uid: int, pid: int):
    try:
        con = db()
        ex = con.execute(
            "SELECT id, quantity FROM cart WHERE user_id=? AND product_id=?", (uid, pid)
        ).fetchone()
        if ex:
            con.execute("UPDATE cart SET quantity=? WHERE id=?", (ex["quantity"] + 1, ex["id"]))
        else:
            con.execute("INSERT INTO cart(user_id,product_id) VALUES(?,?)", (uid, pid))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"cart_add error: {e}")

def cart_remove(cid: int):
    try:
        con = db()
        con.execute("DELETE FROM cart WHERE id=?", (cid,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"cart_remove error: {e}")

def cart_clear(uid: int):
    try:
        con = db()
        con.execute("DELETE FROM cart WHERE user_id=?", (uid,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"cart_clear error: {e}")

def cart_total(items) -> float:
    return sum(i["price"] * i["quantity"] for i in items)


# ─── Coupon helpers ──────────────────────────────────────────────────────────
def coupon_get(code: str):
    try:
        con = db()
        row = con.execute(
            "SELECT * FROM coupons WHERE code=? AND active=1 AND uses_left>0", (code,)
        ).fetchone()
        con.close()
        return row
    except Exception:
        return None

def coupon_use(code: str):
    try:
        con = db()
        con.execute("UPDATE coupons SET uses_left = uses_left - 1 WHERE code=?", (code,))
        con.execute("UPDATE coupons SET active=0 WHERE code=? AND uses_left <= 0", (code,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"coupon_use error: {e}")

def all_coupons():
    try:
        con = db()
        rows = con.execute("SELECT * FROM coupons ORDER BY id DESC").fetchall()
        con.close()
        return rows
    except Exception:
        return []

def delete_coupon(cid: int):
    try:
        con = db()
        con.execute("DELETE FROM coupons WHERE id=?", (cid,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"delete_coupon error: {e}")

def toggle_coupon(cid: int):
    try:
        con = db()
        con.execute("UPDATE coupons SET active = 1 - active WHERE id=?", (cid,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"toggle_coupon error: {e}")

def apply_coupon_discount(subtotal: float, coupon) -> float:
    if not coupon:
        return 0.0
    if coupon["type"] == "percent":
        return round(subtotal * coupon["discount"] / 100, 2)
    return min(coupon["discount"], subtotal)


# ─── Order helpers ──────────────────────────────────────────────────────────
def order_create(uid, items, sub, disc, total, coupon, method) -> str:
    con = db()
    try:
        for it in items:
            pid = it["product_id"]
            qty = it["quantity"]
            p   = con.execute("SELECT stock FROM products WHERE id=?", (pid,)).fetchone()
            if p is None:
                raise ValueError(f"Product '{it['name']}' not found")
            if p["stock"] != -1 and p["stock"] < qty:
                raise ValueError(f"Insufficient stock for '{it['name']}'")
            if p["stock"] != -1:
                con.execute("UPDATE products SET stock = stock - ? WHERE id=?", (qty, pid))
        code = "ORD-" + uuid.uuid4().hex[:8].upper()
        js   = json.dumps([{
            "name":         it["name"],
            "price":        it["price"],
            "qty":          it["quantity"],
            "file_id":      it["file_id"],
            "file_type":    it["file_type"],
            "file_content": it["file_content"],
        } for it in items], ensure_ascii=False)
        con.execute(
            """INSERT INTO orders(order_code,user_id,items_json,subtotal,discount,total,
               coupon_code,payment_method) VALUES(?,?,?,?,?,?,?,?)""",
            (code, uid, js, sub, disc, total, coupon, method)
        )
        con.commit()
        return code
    finally:
        con.close()

def order_get(code: str):
    try:
        con = db()
        row = con.execute("SELECT * FROM orders WHERE order_code=?", (code,)).fetchone()
        con.close()
        return row
    except Exception:
        return None

def order_update(code: str, **kw):
    if not kw:
        return
    try:
        con = db()
        for k, v in kw.items():
            con.execute(f"UPDATE orders SET {k}=? WHERE order_code=?", (v, code))
        con.execute("UPDATE orders SET updated_at=datetime('now') WHERE order_code=?", (code,))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"order_update error: {e}")

def orders_get(status: str = None, limit: int = 20, offset: int = 0):
    try:
        con = db()
        if status:
            rows = con.execute(
                "SELECT * FROM orders WHERE status=? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (status, limit, offset)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM orders ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
        con.close()
        return rows
    except Exception:
        return []

def user_orders(uid: int):
    try:
        con = db()
        rows = con.execute(
            "SELECT * FROM orders WHERE user_id=? ORDER BY created_at DESC",
            (uid,)
        ).fetchall()
        con.close()
        return rows
    except Exception:
        return []

def count_orders(status: str = None) -> int:
    try:
        con = db()
        if status:
            n = con.execute("SELECT COUNT(*) FROM orders WHERE status=?", (status,)).fetchone()[0]
        else:
            n = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        con.close()
        return n
    except Exception:
        return 0


# ─── Payment helpers ──────────────────────────────────────────────────────────
def pay_methods(active_only: bool = True):
    try:
        con = db()
        if active_only:
            rows = con.execute("SELECT * FROM payment_methods WHERE active=1").fetchall()
        else:
            rows = con.execute("SELECT * FROM payment_methods").fetchall()
        con.close()
        return rows
    except Exception:
        return []


# ─── Stats ────────────────────────────────────────────────────────────────────
def get_stats() -> dict:
    try:
        con = db()
        s = {
            "users":     con.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "banned":    con.execute("SELECT COUNT(*) FROM users WHERE banned=1").fetchone()[0],
            "products":  con.execute("SELECT COUNT(*) FROM products WHERE active=1").fetchone()[0],
            "cats":      con.execute("SELECT COUNT(*) FROM categories WHERE active=1").fetchone()[0],
            "orders":    con.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
            "pending":   con.execute("SELECT COUNT(*) FROM orders WHERE status='pending'").fetchone()[0],
            "paid":      con.execute("SELECT COUNT(*) FROM orders WHERE status='paid'").fetchone()[0],
            "approved":  con.execute("SELECT COUNT(*) FROM orders WHERE status='approved'").fetchone()[0],
            "delivered": con.execute("SELECT COUNT(*) FROM orders WHERE status='delivered'").fetchone()[0],
            "cancelled": con.execute("SELECT COUNT(*) FROM orders WHERE status='cancelled'").fetchone()[0],
            "revenue":   con.execute(
                "SELECT COALESCE(SUM(total),0) FROM orders WHERE status IN('paid','delivered','approved')"
            ).fetchone()[0],
            "today_orders": con.execute(
                "SELECT COUNT(*) FROM orders WHERE date(created_at)=date('now')"
            ).fetchone()[0],
            "today_revenue": con.execute(
                "SELECT COALESCE(SUM(total),0) FROM orders WHERE date(created_at)=date('now') AND status IN('paid','delivered','approved')"
            ).fetchone()[0],
            "coupons":   con.execute("SELECT COUNT(*) FROM coupons WHERE active=1").fetchone()[0],
            "top_product": con.execute(
                "SELECT name, sales FROM products ORDER BY sales DESC LIMIT 1"
            ).fetchone(),
        }
        con.close()
        return s
    except Exception as e:
        logger.error(f"get_stats error: {e}")
        return {}


# ════════════════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def currency() -> str:
    return cfg("currency", "$")

def cur_symbol() -> str:
    return cfg("currency", "$")

def status_lbl(s: str) -> str:
    return {
        "pending":   "⏳ Pending",
        "paid":      "💰 Paid — Awaiting Approval",
        "approved":  "✅ Approved",
        "delivered": "📦 Delivered",
        "cancelled": "❌ Cancelled",
        "refunded":  "↩️ Refunded",
    }.get(s, s)

def status_emoji(s: str) -> str:
    return {
        "pending":   "⏳",
        "paid":      "💰",
        "approved":  "✅",
        "delivered": "📦",
        "cancelled": "❌",
        "refunded":  "↩️",
    }.get(s, "❓")


# ─── Force Join Channel ──────────────────────────────────────────────────────
async def check_membership(uid: int, bot) -> bool:
    """
    Returns True  → user is a member (or check is not possible / channel not configured)
    Returns False → user has NOT joined the channel
    """
    channel = cfg("force_join_channel", "").strip()
    if not channel:
        return True
    if bot is None:
        logger.error("check_membership: bot is None — cannot verify membership")
        return True   # Can't check, let user through rather than block forever

    # Normalise channel: ensure it starts with @ for usernames, or parse as int for IDs
    try:
        if channel.startswith("-") or channel.lstrip("-").isdigit():
            chat_id = int(channel)
        else:
            chat_id = channel if channel.startswith("@") else f"@{channel}"
    except ValueError:
        logger.error(f"Force join: invalid channel value '{channel}'")
        return True

    try:
        member = await bot.get_chat_member(chat_id, uid)
        # "left" and "kicked/banned" mean NOT a member
        return member.status not in ("left", "kicked")
    except Forbidden:
        logger.error(
            f"Force join: Bot is NOT an admin of '{channel}'. "
            "Add the bot as admin (or at least give 'Add Members' right) to enable membership check."
        )
        return True   # Bot can't check — allow user through so they're not permanently blocked
    except BadRequest as e:
        err = str(e).lower()
        if "user not found" in err or "participant not found" in err:
            # User definitely not in the channel
            return False
        # Channel not found / other config issue — don't punish the user
        logger.error(f"Force join BadRequest for '{channel}': {e}")
        return True
    except Exception as e:
        logger.warning(f"Membership check error for '{channel}': {e}")
        return True


async def guard(update: Update) -> bool:
    """Rate limit check + ban check + force-join check."""
    if not update.effective_user:
        return False
    uid = update.effective_user.id

    # Rate limit check — silent ignore, no response sent
    if not rate_check(uid):
        return False

    if is_banned(uid):
        try:
            await update.effective_message.reply_text(
                "⛔ *You have been banned from this store.*\n\nContact support if you think this is a mistake.",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
        return False

    if cfg("maintenance_mode", "0") == "1" and not is_admin(uid):
        try:
            await update.effective_message.reply_text(
                "🔧 *Maintenance Mode*\n\nThe store is temporarily down for maintenance.\nPlease try again later.",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
        return False

    channel = cfg("force_join_channel", "").strip()
    if channel and not is_admin(uid):
        bot = update.get_bot()
        if not await check_membership(uid, bot):
            label = cfg("force_join_label", "📢 Join Our Channel")
            kb    = InlineKeyboardMarkup([
                [InlineKeyboardButton(label, url=f"https://t.me/{channel.lstrip('@')}")],
                [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
            ])
            try:
                await update.effective_message.reply_text(
                    f"🔒 *Access Required!*\n\n"
                    f"Please join our channel to use this bot:\n"
                    f"`{channel}`\n\n"
                    f"1️⃣ Click the button below to join\n"
                    f"2️⃣ Then click *I've Joined*",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb
                )
            except Exception:
                pass
            return False
    return True


async def guard_query(query, uid: int, bot=None) -> bool:
    """Guard for callback queries. Pass context.bot as the bot argument."""
    # Rate limit — silent ignore
    if not rate_check(uid):
        try:
            await query.answer()
        except Exception:
            pass
        return False

    if is_banned(uid):
        try:
            await query.answer("⛔ You are banned from this store.", show_alert=True)
        except Exception:
            pass
        return False

    if cfg("maintenance_mode", "0") == "1" and not is_admin(uid):
        try:
            await query.answer("🔧 Store is under maintenance.", show_alert=True)
        except Exception:
            pass
        return False

    channel = cfg("force_join_channel", "").strip()
    if channel and not is_admin(uid):
        # Prefer explicitly passed bot; fallback to query's associated bot
        effective_bot = bot or getattr(query, "_bot", None) or getattr(query, "bot", None)
        if not await check_membership(uid, effective_bot):
            label = cfg("force_join_label", "📢 Join Our Channel")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(label, url=f"https://t.me/{channel.lstrip('@')}")],
                [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
            ])
            try:
                await query.answer("🔒 Please join our channel first!", show_alert=True)
                await query.message.reply_text(
                    f"🔒 *Access Required!*\n\n"
                    f"Please join our channel to use this bot:\n"
                    f"`{channel}`\n\n"
                    f"1️⃣ Click the button below to join\n"
                    f"2️⃣ Then click *I've Joined*",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb
                )
            except Exception:
                pass
            return False
    return True


# ════════════════════════════════════════════════════════════════════════════
#  KEYBOARDS
# ════════════════════════════════════════════════════════════════════════════

def main_kb(uid: int) -> ReplyKeyboardMarkup:
    rows = [
        ["🛍 Browse Products", "🔍 Search"],
        ["🛒 My Cart",         "📦 My Orders"],
        ["🎟 Coupon Code",     "📞 Support"],
        ["❓ Help"],
    ]
    if is_admin(uid):
        rows.append(["⚙️ Admin Panel"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def cat_kb(cats) -> InlineKeyboardMarkup:
    try:
        con    = db()
        all_c  = {r["name"]: r["icon"] for r in con.execute("SELECT name, icon FROM categories").fetchall()}
        con.close()
    except Exception:
        all_c  = {}

    btns = []
    for c in cats:
        icon = all_c.get(c, "📦")
        btns.append([InlineKeyboardButton(f"{icon} {c}", callback_data=f"cat_{c}")])
    btns.append([InlineKeyboardButton("📦 All Products", callback_data="cat_all")])
    return InlineKeyboardMarkup(btns)


def prod_list_kb(products, cur: str) -> InlineKeyboardMarkup:
    btns = []
    for p in products:
        stock_txt = f" [{p['stock']} left]" if p["stock"] != -1 and p["stock"] <= 10 else ""
        btns.append([InlineKeyboardButton(
            f"{p['name']} — {cur}{p['price']:.2f}{stock_txt}",
            callback_data=f"prod_{p['id']}"
        )])
    btns.append([InlineKeyboardButton("« Back to Categories", callback_data="back_cat")])
    return InlineKeyboardMarkup(btns)


def prod_kb(pid: int, in_cart: bool = False) -> InlineKeyboardMarkup:
    lbl = "✅ In Cart — Add More" if in_cart else "🛒 Add to Cart"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(lbl, callback_data=f"addcart_{pid}")],
        [InlineKeyboardButton("🖼 Preview", callback_data=f"preview_{pid}"),
         InlineKeyboardButton("« Back", callback_data="back_cat")],
    ])


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Products",   callback_data="adm_products"),
         InlineKeyboardButton("📋 Orders",     callback_data="adm_orders")],
        [InlineKeyboardButton("💳 Payments",   callback_data="adm_payments"),
         InlineKeyboardButton("🎟 Coupons",    callback_data="adm_coupons")],
        [InlineKeyboardButton("📊 Statistics", callback_data="adm_stats"),
         InlineKeyboardButton("👥 Users",      callback_data="adm_users")],
        [InlineKeyboardButton("🗂 Categories", callback_data="adm_categories"),
         InlineKeyboardButton("⚙️ Settings",   callback_data="adm_settings")],
        [InlineKeyboardButton("📢 Broadcast",  callback_data="adm_broadcast"),
         InlineKeyboardButton("🔗 Force Join", callback_data="adm_force_channel")],
    ])


def products_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add New Product", callback_data="adm_addproduct")],
        [InlineKeyboardButton("📋 List Products",   callback_data="adm_listproducts_0")],
        [InlineKeyboardButton("🔍 Search Product",  callback_data="adm_searchprod")],
        [InlineKeyboardButton("« Back",             callback_data="adm_back")],
    ])


def admin_prod_kb(pid: int, active: int = 1) -> InlineKeyboardMarkup:
    toggle_lbl = "🔴 Deactivate" if active else "🟢 Activate"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Fields",    callback_data=f"adm_editprod_{pid}"),
         InlineKeyboardButton("📤 Change File",    callback_data=f"adm_refile_{pid}")],
        [InlineKeyboardButton("🖼 Change Preview", callback_data=f"adm_repreview_{pid}"),
         InlineKeyboardButton(toggle_lbl,          callback_data=f"adm_toggleprod_{pid}")],
        [InlineKeyboardButton("🗑 Delete",         callback_data=f"adm_delprod_{pid}")],
        [InlineKeyboardButton("« Back",            callback_data="adm_listproducts_0")],
    ])


def edit_fields_kb(pid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📛 Name",        callback_data=f"adm_ef_name_{pid}"),
         InlineKeyboardButton("📝 Description", callback_data=f"adm_ef_desc_{pid}")],
        [InlineKeyboardButton("💰 Price",       callback_data=f"adm_ef_price_{pid}"),
         InlineKeyboardButton("📂 Category",    callback_data=f"adm_ef_cat_{pid}")],
        [InlineKeyboardButton("📦 Stock",       callback_data=f"adm_ef_stock_{pid}")],
        [InlineKeyboardButton("« Back",         callback_data=f"adm_viewprod_{pid}")],
    ])


def order_admin_kb(code: str, status: str = "pending") -> InlineKeyboardMarkup:
    rows = []
    if status == "pending":
        rows.append([
            InlineKeyboardButton("💰 Mark Paid", callback_data=f"ord_paid_{code}"),
            InlineKeyboardButton("❌ Cancel",    callback_data=f"ord_cancel_{code}"),
        ])
    elif status == "paid":
        rows.append([
            InlineKeyboardButton("✅ Approve & Deliver", callback_data=f"ord_approve_{code}"),
            InlineKeyboardButton("❌ Cancel",             callback_data=f"ord_cancel_{code}"),
        ])
    elif status == "approved":
        rows.append([
            InlineKeyboardButton("📦 Deliver Now", callback_data=f"ord_deliver_{code}"),
            InlineKeyboardButton("↩️ Refund",       callback_data=f"ord_refund_{code}"),
        ])
    elif status == "delivered":
        rows.append([
            InlineKeyboardButton("↩️ Refund", callback_data=f"ord_refund_{code}"),
        ])
    rows.append([
        InlineKeyboardButton("✏️ Add Note",     callback_data=f"ord_note_{code}"),
        InlineKeyboardButton("💬 Message User", callback_data=f"ord_msg_{code}"),
    ])
    return InlineKeyboardMarkup(rows)


def orders_list_kb(page: int = 0, status: str = "all") -> InlineKeyboardMarkup:
    statuses = ["all", "pending", "paid", "approved", "delivered", "cancelled"]
    rows     = []
    nav      = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"adm_orders_page_{page-1}_{status}"))
    nav.append(InlineKeyboardButton(f"📋 {status.upper()}", callback_data="noop"))
    nav.append(InlineKeyboardButton("Next ▶", callback_data=f"adm_orders_page_{page+1}_{status}"))
    rows.append(nav)
    filter_row = []
    for s in statuses:
        emoji = "✅" if s == status else ""
        filter_row.append(InlineKeyboardButton(
            f"{emoji}{s[:4]}", callback_data=f"adm_orders_filter_{s}"
        ))
    rows.append(filter_row)
    rows.append([InlineKeyboardButton("« Back", callback_data="adm_back")])
    return InlineKeyboardMarkup(rows)


def pm_kb(methods, order_code: str) -> InlineKeyboardMarkup:
    btns = [
        [InlineKeyboardButton(f"{m['name']}", callback_data=f"pay_{m['id']}_{order_code}")]
        for m in methods
    ]
    btns.append([InlineKeyboardButton("❌ Cancel Order", callback_data="pay_cancel")])
    return InlineKeyboardMarkup(btns)


def categories_panel_kb() -> InlineKeyboardMarkup:
    cats = get_all_categories()
    btns = []
    for c in cats:
        status = "🟢" if c["active"] else "🔴"
        btns.append([
            InlineKeyboardButton(f"{status} {c['icon']} {c['name']}", callback_data=f"adm_cat_view_{c['id']}"),
        ])
    btns.append([InlineKeyboardButton("➕ Add Category", callback_data="adm_cat_add")])
    btns.append([InlineKeyboardButton("« Back",          callback_data="adm_back")])
    return InlineKeyboardMarkup(btns)


def category_action_kb(cid: int, active: int) -> InlineKeyboardMarkup:
    toggle_lbl = "🔴 Deactivate" if active else "🟢 Activate"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Name", callback_data=f"adm_cat_editname_{cid}"),
         InlineKeyboardButton("🎨 Edit Icon", callback_data=f"adm_cat_editicon_{cid}")],
        [InlineKeyboardButton(toggle_lbl,     callback_data=f"adm_cat_toggle_{cid}"),
         InlineKeyboardButton("🗑 Delete",    callback_data=f"adm_cat_delete_{cid}")],
        [InlineKeyboardButton("« Back",       callback_data="adm_categories")],
    ])


def settings_kb() -> InlineKeyboardMarkup:
    s  = all_cfg()
    ad = "✅ ON" if s.get("auto_deliver") == "1" else "❌ OFF"
    rp = "✅ ON" if s.get("require_proof") == "1" else "❌ OFF"
    mm = "🔧 ON" if s.get("maintenance_mode") == "1" else "✅ OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏪 Shop Name",       callback_data="adm_set_shop_name")],
        [InlineKeyboardButton("👋 Welcome Message", callback_data="adm_set_welcome_msg")],
        [InlineKeyboardButton("📞 Support Message", callback_data="adm_set_support_msg")],
        [InlineKeyboardButton("💬 Footer Message",  callback_data="adm_set_footer_msg")],
        [InlineKeyboardButton("💱 Currency Symbol", callback_data="adm_set_currency")],
        [InlineKeyboardButton(f"🚀 Auto-Deliver: {ad}",   callback_data="adm_tog_auto_deliver")],
        [InlineKeyboardButton(f"📸 Require Proof: {rp}",  callback_data="adm_tog_require_proof")],
        [InlineKeyboardButton(f"🔧 Maintenance: {mm}",    callback_data="adm_tog_maintenance")],
        [InlineKeyboardButton("⏱ Order Timeout (hrs)",    callback_data="adm_set_order_timeout")],
        [InlineKeyboardButton("📊 Tax (%)",               callback_data="adm_set_tax")],
        [InlineKeyboardButton("« Back",                   callback_data="adm_back")],
    ])


def force_channel_kb() -> InlineKeyboardMarkup:
    cur    = cfg("force_join_channel", "").strip()
    status_txt = f"✅ Active: {cur}" if cur else "❌ Disabled"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(status_txt,            callback_data="noop")],
        [InlineKeyboardButton("✏️ Set Channel",      callback_data="adm_set_force_channel")],
        [InlineKeyboardButton("🏷 Set Button Label", callback_data="adm_set_force_label")],
        [InlineKeyboardButton("🚫 Disable",          callback_data="adm_disable_force_channel")],
        [InlineKeyboardButton("« Back",              callback_data="adm_back")],
    ])


def payment_methods_kb() -> InlineKeyboardMarkup:
    methods = pay_methods(active_only=False)
    btns    = []
    for m in methods:
        status = "🟢" if m["active"] else "🔴"
        btns.append([
            InlineKeyboardButton(f"{status} {m['name']}", callback_data=f"adm_edit_pm_{m['id']}"),
            InlineKeyboardButton("🗑",                     callback_data=f"adm_del_pm_{m['id']}"),
            InlineKeyboardButton("🔄",                     callback_data=f"adm_toggle_pm_{m['id']}"),
        ])
    btns.append([InlineKeyboardButton("➕ Add Method", callback_data="adm_addpm")])
    btns.append([InlineKeyboardButton("« Back",        callback_data="adm_back")])
    return InlineKeyboardMarkup(btns)


def users_panel_kb(page: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 All Users",    callback_data=f"adm_users_list_{page}"),
         InlineKeyboardButton("🚫 Banned Users", callback_data="adm_users_banned")],
        [InlineKeyboardButton("🔍 Find User",    callback_data="adm_users_search")],
        [InlineKeyboardButton("📢 Message User", callback_data="adm_msg_user")],
        [InlineKeyboardButton("« Back",          callback_data="adm_back")],
    ])


def coupons_panel_kb() -> InlineKeyboardMarkup:
    coupons = all_coupons()
    btns    = []
    cur     = currency()
    for c in coupons:
        status = "🟢" if c["active"] else "🔴"
        disc   = f"{c['discount']}%" if c["type"] == "percent" else f"{cur}{c['discount']}"
        btns.append([
            InlineKeyboardButton(
                f"{status} {c['code']} — {disc} ({c['uses_left']} left)",
                callback_data=f"adm_coupon_view_{c['id']}"
            ),
        ])
    btns.append([InlineKeyboardButton("➕ Add Coupon", callback_data="adm_addcoupon")])
    btns.append([InlineKeyboardButton("« Back",        callback_data="adm_back")])
    return InlineKeyboardMarkup(btns)


def coupon_action_kb(cid: int, active: int) -> InlineKeyboardMarkup:
    toggle_lbl = "🔴 Deactivate" if active else "🟢 Activate"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_lbl, callback_data=f"adm_coupon_toggle_{cid}"),
         InlineKeyboardButton("🗑 Delete", callback_data=f"adm_coupon_del_{cid}")],
        [InlineKeyboardButton("« Back",   callback_data="adm_coupons")],
    ])


# ════════════════════════════════════════════════════════════════════════════
#  DISPLAY HELPERS
# ════════════════════════════════════════════════════════════════════════════

async def send_admin_panel(target, edit: bool = False):
    s     = get_stats()
    sn    = cfg("shop_name")
    cur   = currency()
    maint = " 🔧 MAINTENANCE" if cfg("maintenance_mode", "0") == "1" else ""
    top   = s.get("top_product")
    top_txt = f"{top['name']} ({top['sales']} sales)" if top else "—"
    text  = (
        f"*{sn} — Admin Panel{maint}*\n"
        f"{'━' * 30}\n\n"
        f"👥 Users: *{s.get('users', 0)}*  🚫 Banned: *{s.get('banned', 0)}*\n"
        f"📦 Products: *{s.get('products', 0)}*  🗂 Categories: *{s.get('cats', 0)}*\n"
        f"🎟 Coupons: *{s.get('coupons', 0)}*\n\n"
        f"*📋 Orders:* {s.get('orders', 0)}\n"
        f"⏳ Pending: {s.get('pending', 0)}  💰 Paid: {s.get('paid', 0)}\n"
        f"✅ Approved: {s.get('approved', 0)}  📦 Delivered: {s.get('delivered', 0)}\n\n"
        f"*📅 Today:* {s.get('today_orders', 0)} orders\n"
        f"*💵 Total Revenue:* {cur}{s.get('revenue', 0):.2f}\n\n"
        f"🏆 *Top Product:* {top_txt}"
    )
    try:
        if edit:
            await target.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_kb())
        else:
            await target.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_kb())
    except BadRequest:
        await target.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_kb())


async def show_cart(target, uid: int, context: ContextTypes.DEFAULT_TYPE):
    items = get_cart(uid)
    cur   = currency()
    if not items:
        await target.reply_text(
            "*🛒 Your cart is empty.*\n\nBrowse products with /products",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(uid)
        )
        return
    sub  = cart_total(items)
    disc = 0.0
    coupon_data = context.user_data.get("coupon")
    if coupon_data:
        disc = coupon_data["discount"]
    total = max(0, sub - disc)
    text  = f"*🛒 Your Cart*\n{'━' * 26}\n\n"
    btns  = []
    for it in items:
        text += f"• {it['name']} ×{it['quantity']} — {cur}{it['price'] * it['quantity']:.2f}\n"
        btns.append([InlineKeyboardButton(
            f"🗑 Remove: {it['name'][:25]}",
            callback_data=f"rmcart_{it['id']}"
        )])
    text += f"\n{'─' * 26}\n"
    text += f"*Subtotal:* {cur}{sub:.2f}\n"
    if disc:
        text += f"*Discount:* −{cur}{disc:.2f}\n"
    text += f"*Total:* {cur}{total:.2f}"
    if coupon_data:
        text += f"\n🎟 Coupon: `{coupon_data['code']}`"
    btns.append([InlineKeyboardButton("💳 Checkout", callback_data="checkout")])
    btns.append([InlineKeyboardButton("🗑 Clear Cart", callback_data="clear_cart")])
    await target.reply_text(
        text, parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(btns)
    )


# ════════════════════════════════════════════════════════════════════════════
#  DELIVERY & NOTIFICATIONS
# ════════════════════════════════════════════════════════════════════════════

async def deliver_order(context, code: str):
    order = order_get(code)
    if not order or order["delivered"]:
        return False
    items  = json.loads(order["items_json"])
    uid    = order["user_id"]
    cur    = currency()
    footer = cfg("footer_msg", "")

    try:
        await context.bot.send_message(
            uid,
            f"*🎉 Order Delivered!*\n{'━' * 28}\n\n"
            f"Order: `{code}`\n"
            f"Total: *{cur}{order['total']:.2f}*\n\n"
            f"Your product(s) are ready! 👇",
            parse_mode=ParseMode.MARKDOWN
        )
        for it in items:
            caption = f"*{it['name']}*\n\n_{footer}_"
            fid     = it.get("file_id")
            ftyp    = it.get("file_type", "text")
            if fid:
                send_map = {
                    "document": context.bot.send_document,
                    "photo":    context.bot.send_photo,
                    "video":    context.bot.send_video,
                    "audio":    context.bot.send_audio,
                }
                fn = send_map.get(ftyp, context.bot.send_document)
                await fn(uid, fid, caption=caption, parse_mode=ParseMode.MARKDOWN)
            else:
                content = it.get("file_content") or "Content not available."
                await context.bot.send_message(
                    uid,
                    f"*{it['name']}*\n{'─' * 24}\n\n{content}\n\n_{footer}_",
                    parse_mode=ParseMode.MARKDOWN
                )
        order_update(code, status="delivered", delivered=1)
        try:
            con = db()
            for it in items:
                p = con.execute("SELECT id FROM products WHERE name=?", (it["name"],)).fetchone()
                if p:
                    con.execute("UPDATE products SET sales = sales + ? WHERE id=?", (it.get("qty", 1), p["id"]))
            con.commit()
            con.close()
        except Exception as e:
            logger.error(f"Sales update error: {e}")
        logger.info(f"✅ Order {code} delivered to {uid}")
        return True
    except Forbidden:
        logger.warning(f"User {uid} blocked the bot — cannot deliver {code}")
        return False
    except Exception as e:
        logger.error(f"Delivery error for {code}: {e}")
        return False


async def notify_admin_new_order(context, code: str):
    order = order_get(code)
    if not order:
        return
    cur   = currency()
    items = json.loads(order["items_json"])
    lines = "\n".join(
        f"  • {it['name']} ×{it['qty']} — {cur}{it['price']:.2f}"
        for it in items
    )
    proof = f"\n📎 Proof: {str(order['payment_proof'])[:80]}" if order.get("payment_proof") else ""
    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"*🔔 New Order!*\n{'━' * 28}\n\n"
            f"ID: `{code}`\n"
            f"User: `{order['user_id']}`\n"
            f"Payment: {order['payment_method'] or '—'}\n"
            f"Items:\n{lines}\n\n"
            f"Subtotal: {cur}{order['subtotal']:.2f}\n"
            f"Discount: −{cur}{order['discount']:.2f}\n"
            f"*Total: {cur}{order['total']:.2f}*\n"
            f"Status: {status_lbl(order['status'])}"
            f"{proof}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=order_admin_kb(code, order["status"])
        )
    except Exception as e:
        logger.error(f"Admin notify error: {e}")


async def notify_user_order_status(context, code: str, message: str):
    order = order_get(code)
    if not order:
        return
    try:
        await context.bot.send_message(
            order["user_id"], message, parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"User notify error: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    reg_user(u.id, u.username, u.full_name)
    if not await guard(update):
        return
    sn    = cfg("shop_name")
    wm    = cfg("welcome_msg")
    prods = get_products()
    cats  = get_cats()
    try:
        await update.message.reply_text(
            f"*{sn}*\n{'━' * 30}\n\n"
            f"{wm}\n\n"
            f"📦 *{len(prods)}* products in *{len(cats)}* categories\n\n"
            f"Use the menu below:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(u.id)
        )
    except Exception as e:
        logger.error(f"cmd_start error: {e}")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    uid  = update.effective_user.id
    text = (
        f"*❓ Help Guide*\n{'━' * 30}\n\n"
        "*How to buy:*\n"
        "1️⃣ Browse or search products\n"
        "2️⃣ Add items to your cart\n"
        "3️⃣ Go to cart and checkout\n"
        "4️⃣ Select payment method\n"
        "5️⃣ Send payment proof (if required)\n"
        "6️⃣ Admin reviews and approves your order\n"
        "7️⃣ Products delivered automatically! 🎉\n\n"
        "*Commands:*\n"
        "/start — Restart the bot\n"
        "/products — Browse all products\n"
        "/cart — View & manage your cart\n"
        "/orders — View your order history\n"
        "/coupon CODE — Apply a coupon code\n"
        "/support — Contact support\n"
        "/help — Show this help message\n"
    )
    if is_admin(uid):
        text += (
            "\n*🔧 Admin Commands:*\n"
            "/admin — Open admin panel\n"
            "/addproduct — Add product wizard\n"
            "/ban USER_ID — Ban a user\n"
            "/unban USER_ID — Unban a user\n"
            "/stats — Quick statistics\n"
            "/orders_pending — View pending orders\n"
            "/maintenance — Toggle maintenance mode\n"
        )
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"cmd_help error: {e}")


async def cmd_products(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    # Use get_active_categories() so ALL admin-added categories appear,
    # even if they have no products yet
    active_cats = get_active_categories()
    cats = [c["name"] for c in active_cats]
    if not cats:
        await update.message.reply_text("❌ No categories available yet.")
        return
    try:
        await update.message.reply_text(
            f"*🛍 Browse Products*\n{'━' * 26}\n\nChoose a category:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=cat_kb(cats)
        )
    except Exception as e:
        logger.error(f"cmd_products error: {e}")


async def cmd_cart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    await show_cart(update.message, update.effective_user.id, context)


async def cmd_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    uid  = update.effective_user.id
    cur  = currency()
    ords = user_orders(uid)
    if not ords:
        await update.message.reply_text(
            "*📦 No orders yet.*\n\nStart shopping with /products!",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    text = f"*📦 Your Orders*\n{'━' * 26}\n\n"
    btns = []
    for o in ords[:10]:
        try:
            its   = json.loads(o["items_json"])
            names = ", ".join(it["name"][:20] for it in its)
            e     = status_emoji(o["status"])
            text += (
                f"{e} *{o['order_code']}*\n"
                f"🛍 {names}\n"
                f"💵 {cur}{o['total']:.2f}  {status_lbl(o['status'])}\n"
                f"📅 {o['created_at'][:10]}\n\n"
            )
            btns.append([InlineKeyboardButton(
                f"{e} {o['order_code']} — {cur}{o['total']:.2f}",
                callback_data=f"user_order_{o['order_code']}"
            )])
        except Exception:
            continue
    kb = InlineKeyboardMarkup(btns) if btns else None
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except Exception as e:
        logger.error(f"cmd_orders error: {e}")


async def cmd_coupon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    if not context.args:
        await update.message.reply_text(
            "Usage: `/coupon YOURCODE`\nExample: `/coupon SAVE20`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    code = context.args[0].upper().strip()
    cp   = coupon_get(code)
    if not cp:
        await update.message.reply_text("❌ Invalid or expired coupon code.")
        return
    items = get_cart(update.effective_user.id)
    sub   = cart_total(items)
    disc  = apply_coupon_discount(sub, cp)
    if cp["type"] == "percent":
        disc_txt = f"{cp['discount']}% off → saves {currency()}{disc:.2f}"
    else:
        disc_txt = f"{currency()}{disc:.2f} off"
    context.user_data["coupon"] = {
        "code": code, "discount": disc,
        "type": cp["type"], "raw": cp["discount"]
    }
    await update.message.reply_text(
        f"*🎟 Coupon Applied!*\n\n"
        f"Code: `{code}`\n"
        f"Discount: {disc_txt}\n\n"
        f"View your cart: /cart",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    try:
        await update.message.reply_text(
            f"*📞 Support*\n{'━' * 26}\n\n{cfg('support_msg')}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"cmd_support error: {e}")


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin access only.")
        return
    await send_admin_panel(update.message)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    s   = get_stats()
    cur = currency()
    top = s.get("top_product")
    top_txt = f"{top['name']} ({top['sales']} sales)" if top else "—"
    try:
        await update.message.reply_text(
            f"*📊 Quick Statistics*\n{'━' * 30}\n\n"
            f"*👥 Users:* {s.get('users', 0)}  🚫 Banned: {s.get('banned', 0)}\n"
            f"*📦 Products:* {s.get('products', 0)}  🗂 Cats: {s.get('cats', 0)}\n\n"
            f"*📋 Orders:* {s.get('orders', 0)}\n"
            f"⏳ Pending: {s.get('pending', 0)}  💰 Paid: {s.get('paid', 0)}\n"
            f"✅ Approved: {s.get('approved', 0)}  📦 Delivered: {s.get('delivered', 0)}\n"
            f"❌ Cancelled: {s.get('cancelled', 0)}\n\n"
            f"*📅 Today:* {s.get('today_orders', 0)} orders\n"
            f"*💵 Today Revenue:* {cur}{s.get('today_revenue', 0):.2f}\n"
            f"*💵 Total Revenue:* {cur}{s.get('revenue', 0):.2f}\n\n"
            f"🏆 *Top Product:* {top_txt}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"cmd_stats error: {e}")


async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/ban USER_ID`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    try:
        con = db()
        con.execute("UPDATE users SET banned=1 WHERE user_id=?", (uid,))
        con.commit()
        con.close()
        await update.message.reply_text(f"✅ Banned user `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"cmd_ban error: {e}")


async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/unban USER_ID`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    try:
        con = db()
        con.execute("UPDATE users SET banned=0 WHERE user_id=?", (uid,))
        con.commit()
        con.close()
        await update.message.reply_text(f"✅ Unbanned user `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"cmd_unban error: {e}")


async def cmd_orders_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    ords = orders_get(status="paid", limit=10)
    if not ords:
        ords = orders_get(status="pending", limit=10)
    if not ords:
        await update.message.reply_text("✅ No pending orders!")
        return
    cur = currency()
    for o in ords:
        try:
            items = json.loads(o["items_json"])
            names = ", ".join(it["name"] for it in items)
            await update.message.reply_text(
                f"*{status_emoji(o['status'])} Order:* `{o['order_code']}`\n"
                f"User: `{o['user_id']}`\n"
                f"Items: {names}\n"
                f"Total: *{cur}{o['total']:.2f}*\n"
                f"Payment: {o['payment_method'] or '—'}\n"
                f"Date: {o['created_at'][:16]}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=order_admin_kb(o["order_code"], o["status"])
            )
        except Exception as e:
            logger.error(f"orders_pending item error: {e}")


async def cmd_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    cur = cfg("maintenance_mode", "0")
    new = "0" if cur == "1" else "1"
    set_cfg("maintenance_mode", new)
    status = "🔧 ENABLED" if new == "1" else "✅ DISABLED"
    await update.message.reply_text(f"Maintenance mode: *{status}*", parse_mode=ParseMode.MARKDOWN)


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    try:
        await update.message.reply_text(
            "✅ Cancelled.",
            reply_markup=main_kb(update.effective_user.id)
        )
    except Exception:
        pass
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  SEARCH CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def search_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    try:
        await update.message.reply_text(
            "🔍 Enter a product name or keyword:",
            reply_markup=ReplyKeyboardRemove()
        )
    except Exception:
        pass
    return SEARCH_Q


async def search_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    q    = update.message.text.strip().lower()
    cur  = currency()
    rows = search_products(q)
    uid  = update.effective_user.id
    if not rows:
        await update.message.reply_text(
            f"❌ No products found for: *{update.message.text}*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(uid)
        )
        return ConversationHandler.END
    btns = [[InlineKeyboardButton(
        f"{p['name']} — {cur}{p['price']:.2f}",
        callback_data=f"prod_{p['id']}"
    )] for p in rows]
    try:
        await update.message.reply_text(
            f"*🔍 {len(rows)} result(s) for: {update.message.text}*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(btns)
        )
    except Exception as e:
        logger.error(f"search_do error: {e}")
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  ADD PRODUCT CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def ap_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id if q else update.effective_user.id
    if not is_admin(uid):
        if q:
            await q.answer("⛔ Admin only.")
        else:
            await update.message.reply_text("⛔ Admin only.")
        return ConversationHandler.END
    if q:
        await q.answer()
    context.user_data["np"] = {}
    msg = q.message if q else update.message
    try:
        await msg.reply_text(
            "*📦 Add New Product — Step 1/7*\n\n"
            "Enter the product *name*:\n"
            "_(send /cancel to stop)_",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"ap_entry error: {e}")
    return AP_NAME


async def ap_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    context.user_data["np"]["name"] = update.message.text.strip()
    await update.message.reply_text("*Step 2/7* — Enter a description:", parse_mode=ParseMode.MARKDOWN)
    return AP_DESC


async def ap_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    context.user_data["np"]["description"] = update.message.text.strip()
    await update.message.reply_text(
        f"*Step 3/7* — Enter the price ({currency()}):", parse_mode=ParseMode.MARKDOWN
    )
    return AP_PRICE


async def ap_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    raw = update.message.text.strip().replace(",", "").lstrip(currency())
    try:
        context.user_data["np"]["price"] = float(raw)
        cats    = get_active_categories()
        cat_txt = "\n".join(f"  {c['icon']} {c['name']}" for c in cats) if cats else "  General"
        await update.message.reply_text(
            f"*Step 4/7* — Enter a category:\n\n{cat_txt}\n\nType the exact name:",
            parse_mode=ParseMode.MARKDOWN
        )
        return AP_CAT
    except ValueError:
        await update.message.reply_text("❌ Invalid price. Enter a number:")
        return AP_PRICE


async def ap_cat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    context.user_data["np"]["category"] = update.message.text.strip()
    await update.message.reply_text(
        "*Step 5/7* — Upload the product file:\n\n"
        "• Send any file (PDF, ZIP, script...)\n"
        "• Or type a link / license key / any text to deliver.",
        parse_mode=ParseMode.MARKDOWN
    )
    return AP_FILE


async def ap_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    np  = context.user_data["np"]
    msg = update.message
    if msg.document:
        np.update(file_id=msg.document.file_id, file_type="document", file_content=msg.document.file_name or "")
    elif msg.photo:
        np.update(file_id=msg.photo[-1].file_id, file_type="photo", file_content="")
    elif msg.video:
        np.update(file_id=msg.video.file_id, file_type="video", file_content="")
    elif msg.audio:
        np.update(file_id=msg.audio.file_id, file_type="audio", file_content="")
    elif msg.text:
        np.update(file_id=None, file_type="text", file_content=msg.text.strip())
    else:
        await msg.reply_text("❌ Unsupported format. Send a file or type text:")
        return AP_FILE
    await msg.reply_text(
        "*Step 6/7* — Send a preview image (optional):\n\nSend a photo, or type `skip`.",
        parse_mode=ParseMode.MARKDOWN
    )
    return AP_PREVIEW


async def ap_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    np  = context.user_data["np"]
    msg = update.message
    if msg.text and msg.text.lower() == "skip":
        np.update(preview_id=None, preview_type=None)
    elif msg.photo:
        np.update(preview_id=msg.photo[-1].file_id, preview_type="photo")
    elif msg.video:
        np.update(preview_id=msg.video.file_id, preview_type="video")
    elif msg.document:
        np.update(preview_id=msg.document.file_id, preview_type="document")
    else:
        np.update(preview_id=None, preview_type=None)
    await msg.reply_text(
        "*Step 7/7* — Enter stock quantity:\n`-1` = Unlimited",
        parse_mode=ParseMode.MARKDOWN
    )
    return AP_STOCK


async def ap_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    try:
        stock = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number (-1 for unlimited):")
        return AP_STOCK
    np = context.user_data.pop("np", {})
    np["stock"] = stock
    try:
        con = db()
        con.execute(
            """INSERT INTO products(name,description,price,category,file_id,file_type,
               file_content,preview_id,preview_type,stock) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (np.get("name"), np.get("description"), np.get("price"), np.get("category"),
             np.get("file_id"), np.get("file_type", "text"), np.get("file_content", ""),
             np.get("preview_id"), np.get("preview_type"), stock)
        )
        con.commit()
        con.close()
        await update.message.reply_text(
            f"*✅ Product Added!*\n\n"
            f"📛 Name: {np.get('name', '')}\n"
            f"💰 Price: {currency()}{np.get('price', 0):.2f}\n"
            f"📂 Category: {np.get('category', '')}\n"
            f"📦 Stock: {'Unlimited' if stock == -1 else stock}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(update.effective_user.id)
        )
    except Exception as e:
        logger.error(f"ap_stock DB error: {e}")
        await update.message.reply_text("❌ Error saving product. Please try again.")
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  EDIT PRODUCT FIELD CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def ep_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    await q.answer()
    data  = q.data
    parts = data.split("_")
    field = parts[2]
    pid   = int(parts[3])
    context.user_data["ep"] = {"field": field, "pid": pid}
    labels = {
        "name": "product name", "desc": "description",
        "price": "price", "cat": "category", "stock": "stock quantity (-1 = unlimited)"
    }
    try:
        await q.message.reply_text(
            f"✏️ Enter new *{labels.get(field, field)}*:",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"ep_entry error: {e}")
    return EP_VALUE


async def ep_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    ep    = context.user_data.pop("ep", {})
    field = ep.get("field", "")
    pid   = ep.get("pid", 0)
    val   = update.message.text.strip()
    field_map = {"name": "name", "desc": "description", "price": "price", "cat": "category", "stock": "stock"}
    db_field  = field_map.get(field, field)
    try:
        if db_field in ("price",):
            val = float(val)
        elif db_field in ("stock",):
            val = int(val)
        update_product_field(pid, db_field, val)
        await update.message.reply_text(
            f"✅ *{field.capitalize()}* updated!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(update.effective_user.id)
        )
    except ValueError:
        await update.message.reply_text("❌ Invalid value. Please enter a valid number.")
    except Exception as e:
        logger.error(f"ep_value error: {e}")
        await update.message.reply_text("❌ Error updating field.")
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  RE-FILE / RE-PREVIEW CONVERSATIONS
# ════════════════════════════════════════════════════════════════════════════

async def refile_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    pid = int(q.data.split("_")[-1])
    context.user_data["refile_pid"] = pid
    try:
        await q.message.reply_text(
            "📤 Send the new product file (document, photo, video, audio, or text):"
        )
    except Exception as e:
        logger.error(f"refile_entry error: {e}")
    return REFILE_FILE


async def refile_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    pid = context.user_data.pop("refile_pid", None)
    msg = update.message
    if pid is None:
        return ConversationHandler.END
    try:
        if msg.document:
            update_product_field(pid, "file_id",   msg.document.file_id)
            update_product_field(pid, "file_type", "document")
        elif msg.photo:
            update_product_field(pid, "file_id",   msg.photo[-1].file_id)
            update_product_field(pid, "file_type", "photo")
        elif msg.video:
            update_product_field(pid, "file_id",   msg.video.file_id)
            update_product_field(pid, "file_type", "video")
        elif msg.audio:
            update_product_field(pid, "file_id",   msg.audio.file_id)
            update_product_field(pid, "file_type", "audio")
        elif msg.text:
            update_product_field(pid, "file_id",      None)
            update_product_field(pid, "file_type",    "text")
            update_product_field(pid, "file_content", msg.text.strip())
        else:
            await msg.reply_text("❌ Unsupported format.")
            return ConversationHandler.END
        await msg.reply_text("✅ Product file updated!")
    except Exception as e:
        logger.error(f"refile_do error: {e}")
        await msg.reply_text("❌ Error updating file.")
    return ConversationHandler.END


async def repreview_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    pid = int(q.data.split("_")[-1])
    context.user_data["repreview_pid"] = pid
    try:
        await q.message.reply_text("🖼 Send a new preview image/video (or type `skip`):")
    except Exception as e:
        logger.error(f"repreview_entry error: {e}")
    return REPREVIEW_FILE


async def repreview_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    pid = context.user_data.pop("repreview_pid", None)
    msg = update.message
    if pid is None:
        return ConversationHandler.END
    try:
        if msg.text and msg.text.lower() == "skip":
            update_product_field(pid, "preview_id",   None)
            update_product_field(pid, "preview_type", None)
        elif msg.photo:
            update_product_field(pid, "preview_id",   msg.photo[-1].file_id)
            update_product_field(pid, "preview_type", "photo")
        elif msg.video:
            update_product_field(pid, "preview_id",   msg.video.file_id)
            update_product_field(pid, "preview_type", "video")
        elif msg.document:
            update_product_field(pid, "preview_id",   msg.document.file_id)
            update_product_field(pid, "preview_type", "document")
        else:
            update_product_field(pid, "preview_id",   None)
            update_product_field(pid, "preview_type", None)
        await msg.reply_text("✅ Preview updated!")
    except Exception as e:
        logger.error(f"repreview_do error: {e}")
        await msg.reply_text("❌ Error updating preview.")
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  ADD COUPON CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def cp_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["nc"] = {}
    try:
        await q.message.reply_text(
            "*🎟 Add Coupon — Step 1/3*\n\nEnter coupon *code* (e.g. SAVE20):",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"cp_entry error: {e}")
    return CP_CODE


async def cp_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    context.user_data["nc"]["code"] = update.message.text.strip().upper()
    await update.message.reply_text(
        "*Step 2/3* — Enter the discount:\n\n"
        "• For fixed: e.g. `5` (means $5 off)\n"
        "• For percentage: e.g. `20%` (means 20% off)",
        parse_mode=ParseMode.MARKDOWN
    )
    return CP_DISC


async def cp_disc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    raw  = update.message.text.strip()
    is_p = raw.endswith("%")
    try:
        val = float(raw.rstrip("%"))
    except ValueError:
        await update.message.reply_text("❌ Invalid. Enter a number (e.g. 5 or 20%):")
        return CP_DISC
    context.user_data["nc"]["discount"] = val
    context.user_data["nc"]["type"]     = "percent" if is_p else "fixed"
    await update.message.reply_text(
        "*Step 3/3* — How many uses? (e.g. `1`, `100`, `999` for many):",
        parse_mode=ParseMode.MARKDOWN
    )
    return CP_LIMIT


async def cp_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    try:
        limit = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number:")
        return CP_LIMIT
    nc = context.user_data.pop("nc", {})
    con = db()
    try:
        con.execute(
            "INSERT INTO coupons(code,discount,type,uses_left) VALUES(?,?,?,?)",
            (nc["code"], nc["discount"], nc["type"], limit)
        )
        con.commit()
        disc_txt = f"{nc['discount']}%" if nc["type"] == "percent" else f"{currency()}{nc['discount']}"
        await update.message.reply_text(
            f"*✅ Coupon Created!*\n\n"
            f"Code: `{nc['code']}`\n"
            f"Discount: {disc_txt}\n"
            f"Uses: {limit}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(update.effective_user.id)
        )
    except sqlite3.IntegrityError:
        await update.message.reply_text(
            f"❌ Code `{nc.get('code', '')}` already exists.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"cp_limit error: {e}")
        await update.message.reply_text("❌ Error creating coupon.")
    finally:
        con.close()
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  PAYMENT METHOD CONVERSATIONS
# ════════════════════════════════════════════════════════════════════════════

async def pm_add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["npm"] = {}
    await q.message.reply_text(
        "*➕ Add Payment Method — Step 1/2*\n\nEnter the payment method *name*:",
        parse_mode=ParseMode.MARKDOWN
    )
    return PM_NAME


async def pm_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    context.user_data["npm"]["name"] = update.message.text.strip()
    await update.message.reply_text("*Step 2/2* — Enter payment *details/instructions*:", parse_mode=ParseMode.MARKDOWN)
    return PM_DETAIL


async def pm_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    npm = context.user_data.pop("npm", {})
    npm["details"] = update.message.text.strip()
    try:
        con = db()
        con.execute("INSERT INTO payment_methods(name,details) VALUES(?,?)", (npm["name"], npm["details"]))
        con.commit()
        con.close()
        await update.message.reply_text(
            f"✅ Payment method *{npm['name']}* added!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(update.effective_user.id)
        )
    except Exception as e:
        logger.error(f"pm_detail error: {e}")
        await update.message.reply_text("❌ Error adding payment method.")
    return ConversationHandler.END


async def pm_edit_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    mid = int(q.data.split("_")[-1])
    context.user_data["epm"] = {"id": mid}
    try:
        con = db()
        m   = con.execute("SELECT * FROM payment_methods WHERE id=?", (mid,)).fetchone()
        con.close()
        if not m:
            await q.answer("Not found.", show_alert=True)
            return ConversationHandler.END
        await q.message.reply_text(
            f"✏️ Editing: *{m['name']}*\n\nEnter new name (or send `-` to keep):",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"pm_edit_entry error: {e}")
    return PM_EDIT_NAME


async def pm_edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    val = update.message.text.strip()
    if val != "-":
        context.user_data["epm"]["name"] = val
    await update.message.reply_text("Enter new details (or send `-` to keep):")
    return PM_EDIT_DETAIL


async def pm_edit_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    val = update.message.text.strip()
    epm = context.user_data.pop("epm", {})
    mid = epm.get("id")
    if not mid:
        return ConversationHandler.END
    try:
        con = db()
        if "name" in epm:
            con.execute("UPDATE payment_methods SET name=? WHERE id=?", (epm["name"], mid))
        if val != "-":
            con.execute("UPDATE payment_methods SET details=? WHERE id=?", (val, mid))
        con.commit()
        con.close()
        await update.message.reply_text("✅ Payment method updated!", reply_markup=main_kb(update.effective_user.id))
    except Exception as e:
        logger.error(f"pm_edit_detail error: {e}")
        await update.message.reply_text("❌ Error updating payment method.")
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  CATEGORY CONVERSATIONS
# ════════════════════════════════════════════════════════════════════════════

async def cat_add_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.message.reply_text("🗂 *Add Category — Step 1/2*\n\nEnter the category *name*:", parse_mode=ParseMode.MARKDOWN)
    return CAT_NAME


async def cat_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    context.user_data["ncat"] = {"name": update.message.text.strip()}
    await update.message.reply_text(
        "*Step 2/2* — Enter an *emoji icon*:\nExample: 🐍 🌐 🤖 📚 💾 🔑 🎓\n\nOr type `skip` for default 📦:",
        parse_mode=ParseMode.MARKDOWN
    )
    return CAT_ICON


async def cat_icon_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    ncat = context.user_data.pop("ncat", {})
    icon = update.message.text.strip()
    if icon.lower() == "skip" or not icon:
        icon = "📦"
    success = add_category(ncat.get("name", ""), icon)
    if success:
        await update.message.reply_text(
            f"✅ Category added: *{icon} {ncat.get('name', '')}*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(update.effective_user.id)
        )
    else:
        await update.message.reply_text(
            f"❌ Category *{ncat.get('name', '')}* already exists.",
            parse_mode=ParseMode.MARKDOWN
        )
    return ConversationHandler.END


async def cat_editname_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    cid = int(q.data.split("_")[-1])
    context.user_data["ecat"] = {"id": cid, "field": "name"}
    cat = get_category_by_id(cid)
    if not cat:
        await q.answer("Not found.", show_alert=True)
        return ConversationHandler.END
    await q.message.reply_text(f"✏️ Enter new name for *{cat['name']}*:", parse_mode=ParseMode.MARKDOWN)
    return CAT_EDIT_NAME


async def cat_editname_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    ecat = context.user_data.pop("ecat", {})
    update_category(ecat["id"], name=update.message.text.strip())
    await update.message.reply_text("✅ Category name updated!", reply_markup=main_kb(update.effective_user.id))
    return ConversationHandler.END


async def cat_editicon_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    cid = int(q.data.split("_")[-1])
    context.user_data["ecat"] = {"id": cid, "field": "icon"}
    cat = get_category_by_id(cid)
    if not cat:
        await q.answer("Not found.", show_alert=True)
        return ConversationHandler.END
    await q.message.reply_text(f"🎨 Enter new icon for *{cat['name']}*:\nExample: 🐍 🌐 🤖 📚", parse_mode=ParseMode.MARKDOWN)
    return CAT_EDIT_ICON


async def cat_editicon_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    ecat = context.user_data.pop("ecat", {})
    update_category(ecat["id"], icon=update.message.text.strip())
    await update.message.reply_text("✅ Category icon updated!", reply_markup=main_kb(update.effective_user.id))
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  SETTINGS CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def set_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    key = q.data.replace("adm_set_", "")
    context.user_data["set_key"] = key
    labels = {
        "shop_name":     "Shop Name",
        "welcome_msg":   "Welcome Message",
        "support_msg":   "Support Message",
        "footer_msg":    "Footer Message",
        "currency":      "Currency Symbol",
        "order_timeout": "Order Timeout (hours)",
        "tax":           "Tax Percentage",
        "force_label":   "Force Join Button Label",
    }
    key_map = {
        "order_timeout": "order_timeout_hrs",
        "tax":           "tax_percent",
        "force_label":   "force_join_label",
    }
    db_key  = key_map.get(key, key)
    cur_val = cfg(db_key, "")
    try:
        await q.message.reply_text(
            f"✏️ Enter new *{labels.get(key, key)}*:\n\nCurrent: `{cur_val}`",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"set_entry error: {e}")
    return SET_VALUE


async def set_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    key = context.user_data.pop("set_key", "")
    val = update.message.text.strip()
    db_key_map = {
        "order_timeout": "order_timeout_hrs",
        "tax":           "tax_percent",
        "force_label":   "force_join_label",
    }
    db_key = db_key_map.get(key, key)
    set_cfg(db_key, val)
    await update.message.reply_text(
        f"✅ *{key.replace('_', ' ').title()}* updated!",
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  FORCE CHANNEL CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def force_channel_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.message.reply_text(
        "📢 Enter channel username or ID:\n\n"
        "Examples:\n`@mychannel`\n`-1001234567890`",
        parse_mode=ParseMode.MARKDOWN
    )
    return FORCE_CHANNEL


async def force_channel_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    val = update.message.text.strip()
    set_cfg("force_join_channel", val)
    await update.message.reply_text(
        f"✅ Force join channel set to: `{val}`\n\n"
        "Make sure the bot is an admin of that channel!",
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  BROADCAST CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def bc_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        target = q.message
    else:
        target = update.message
    await target.reply_text(
        "📢 *Broadcast*\n\nSend the message you want to broadcast to ALL users:\n"
        "(supports text, photos, videos, documents)",
        parse_mode=ParseMode.MARKDOWN
    )
    return BC_MSG


async def bc_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    context.user_data["bc_msg"] = update.message
    users = all_users()
    await update.message.reply_text(
        f"⚠️ This will send to *{len(users)} users*.\n\nReply *YES* to confirm or anything else to cancel.",
        parse_mode=ParseMode.MARKDOWN
    )
    return BC_OK


async def bc_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    if update.message.text.strip().upper() != "YES":
        context.user_data.pop("bc_msg", None)
        await update.message.reply_text("❌ Broadcast cancelled.")
        return ConversationHandler.END
    orig   = context.user_data.pop("bc_msg", None)
    users  = all_users()
    sent   = 0
    failed = 0
    status_msg = await update.message.reply_text(f"📢 Sending to {len(users)} users...")
    for u in users:
        try:
            if orig and orig.text:
                await context.bot.send_message(u["user_id"], orig.text)
            elif orig and orig.photo:
                await context.bot.send_photo(u["user_id"], orig.photo[-1].file_id, caption=orig.caption or "")
            elif orig and orig.video:
                await context.bot.send_video(u["user_id"], orig.video.file_id, caption=orig.caption or "")
            elif orig and orig.document:
                await context.bot.send_document(u["user_id"], orig.document.file_id, caption=orig.caption or "")
            sent   += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    try:
        con = db()
        con.execute(
            "INSERT INTO broadcast_log(message,sent_count,fail_count) VALUES(?,?,?)",
            (str(orig.text if orig else "media")[:200], sent, failed)
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"broadcast_log error: {e}")
    try:
        await status_msg.edit_text(
            f"✅ *Broadcast Complete!*\n\n✅ Sent: {sent}\n❌ Failed: {failed}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  ORDER NOTE CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def order_note_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    code = q.data.replace("ord_note_", "")
    context.user_data["note_code"] = code
    await q.message.reply_text(f"✏️ Enter a note for order `{code}`:", parse_mode=ParseMode.MARKDOWN)
    return ORDER_NOTE


async def order_note_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    code = context.user_data.pop("note_code", "")
    note = update.message.text.strip()
    order_update(code, admin_note=note)
    await update.message.reply_text(f"✅ Note added to order `{code}`.", parse_mode=ParseMode.MARKDOWN)
    await notify_user_order_status(
        context, code,
        f"ℹ️ *Update on your order `{code}`:*\n\n📝 {note}"
    )
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  ADMIN REPLY TO USER CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def admin_reply_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    code = q.data.replace("ord_msg_", "")
    order = order_get(code)
    if order:
        context.user_data["reply_uid"]  = order["user_id"]
        context.user_data["reply_code"] = code
    await q.message.reply_text(f"💬 Enter message for user (order `{code}`):", parse_mode=ParseMode.MARKDOWN)
    return AR_MSG


async def admin_reply_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    uid  = context.user_data.pop("reply_uid",  None)
    code = context.user_data.pop("reply_code", None)
    if not uid:
        await update.message.reply_text("❌ No user found.")
        return ConversationHandler.END
    try:
        await context.bot.send_message(
            uid,
            f"*📩 Message from Store Admin:*\n\n{update.message.text}\n\n"
            f"_(Regarding order: `{code}`)_",
            parse_mode=ParseMode.MARKDOWN
        )
        await update.message.reply_text(f"✅ Message sent to user `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text(f"❌ Could not send message: {e}")
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  PAYMENT PROOF CONVERSATION
# ════════════════════════════════════════════════════════════════════════════

async def pay_proof_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    await q.answer()
    parts = q.data.split("_")
    mid   = parts[1]
    code  = "_".join(parts[2:])
    try:
        con = db()
        m   = con.execute("SELECT * FROM payment_methods WHERE id=?", (mid,)).fetchone()
        con.close()
        if not m:
            await q.answer("❌ Payment method not found.", show_alert=True)
            return ConversationHandler.END
        order_update(code, payment_method=m["name"])
        context.user_data["proof_code"] = code
        order = order_get(code)
        total = order["total"] if order else 0
        text  = (
            f"*💳 {m['name']}*\n{'━' * 26}\n\n"
            f"{m['details']}\n\n"
            f"*Order:* `{code}`\n"
            f"*Total:* {currency()}{total:.2f}\n\n"
        )
        if cfg("require_proof", "1") == "1":
            text += "📸 Now send your payment proof (screenshot/TxID)."
        else:
            text += "✅ No proof required. Your order will be processed shortly."
        await q.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"pay_proof_entry error: {e}")
        return ConversationHandler.END

    if cfg("require_proof", "1") == "0":
        code_to_process = context.user_data.pop("proof_code", None)
        if code_to_process:
            order_update(code_to_process, status="paid")
            cart_clear(q.from_user.id)
            context.user_data.pop("coupon", None)
            await notify_admin_new_order(context, code_to_process)
        return ConversationHandler.END
    return PAY_PROOF


async def pay_proof_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return ConversationHandler.END
    code = context.user_data.pop("proof_code", None)
    msg  = update.message
    if not code:
        return ConversationHandler.END
    proof = None
    if msg.photo:
        proof = msg.photo[-1].file_id
    elif msg.document:
        proof = msg.document.file_id
    elif msg.text:
        proof = msg.text.strip()
    if not proof:
        await msg.reply_text("❌ Please send a photo, file, or text as proof.")
        return PAY_PROOF
    order_update(code, payment_proof=str(proof), status="paid")
    order = order_get(code)
    try:
        await msg.reply_text(
            f"*✅ Payment Received!*\n{'━' * 28}\n\n"
            f"Order: `{code}`\n"
            f"Total: {currency()}{order['total']:.2f}\n\n"
            "Your order is now under review. You'll be notified once approved and delivered! 🎉",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_kb(update.effective_user.id)
        )
    except Exception as e:
        logger.error(f"pay_proof_receive reply error: {e}")
    cart_clear(update.effective_user.id)
    context.user_data.pop("coupon", None)
    await notify_admin_new_order(context, code)
    try:
        if msg.photo:
            await context.bot.send_photo(
                ADMIN_ID, msg.photo[-1].file_id,
                caption=f"📎 Payment proof for order `{code}`",
                parse_mode=ParseMode.MARKDOWN
            )
        elif msg.document:
            await context.bot.send_document(
                ADMIN_ID, msg.document.file_id,
                caption=f"📎 Payment proof for order `{code}`",
                parse_mode=ParseMode.MARKDOWN
            )
    except Exception:
        pass
    return ConversationHandler.END


# ════════════════════════════════════════════════════════════════════════════
#  SHOW ORDERS LIST HELPER
# ════════════════════════════════════════════════════════════════════════════

async def show_orders_list(target, page: int, status: str, cur: str, edit: bool = False):
    limit  = 8
    offset = page * limit
    if status == "all":
        ords  = orders_get(limit=limit, offset=offset)
        total = count_orders()
    else:
        ords  = orders_get(status=status, limit=limit, offset=offset)
        total = count_orders(status)
    if not ords:
        txt = f"📭 No {status} orders."
        kb  = InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="adm_back")]])
        try:
            if edit:
                await target.edit_text(txt, reply_markup=kb)
            else:
                await target.reply_text(txt, reply_markup=kb)
        except Exception:
            await target.reply_text(txt, reply_markup=kb)
        return
    btns = []
    for o in ords:
        e = status_emoji(o["status"])
        btns.append([InlineKeyboardButton(
            f"{e} {o['order_code']} — {cur}{o['total']:.2f} [{o['status']}]",
            callback_data=f"adm_vieworder_{o['order_code']}"
        )])
    total_pages = max(1, (total + limit - 1) // limit)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"adm_orders_page_{page-1}_{status}"))
    nav.append(InlineKeyboardButton(f"p{page+1}/{total_pages}", callback_data="noop"))
    if (page+1)*limit < total:
        nav.append(InlineKeyboardButton("Next ▶", callback_data=f"adm_orders_page_{page+1}_{status}"))
    btns.append(nav)
    statuses = ["all", "pending", "paid", "approved", "delivered", "cancelled"]
    f_row = [InlineKeyboardButton(
        ("✅" if s == status else "") + s[:5],
        callback_data=f"adm_orders_filter_{s}"
    ) for s in statuses]
    btns.append(f_row)
    btns.append([InlineKeyboardButton("« Back", callback_data="adm_back")])
    text = f"*📋 Orders — {status.upper()}* ({total} total, page {page+1}/{total_pages})"
    kb   = InlineKeyboardMarkup(btns)
    try:
        if edit:
            await target.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        else:
            await target.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except BadRequest:
        await target.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except Exception as e:
        logger.error(f"show_orders_list error: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  CALLBACK QUERY HANDLER
# ════════════════════════════════════════════════════════════════════════════

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    data = q.data

    if data == "noop":
        await q.answer()
        return

    if data == "check_join":
        # Use context.bot — reliable in both polling and webhook modes
        if await check_membership(uid, context.bot):
            await q.answer("✅ Welcome! You can now use the store.", show_alert=True)
            u = q.from_user
            reg_user(u.id, u.username, u.full_name)
            sn       = cfg("shop_name")
            wm       = cfg("welcome_msg")
            prods    = get_products()
            all_cats = get_active_categories()
            try:
                await q.message.reply_text(
                    f"*{sn}*\n{'━' * 30}\n\n{wm}\n\n"
                    f"📦 *{len(prods)}* products in *{len(all_cats)}* categories\n\nUse the menu below:",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=main_kb(uid)
                )
            except Exception as e:
                logger.error(f"check_join reply error: {e}")
        else:
            channel = cfg("force_join_channel", "").strip()
            label   = cfg("force_join_label", "📢 Join Our Channel")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(label, url=f"https://t.me/{channel.lstrip('@')}")],
                [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
            ])
            await q.answer("❌ You haven't joined yet. Please join first!", show_alert=True)
            try:
                await q.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
        return

    if not await guard_query(q, uid, bot=context.bot):
        return

    await q.answer()
    cur = currency()

    try:
        # ── Browse / Category ──────────────────────────────────────────────────
        if data == "back_cat":
            active_cats = get_active_categories()
            cats = [c["name"] for c in active_cats]
            if not cats:
                await q.message.reply_text("❌ No categories available.")
                return
            await q.message.edit_text(
                f"*🛍 Browse Products*\n{'━' * 26}\n\nChoose a category:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=cat_kb(cats)
            )
            return

        if data.startswith("cat_"):
            cat   = data[4:]
            prods = get_products(None if cat == "all" else cat)
            if not prods:
                await q.message.edit_text(
                    f"❌ No products in *{cat}*.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="back_cat")]])
                )
                return
            title = "All Products" if cat == "all" else cat
            await q.message.edit_text(
                f"*📦 {title}* — {len(prods)} items",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=prod_list_kb(prods, cur)
            )
            return

        if data.startswith("prod_"):
            pid = int(data[5:])
            p   = get_product(pid)
            if not p:
                await q.answer("❌ Product not found.", show_alert=True)
                return
            items   = get_cart(uid)
            in_cart = any(i["product_id"] == pid for i in items)
            stock_txt = "Unlimited" if p["stock"] == -1 else str(p["stock"])
            text = (
                f"*{p['name']}*\n{'━' * 26}\n\n"
                f"{p['description']}\n\n"
                f"💰 *Price:* {cur}{p['price']:.2f}\n"
                f"📦 *Stock:* {stock_txt}\n"
                f"📂 *Category:* {p['category']}"
            )
            if p.get("preview_id") and p.get("preview_type") == "photo":
                try:
                    await q.message.reply_photo(
                        p["preview_id"], caption=text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=prod_kb(pid, in_cart)
                    )
                    return
                except Exception:
                    pass
            await q.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=prod_kb(pid, in_cart))
            return

        if data.startswith("preview_"):
            pid = int(data[8:])
            p   = get_product(pid)
            if not p:
                await q.answer("Product not found.", show_alert=True)
                return
            if p.get("preview_id"):
                ftyp = p.get("preview_type", "photo")
                try:
                    send_map = {
                        "photo":    context.bot.send_photo,
                        "video":    context.bot.send_video,
                        "document": context.bot.send_document,
                    }
                    fn = send_map.get(ftyp, context.bot.send_photo)
                    await fn(uid, p["preview_id"], caption=f"🖼 Preview: *{p['name']}*", parse_mode=ParseMode.MARKDOWN)
                except Exception as e:
                    await q.answer(f"Could not send preview: {e}", show_alert=True)
            else:
                await q.answer("ℹ️ No preview available for this product.", show_alert=True)
            return

        if data.startswith("addcart_"):
            pid = int(data[8:])
            p   = get_product(pid)
            if not p:
                await q.answer("❌ Product not found.", show_alert=True)
                return
            if p["stock"] == 0:
                await q.answer("❌ Out of stock!", show_alert=True)
                return
            cart_add(uid, pid)
            await q.answer(f"✅ {p['name']} added to cart!")
            return

        if data.startswith("rmcart_"):
            cid = int(data[7:])
            cart_remove(cid)
            await show_cart(q.message, uid, context)
            return

        if data == "clear_cart":
            cart_clear(uid)
            await q.message.edit_text("🗑 Cart cleared.", reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🛍 Browse Products", callback_data="back_cat")
            ]]))
            return

        if data == "checkout":
            items = get_cart(uid)
            if not items:
                await q.answer("Your cart is empty!", show_alert=True)
                return
            sub   = cart_total(items)
            disc  = 0.0
            cdata = context.user_data.get("coupon")
            if cdata:
                disc = cdata["discount"]
            total = max(0.0, sub - disc)
            try:
                code = order_create(uid, items, sub, disc, total, cdata["code"] if cdata else None, None)
            except ValueError as ve:
                await q.answer(str(ve), show_alert=True)
                return
            methods = pay_methods()
            if not methods:
                await q.message.reply_text("❌ No payment methods configured. Contact admin.")
                return
            if cdata:
                coupon_use(cdata["code"])
            await q.message.reply_text(
                f"*💳 Select Payment Method*\n{'━' * 26}\n\n"
                f"Order: `{code}`\n"
                f"Subtotal: {cur}{sub:.2f}\n"
                f"Discount: −{cur}{disc:.2f}\n"
                f"*Total: {cur}{total:.2f}*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=pm_kb(methods, code)
            )
            return

        if data == "pay_cancel":
            await q.message.edit_text(
                "❌ Order cancelled.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛍 Continue Shopping", callback_data="back_cat")]])
            )
            return

        if data.startswith("user_order_"):
            code  = data.replace("user_order_", "")
            order = order_get(code)
            if not order:
                await q.answer("Order not found.", show_alert=True)
                return
            items = json.loads(order["items_json"])
            lines = "\n".join(f"  • {it['name']} ×{it['qty']} — {cur}{it['price']:.2f}" for it in items)
            note_line = f"\n📝 Note: {order['admin_note']}" if order.get("admin_note") else ""
            await q.message.edit_text(
                f"*📋 Order Details*\n{'━' * 26}\n\n"
                f"ID: `{order['order_code']}`\n"
                f"Status: {status_lbl(order['status'])}\n\n"
                f"Items:\n{lines}\n\n"
                f"*Subtotal:* {cur}{order['subtotal']:.2f}\n"
                f"*Discount:* −{cur}{order['discount']:.2f}\n"
                f"*Total:* {cur}{order['total']:.2f}\n"
                f"*Payment:* {order['payment_method'] or '—'}\n"
                f"*Date:* {order['created_at'][:16]}"
                f"{note_line}",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # ── Admin Back ─────────────────────────────────────────────────────────
        if data == "adm_back":
            if not is_admin(uid):
                return
            await send_admin_panel(q.message, edit=True)
            return

        # ── Admin Products ────────────────────────────────────────────────────
        if data == "adm_products":
            if not is_admin(uid):
                return
            await q.message.edit_text(
                "*📦 Product Management*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=products_panel_kb()
            )
            return

        if data.startswith("adm_listproducts_"):
            if not is_admin(uid):
                return
            page   = int(data.split("_")[-1])
            limit  = 8
            try:
                con    = db()
                total  = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]
                prods  = con.execute(
                    "SELECT * FROM products ORDER BY id DESC LIMIT ? OFFSET ?",
                    (limit, page * limit)
                ).fetchall()
                con.close()
            except Exception:
                prods = []
                total = 0
            if not prods:
                await q.message.edit_text(
                    "📭 No products found.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="adm_products")]])
                )
                return
            btns = []
            for p in prods:
                status = "🟢" if p["active"] else "🔴"
                btns.append([InlineKeyboardButton(
                    f"{status} {p['name']} — {cur}{p['price']:.2f}",
                    callback_data=f"adm_viewprod_{p['id']}"
                )])
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"adm_listproducts_{page-1}"))
            total_pages = max(1, (total + limit - 1) // limit)
            nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
            if (page+1)*limit < total:
                nav.append(InlineKeyboardButton("Next ▶", callback_data=f"adm_listproducts_{page+1}"))
            if nav:
                btns.append(nav)
            btns.append([InlineKeyboardButton("« Back", callback_data="adm_products")])
            await q.message.edit_text(
                f"*📋 Products* ({total} total)",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(btns)
            )
            return

        if data.startswith("adm_viewprod_"):
            if not is_admin(uid):
                return
            pid = int(data.split("_")[-1])
            p   = get_product(pid)
            if not p:
                await q.answer("Product not found.", show_alert=True)
                return
            stock_txt = "Unlimited" if p["stock"] == -1 else str(p["stock"])
            status    = "🟢 Active" if p["active"] else "🔴 Inactive"
            await q.message.edit_text(
                f"*📦 {p['name']}*\n{'━' * 26}\n\n"
                f"📝 Description: {p['description'][:100]}\n"
                f"💰 Price: {cur}{p['price']:.2f}\n"
                f"📂 Category: {p['category']}\n"
                f"📦 Stock: {stock_txt}\n"
                f"🏆 Sales: {p['sales']}\n"
                f"Status: {status}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=admin_prod_kb(pid, p["active"])
            )
            return

        if data.startswith("adm_editprod_"):
            if not is_admin(uid):
                return
            pid = int(data.split("_")[-1])
            await q.message.edit_text(
                "*✏️ Edit Product Fields*\n\nChoose a field to edit:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=edit_fields_kb(pid)
            )
            return

        if data.startswith("adm_toggleprod_"):
            if not is_admin(uid):
                return
            pid = int(data.split("_")[-1])
            toggle_product(pid)
            p   = get_product(pid)
            if p:
                await q.answer(f"{'✅ Activated' if p['active'] else '🔴 Deactivated'}: {p['name']}")
                await q.message.edit_reply_markup(reply_markup=admin_prod_kb(pid, p["active"]))
            return

        if data.startswith("adm_delprod_"):
            if not is_admin(uid):
                return
            pid = int(data.split("_")[-1])
            p   = get_product(pid)
            if not p:
                await q.answer("Already deleted.", show_alert=True)
                return
            delete_product(pid)
            await q.message.edit_text(
                f"🗑 *Product Deleted:* {p['name']}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="adm_products")]])
            )
            return

        # ── Admin Orders ────────────────────────────────────────────────────────
        if data == "adm_orders":
            if not is_admin(uid):
                return
            await show_orders_list(q.message, 0, "all", cur, edit=True)
            return

        if data.startswith("adm_orders_page_"):
            if not is_admin(uid):
                return
            parts  = data.split("_")
            page   = int(parts[3])
            status = parts[4] if len(parts) > 4 else "all"
            await show_orders_list(q.message, page, status, cur, edit=True)
            return

        if data.startswith("adm_orders_filter_"):
            if not is_admin(uid):
                return
            status = data.replace("adm_orders_filter_", "")
            await show_orders_list(q.message, 0, status, cur, edit=True)
            return

        if data.startswith("adm_vieworder_"):
            if not is_admin(uid):
                return
            code  = data.replace("adm_vieworder_", "")
            order = order_get(code)
            if not order:
                await q.answer("❌ Order not found.", show_alert=True)
                return
            items = json.loads(order["items_json"])
            lines = "\n".join(f"  • {it['name']} ×{it['qty']} — {cur}{it['price']:.2f}" for it in items)
            proof = f"\n📎 Proof: {str(order['payment_proof'])[:80]}" if order.get("payment_proof") else ""
            note  = f"\n📝 Note: {order['admin_note']}" if order.get("admin_note") else ""
            await q.message.edit_text(
                f"*📋 Order Details*\n{'━' * 28}\n\n"
                f"ID: `{order['order_code']}`\n"
                f"User: `{order['user_id']}`\n"
                f"Items:\n{lines}\n\n"
                f"Subtotal: {cur}{order['subtotal']:.2f}\n"
                f"Discount: −{cur}{order['discount']:.2f}\n"
                f"*Total: {cur}{order['total']:.2f}*\n\n"
                f"Payment: {order['payment_method'] or '—'}\n"
                f"Status: {status_lbl(order['status'])}\n"
                f"Date: {order['created_at'][:16]}"
                f"{proof}{note}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=order_admin_kb(code, order["status"])
            )
            return

        # ── Order Actions ───────────────────────────────────────────────────────
        if data.startswith("ord_paid_"):
            if not is_admin(uid):
                return
            code = data[9:]
            order_update(code, status="paid")
            await q.message.edit_reply_markup(reply_markup=order_admin_kb(code, "paid"))
            await q.answer("✅ Marked as Paid")
            await notify_user_order_status(context, code,
                f"💰 *Your order `{code}` is marked as paid!*\n\nAdmin will review and deliver soon.")
            return

        if data.startswith("ord_approve_"):
            if not is_admin(uid):
                return
            code = data[12:]
            order_update(code, status="approved")
            if cfg("auto_deliver", "0") == "1":
                delivered = await deliver_order(context, code)
                if delivered:
                    await q.answer("✅ Approved & Delivered!")
                    await q.message.edit_reply_markup(reply_markup=order_admin_kb(code, "delivered"))
                else:
                    await q.answer("✅ Approved (delivery failed — check manually)")
            else:
                await q.answer("✅ Order Approved!")
                await q.message.edit_reply_markup(reply_markup=order_admin_kb(code, "approved"))
                await notify_user_order_status(context, code,
                    f"✅ *Order `{code}` Approved!*\n\nYour order has been approved. Delivery incoming!")
            return

        if data.startswith("ord_deliver_"):
            if not is_admin(uid):
                return
            code      = data[12:]
            delivered = await deliver_order(context, code)
            if delivered:
                await q.answer("📦 Delivered!")
                await q.message.edit_reply_markup(reply_markup=order_admin_kb(code, "delivered"))
            else:
                await q.answer("❌ Delivery failed or already delivered.", show_alert=True)
            return

        if data.startswith("ord_cancel_"):
            if not is_admin(uid):
                return
            code = data[11:]
            order_update(code, status="cancelled")
            await q.message.edit_reply_markup(reply_markup=order_admin_kb(code, "cancelled"))
            await q.answer("❌ Order Cancelled")
            await notify_user_order_status(context, code,
                f"❌ *Order `{code}` has been cancelled.*\n\nContact support if you think this is a mistake.")
            return

        if data.startswith("ord_refund_"):
            if not is_admin(uid):
                return
            code = data[11:]
            order_update(code, status="refunded")
            await q.answer("↩️ Marked as Refunded")
            await notify_user_order_status(context, code,
                f"↩️ *Order `{code}` has been refunded.*\n\nPlease allow 3-5 business days for processing.")
            return

        # ── Payment Methods ─────────────────────────────────────────────────────
        if data == "adm_payments":
            if not is_admin(uid):
                return
            await q.message.edit_text(
                "*💳 Payment Methods*\n\nManage your payment options:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=payment_methods_kb()
            )
            return

        if data.startswith("adm_del_pm_"):
            if not is_admin(uid):
                return
            mid = int(data.split("_")[-1])
            try:
                con = db()
                con.execute("DELETE FROM payment_methods WHERE id=?", (mid,))
                con.commit()
                con.close()
            except Exception as e:
                logger.error(f"del_pm error: {e}")
            await q.message.edit_text(
                "*💳 Payment Methods*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=payment_methods_kb()
            )
            await q.answer("🗑 Deleted")
            return

        if data.startswith("adm_toggle_pm_"):
            if not is_admin(uid):
                return
            mid = int(data.split("_")[-1])
            try:
                con = db()
                con.execute("UPDATE payment_methods SET active = 1 - active WHERE id=?", (mid,))
                con.commit()
                con.close()
            except Exception as e:
                logger.error(f"toggle_pm error: {e}")
            await q.message.edit_reply_markup(reply_markup=payment_methods_kb())
            await q.answer("🔄 Toggled")
            return

        # ── Coupons ─────────────────────────────────────────────────────────────
        if data == "adm_coupons":
            if not is_admin(uid):
                return
            await q.message.edit_text(
                "*🎟 Coupons*\n\nManage discount coupons:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=coupons_panel_kb()
            )
            return

        if data.startswith("adm_coupon_view_"):
            if not is_admin(uid):
                return
            cid = int(data.split("_")[-1])
            try:
                con = db()
                c   = con.execute("SELECT * FROM coupons WHERE id=?", (cid,)).fetchone()
                con.close()
            except Exception:
                c = None
            if not c:
                await q.answer("Not found.", show_alert=True)
                return
            disc_txt = f"{c['discount']}%" if c["type"] == "percent" else f"{cur}{c['discount']}"
            status   = "🟢 Active" if c["active"] else "🔴 Inactive"
            await q.message.edit_text(
                f"*🎟 Coupon: {c['code']}*\n{'━' * 26}\n\n"
                f"Discount: {disc_txt}\n"
                f"Type: {c['type'].capitalize()}\n"
                f"Uses left: {c['uses_left']}\n"
                f"Status: {status}\n"
                f"Created: {c['created_at'][:16]}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=coupon_action_kb(cid, c["active"])
            )
            return

        if data.startswith("adm_coupon_toggle_"):
            if not is_admin(uid):
                return
            cid = int(data.split("_")[-1])
            toggle_coupon(cid)
            await q.answer("🔄 Toggled")
            await q.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("« Back", callback_data="adm_coupons")
            ]]))
            return

        if data.startswith("adm_coupon_del_"):
            if not is_admin(uid):
                return
            cid = int(data.split("_")[-1])
            delete_coupon(cid)
            await q.message.edit_text(
                "🗑 Coupon deleted.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="adm_coupons")]])
            )
            return

        # ── Stats ────────────────────────────────────────────────────────────────
        if data == "adm_stats":
            if not is_admin(uid):
                return
            s   = get_stats()
            top = s.get("top_product")
            top_txt = f"{top['name']} ({top['sales']} sales)" if top else "—"
            await q.message.edit_text(
                f"*📊 Statistics*\n{'━' * 30}\n\n"
                f"👥 Users: *{s.get('users', 0)}*  🚫 Banned: *{s.get('banned', 0)}*\n"
                f"📦 Products: *{s.get('products', 0)}*  🗂 Cats: *{s.get('cats', 0)}*\n"
                f"🎟 Active Coupons: *{s.get('coupons', 0)}*\n\n"
                f"*📋 Orders:* {s.get('orders', 0)}\n"
                f"⏳ Pending: {s.get('pending', 0)}  💰 Paid: {s.get('paid', 0)}\n"
                f"✅ Approved: {s.get('approved', 0)}  📦 Delivered: {s.get('delivered', 0)}\n"
                f"❌ Cancelled: {s.get('cancelled', 0)}\n\n"
                f"*📅 Today:* {s.get('today_orders', 0)} orders — {cur}{s.get('today_revenue', 0):.2f}\n"
                f"*💵 Total Revenue:* {cur}{s.get('revenue', 0):.2f}\n\n"
                f"🏆 Top Product: {top_txt}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="adm_back")]])
            )
            return

        # ── Users ────────────────────────────────────────────────────────────────
        if data == "adm_users":
            if not is_admin(uid):
                return
            await q.message.edit_text("*👥 User Management*", parse_mode=ParseMode.MARKDOWN, reply_markup=users_panel_kb())
            return

        if data.startswith("adm_users_list_"):
            if not is_admin(uid):
                return
            page   = int(data.split("_")[-1])
            limit  = 8
            offset = page * limit
            users  = all_users()
            chunk  = users[offset:offset+limit]
            btns   = []
            for u in chunk:
                b = "🚫" if u["banned"] else "👤"
                btns.append([InlineKeyboardButton(
                    f"{b} {u['full_name'] or u['username'] or str(u['user_id'])} [{u['user_id']}]",
                    callback_data=f"adm_user_view_{u['user_id']}"
                )])
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"adm_users_list_{page-1}"))
            total_pages = max(1, (len(users) + limit - 1) // limit)
            nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
            if (page+1)*limit < len(users):
                nav.append(InlineKeyboardButton("Next ▶", callback_data=f"adm_users_list_{page+1}"))
            if nav:
                btns.append(nav)
            btns.append([InlineKeyboardButton("« Back", callback_data="adm_users")])
            await q.message.edit_text(
                f"*👥 Users* ({len(users)} total)",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(btns)
            )
            return

        if data.startswith("adm_user_view_"):
            if not is_admin(uid):
                return
            tuid = int(data.split("_")[-1])
            u    = get_user(tuid)
            ords = user_orders(tuid)
            total_spent = sum(o["total"] for o in ords if o["status"] in ("paid", "delivered", "approved"))
            ban_lbl = "🔴 Unban User" if (u and u["banned"]) else "🚫 Ban User"
            ban_cb  = f"adm_unban_{tuid}" if (u and u["banned"]) else f"adm_ban_{tuid}"
            await q.message.edit_text(
                f"*👤 User Profile*\n{'━' * 26}\n\n"
                f"ID: `{tuid}`\n"
                f"Name: {u['full_name'] if u else '—'}\n"
                f"Username: @{u['username'] if u else '—'}\n"
                f"Status: {'🚫 Banned' if u and u['banned'] else '✅ Active'}\n"
                f"Joined: {u['joined_at'][:10] if u else '—'}\n\n"
                f"📋 Orders: {len(ords)}\n"
                f"💵 Total Spent: {cur}{total_spent:.2f}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(ban_lbl, callback_data=ban_cb)],
                    [InlineKeyboardButton("💬 Send Message", callback_data=f"adm_msguser_{tuid}")],
                    [InlineKeyboardButton("📋 Orders", callback_data=f"adm_user_orders_{tuid}")],
                    [InlineKeyboardButton("« Back", callback_data="adm_users")],
                ])
            )
            return

        if data.startswith("adm_ban_"):
            if not is_admin(uid):
                return
            tuid = int(data.split("_")[-1])
            try:
                con = db()
                con.execute("UPDATE users SET banned=1 WHERE user_id=?", (tuid,))
                con.commit()
                con.close()
                await q.answer(f"🚫 Banned user {tuid}")
            except Exception as e:
                logger.error(f"adm_ban error: {e}")
            return

        if data.startswith("adm_unban_"):
            if not is_admin(uid):
                return
            tuid = int(data.split("_")[-1])
            try:
                con = db()
                con.execute("UPDATE users SET banned=0 WHERE user_id=?", (tuid,))
                con.commit()
                con.close()
                await q.answer(f"✅ Unbanned user {tuid}")
            except Exception as e:
                logger.error(f"adm_unban error: {e}")
            return

        if data.startswith("adm_user_orders_"):
            if not is_admin(uid):
                return
            tuid = int(data.split("_")[-1])
            ords = user_orders(tuid)
            if not ords:
                await q.answer("No orders.", show_alert=True)
                return
            btns = [[InlineKeyboardButton(
                f"{status_emoji(o['status'])} {o['order_code']} — {cur}{o['total']:.2f}",
                callback_data=f"adm_vieworder_{o['order_code']}"
            )] for o in ords[:10]]
            btns.append([InlineKeyboardButton("« Back", callback_data=f"adm_user_view_{tuid}")])
            await q.message.edit_text(
                f"*📋 Orders for user {tuid}*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(btns)
            )
            return

        if data.startswith("adm_users_banned"):
            if not is_admin(uid):
                return
            try:
                con    = db()
                banned = con.execute("SELECT * FROM users WHERE banned=1").fetchall()
                con.close()
            except Exception:
                banned = []
            if not banned:
                await q.answer("No banned users.", show_alert=True)
                return
            btns = [[InlineKeyboardButton(
                f"🚫 {u['full_name'] or u['username'] or str(u['user_id'])}",
                callback_data=f"adm_user_view_{u['user_id']}"
            )] for u in banned]
            btns.append([InlineKeyboardButton("« Back", callback_data="adm_users")])
            await q.message.edit_text(
                f"*🚫 Banned Users ({len(banned)})*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(btns)
            )
            return

        # ── Categories ────────────────────────────────────────────────────────────
        if data == "adm_categories":
            if not is_admin(uid):
                return
            await q.message.edit_text(
                "*🗂 Category Management*\n\nManage product categories:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=categories_panel_kb()
            )
            return

        if data.startswith("adm_cat_view_"):
            if not is_admin(uid):
                return
            cid = int(data.split("_")[-1])
            cat = get_category_by_id(cid)
            if not cat:
                await q.answer("Not found.", show_alert=True)
                return
            try:
                con    = db()
                n_prod = con.execute("SELECT COUNT(*) FROM products WHERE category=? AND active=1", (cat["name"],)).fetchone()[0]
                con.close()
            except Exception:
                n_prod = 0
            status = "🟢 Active" if cat["active"] else "🔴 Inactive"
            await q.message.edit_text(
                f"*🗂 Category: {cat['icon']} {cat['name']}*\n{'━' * 26}\n\n"
                f"Icon: {cat['icon']}\n"
                f"Status: {status}\n"
                f"Products: {n_prod}\n"
                f"Created: {cat['created_at'][:10]}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=category_action_kb(cid, cat["active"])
            )
            return

        if data.startswith("adm_cat_toggle_"):
            if not is_admin(uid):
                return
            cid   = int(data.split("_")[-1])
            cat   = get_category_by_id(cid)
            if not cat:
                await q.answer("Not found.", show_alert=True)
                return
            new_a = 1 - cat["active"]
            update_category(cid, active=new_a)
            await q.answer(f"{'🟢 Activated' if new_a else '🔴 Deactivated'}: {cat['name']}")
            await q.message.edit_reply_markup(reply_markup=categories_panel_kb())
            return

        if data.startswith("adm_cat_delete_"):
            if not is_admin(uid):
                return
            cid = int(data.split("_")[-1])
            cat = get_category_by_id(cid)
            delete_category(cid)
            name = cat["name"] if cat else "Unknown"
            await q.message.edit_text(
                f"🗑 Category *{name}* deleted.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="adm_categories")]])
            )
            return

        # ── Settings ─────────────────────────────────────────────────────────────
        if data == "adm_settings":
            if not is_admin(uid):
                return
            await q.message.edit_text(
                "*⚙️ Settings*\n\nConfigure your store:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=settings_kb()
            )
            return

        if data == "adm_tog_auto_deliver":
            if not is_admin(uid):
                return
            cur_v = cfg("auto_deliver", "0")
            set_cfg("auto_deliver", "0" if cur_v == "1" else "1")
            await q.message.edit_reply_markup(reply_markup=settings_kb())
            await q.answer(f"Auto-Deliver: {'✅ ON' if cur_v == '0' else '❌ OFF'}")
            return

        if data == "adm_tog_require_proof":
            if not is_admin(uid):
                return
            cur_v = cfg("require_proof", "1")
            set_cfg("require_proof", "0" if cur_v == "1" else "1")
            await q.message.edit_reply_markup(reply_markup=settings_kb())
            await q.answer(f"Require Proof: {'✅ ON' if cur_v == '0' else '❌ OFF'}")
            return

        if data == "adm_tog_maintenance":
            if not is_admin(uid):
                return
            cur_v = cfg("maintenance_mode", "0")
            set_cfg("maintenance_mode", "0" if cur_v == "1" else "1")
            await q.message.edit_reply_markup(reply_markup=settings_kb())
            await q.answer(f"Maintenance: {'🔧 ON' if cur_v == '0' else '✅ OFF'}")
            return

        # ── Force channel ─────────────────────────────────────────────────────────
        if data == "adm_force_channel":
            if not is_admin(uid):
                return
            cur_ch = cfg("force_join_channel", "").strip()
            status_line = f"📢 Current channel: `{cur_ch}`" if cur_ch else "❌ Force join is currently *disabled*"
            await q.message.edit_text(
                f"*🔗 Force Join Channel*\n{'━'*26}\n\n"
                f"{status_line}\n\n"
                "⚠️ *Requirements for force join to work:*\n"
                "1. Add your bot as an **Admin** of the channel\n"
                "2. Grant the bot **'Add Members'** permission\n"
                "3. Set the channel username (e.g. `@mychannel`) or numeric ID below\n\n"
                "Users must join the channel before they can use the bot.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=force_channel_kb()
            )
            return

        if data == "adm_disable_force_channel":
            if not is_admin(uid):
                return
            set_cfg("force_join_channel", "")
            await q.message.edit_reply_markup(reply_markup=force_channel_kb())
            await q.answer("✅ Force Join Disabled")
            return

        # ── Broadcast ─────────────────────────────────────────────────────────────
        if data == "adm_broadcast":
            if not is_admin(uid):
                return
            await q.message.edit_text(
                "*📢 Broadcast*\n\nSend the message you want to broadcast to ALL users:",
                parse_mode=ParseMode.MARKDOWN
            )
            return

    except BadRequest as e:
        logger.warning(f"BadRequest in callback_handler: {e}")
    except TelegramError as e:
        logger.error(f"TelegramError in callback_handler: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in callback_handler (data={data}): {e}")


# ════════════════════════════════════════════════════════════════════════════
#  TEXT MESSAGE HANDLER
# ════════════════════════════════════════════════════════════════════════════

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    txt = update.message.text.strip()
    uid = update.effective_user.id
    try:
        if txt == "🛍 Browse Products":
            await cmd_products(update, context)
        elif txt == "🔍 Search":
            await search_entry(update, context)
        elif txt == "🛒 My Cart":
            await cmd_cart(update, context)
        elif txt == "📦 My Orders":
            await cmd_orders(update, context)
        elif txt == "🎟 Coupon Code":
            await update.message.reply_text(
                "🎟 Apply a coupon:\n`/coupon YOUR_CODE`\n\nExample: `/coupon SAVE20`",
                parse_mode=ParseMode.MARKDOWN
            )
        elif txt == "📞 Support":
            await cmd_support(update, context)
        elif txt == "❓ Help":
            await cmd_help(update, context)
        elif txt == "⚙️ Admin Panel":
            if is_admin(uid):
                await cmd_admin(update, context)
            else:
                await update.message.reply_text("⛔ Admin only.")
        else:
            await update.message.reply_text(
                "Use the menu buttons or /help for commands.",
                reply_markup=main_kb(uid)
            )
    except Exception as e:
        logger.error(f"text_handler error: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  GLOBAL ERROR HANDLER
# ════════════════════════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if isinstance(err, TelegramError):
        logger.warning(f"TelegramError: {err}")
    else:
        logger.error(f"Unhandled error: {err}", exc_info=True)


# ════════════════════════════════════════════════════════════════════════════
#  FLASK HEALTH ENDPOINT
# ════════════════════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="bn">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Premium Store Bot Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; }
  .header { background: linear-gradient(135deg, #1a1f2e, #2d3561); padding: 20px 30px; border-bottom: 1px solid #2d3748; }
  .header h1 { font-size: 1.8rem; font-weight: 700; color: #63b3ed; }
  .header p { color: #a0aec0; margin-top: 4px; }
  .status-badge { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; margin-top: 8px; }
  .status-running { background: #1a4731; color: #68d391; border: 1px solid #276749; }
  .container { max-width: 1200px; margin: 0 auto; padding: 30px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 30px; }
  .card { background: #1a1f2e; border: 1px solid #2d3748; border-radius: 12px; padding: 24px; }
  .card-title { font-size: 0.85rem; color: #718096; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
  .card-value { font-size: 2rem; font-weight: 700; color: #e2e8f0; }
  .card-icon { font-size: 2rem; margin-bottom: 10px; }
  .card.blue { border-color: #2b6cb0; }
  .card.green { border-color: #276749; }
  .card.purple { border-color: #553c9a; }
  .card.orange { border-color: #c05621; }
  .section { background: #1a1f2e; border: 1px solid #2d3748; border-radius: 12px; padding: 24px; margin-bottom: 20px; }
  .section h2 { font-size: 1.1rem; font-weight: 600; color: #63b3ed; margin-bottom: 16px; }
  .info-row { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #2d3748; }
  .info-row:last-child { border-bottom: none; }
  .info-label { color: #718096; }
  .info-value { color: #e2e8f0; font-weight: 500; }
  .fix-box { background: #1a2a1a; border: 1px solid #276749; border-radius: 8px; padding: 16px; }
  .fix-box h3 { color: #68d391; font-size: 0.95rem; margin-bottom: 8px; }
  .fix-box p { color: #9ae6b4; font-size: 0.85rem; line-height: 1.6; }
  .code { background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 12px; font-family: monospace; font-size: 0.8rem; color: #e6edf3; margin-top: 8px; white-space: pre-wrap; }
  .refresh-btn { background: #2b6cb0; color: white; border: none; padding: 8px 18px; border-radius: 8px; cursor: pointer; font-size: 0.85rem; }
  .refresh-btn:hover { background: #3182ce; }
</style>
</head>
<body>
<div class="header">
  <h1>🛍️ Premium Digital Store Bot</h1>
  <p>Telegram Bot Admin Dashboard</p>
  <span class="status-badge status-running">✅ Bot Running</span>
</div>
<div class="container">
  <div class="grid">
    <div class="card blue">
      <div class="card-icon">👥</div>
      <div class="card-title">Total Users</div>
      <div class="card-value">{{ stats.total_users }}</div>
    </div>
    <div class="card green">
      <div class="card-icon">📦</div>
      <div class="card-title">Total Products</div>
      <div class="card-value">{{ stats.total_products }}</div>
    </div>
    <div class="card purple">
      <div class="card-icon">📋</div>
      <div class="card-title">Total Orders</div>
      <div class="card-value">{{ stats.total_orders }}</div>
    </div>
    <div class="card orange">
      <div class="card-icon">⏳</div>
      <div class="card-title">Pending Orders</div>
      <div class="card-value">{{ stats.pending_orders }}</div>
    </div>
  </div>

  <div class="section">
    <h2>🏪 Shop Information</h2>
    <div class="info-row"><span class="info-label">Shop Name</span><span class="info-value">{{ shop_name }}</span></div>
    <div class="info-row"><span class="info-label">Status</span><span class="info-value" style="color:#68d391">🟢 Online</span></div>
    <div class="info-row"><span class="info-label">Rate Limit</span><span class="info-value">3 msgs/1s → 10s ban</span></div>
    <div class="info-row"><span class="info-label">Database</span><span class="info-value">SQLite (premium_store.db)</span></div>
  </div>

  <div class="section">
    <h2>✅ Bug Fix Applied</h2>
    <div class="fix-box">
      <h3>Fixed: TypeError in run_polling()</h3>
      <p>The original error was caused by passing deprecated timeout arguments to <code>run_polling()</code> in python-telegram-bot v21+.</p>
      <div class="code">TypeError: Application.run_polling() got an unexpected keyword argument 'read_timeout'

# Fixed: Migrated to Flask Webhook mode
# - .updater(None) disables built-in polling
# - Background asyncio loop handles updates
# - Flask /webhook endpoint receives Telegram updates
# - Auto-registers webhook URL via REPLIT_DOMAINS</div>
    </div>
  </div>

  <div class="section">
    <h2>📊 Bot Commands Available</h2>
    <div class="info-row"><span class="info-label">/start</span><span class="info-value">Start the bot</span></div>
    <div class="info-row"><span class="info-label">/products</span><span class="info-value">Browse products</span></div>
    <div class="info-row"><span class="info-label">/cart</span><span class="info-value">View shopping cart</span></div>
    <div class="info-row"><span class="info-label">/orders</span><span class="info-value">View your orders</span></div>
    <div class="info-row"><span class="info-label">/coupon</span><span class="info-value">Apply coupon code</span></div>
    <div class="info-row"><span class="info-label">/admin</span><span class="info-value">Admin panel (admin only)</span></div>
    <div class="info-row"><span class="info-label">/stats</span><span class="info-value">View statistics</span></div>
  </div>

  <div style="text-align:center;padding:20px;color:#4a5568;font-size:0.8rem;">
    <button class="refresh-btn" onclick="location.reload()">🔄 Refresh Stats</button>
    <p style="margin-top:12px;">Premium Digital Store Bot • Auto-refreshes every 30s</p>
  </div>
</div>
<script>setTimeout(() => location.reload(), 30000);</script>
</body>
</html>"""

@flask_app.route("/", methods=["GET"])
def health():
    try:
        s = get_stats()
        html = DASHBOARD_HTML.replace("{{ stats.total_users }}", str(s.get("users", 0))) \
                             .replace("{{ stats.total_products }}", str(s.get("products", 0))) \
                             .replace("{{ stats.total_orders }}", str(s.get("orders", 0))) \
                             .replace("{{ stats.pending_orders }}", str(s.get("pending", 0))) \
                             .replace("{{ shop_name }}", str(cfg("shop_name") or "Premium Store"))
        from flask import Response
        return Response(html, mimetype="text/html")
    except Exception as e:
        return f"<h1>Bot Dashboard</h1><p>Error: {e}</p>", 500

@flask_app.route("/api/stats", methods=["GET"])
def api_stats():
    try:
        s = get_stats()
        return {
            "status": "running",
            "shop":   cfg("shop_name"),
            "stats":  {k: (v if not isinstance(v, sqlite3.Row) else dict(v)) for k, v in s.items()}
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@flask_app.route("/webhook", methods=["POST"])
def webhook():
    global tg_app, _bg_loop
    if tg_app is None or _bg_loop is None:
        return "not ready", 503
    data = flask_request.get_json(force=True, silent=True)
    if data:
        try:
            update = Update.de_json(data, tg_app.bot)
            future = asyncio.run_coroutine_threadsafe(
                tg_app.process_update(update), _bg_loop
            )
            future.result(timeout=30)
        except Exception as e:
            logger.error(f"Webhook processing error: {e}")
    return "ok", 200


# ════════════════════════════════════════════════════════════════════════════
#  APPLICATION SETUP
# ════════════════════════════════════════════════════════════════════════════

def build_app() -> Application:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .updater(None)          # Disable built-in updater — Flask handles webhook
        .concurrent_updates(True)
        .build()
    )

    app.add_error_handler(error_handler)

    # ── Commands ────────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",          cmd_start))
    app.add_handler(CommandHandler("help",           cmd_help))
    app.add_handler(CommandHandler("products",       cmd_products))
    app.add_handler(CommandHandler("cart",           cmd_cart))
    app.add_handler(CommandHandler("orders",         cmd_orders))
    app.add_handler(CommandHandler("coupon",         cmd_coupon))
    app.add_handler(CommandHandler("support",        cmd_support))
    app.add_handler(CommandHandler("admin",          cmd_admin))
    app.add_handler(CommandHandler("stats",          cmd_stats))
    app.add_handler(CommandHandler("ban",            cmd_ban))
    app.add_handler(CommandHandler("unban",          cmd_unban))
    app.add_handler(CommandHandler("orders_pending", cmd_orders_pending))
    app.add_handler(CommandHandler("maintenance",    cmd_maintenance))

    # ── Search Conversation ─────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("search", search_entry),
            MessageHandler(filters.Regex("^🔍 Search$"), search_entry),
        ],
        states={SEARCH_Q: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_do)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="search",
        per_user=True,
        per_chat=True,
    ))

    # ── Add Product Conversation ─────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("addproduct", ap_entry),
            CallbackQueryHandler(ap_entry, pattern="^adm_addproduct$"),
        ],
        states={
            AP_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_name)],
            AP_DESC:    [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_desc)],
            AP_PRICE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_price)],
            AP_CAT:     [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_cat)],
            AP_FILE:    [MessageHandler(filters.ALL & ~filters.COMMAND,  ap_file)],
            AP_PREVIEW: [MessageHandler(filters.ALL & ~filters.COMMAND,  ap_preview)],
            AP_STOCK:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_stock)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="add_product",
        per_user=True,
        per_chat=True,
    ))

    # ── Edit Product Field Conversation ──────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(ep_entry, pattern=r"^adm_ef_\w+_\d+$")],
        states={EP_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ep_value)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="edit_product_field",
        per_user=True,
        per_chat=True,
    ))

    # ── Re-file Conversation ─────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(refile_entry, pattern=r"^adm_refile_\d+$")],
        states={REFILE_FILE: [MessageHandler(filters.ALL & ~filters.COMMAND, refile_do)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="refile",
        per_user=True,
        per_chat=True,
    ))

    # ── Re-preview Conversation ──────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(repreview_entry, pattern=r"^adm_repreview_\d+$")],
        states={REPREVIEW_FILE: [MessageHandler(filters.ALL & ~filters.COMMAND, repreview_do)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="repreview",
        per_user=True,
        per_chat=True,
    ))

    # ── Add Coupon Conversation ──────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(cp_entry, pattern="^adm_addcoupon$")],
        states={
            CP_CODE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, cp_code)],
            CP_DISC:  [MessageHandler(filters.TEXT & ~filters.COMMAND, cp_disc)],
            CP_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, cp_limit)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="add_coupon",
        per_user=True,
        per_chat=True,
    ))

    # ── Add Payment Method Conversation ──────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(pm_add_entry, pattern="^adm_addpm$")],
        states={
            PM_NAME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, pm_name)],
            PM_DETAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, pm_detail)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="add_payment_method",
        per_user=True,
        per_chat=True,
    ))

    # ── Edit Payment Method Conversation ─────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(pm_edit_entry, pattern=r"^adm_edit_pm_\d+$")],
        states={
            PM_EDIT_NAME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, pm_edit_name)],
            PM_EDIT_DETAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, pm_edit_detail)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="edit_payment_method",
        per_user=True,
        per_chat=True,
    ))

    # ── Add Category Conversation ────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(cat_add_entry, pattern="^adm_cat_add$")],
        states={
            CAT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, cat_name)],
            CAT_ICON: [MessageHandler(filters.TEXT & ~filters.COMMAND, cat_icon_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="add_category",
        per_user=True,
        per_chat=True,
    ))

    # ── Edit Category Name Conversation ──────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(cat_editname_entry, pattern=r"^adm_cat_editname_\d+$")],
        states={CAT_EDIT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, cat_editname_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="edit_cat_name",
        per_user=True,
        per_chat=True,
    ))

    # ── Edit Category Icon Conversation ──────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(cat_editicon_entry, pattern=r"^adm_cat_editicon_\d+$")],
        states={CAT_EDIT_ICON: [MessageHandler(filters.TEXT & ~filters.COMMAND, cat_editicon_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="edit_cat_icon",
        per_user=True,
        per_chat=True,
    ))

    # ── Settings Conversation ─────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(
            set_entry,
            pattern=r"^adm_set_(shop_name|welcome_msg|support_msg|footer_msg|currency|order_timeout|tax|force_label)$"
        )],
        states={SET_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_value)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="settings_edit",
        per_user=True,
        per_chat=True,
    ))

    # ── Force Channel Conversation ────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(force_channel_entry, pattern="^adm_set_force_channel$")],
        states={FORCE_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, force_channel_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="force_channel",
        per_user=True,
        per_chat=True,
    ))

    # ── Broadcast Conversation ────────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("broadcast", bc_entry),
            CallbackQueryHandler(bc_entry, pattern="^adm_broadcast$"),
        ],
        states={
            BC_MSG: [MessageHandler(filters.ALL & ~filters.COMMAND, bc_msg)],
            BC_OK:  [MessageHandler(filters.TEXT & ~filters.COMMAND, bc_ok)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="broadcast",
        per_user=True,
        per_chat=True,
    ))

    # ── Order Note Conversation ───────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(order_note_entry, pattern=r"^ord_note_")],
        states={ORDER_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_note_save)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="order_note",
        per_user=True,
        per_chat=True,
    ))

    # ── Admin Reply Conversation ──────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_reply_entry, pattern=r"^ord_msg_")],
        states={AR_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reply_send)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="admin_reply",
        per_user=True,
        per_chat=True,
    ))

    # ── Payment Proof Conversation ────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(pay_proof_entry, pattern=r"^pay_\d+_ORD-")],
        states={PAY_PROOF: [MessageHandler(filters.ALL & ~filters.COMMAND, pay_proof_receive)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        name="payment_proof",
        per_user=True,
        per_chat=True,
    ))

    # ── Callback Query Handler (catch-all) ────────────────────────────────────
    app.add_handler(CallbackQueryHandler(callback_handler))

    # ── Text Message Handler ──────────────────────────────────────────────────
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    return app


# ════════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════════

def _run_bg_loop(loop: asyncio.AbstractEventLoop):
    """Run the asyncio event loop in a background thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def _setup_bot(app: "Application"):
    """Initialize bot and register webhook with Telegram."""
    await app.initialize()
    await app.start()

    # Determine webhook URL
    webhook_url = WEBHOOK_URL
    if not webhook_url:
        replit_domains = os.environ.get("REPLIT_DOMAINS", "")
        if replit_domains:
            domain = replit_domains.split(",")[0].strip()
            webhook_url = f"https://{domain}/webhook"

    if webhook_url:
        await app.bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=list(Update.ALL_TYPES),
        )
        logger.info(f"🌐 Webhook registered: {webhook_url}")
    else:
        logger.warning("⚠️  No WEBHOOK_URL or REPLIT_DOMAINS found — webhook NOT registered")


def main():
    global tg_app, _bg_loop

    ensure_tables(DB_PATH)
    logger.info(f"✅ Database ready: {DB_PATH}")
    logger.info(f"⚡ Rate limit: {RATE_LIMIT_MESSAGES} msgs/{RATE_LIMIT_WINDOW}s → {RATE_LIMIT_BAN_SEC}s ban")

    # Build the Telegram application (updater disabled — Flask handles webhook)
    tg_app = build_app()

    # Start a dedicated asyncio event loop in a background thread
    _bg_loop = asyncio.new_event_loop()
    bg_thread = threading.Thread(target=_run_bg_loop, args=(_bg_loop,), daemon=True)
    bg_thread.start()

    # Initialize bot and register webhook (blocks until done)
    future = asyncio.run_coroutine_threadsafe(_setup_bot(tg_app), _bg_loop)
    future.result(timeout=30)

    # Flask is the primary server — handles webhook + dashboard
    logger.info(f"🚀 Bot started in Flask webhook mode on port {PORT}!")
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
