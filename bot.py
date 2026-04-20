"""
╔══════════════════════════════════════════════════╗
║       GUARD BOT — Professional Edition v2.0      ║
║   Owner → Premium Users → Group Protection       ║
║   Storage: MongoDB Atlas | 24/7 on Render        ║
╚══════════════════════════════════════════════════╝
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import uvicorn

from pymongo import MongoClient, DESCENDING
from telegram import (
    Update, ChatMember,
    InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.ext import (
    Application, CommandHandler, ChatMemberHandler,
    CallbackQueryHandler, MessageHandler,
    ContextTypes, filters,
)
from telegram.error import TelegramError

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Environment ──────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID  = int(os.environ.get("OWNER_ID", "0"))
MONGO_URI = os.environ.get("MONGO_URI", "")
PORT      = int(os.environ.get("PORT", 8080))

# ── In-memory appeal conversation state ──────────────────────
appeal_waiting: dict[int, int] = {}   # {user_id: group_id}


# ══════════════════════════════════════════════════════════════
# MONGODB
# ══════════════════════════════════════════════════════════════

def connect_mongo():
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is not set!")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    logger.info("✅ MongoDB connected!")
    return client


mongo_client = connect_mongo()
mdb          = mongo_client["guard_bot"]

col_config    = mdb["config"]
col_premium   = mdb["premium_users"]
col_groups    = mdb["groups"]
col_bans      = mdb["banned_log"]
col_appeals   = mdb["appeals"]
col_bot_chats = mdb["bot_chats"]   # all chats where bot is admin


# ── Config ────────────────────────────────────────────────────

def get_config() -> dict:
    return col_config.find_one({"_id": "main"}) or {}

def set_cfg(key: str, value):
    col_config.update_one({"_id": "main"}, {"$set": {key: value}}, upsert=True)


# ── Premium ───────────────────────────────────────────────────

def get_premium(uid: int) -> dict | None:
    return col_premium.find_one({"_id": uid})

def set_premium(uid: int, data: dict):
    col_premium.update_one({"_id": uid}, {"$set": data}, upsert=True)

def del_premium(uid: int):
    col_premium.delete_one({"_id": uid})

def all_premium() -> list:
    return list(col_premium.find())


# ── Groups ────────────────────────────────────────────────────

def get_group(gid: int) -> dict | None:
    return col_groups.find_one({"_id": gid})

def set_group(gid: int, data: dict):
    col_groups.update_one({"_id": gid}, {"$set": data}, upsert=True)

def all_groups() -> list:
    return list(col_groups.find())

def inc_ban(gid: int):
    col_groups.update_one({"_id": gid}, {"$inc": {"ban_count": 1}}, upsert=True)

def track_member_join(gid: int, uid: int, name: str):
    col_groups.update_one(
        {"_id": gid},
        {"$set": {f"tracked.{uid}": name}},
        upsert=False,
    )

def track_member_leave(gid: int, uid: int):
    col_groups.update_one(
        {"_id": gid},
        {"$unset": {f"tracked.{uid}": ""}},
        upsert=False,
    )

def get_tracked_members(gid: int) -> dict:
    g = get_group(gid)
    if not g:
        return {}
    return {int(k): v for k, v in g.get("tracked", {}).items()}


# ── Bot Chats (channels/groups where bot is admin) ────────────

def save_bot_chat(cid: int, data: dict):
    col_bot_chats.update_one({"_id": cid}, {"$set": data}, upsert=True)

def remove_bot_chat(cid: int):
    col_bot_chats.delete_one({"_id": cid})

def get_bot_chats(chat_type: str = None) -> list:
    query = {}
    if chat_type:
        query["type"] = chat_type
    return list(col_bot_chats.find(query))


# ── Bans ──────────────────────────────────────────────────────

def log_ban_entry(entry: dict):
    col_bans.insert_one(entry)

def total_bans() -> int:
    return col_bans.count_documents({})


# ── Appeals ───────────────────────────────────────────────────

def create_appeal(uid: int, uname: str, gid: int, gname: str, reason: str):
    col_appeals.update_one(
        {"user_id": uid, "group_id": gid, "status": "pending"},
        {"$set": {
            "user_id": uid, "user_name": uname,
            "group_id": gid, "group_name": gname,
            "reason": reason, "status": "pending",
            "timestamp": now_ts(),
        }},
        upsert=True,
    )

def get_pending_appeals() -> list:
    return list(col_appeals.find({"status": "pending"}).sort("timestamp", DESCENDING))

def resolve_appeal(uid: int, gid: int, status: str):
    col_appeals.update_one(
        {"user_id": uid, "group_id": gid, "status": "pending"},
        {"$set": {"status": status}},
    )

def has_pending_appeal(uid: int, gid: int) -> bool:
    return col_appeals.find_one(
        {"user_id": uid, "group_id": gid, "status": "pending"}
    ) is not None


# ══════════════════════════════════════════════════════════════
# UTILITY
# ══════════════════════════════════════════════════════════════

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def fmt_date(ts) -> str:
    if not ts:
        return "♾️  Lifetime"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d %b %Y")

def fmt_time(ts) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d %b %Y, %H:%M UTC")

def is_owner(uid: int) -> bool:
    if OWNER_ID and uid == OWNER_ID:
        return True
    return get_config().get("bot_owner_id") == uid

def is_premium(uid: int) -> bool:
    if is_owner(uid):
        return True
    doc = get_premium(uid)
    if not doc:
        return False
    exp = doc.get("expires")
    return not (exp and exp < now_ts())

def is_whitelisted(uid: int, gid: int) -> bool:
    g = get_group(gid)
    return g is not None and uid in g.get("whitelist", [])

async def can_manage(context, gid: int, uid: int) -> bool:
    g = get_group(gid)
    if g and g.get("owner_id") == uid:
        return True
    try:
        admins = await context.bot.get_chat_administrators(gid)
        return uid in [a.user.id for a in admins]
    except TelegramError:
        return False


# ══════════════════════════════════════════════════════════════
# FASTAPI APP  (replaces threading keep-alive — same as Protector)
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(web: FastAPI):
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not set!")
        yield
        return

    if OWNER_ID:
        col_config.update_one(
            {"_id": "main"},
            {"$set": {"bot_owner_id": OWNER_ID}},
            upsert=True,
        )
        logger.info(f"Owner synced: {OWNER_ID}")

    tg_app = build_bot_app()
    async with tg_app:
        await tg_app.start()
        await tg_app.updater.start_polling(
            allowed_updates=[
                "message",
                "chat_member",
                "my_chat_member",
                "callback_query",
            ],
            drop_pending_updates=True,
        )
        logger.info("✅ Guard Bot polling started!")
        yield
        await tg_app.updater.stop()
        await tg_app.stop()
        logger.info("Guard Bot stopped.")


web_app = FastAPI(lifespan=lifespan)


@web_app.get("/")
async def root():
    prem  = col_premium.count_documents({})
    grps  = col_groups.count_documents({})
    bans  = total_bans()
    return PlainTextResponse(f"🛡️ Guard Bot | Premium: {prem} | Groups: {grps} | Bans: {bans}")


@web_app.get("/health")
async def health():
    return PlainTextResponse("OK")


# ══════════════════════════════════════════════════════════════
# LOG CHANNEL
# ══════════════════════════════════════════════════════════════

async def send_log(context, gid: int, text: str):
    g = get_group(gid)
    if not g:
        return
    log_ch = g.get("log_channel_id")
    if not log_ch:
        return
    try:
        await context.bot.send_message(log_ch, text, parse_mode="Markdown")
    except TelegramError:
        pass


# ══════════════════════════════════════════════════════════════
# BAN / UNBAN CORE
# ══════════════════════════════════════════════════════════════

async def do_ban(
    context, uid: int, uname: str,
    gid: int, gname: str, reason: str,
) -> bool:
    if is_whitelisted(uid, gid):
        logger.info(f"Skipped — whitelisted: {uid} in {gid}")
        return False

    appeal_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📩 Submit Appeal", callback_data=f"appeal_start:{gid}")
    ]])
    try:
        await context.bot.send_message(
            uid,
            f"🚫 *You've Been Banned*\n"
            f"┌ 📌 Group: {gname}\n"
            f"└ ❌ Reason: Left *{reason}*\n\n"
            f"_Submit an appeal to get unbanned._",
            parse_mode="Markdown",
            reply_markup=appeal_kb,
        )
    except TelegramError:
        pass

    try:
        await context.bot.ban_chat_member(gid, uid)
    except TelegramError as e:
        logger.error(f"Ban failed {uid} in {gid}: {e}")
        return False

    inc_ban(gid)
    track_member_leave(gid, uid)
    log_ban_entry({
        "user_id": uid, "user_name": uname,
        "group_id": gid, "group_name": gname,
        "reason": reason, "time": now_ts(),
    })

    try:
        await context.bot.send_message(
            gid,
            f"🚫 *Banned:* {uname}\n└ Left *{reason}*",
            parse_mode="Markdown",
        )
    except TelegramError:
        pass

    await send_log(context, gid,
        f"🚫 *Ban* | {uname} (`{uid}`)\n"
        f"└ Left *{reason}* · {fmt_time(now_ts())}"
    )

    logger.info(f"Banned {uid} ({uname}) from {gid}")

    # ── Notify GROUP OWNER (premium user) — not bot owner ───────
    g_data   = get_group(gid)
    notify_id = (g_data.get("owner_id") if g_data else None) or \
                get_config().get("bot_owner_id") or OWNER_ID
    if notify_id:
        unban_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Unban Now", callback_data=f"direct_unban:{uid}:{gid}"),
        ]])
        try:
            await context.bot.send_message(
                notify_id,
                f"🚫 *Ban Alert*\n"
                f"┌ 👤 {uname} (`{uid}`)\n"
                f"├ 📌 {gname}\n"
                f"└ ❌ Left: *{reason}*",
                parse_mode="Markdown",
                reply_markup=unban_kb,
            )
        except TelegramError:
            pass

    return True


async def cb_direct_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    if not is_owner(uid):
        await q.answer("❌  Owner only.", show_alert=True)
        return

    parts  = q.data.split(":")
    target = int(parts[1])
    gid    = int(parts[2])
    g      = get_group(gid)
    gname  = g["name"] if g else str(gid)

    ok = await do_unban(context, target, gid, gname, "Owner")
    if ok:
        await q.edit_message_text(
            q.message.text + "\n\n✅  *User has been unbanned.*",
            parse_mode="Markdown",
        )
    else:
        await q.answer("❌  Unban failed.", show_alert=True)


async def do_unban(
    context, uid: int,
    gid: int, gname: str, by_name: str,
) -> bool:
    try:
        await context.bot.unban_chat_member(gid, uid)
    except TelegramError as e:
        logger.error(f"Unban failed {uid} in {gid}: {e}")
        return False

    try:
        await context.bot.send_message(
            uid,
            f"✅  *Your Ban Has Been Lifted!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌  *Group:* {gname}\n\n"
            f"You can now rejoin — make sure you've joined\n"
            f"all required channels first.",
            parse_mode="Markdown",
        )
    except TelegramError:
        pass

    await send_log(context, gid,
        f"✅  *User Unbanned*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤  *User ID:* `{uid}`\n"
        f"📌  *Group:* {gname}\n"
        f"👮  *By:* {by_name}\n"
        f"🕐  *Time:* {fmt_time(now_ts())}"
    )
    return True


# ══════════════════════════════════════════════════════════════
# APPEAL SYSTEM
# ══════════════════════════════════════════════════════════════

async def cb_appeal_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    gid = int(q.data.split(":")[1])
    g   = get_group(gid)
    if not g:
        await q.edit_message_text("❌  Group not found.")
        return

    if has_pending_appeal(uid, gid):
        await q.edit_message_text(
            "⏳  *Your appeal is already pending.*\n\n"
            "Please wait for the admin to review it.",
            parse_mode="Markdown",
        )
        return

    appeal_waiting[uid] = gid
    await q.edit_message_text(
        f"📩  *Submit Appeal*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌  *Group:* {g['name']}\n\n"
        f"Please type your reason in the next message.\n"
        f"_Example: I accidentally left and have rejoined._",
        parse_mode="Markdown",
    )


async def handle_appeal_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if chat.type != "private" or uid not in appeal_waiting:
        return

    gid    = appeal_waiting.pop(uid)
    reason = update.message.text.strip()[:500]
    g      = get_group(gid)
    uname  = update.effective_user.full_name

    if not g:
        await update.message.reply_text("❌  Group not found.")
        return

    create_appeal(uid, uname, gid, g["name"], reason)

    owner_id = get_config().get("bot_owner_id") or OWNER_ID
    if owner_id:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅  Unban", callback_data=f"appeal_accept:{uid}:{gid}"),
            InlineKeyboardButton("❌  Reject", callback_data=f"appeal_reject:{uid}:{gid}"),
        ]])
        try:
            await context.bot.send_message(
                owner_id,
                f"📩  *New Ban Appeal*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤  *User:* {uname} (`{uid}`)\n"
                f"📌  *Group:* {g['name']}\n"
                f"✍️   *Reason:* {reason}\n"
                f"🕐  *Time:* {fmt_time(now_ts())}",
                parse_mode="Markdown",
                reply_markup=kb,
            )
        except TelegramError:
            pass

    await update.message.reply_text(
        f"✅  *Appeal Submitted!*\n\n"
        f"Your appeal has been sent to the admin for review.\n"
        f"You'll be notified once a decision is made.",
        parse_mode="Markdown",
    )


async def cb_appeal_accept(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    if not is_owner(uid):
        await q.answer("❌  Owner only.", show_alert=True)
        return

    parts  = q.data.split(":")
    target = int(parts[1])
    gid    = int(parts[2])
    g      = get_group(gid)

    resolve_appeal(target, gid, "accepted")
    await do_unban(context, target, gid, g["name"] if g else str(gid), "Owner")

    await q.edit_message_text(
        q.message.text + "\n\n✅  *ACCEPTED — User has been unbanned.*",
        parse_mode="Markdown",
    )

    if g and g.get("owner_id") and g["owner_id"] != uid:
        try:
            await context.bot.send_message(
                g["owner_id"],
                f"✅  Appeal accepted for `{target}` in *{g['name']}* — unbanned.",
                parse_mode="Markdown",
            )
        except TelegramError:
            pass


async def cb_appeal_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    if not is_owner(uid):
        await q.answer("❌  Owner only.", show_alert=True)
        return

    parts  = q.data.split(":")
    target = int(parts[1])
    gid    = int(parts[2])

    resolve_appeal(target, gid, "rejected")

    try:
        await context.bot.send_message(
            target,
            "❌  *Your Appeal Has Been Rejected*\n\n"
            "Your ban remains in place.\n"
            "For further queries, contact the group admin directly.",
            parse_mode="Markdown",
        )
    except TelegramError:
        pass

    await q.edit_message_text(
        q.message.text + "\n\n❌  *REJECTED — User remains banned.*",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════════════════════
# INLINE MENU KEYBOARDS
# ══════════════════════════════════════════════════════════════

def group_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👁️  Monitored Chats", callback_data="gm:chats"),
            InlineKeyboardButton("📋  Whitelist",        callback_data="gm:whitelist"),
        ],
        [
            InlineKeyboardButton("⚙️  Settings",  callback_data="gm:settings"),
            InlineKeyboardButton("📊  Statistics", callback_data="gm:stats"),
        ],
        [
            InlineKeyboardButton("🔍  Check All Members Now", callback_data="gm:check"),
        ],
    ])


def owner_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💎  Premium Users",   callback_data="om:premium"),
            InlineKeyboardButton("📊  Bot Stats",       callback_data="om:stats"),
        ],
        [
            InlineKeyboardButton("📩  Pending Appeals", callback_data="om:appeals"),
            InlineKeyboardButton("📢  Broadcast",       callback_data="om:broadcast"),
        ],
    ])


def back_to_group_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️  Back to Panel", callback_data="gm:back")
    ]])


def back_to_owner_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️  Back to Panel", callback_data="om:back")
    ]])


# ══════════════════════════════════════════════════════════════
# /start  /help
# ══════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if OWNER_ID and get_config().get("bot_owner_id") != OWNER_ID:
        set_cfg("bot_owner_id", OWNER_ID)

    # ── PM deep-link: /start sel_GID → show channel picker ───
    if chat.type == "private" and context.args and context.args[0].startswith("sel_"):
        try:
            gid   = int(context.args[0][4:])
            g     = get_group(gid)
            if not g:
                await update.message.reply_text("❌ Group not found. Run /setup first.")
                return
            if not is_premium(uid) or not await can_manage(context, gid, uid):
                await update.message.reply_text("❌ Admins only.")
                return

            mc         = g.get("monitored_chats", {})
            bot_chats  = get_bot_chats()
            available  = [c for c in bot_chats if str(c["_id"]) not in mc and c["_id"] != gid]

            if not available:
                await update.message.reply_text(
                    f"⚠️ *No chats available*\n\n"
                    f"Add the bot as *admin* to your channels/groups first,\n"
                    f"then come back here.",
                    parse_mode="Markdown",
                )
                return

            buttons = []
            for c in available[:24]:
                icon = "📢" if c.get("type") == "channel" else "👥"
                buttons.append([InlineKeyboardButton(
                    f"{icon} {c['name']}",
                    callback_data=f"pm_pick:{gid}:{c['_id']}",
                )])
            kb = InlineKeyboardMarkup(buttons)

            await update.message.reply_text(
                f"➕ *Select Chat to Monitor*\n"
                f"└ For: *{g['name']}*\n\n"
                f"_Tap to add — member who leaves gets instant ban._",
                parse_mode="Markdown",
                reply_markup=kb,
            )
        except Exception:
            await update.message.reply_text("❌ Invalid link. Use /addchat in your group.")
        return

    if chat.type in ["group", "supergroup"]:
        g  = get_group(chat.id)
        mc = g.get("monitored_chats", {}) if g else {}
        if g:
            await update.message.reply_text(
                f"🛡️ *Guard Bot — Active*\n"
                f"├ 👁️ Monitoring: *{len(mc)}* chat(s)\n"
                f"└ 🚫 Bans: *{g.get('ban_count', 0)}*\n\n"
                f"`/menu` `/mycheck` `/rules`",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(
                "🛡️ *Guard Bot*\n"
                "Not set up here. Use `/setup` _(Premium required)_.",
                parse_mode="Markdown",
            )
        return

    if is_owner(uid):
        prem = all_premium()
        grps = all_groups()
        await update.message.reply_text(
            f"👑 *Guard Bot — Owner Panel*\n"
            f"├ 💎 Premium: *{len(prem)}* users\n"
            f"├ 👥 Groups: *{len(grps)}*\n"
            f"└ 🚫 Total Bans: *{total_bans()}*\n\n"
            f"`/menu` — Control Panel",
            parse_mode="Markdown",
        )
    elif is_premium(uid):
        pdata     = get_premium(uid) or {}
        my_groups = [g for g in all_groups() if g.get("owner_id") == uid]
        await update.message.reply_text(
            f"💎 *Guard Bot — Premium*\n"
            f"├ 📅 Expires: {fmt_date(pdata.get('expires'))}\n"
            f"└ 👥 Groups: *{len(my_groups)}*\n\n"
            f"`/menu` in your group to manage it.",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            "🛡️ *Guard Bot*\n"
            "Auto-bans members who leave required channels.\n\n"
            "`/mycheck` — Your status\n"
            "`/rules` — Group rules\n\n"
            "_Premium required to activate protection._",
            parse_mode="Markdown",
        )


# ══════════════════════════════════════════════════════════════
# /menu  + INLINE CALLBACKS
# ══════════════════════════════════════════════════════════════

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if chat.type in ["group", "supergroup"]:
        g = get_group(chat.id)
        if not g:
            await update.message.reply_text(
                "❌  Bot is not set up here. Use `/setup` first.",
                parse_mode="Markdown",
            )
            return
        if not await can_manage(context, chat.id, uid):
            await update.message.reply_text("❌  Only group admins can access the panel.")
            return
        mc = g.get("monitored_chats", {})
        await update.message.reply_text(
            f"🛡️  *Guard Bot — Control Panel*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌  *Group:* {g['name']}\n"
            f"👁️  *Monitoring:* {len(mc)} chat(s)\n"
            f"🚫  *Total Bans:* {g.get('ban_count', 0)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown",
            reply_markup=group_main_kb(),
        )
    else:
        if not is_owner(uid):
            await update.message.reply_text("❌  Owner only.")
            return
        prem = all_premium()
        grps = all_groups()
        await update.message.reply_text(
            f"👑  *Owner Control Panel*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💎  *Premium Users:* {len(prem)}\n"
            f"👥  *Active Groups:* {len(grps)}\n"
            f"🚫  *Total Bans:* {total_bans()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown",
            reply_markup=owner_main_kb(),
        )


async def cb_group_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    gid  = q.message.chat.id
    page = q.data.split(":")[1]
    await q.answer()

    g = get_group(gid)
    if not g:
        await q.edit_message_text("❌  Group not found.")
        return
    if not await can_manage(context, gid, uid):
        await q.answer("❌  Admins only.", show_alert=True)
        return

    if page == "back":
        mc = g.get("monitored_chats", {})
        await q.edit_message_text(
            f"🛡️  *Guard Bot — Control Panel*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌  *Group:* {g['name']}\n"
            f"👁️  *Monitoring:* {len(mc)} chat(s)\n"
            f"🚫  *Total Bans:* {g.get('ban_count', 0)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown",
            reply_markup=group_main_kb(),
        )

    elif page == "chats":
        mc    = g.get("monitored_chats", {})
        lines = [f"👁️  *Monitored Chats — {g['name']}*\n━━━━━━━━━━━━━━━━━━\n"]
        if mc:
            for cid, info in mc.items():
                icon = "📢" if "channel" in info.get("type", "") else "👥"
                lines.append(f"{icon}  *{info['name']}*\n    ID: `{cid}`")
        else:
            lines.append("_No monitored chats yet._\n\n`/addchat <chat_id>` — Add a chat")
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode="Markdown",
            reply_markup=back_to_group_kb(),
        )

    elif page == "whitelist":
        wl    = g.get("whitelist", [])
        lines = [f"📋  *Whitelist — {g['name']}*\n━━━━━━━━━━━━━━━━━━\n"]
        if wl:
            for wuid in wl:
                lines.append(f"•  `{wuid}`")
        else:
            lines.append("_Whitelist is empty._")
        lines.append("\n`/whitelist <user_id>` — Add\n`/unwhitelist <user_id>` — Remove")
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode="Markdown",
            reply_markup=back_to_group_kb(),
        )

    elif page == "settings":
        sch = g.get("schedule_hours")
        mm  = g.get("min_members")
        lc  = g.get("log_channel_id")
        await q.edit_message_text(
            f"⚙️  *Settings — {g['name']}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📅  Auto-Check: *{'Every ' + str(sch) + 'h' if sch else 'Off'}*\n"
            f"👥  Min Members Alert: *{mm if mm else 'Off'}*\n"
            f"📢  Log Channel: *{'Connected ✅' if lc else 'Not set'}*\n\n"
            f"`/setschedule <hours>` — Set auto-check interval\n"
            f"`/setminmembers <num>` — Set member alert threshold\n"
            f"`/setlog <channel_id>` — Set log channel",
            parse_mode="Markdown",
            reply_markup=back_to_group_kb(),
        )

    elif page == "stats":
        mc   = g.get("monitored_chats", {})
        wl   = g.get("whitelist", [])
        last = g.get("last_auto_check")
        trk  = g.get("tracked", {})
        await q.edit_message_text(
            f"📊  *Statistics — {g['name']}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👁️  Monitored Chats: *{len(mc)}*\n"
            f"🚫  Total Bans: *{g.get('ban_count', 0)}*\n"
            f"📋  Whitelisted: *{len(wl)}* user(s)\n"
            f"👥  Tracked Members: *{len(trk)}*\n"
            f"🕐  Last Auto-Check: *{fmt_time(last)}*\n"
            f"📅  Setup: *{fmt_date(g.get('setup_on'))}*",
            parse_mode="Markdown",
            reply_markup=back_to_group_kb(),
        )

    elif page == "check":
        mc = g.get("monitored_chats", {})
        if not mc:
            await q.answer("❌  No monitored chats set.", show_alert=True)
            return

        await q.edit_message_text(
            "⏳  *Checking all tracked members...*\n_This may take a moment._",
            parse_mode="Markdown",
        )

        members  = get_tracked_members(gid)
        checked  = 0
        banned_c = 0

        for uid_m, uname_m in members.items():
            checked += 1
            for cid, info in mc.items():
                try:
                    cm = await context.bot.get_chat_member(int(cid), uid_m)
                    if cm.status in [ChatMember.LEFT, ChatMember.BANNED]:
                        ok = await do_ban(
                            context, uid_m, uname_m,
                            gid, g["name"], info["name"],
                        )
                        if ok:
                            banned_c += 1
                        break
                except TelegramError:
                    pass

        set_group(gid, {"last_auto_check": now_ts()})
        await q.edit_message_text(
            f"✅  *Check Complete!*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👥  *Checked:* {checked}\n"
            f"🚫  *Banned:* {banned_c}\n"
            f"🕐  *Time:* {fmt_time(now_ts())}",
            parse_mode="Markdown",
            reply_markup=back_to_group_kb(),
        )


async def cb_owner_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    page = q.data.split(":")[1]
    await q.answer()

    if not is_owner(uid):
        await q.answer("❌  Owner only.", show_alert=True)
        return

    if page == "back":
        prem = all_premium()
        grps = all_groups()
        await q.edit_message_text(
            f"👑  *Owner Control Panel*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💎  *Premium Users:* {len(prem)}\n"
            f"👥  *Active Groups:* {len(grps)}\n"
            f"🚫  *Total Bans:* {total_bans()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown",
            reply_markup=owner_main_kb(),
        )

    elif page == "premium":
        prem  = all_premium()
        lines = [f"💎  *Premium Users ({len(prem)})*\n━━━━━━━━━━━━━━━━━━\n"]
        for doc in prem[:15]:
            exp  = doc.get("expires")
            tag  = "⛔  Expired" if (exp and exp < now_ts()) else fmt_date(exp)
            name = doc.get("name", str(doc["_id"]))
            uh   = doc.get("username", "—")
            lines.append(f"•  *{name}* {uh}\n    ID: `{doc['_id']}` | {tag}")
        if not prem:
            lines.append("_No premium users yet._")
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode="Markdown",
            reply_markup=back_to_owner_kb(),
        )

    elif page == "stats":
        prem  = all_premium()
        grps  = all_groups()
        actv  = sum(1 for d in prem if not d.get("expires") or d["expires"] > now_ts())
        lines = [
            f"📊  *Bot Statistics*\n━━━━━━━━━━━━━━━━━━\n",
            f"💎  Premium Users: *{len(prem)}* (active: {actv})",
            f"👥  Groups: *{len(grps)}*",
            f"🚫  Total Bans: *{total_bans()}*\n",
            f"*Group Breakdown:*",
        ]
        for g in grps:
            mc  = g.get("monitored_chats", {})
            sch = g.get("schedule_hours")
            lines.append(
                f"📌  *{g['name']}*\n"
                f"    Chats: {len(mc)} | Bans: {g.get('ban_count', 0)} | "
                f"Schedule: {'Every ' + str(sch) + 'h' if sch else 'Off'}"
            )
        if not grps:
            lines.append("_No groups set up yet._")
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode="Markdown",
            reply_markup=back_to_owner_kb(),
        )

    elif page == "appeals":
        apps = get_pending_appeals()
        if not apps:
            await q.edit_message_text(
                "📩  *Pending Appeals*\n━━━━━━━━━━━━━━━━━━\n\n_No pending appeals._",
                parse_mode="Markdown",
                reply_markup=back_to_owner_kb(),
            )
            return
        await q.edit_message_text(
            f"📩  *{len(apps)} pending appeal(s) — sending now...*",
            parse_mode="Markdown",
            reply_markup=back_to_owner_kb(),
        )
        for ap in apps[:5]:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "✅  Unban", callback_data=f"appeal_accept:{ap['user_id']}:{ap['group_id']}"
                ),
                InlineKeyboardButton(
                    "❌  Reject", callback_data=f"appeal_reject:{ap['user_id']}:{ap['group_id']}"
                ),
            ]])
            try:
                await context.bot.send_message(
                    uid,
                    f"📩  *Appeal*\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"👤  *{ap['user_name']}* (`{ap['user_id']}`)\n"
                    f"📌  *Group:* {ap['group_name']}\n"
                    f"✍️   *Reason:* {ap['reason']}\n"
                    f"🕐  {fmt_time(ap['timestamp'])}",
                    parse_mode="Markdown",
                    reply_markup=kb,
                )
            except TelegramError:
                pass

    elif page == "broadcast":
        await q.edit_message_text(
            "📢  *Broadcast*\n━━━━━━━━━━━━━━━━━━\n\n"
            "Use the command:\n`/broadcast <your message>`\n\n"
            "_Message will be sent to all premium users._",
            parse_mode="Markdown",
            reply_markup=back_to_owner_kb(),
        )


# ══════════════════════════════════════════════════════════════
# OWNER COMMANDS
# ══════════════════════════════════════════════════════════════

async def cmd_addpremium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌  Owner only.")
        return
    if not context.args:
        await update.message.reply_text(
            "📋  *Usage:*\n"
            "`/addpremium <user_id>` — Lifetime\n"
            "`/addpremium <user_id> 30` — 30 days",
            parse_mode="Markdown",
        )
        return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid user ID.")
        return

    days    = int(context.args[1]) if len(context.args) > 1 else None
    expires = now_ts() + days * 86400 if days else None

    try:
        tuser   = await context.bot.get_chat(target)
        uname   = tuser.full_name or str(target)
        uhandle = f"@{tuser.username}" if tuser.username else "—"
    except TelegramError:
        uname, uhandle = str(target), "—"

    set_premium(target, {
        "name": uname, "username": uhandle,
        "expires": expires, "added_on": now_ts(), "added_by": uid,
    })

    exp_text = f"{days} days" if days else "Lifetime"
    await update.message.reply_text(
        f"✅  *Premium Granted!*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤  *User:* {uname} (`{target}`)\n"
        f"📅  *Validity:* {exp_text}\n"
        f"🗓️   *Expires:* {fmt_date(expires)}",
        parse_mode="Markdown",
    )
    try:
        bot_info = await context.bot.get_me()
        bot_username = f"@{bot_info.username}" if bot_info.username else "the bot"
    except TelegramError:
        bot_username = "the bot"

    try:
        await context.bot.send_message(
            target,
            f"🎊 *Premium Access Activated!*\n"
            f"├ 👤 {uname}\n"
            f"├ 📅 Validity: *{exp_text}*\n"
            f"└ 🗓️ Expires: *{fmt_date(expires)}*\n\n"
            f"⚡ *What you get:*\n"
            f"› Auto-ban anyone who leaves monitored channels\n"
            f"› Instant ban alert + one-tap *Unban* button\n"
            f"› Multi-group support · Full control panel\n"
            f"› Appeal system · Log channel · Auto-schedule\n\n"
            f"🚀 *3-Step Setup:*\n"
            f"1️⃣ Add {bot_username} to your group as *Admin*\n"
            f"2️⃣ Type `/setup` in the group\n"
            f"3️⃣ Type `/addchat` → pick channels from PM\n\n"
            f"📌 Key: `/menu` `/addchat` `/checkall` `/mystats`",
            parse_mode="Markdown",
        )
    except TelegramError:
        pass


async def cmd_removepremium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌  Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/removepremium <user_id>`", parse_mode="Markdown")
        return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid ID.")
        return

    doc = get_premium(target)
    if not doc:
        await update.message.reply_text("❌  User is not in the premium list.")
        return

    name = doc.get("name", str(target))
    del_premium(target)
    await update.message.reply_text(f"✅  Premium removed for *{name}*.", parse_mode="Markdown")
    try:
        await context.bot.send_message(
            target,
            "⚠️  *Your Premium Access Has Been Revoked*\n\n"
            "The bot will no longer protect your groups.",
            parse_mode="Markdown",
        )
    except TelegramError:
        pass


async def cmd_listpremium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌  Owner only.")
        return

    prem  = all_premium()
    lines = [f"💎  *Premium Users ({len(prem)})*\n━━━━━━━━━━━━━━━━━━\n"]
    for doc in prem:
        exp  = doc.get("expires")
        tag  = "⛔  Expired" if (exp and exp < now_ts()) else fmt_date(exp)
        lines.append(
            f"•  *{doc.get('name', doc['_id'])}* (`{doc['_id']}`)\n"
            f"    {doc.get('username', '—')} | {tag}"
        )
    if not prem:
        lines.append("_No premium users yet._")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_botstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌  Owner only.")
        return

    prem  = all_premium()
    grps  = all_groups()
    actv  = sum(1 for d in prem if not d.get("expires") or d["expires"] > now_ts())
    lines = [
        f"📊  *Bot Statistics*\n━━━━━━━━━━━━━━━━━━\n",
        f"💎  Premium Users: *{len(prem)}* (active: {actv})",
        f"👥  Groups: *{len(grps)}*",
        f"🚫  Total Bans: *{total_bans()}*\n",
        "*Group Breakdown:*",
    ]
    for g in grps:
        mc  = g.get("monitored_chats", {})
        sch = g.get("schedule_hours")
        lines.append(
            f"📌  *{g['name']}*\n"
            f"    Chats: {len(mc)} | Bans: {g.get('ban_count', 0)} | "
            f"Schedule: {'Every ' + str(sch) + 'h' if sch else 'Off'}"
        )
    if not grps:
        lines.append("_No groups set up yet._")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌  Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/broadcast <message>`", parse_mode="Markdown")
        return

    msg  = " ".join(context.args)
    sent = failed = 0
    for doc in all_premium():
        try:
            await context.bot.send_message(
                doc["_id"],
                f"📢  *Message from Bot Owner*\n"
                f"━━━━━━━━━━━━━━━━━━\n\n{msg}",
                parse_mode="Markdown",
            )
            sent += 1
        except TelegramError:
            failed += 1
    await update.message.reply_text(
        f"✅  *Broadcast Complete!*\n\n"
        f"📤  Sent: {sent} | ❌  Failed: {failed}",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════════════════════
# PREMIUM COMMANDS — GROUP SETUP
# ══════════════════════════════════════════════════════════════

async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid):
        await update.message.reply_text(
            "❌  *Premium required.*\n\n"
            "Contact the bot owner to get premium access.",
            parse_mode="Markdown",
        )
        return
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("⚠️  Use `/setup` inside a group.", parse_mode="Markdown")
        return

    gid = chat.id
    g   = get_group(gid)

    if g and g.get("owner_id") != uid and not is_owner(uid):
        await update.message.reply_text("❌  This group was set up by another premium user.")
        return

    try:
        bm = await context.bot.get_chat_member(gid, context.bot.id)
        if bm.status != ChatMember.ADMINISTRATOR:
            await update.message.reply_text(
                "⚠️  *Bot is not an Admin!*\n\n"
                "Please grant the bot Admin rights\n"
                "_(Ban Members permission required)_\nthen run `/setup` again.",
                parse_mode="Markdown",
            )
            return
    except TelegramError:
        pass

    if g:
        set_group(gid, {"name": chat.title})
        await update.message.reply_text(
            f"✅  *{chat.title}* is already active.\n\nUse `/menu` to manage this group.",
            parse_mode="Markdown",
        )
        return

    set_group(gid, {
        "name": chat.title, "owner_id": uid,
        "monitored_chats": {}, "ban_count": 0,
        "setup_on": now_ts(), "whitelist": [],
        "log_channel_id": None, "schedule_hours": None,
        "last_auto_check": None, "min_members": None,
        "tracked": {},
    })
    await update.message.reply_text(
        f"✅ *{chat.title}* — Protection Active!\n\n"
        f"🚀 *Next Steps:*\n"
        f"1️⃣ `/addchat` → pick channels to monitor\n"
        f"2️⃣ `/setlog <id>` → set ban log channel\n"
        f"3️⃣ `/setschedule 6` → auto-check every 6h\n"
        f"4️⃣ `/menu` → full control panel\n\n"
        f"⚡ Member leaves monitored channel → *instant ban*",
        parse_mode="Markdown",
    )


async def cmd_addchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid): return
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("⚠️  Use this inside a group.")
        return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("❌  Run `/setup` first.", parse_mode="Markdown")
        return
    if not await can_manage(context, chat.id, uid):
        await update.message.reply_text("❌  Admins only.")
        return
    # ── No args → show PM-based picker + manual option ────────
    if not context.args:
        mc          = g.get("monitored_chats", {})
        bot_uname   = (await context.bot.get_me()).username
        pm_url      = f"https://t.me/{bot_uname}?start=sel_{chat.id}"

        buttons = [
            [InlineKeyboardButton("📋 Choose Channel / Group", url=pm_url)],
            [InlineKeyboardButton("✏️ Enter ID: /addchat -100xxx", callback_data=f"addchat_manual:{chat.id}")],
        ]
        kb = InlineKeyboardMarkup(buttons)

        mc_text = ""
        if mc:
            lines = [f"  {'📢' if v.get('type')=='channel' else '👥'} {v['name']}" for v in mc.values()]
            mc_text = "\n\n📋 *Monitored now:*\n" + "\n".join(lines)

        await update.message.reply_text(
            f"➕ *Add Monitored Chat*\n"
            f"└ Group: *{g['name']}*{mc_text}\n\n"
            f"↓ Tap a button below ↓",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid chat ID. Must be a number like `-100xxxxxxxxxx`.", parse_mode="Markdown")
        return

    mc = g.get("monitored_chats", {})
    if str(cid) in mc:
        await update.message.reply_text(f"⚠️  Already monitoring `{cid}`.", parse_mode="Markdown")
        return

    try:
        tchat = await context.bot.get_chat(cid)
        icon  = "📢" if tchat.type == "channel" else "👥"
        mc[str(cid)] = {"name": tchat.title, "type": tchat.type}
        set_group(chat.id, {"monitored_chats": mc})
        await update.message.reply_text(
            f"✅  *{icon}  {tchat.title}* added!\n\n"
            f"Members who leave this chat will be\n"
            f"automatically banned from *{g['name']}*.\n\n"
            f"📋  Total monitored: *{len(mc)}* chat(s)",
            parse_mode="Markdown",
        )
    except TelegramError as e:
        await update.message.reply_text(
            f"❌  *Could not access that chat.*\n\n"
            f"Make sure:\n"
            f"• Bot is a member/admin of that chat\n"
            f"• The ID is correct\n\n"
            f"_Error: {e}_",
            parse_mode="Markdown",
        )


async def cmd_removechat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid): return
    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("❌  Run `/setup` first.", parse_mode="Markdown")
        return
    if not await can_manage(context, chat.id, uid):
        await update.message.reply_text("❌  Admins only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/removechat <chat_id>`", parse_mode="Markdown")
        return
    try:
        cid = str(int(context.args[0]))
    except ValueError:
        await update.message.reply_text("❌  Invalid chat ID.")
        return

    mc = g.get("monitored_chats", {})
    if cid not in mc:
        await update.message.reply_text(f"❌  `{cid}` is not monitored.", parse_mode="Markdown")
        return

    name = mc[cid].get("name", cid)
    del mc[cid]
    set_group(chat.id, {"monitored_chats": mc})
    await update.message.reply_text(f"✅  *{name}* removed from monitored chats.", parse_mode="Markdown")


async def cmd_listchats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("❌  Bot not set up here.")
        return
    mc    = g.get("monitored_chats", {})
    lines = [f"👁️  *Monitored Chats — {g['name']}*\n━━━━━━━━━━━━━━━━━━\n"]
    for cid, info in mc.items():
        icon = "📢" if "channel" in info.get("type", "") else "👥"
        lines.append(f"{icon}  *{info['name']}*\n    ID: `{cid}`")
    if not mc:
        lines.append("_No monitored chats yet._")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_whitelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid): return
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("⚠️  Use inside a group.")
        return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("❌  Run `/setup` first.", parse_mode="Markdown")
        return
    if not await can_manage(context, chat.id, uid):
        await update.message.reply_text("❌  Admins only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/whitelist <user_id>`", parse_mode="Markdown")
        return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid user ID.")
        return

    wl = g.get("whitelist", [])
    if target in wl:
        await update.message.reply_text(f"⚠️  User `{target}` is already whitelisted.", parse_mode="Markdown")
        return

    wl.append(target)
    set_group(chat.id, {"whitelist": wl})

    try:
        tuser = await context.bot.get_chat(target)
        uname = tuser.full_name
    except TelegramError:
        uname = str(target)

    await update.message.reply_text(
        f"✅  *{uname}* (`{target}`) added to whitelist.\n\nThis user will never be auto-banned.",
        parse_mode="Markdown",
    )


async def cmd_unwhitelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid): return
    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g or not await can_manage(context, chat.id, uid): return
    if not context.args:
        await update.message.reply_text("Usage: `/unwhitelist <user_id>`", parse_mode="Markdown")
        return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid ID.")
        return

    wl = g.get("whitelist", [])
    if target not in wl:
        await update.message.reply_text(f"❌  `{target}` is not in the whitelist.", parse_mode="Markdown")
        return

    wl.remove(target)
    set_group(chat.id, {"whitelist": wl})
    await update.message.reply_text(f"✅  User `{target}` removed from whitelist.", parse_mode="Markdown")


async def cmd_setlog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid): return
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("⚠️  Use inside a group.")
        return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("❌  Run `/setup` first.", parse_mode="Markdown")
        return
    if not await can_manage(context, chat.id, uid):
        await update.message.reply_text("❌  Admins only.")
        return
    if not context.args:
        await update.message.reply_text(
            "Usage: `/setlog <channel_id>`\n\n"
            "_Add the bot as admin in your log channel first._",
            parse_mode="Markdown",
        )
        return
    try:
        log_ch = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid channel ID.")
        return

    try:
        await context.bot.send_message(
            log_ch,
            f"📋  *Log channel connected!*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛡️  Group: *{g['name']}*\n"
            f"All ban/unban events will be logged here.",
            parse_mode="Markdown",
        )
        set_group(chat.id, {"log_channel_id": log_ch})
        await update.message.reply_text(
            "✅  *Log channel connected!*\n\n"
            "All ban and unban events will be sent there.",
            parse_mode="Markdown",
        )
    except TelegramError as e:
        await update.message.reply_text(
            f"❌  Could not connect to that channel.\n"
            f"Make sure the bot is admin there.\n`{e}`",
            parse_mode="Markdown",
        )


async def cmd_setschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid): return
    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g or not await can_manage(context, chat.id, uid): return
    if not context.args:
        await update.message.reply_text(
            "Usage: `/setschedule <hours>`\n\n"
            "Examples:\n"
            "`/setschedule 6` — Check every 6 hours\n"
            "`/setschedule 24` — Check once a day\n"
            "`/setschedule 0` — Disable auto-check",
            parse_mode="Markdown",
        )
        return
    try:
        hrs = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid number.")
        return

    if hrs == 0:
        set_group(chat.id, {"schedule_hours": None})
        await update.message.reply_text("✅  Auto-check *disabled*.", parse_mode="Markdown")
    elif 1 <= hrs <= 168:
        set_group(chat.id, {"schedule_hours": hrs})
        await update.message.reply_text(
            f"✅  Auto-check set to every *{hrs} hour(s)*.\n\n"
            f"The bot will automatically scan all members\n"
            f"and ban anyone who has left required chats.",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text("❌  Please enter a value between 1 and 168 hours.")


async def cmd_setminmembers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if not is_premium(uid): return
    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g or not await can_manage(context, chat.id, uid): return
    if not context.args:
        await update.message.reply_text(
            "Usage: `/setminmembers <number>`\n\n"
            "Example: `/setminmembers 50`\n"
            "You'll get an alert when group drops below 50 members.\n\n"
            "`/setminmembers 0` — Disable alert",
            parse_mode="Markdown",
        )
        return
    try:
        num = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid number.")
        return

    if num == 0:
        set_group(chat.id, {"min_members": None})
        await update.message.reply_text("✅  Member count alert *disabled*.", parse_mode="Markdown")
    else:
        set_group(chat.id, {"min_members": num})
        await update.message.reply_text(
            f"✅  Alert set!\n\n"
            f"You'll be notified when *{g['name']}* drops below *{num}* members.",
            parse_mode="Markdown",
        )


async def cmd_unbanuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("❌  Bot not set up here.")
        return
    if not await can_manage(context, chat.id, uid):
        await update.message.reply_text("❌  Admins only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/unbanuser <user_id>`", parse_mode="Markdown")
        return
    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌  Invalid ID.")
        return

    by_name = update.effective_user.full_name
    ok = await do_unban(context, target, chat.id, g["name"], by_name)
    if ok:
        await update.message.reply_text(
            f"✅  User `{target}` has been unbanned.\n\nThey've been notified via DM.",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(f"❌  Could not unban `{target}`.", parse_mode="Markdown")


async def cmd_checkall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("❌  Bot not set up here.")
        return
    if not await can_manage(context, chat.id, uid):
        await update.message.reply_text("❌  Admins only.")
        return

    mc = g.get("monitored_chats", {})
    if not mc:
        await update.message.reply_text("❌  No monitored chats configured.")
        return

    msg     = await update.message.reply_text("⏳  *Checking all tracked members...*", parse_mode="Markdown")
    members = get_tracked_members(chat.id)
    checked = banned_c = 0

    for uid_m, uname_m in members.items():
        checked += 1
        for cid, info in mc.items():
            try:
                cm = await context.bot.get_chat_member(int(cid), uid_m)
                if cm.status in [ChatMember.LEFT, ChatMember.BANNED]:
                    ok = await do_ban(
                        context, uid_m, uname_m,
                        chat.id, g["name"], info["name"],
                    )
                    if ok:
                        banned_c += 1
                    break
            except TelegramError:
                pass

    set_group(chat.id, {"last_auto_check": now_ts()})
    await msg.edit_text(
        f"✅  *Check Complete!*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥  *Checked:* {checked}\n"
        f"🚫  *Banned:* {banned_c}\n"
        f"🕐  *Time:* {fmt_time(now_ts())}",
        parse_mode="Markdown",
    )


async def cmd_mystats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_premium(uid): return

    my_groups = [g for g in all_groups() if g.get("owner_id") == uid]
    pdata     = get_premium(uid) or {}
    total_b   = sum(g.get("ban_count", 0) for g in my_groups)
    lines     = [
        f"📊  *Your Statistics*\n━━━━━━━━━━━━━━━━━━\n",
        f"👥  Groups: *{len(my_groups)}*  |  🚫  Total Bans: *{total_b}*\n",
    ]
    for g in my_groups:
        mc  = g.get("monitored_chats", {})
        sch = g.get("schedule_hours")
        lines.append(
            f"📌  *{g['name']}*\n"
            f"    Monitored: {len(mc)} | Bans: {g.get('ban_count', 0)}\n"
            f"    Schedule: {'Every ' + str(sch) + 'h' if sch else 'Off'}"
        )
    if not is_owner(uid) and pdata:
        lines.append(f"\n💎  Premium expires: {fmt_date(pdata.get('expires'))}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════
# MEMBER COMMANDS
# ══════════════════════════════════════════════════════════════

async def cmd_mycheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    chat = update.effective_chat

    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("⚠️  Use this command inside a group.")
        return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("ℹ️  Bot is not active in this group.")
        return

    mc     = g.get("monitored_chats", {})
    lines  = ["📋  *Your Membership Status*\n━━━━━━━━━━━━━━━━━━\n"]
    all_ok = True

    for cid, info in mc.items():
        name = info.get("name", cid)
        try:
            cm = await context.bot.get_chat_member(int(cid), uid)
            if cm.status in [ChatMember.LEFT, ChatMember.BANNED]:
                lines.append(f"❌  *{name}* — Not joined")
                all_ok = False
            else:
                lines.append(f"✅  *{name}* — Joined")
        except TelegramError:
            lines.append(f"⚠️  *{name}* — Could not check")

    if not mc:
        lines.append("✅  No required chats — you're all clear!")
    elif all_ok:
        lines.append("\n🎉  *All good!* You're in all required chats.")
    else:
        lines.append("\n⚠️  *Action required!* Join missing chats to avoid being banned.")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]: return
    g = get_group(chat.id)
    if not g:
        await update.message.reply_text("ℹ️  Bot is not active here.")
        return

    mc    = g.get("monitored_chats", {})
    lines = [
        f"📜  *{g['name']} — Group Rules*\n━━━━━━━━━━━━━━━━━━\n",
        "📌  You must remain a member of all required chats listed below.",
        "📌  Leaving any required chat will result in an automatic ban.",
        "📌  You'll receive a DM with the reason and an appeal option.",
        "📌  To get unbanned, submit an appeal or contact an admin.\n",
        "👁️  *Required Chats:*",
    ]
    for info in mc.values():
        icon = "📢" if "channel" in info.get("type", "") else "👥"
        lines.append(f"  {icon}  *{info['name']}*")
    if not mc:
        lines.append("  _(No required chats configured yet)_")
    lines.append("\n`/mycheck` — Check your current status")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════
# FORWARD DETECTION — auto addchat from forwarded message
# ══════════════════════════════════════════════════════════════

async def handle_forward_addchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detects forwarded messages from channels/groups in a protected group and
       offers to add that chat as a monitored chat."""
    msg  = update.message
    chat = update.effective_chat
    uid  = update.effective_user.id

    if not msg or chat.type not in ["group", "supergroup"]:
        return

    # Only admins / premium users get this feature
    if not is_premium(uid) or not await can_manage(context, chat.id, uid):
        return

    g = get_group(chat.id)
    if not g:
        return

    # Extract forward origin (Telegram Bot API v7+)
    forward_chat = None
    try:
        if msg.forward_origin:
            origin = msg.forward_origin
            # MessageOriginChannel or MessageOriginChat
            if hasattr(origin, "chat"):
                forward_chat = origin.chat
    except Exception:
        pass

    # Fallback for older API versions
    if not forward_chat and msg.forward_from_chat:
        forward_chat = msg.forward_from_chat

    if not forward_chat:
        return

    cid  = str(forward_chat.id)
    name = forward_chat.title or str(forward_chat.id)
    icon = "📢" if forward_chat.type == "channel" else "👥"
    mc   = g.get("monitored_chats", {})

    if cid in mc:
        await msg.reply_text(
            f"⚠️  *{icon}  {name}* is already being monitored.",
            parse_mode="Markdown",
        )
        return

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"✅  Add {name}", callback_data=f"fwd_add:{cid}:{chat.id}"),
        InlineKeyboardButton("❌  Cancel",       callback_data="fwd_cancel"),
    ]])
    await msg.reply_text(
        f"🔍  *Forward Detected!*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{icon}  *{name}*\n"
        f"🆔  `{cid}`\n\n"
        f"Add this as a monitored chat?\n"
        f"Members who leave it will be auto-banned from *{g['name']}*.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def cb_fwd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    _, cid_str, gid_str = q.data.split(":")
    gid = int(gid_str)

    if not is_premium(uid) or not await can_manage(context, gid, uid):
        await q.answer("❌  Admins only.", show_alert=True)
        return

    g = get_group(gid)
    if not g:
        await q.edit_message_text("❌  Group not found.")
        return

    try:
        tchat = await context.bot.get_chat(int(cid_str))
        icon  = "📢" if tchat.type == "channel" else "👥"
        mc    = g.get("monitored_chats", {})
        if cid_str in mc:
            await q.edit_message_text(f"⚠️  Already monitoring *{tchat.title}*.", parse_mode="Markdown")
            return
        mc[cid_str] = {"name": tchat.title, "type": tchat.type}
        set_group(gid, {"monitored_chats": mc})
        await q.edit_message_text(
            f"✅  *{icon}  {tchat.title}* added!\n\n"
            f"Members who leave this chat will be banned from *{g['name']}*.\n"
            f"📋  Total monitored: *{len(mc)}* chat(s)",
            parse_mode="Markdown",
        )
    except TelegramError as e:
        await q.edit_message_text(
            f"❌  Could not access that chat.\n"
            f"Make sure the bot is a member/admin there.\n`{e}`",
            parse_mode="Markdown",
        )


async def cb_fwd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.delete_message()


# ══════════════════════════════════════════════════════════════
# AUTO BAN — ChatMemberHandler
# ══════════════════════════════════════════════════════════════

async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result:
        return

    chat_id    = result.chat.id
    user       = result.new_chat_member.user
    new_status = result.new_chat_member.status
    old_status = result.old_chat_member.status if result.old_chat_member else None

    if user.is_bot:
        return

    was_member = old_status in [
        ChatMember.MEMBER, ChatMember.ADMINISTRATOR,
        ChatMember.OWNER, ChatMember.RESTRICTED,
    ]
    now_member = new_status in [
        ChatMember.MEMBER, ChatMember.ADMINISTRATOR,
        ChatMember.OWNER, ChatMember.RESTRICTED,
    ]
    now_left   = new_status in [ChatMember.LEFT, ChatMember.BANNED]

    # ── Track joins in protected groups ──────────────────────
    if now_member:
        g = get_group(chat_id)
        if g:
            track_member_join(chat_id, user.id, user.full_name)

    # ── Track leaves from protected groups ───────────────────
    if was_member and now_left:
        g = get_group(chat_id)
        if g:
            track_member_leave(chat_id, user.id)

    # ── INSTANT BAN if a monitored channel was left ───────────
    if was_member and now_left:
        for g in all_groups():
            gid = g["_id"]
            mc  = g.get("monitored_chats", {})

            if str(chat_id) not in mc:
                continue

            owner_id = g.get("owner_id")
            if owner_id and not is_owner(owner_id) and not is_premium(owner_id):
                continue

            try:
                lm = await context.bot.get_chat_member(gid, user.id)
                if lm.status in [ChatMember.LEFT, ChatMember.BANNED]:
                    continue
            except TelegramError:
                continue

            reason = mc[str(chat_id)].get("name", str(chat_id))
            await do_ban(context, user.id, user.full_name, gid, g["name"], reason)

            # ── Member count alert ────────────────────────────
            mm = g.get("min_members")
            if mm:
                try:
                    count = await context.bot.get_chat_member_count(gid)
                    if count < mm:
                        alert_to = owner_id or get_config().get("bot_owner_id")
                        if alert_to:
                            try:
                                await context.bot.send_message(
                                    alert_to,
                                    f"⚠️  *Member Count Alert!*\n"
                                    f"━━━━━━━━━━━━━━━━━━\n"
                                    f"📌  *Group:* {g['name']}\n"
                                    f"👥  *Current:* {count} members\n"
                                    f"📉  *Threshold:* {mm} members",
                                    parse_mode="Markdown",
                                )
                            except TelegramError:
                                pass
                except TelegramError:
                    pass


# ══════════════════════════════════════════════════════════════
# ADDCHAT PICKER CALLBACKS
# ══════════════════════════════════════════════════════════════

async def cb_addchat_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin selected a chat from the inline picker."""
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    parts   = q.data.split(":")
    gid     = int(parts[1])
    cid_str = parts[2]

    if not is_premium(uid) or not await can_manage(context, gid, uid):
        await q.answer("❌  Admins only.", show_alert=True)
        return

    g = get_group(gid)
    if not g:
        await q.edit_message_text("❌  Group not found. Run `/setup` first.", parse_mode="Markdown")
        return

    mc = g.get("monitored_chats", {})
    if cid_str in mc:
        await q.edit_message_text(f"⚠️  Already monitoring this chat.", parse_mode="Markdown")
        return

    try:
        tchat = await context.bot.get_chat(int(cid_str))
        icon  = "📢" if tchat.type == "channel" else "👥"
        mc[cid_str] = {"name": tchat.title, "type": tchat.type}
        set_group(gid, {"monitored_chats": mc})

        await q.edit_message_text(
            f"✅  *{icon}  {tchat.title}* is now monitored!\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡  Any member who leaves this chat will be\n"
            f"    *instantly banned* from *{g['name']}*.\n\n"
            f"📋  Total monitored chats: *{len(mc)}*",
            parse_mode="Markdown",
        )
    except TelegramError as e:
        await q.edit_message_text(
            f"❌  *Could not access that chat.*\n\n"
            f"Make sure the bot is admin there.\n`{e}`",
            parse_mode="Markdown",
        )


async def cb_addchat_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    await q.edit_message_text(
        f"✏️ *Enter Chat ID*\n\n"
        f"`/addchat -100xxxxxxxxxx`\n\n"
        f"_Channel IDs start with_ `-100`\n"
        f"_Get it from @userinfobot_",
        parse_mode="Markdown",
    )


async def cb_pm_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User selected a chat from the PM picker."""
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()

    parts   = q.data.split(":")
    gid     = int(parts[1])
    cid_str = parts[2]

    if not is_premium(uid) or not await can_manage(context, gid, uid):
        await q.answer("❌ Admins only.", show_alert=True)
        return

    g = get_group(gid)
    if not g:
        await q.edit_message_text("❌ Group not found.")
        return

    mc = g.get("monitored_chats", {})
    if cid_str in mc:
        await q.edit_message_text(f"⚠️ Already monitoring this chat.")
        return

    try:
        tchat = await context.bot.get_chat(int(cid_str))
        icon  = "📢" if tchat.type == "channel" else "👥"
        mc[cid_str] = {"name": tchat.title, "type": tchat.type}
        set_group(gid, {"monitored_chats": mc})

        # Remove this button from list, refresh remaining
        bot_chats = get_bot_chats()
        remaining = [c for c in bot_chats if str(c["_id"]) not in mc and c["_id"] != gid]
        new_buttons = []
        for c in remaining[:24]:
            ic = "📢" if c.get("type") == "channel" else "👥"
            new_buttons.append([InlineKeyboardButton(f"{ic} {c['name']}", callback_data=f"pm_pick:{gid}:{c['_id']}")])

        if new_buttons:
            new_kb = InlineKeyboardMarkup(new_buttons)
            await q.edit_message_text(
                f"✅ *{icon} {tchat.title}* added!\n"
                f"└ {g['name']} · {len(mc)} chat(s) monitored\n\n"
                f"_Select another or close._",
                parse_mode="Markdown",
                reply_markup=new_kb,
            )
        else:
            await q.edit_message_text(
                f"✅ *{icon} {tchat.title}* added!\n"
                f"└ {g['name']} · {len(mc)} chat(s) monitored\n\n"
                f"_No more chats available. Done!_",
                parse_mode="Markdown",
            )
    except TelegramError as e:
        await q.edit_message_text(
            f"❌ Could not access chat.\nMake sure bot is admin there.\n`{e}`",
            parse_mode="Markdown",
        )


# ══════════════════════════════════════════════════════════════
# BOT SELF MEMBERSHIP — track chats where bot is admin
# ══════════════════════════════════════════════════════════════

async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fired when the bot's own status changes in any chat.
       Stores chats where bot is admin so the /addchat picker works."""
    result = update.my_chat_member
    if not result:
        return

    chat       = result.chat
    new_status = result.new_chat_member.status

    is_admin = new_status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
    was_removed = new_status in [ChatMember.LEFT, ChatMember.BANNED]

    if is_admin and chat.type in ["channel", "supergroup", "group"]:
        save_bot_chat(chat.id, {
            "name": chat.title or str(chat.id),
            "type": chat.type,
            "added_on": now_ts(),
        })
        logger.info(f"Bot added as admin in: {chat.title} ({chat.id})")

    elif was_removed:
        remove_bot_chat(chat.id)
        logger.info(f"Bot removed from: {chat.title} ({chat.id})")


# ══════════════════════════════════════════════════════════════
# AUTO SCHEDULE JOB
# ══════════════════════════════════════════════════════════════

async def scheduled_check_job(context: ContextTypes.DEFAULT_TYPE):
    ts = now_ts()
    for g in all_groups():
        gid = g["_id"]
        sch = g.get("schedule_hours")
        if not sch:
            continue

        last = g.get("last_auto_check") or 0
        if ts - last < sch * 3600:
            continue

        mc = g.get("monitored_chats", {})
        if not mc:
            continue

        owner_id = g.get("owner_id")
        if owner_id and not is_owner(owner_id) and not is_premium(owner_id):
            continue

        logger.info(f"⏰ Auto-check: {g['name']} ({gid})")
        members  = get_tracked_members(gid)
        checked  = 0
        banned_c = 0

        for uid_m, uname_m in members.items():
            checked += 1
            for cid, info in mc.items():
                try:
                    cm = await context.bot.get_chat_member(int(cid), uid_m)
                    if cm.status in [ChatMember.LEFT, ChatMember.BANNED]:
                        ok = await do_ban(
                            context, uid_m, uname_m,
                            gid, g["name"], info["name"],
                        )
                        if ok:
                            banned_c += 1
                        break
                except TelegramError:
                    pass

        set_group(gid, {"last_auto_check": ts})

        if owner_id:
            try:
                await context.bot.send_message(
                    owner_id,
                    f"📅  *Scheduled Check Complete*\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📌  *Group:* {g['name']}\n"
                    f"👥  *Checked:* {checked}\n"
                    f"🚫  *Banned:* {banned_c}\n"
                    f"🕐  *Time:* {fmt_time(ts)}",
                    parse_mode="Markdown",
                )
            except TelegramError:
                pass

        mm = g.get("min_members")
        if mm and owner_id:
            try:
                count = await context.bot.get_chat_member_count(gid)
                if count < mm:
                    await context.bot.send_message(
                        owner_id,
                        f"⚠️  *Member Count Alert!*\n\n"
                        f"📌  *{g['name']}* now has only *{count}* members\n"
                        f"📉  Threshold: {mm}",
                        parse_mode="Markdown",
                    )
            except TelegramError:
                pass


# ══════════════════════════════════════════════════════════════
# BOT RUNNER
# ══════════════════════════════════════════════════════════════

def build_bot_app() -> Application:
    logger.info("Building application...")
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    # Owner
    app.add_handler(CommandHandler("addpremium",    cmd_addpremium))
    app.add_handler(CommandHandler("removepremium", cmd_removepremium))
    app.add_handler(CommandHandler("listpremium",   cmd_listpremium))
    app.add_handler(CommandHandler("botstats",      cmd_botstats))
    app.add_handler(CommandHandler("broadcast",     cmd_broadcast))

    # Premium / Group management
    app.add_handler(CommandHandler("setup",         cmd_setup))
    app.add_handler(CommandHandler("addchat",       cmd_addchat))
    app.add_handler(CommandHandler("removechat",    cmd_removechat))
    app.add_handler(CommandHandler("listchats",     cmd_listchats))
    app.add_handler(CommandHandler("whitelist",     cmd_whitelist))
    app.add_handler(CommandHandler("unwhitelist",   cmd_unwhitelist))
    app.add_handler(CommandHandler("setlog",        cmd_setlog))
    app.add_handler(CommandHandler("setschedule",   cmd_setschedule))
    app.add_handler(CommandHandler("setminmembers", cmd_setminmembers))
    app.add_handler(CommandHandler("checkall",      cmd_checkall))
    app.add_handler(CommandHandler("unbanuser",     cmd_unbanuser))
    app.add_handler(CommandHandler("mystats",       cmd_mystats))

    # General
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("help",    cmd_start))
    app.add_handler(CommandHandler("menu",    cmd_menu))
    app.add_handler(CommandHandler("mycheck", cmd_mycheck))
    app.add_handler(CommandHandler("rules",   cmd_rules))

    # Inline callbacks — original
    app.add_handler(CallbackQueryHandler(cb_appeal_start,  pattern=r"^appeal_start:"))
    app.add_handler(CallbackQueryHandler(cb_appeal_accept, pattern=r"^appeal_accept:"))
    app.add_handler(CallbackQueryHandler(cb_appeal_reject, pattern=r"^appeal_reject:"))
    app.add_handler(CallbackQueryHandler(cb_direct_unban,  pattern=r"^direct_unban:"))
    app.add_handler(CallbackQueryHandler(cb_group_menu,    pattern=r"^gm:"))
    app.add_handler(CallbackQueryHandler(cb_owner_menu,    pattern=r"^om:"))

    # Inline callbacks — forward detect
    app.add_handler(CallbackQueryHandler(cb_fwd_add,       pattern=r"^fwd_add:"))
    app.add_handler(CallbackQueryHandler(cb_fwd_cancel,    pattern=r"^fwd_cancel$"))

    # Inline callbacks — addchat picker (group inline + PM picker)
    app.add_handler(CallbackQueryHandler(cb_addchat_pick,  pattern=r"^addchat_pick:"))
    app.add_handler(CallbackQueryHandler(cb_addchat_manual,pattern=r"^addchat_manual:"))
    app.add_handler(CallbackQueryHandler(cb_pm_pick,       pattern=r"^pm_pick:"))

    # ChatMember events — group members
    app.add_handler(ChatMemberHandler(on_chat_member,    ChatMemberHandler.ANY_CHAT_MEMBER))
    # ChatMember events — bot's own status (to track admin chats)
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # Private message — appeal text input
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_appeal_text,
    ))

    # Group message — forward detection for addchat
    app.add_handler(MessageHandler(
        filters.FORWARDED & filters.ChatType.GROUPS,
        handle_forward_addchat,
    ))

    # Scheduled job — every 30 minutes
    if app.job_queue is not None:
        app.job_queue.run_repeating(scheduled_check_job, interval=1800, first=60)
    else:
        logger.warning("JobQueue not available — install python-telegram-bot[job-queue]")

    return app


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        "bot:web_app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
    )
