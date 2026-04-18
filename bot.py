import os
import asyncio
import logging
from collections import defaultdict
from telegram import Update, Chat
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))

# {chat_id: {target_username_or_id: delay_seconds}}
delete_rules: dict[int, dict[str, int]] = defaultdict(dict)

# Premium users (by Telegram user ID) — owner can add/remove
premium_users: set[int] = set()


# ─── Access Helpers ───────────────────────────────────────────────────────────

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


def is_premium(user_id: int) -> bool:
    return user_id in premium_users


def is_authorized(user_id: int) -> bool:
    """Owner or premium user."""
    return is_owner(user_id) or is_premium(user_id)


def role_label(user_id: int) -> str:
    if is_owner(user_id):
        return "👑 Owner"
    if is_premium(user_id):
        return "⭐ Premium"
    return "👤 Member"


# ─── Time Helpers ─────────────────────────────────────────────────────────────

def parse_time(time_str: str) -> int | None:
    time_str = time_str.strip().lower()
    try:
        if time_str.endswith("s"):
            return int(time_str[:-1])
        elif time_str.endswith("m"):
            return int(time_str[:-1]) * 60
        elif time_str.endswith("h"):
            return int(time_str[:-1]) * 3600
        else:
            return int(time_str)
    except ValueError:
        return None


def format_delay(delay: int) -> str:
    if delay < 60:
        return f"{delay} second{'s' if delay != 1 else ''}"
    elif delay < 3600:
        m = delay // 60
        return f"{m} minute{'s' if m != 1 else ''}"
    else:
        h = delay // 3600
        return f"{h} hour{'s' if h != 1 else ''}"


# ─── Commands: General ───────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    role = role_label(user.id)

    if is_owner(user.id):
        extra = (
            "\n👑 Owner Commands:\n"
            "  /addpremium <userid> — grant premium access\n"
            "  /removepremium <userid> — revoke premium access\n"
            "  /listpremium — view all premium users\n"
        )
    elif is_premium(user.id):
        extra = "\n⭐ You have Premium access — you can use all bot commands below.\n"
    else:
        extra = "\n🔒 You don't have access to bot commands.\n"

    await update.message.reply_text(
        f"👋 Hello! I'm the Auto-Delete Bot.\n"
        f"Your role: {role}\n\n"
        "I automatically delete messages from specific users, bots, or channels "
        "after a timer you set.\n"
        + extra +
        "\n📋 Bot Commands (owner & premium):\n"
        "  /setdelete @username 10s — set a delete timer\n"
        "  /setdelete <userid> 5m — set by user ID\n"
        "  /listdeletes — view active rules in this group\n"
        "  /removedelete @username — remove a rule\n"
        "  /cleardeletes — clear all rules in this group\n"
        "  /status — bot status overview\n\n"
        "⏱ Time format:\n"
        "  10s = 10 seconds | 5m = 5 minutes | 2h = 2 hours"
    )


# ─── Commands: Bot Management (Owner + Premium) ───────────────────────────────

async def setdelete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if not is_authorized(user.id):
        await update.message.reply_text("⛔ You don't have permission to use this command.")
        return

    if chat.type == Chat.PRIVATE:
        await update.message.reply_text("⚠️ This command only works inside a group.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ Invalid usage.\n\n"
            "Usage: /setdelete @username|userid <time>\n\n"
            "Examples:\n"
            "  /setdelete @johndoe 30s\n"
            "  /setdelete 123456789 2m\n"
            "  /setdelete @spambot 1h"
        )
        return

    target = context.args[0].lstrip("@")
    time_str = context.args[1]
    delay = parse_time(time_str)

    if delay is None or delay <= 0:
        await update.message.reply_text(
            "❌ Invalid time format.\n"
            "Use: 10s (seconds), 5m (minutes), 2h (hours), or a plain number (seconds)."
        )
        return

    delete_rules[chat.id][target] = delay
    readable = format_delay(delay)

    await update.message.reply_text(
        f"✅ Rule set!\n\n"
        f"Target: {target}\n"
        f"Delete after: {readable}\n\n"
        f"Messages from this user/bot will be automatically removed."
    )
    logger.info("Rule added by %s (%d): chat=%d target=%s delay=%ds",
                user.username or "?", user.id, chat.id, target, delay)


async def listdeletes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if not is_authorized(user.id):
        await update.message.reply_text("⛔ You don't have permission to use this command.")
        return

    rules = delete_rules.get(chat.id, {})
    if not rules:
        await update.message.reply_text("ℹ️ No active delete rules in this group.")
        return

    lines = ["📋 Active Delete Rules:\n"]
    for target, delay in rules.items():
        lines.append(f"  • {target}  →  {format_delay(delay)}")

    await update.message.reply_text("\n".join(lines))


async def removedelete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if not is_authorized(user.id):
        await update.message.reply_text("⛔ You don't have permission to use this command.")
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Please specify a target.\n"
            "Usage: /removedelete @username"
        )
        return

    target = context.args[0].lstrip("@")
    rules = delete_rules.get(chat.id, {})

    if target in rules:
        del rules[target]
        await update.message.reply_text(f"✅ Rule removed for: {target}")
    else:
        await update.message.reply_text(f"⚠️ No rule found for: {target}")


async def cleardeletes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if not is_authorized(user.id):
        await update.message.reply_text("⛔ You don't have permission to use this command.")
        return

    count = len(delete_rules.get(chat.id, {}))
    if chat.id in delete_rules:
        delete_rules[chat.id].clear()

    await update.message.reply_text(
        f"🗑 All delete rules cleared.\n{count} rule(s) removed from this group."
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not is_authorized(user.id):
        await update.message.reply_text("⛔ You don't have permission to use this command.")
        return

    total_rules = sum(len(v) for v in delete_rules.values())
    total_groups = len(delete_rules)
    role = role_label(user.id)

    await update.message.reply_text(
        "🤖 Bot Status\n\n"
        f"  • Running: ✅ Online\n"
        f"  • Active groups: {total_groups}\n"
        f"  • Total rules: {total_rules}\n"
        f"  • Premium users: {len(premium_users)}\n"
        f"  • Your role: {role}"
    )


# ─── Commands: Premium Management (Owner Only) ────────────────────────────────

async def addpremium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not is_owner(user.id):
        await update.message.reply_text("⛔ Only the bot owner can manage premium users.")
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Please provide a user ID.\n"
            "Usage: /addpremium <userid>\n\n"
            "Tip: Ask the user to send /start to the bot,\n"
            "then check @userinfobot or forward their message to get their ID."
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID. Must be a numeric ID.\n"
            "Example: /addpremium 123456789"
        )
        return

    if target_id == OWNER_ID:
        await update.message.reply_text("ℹ️ You are already the owner — no need to add yourself.")
        return

    if target_id in premium_users:
        await update.message.reply_text(f"ℹ️ User {target_id} already has premium access.")
        return

    premium_users.add(target_id)
    await update.message.reply_text(
        f"⭐ Premium granted!\n\n"
        f"User ID: {target_id}\n"
        f"They can now use all bot commands."
    )
    logger.info("Premium granted to user %d by owner", target_id)


async def removepremium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not is_owner(user.id):
        await update.message.reply_text("⛔ Only the bot owner can manage premium users.")
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Please provide a user ID.\n"
            "Usage: /removepremium <userid>"
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Must be a numeric ID.")
        return

    if target_id in premium_users:
        premium_users.discard(target_id)
        await update.message.reply_text(
            f"✅ Premium revoked.\n\n"
            f"User ID: {target_id}\n"
            f"They no longer have bot access."
        )
        logger.info("Premium revoked from user %d by owner", target_id)
    else:
        await update.message.reply_text(f"⚠️ User {target_id} is not a premium user.")


async def listpremium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not is_owner(user.id):
        await update.message.reply_text("⛔ Only the bot owner can view premium users.")
        return

    if not premium_users:
        await update.message.reply_text("ℹ️ No premium users at the moment.")
        return

    lines = [f"⭐ Premium Users ({len(premium_users)} total):\n"]
    for uid in sorted(premium_users):
        lines.append(f"  • {uid}")

    await update.message.reply_text("\n".join(lines))


# ─── Message Handler ──────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.channel_post
    if not msg:
        return

    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        return

    rules = delete_rules.get(chat.id)
    if not rules:
        return

    sender = update.effective_user
    matched_delay = None

    if not sender:
        forward_chat = getattr(msg, "forward_from_chat", None)
        via_bot = getattr(msg, "via_bot", None)

        if forward_chat:
            fc_username = forward_chat.username or ""
            fc_id = str(forward_chat.id)
            if fc_username in rules:
                matched_delay = rules[fc_username]
            elif fc_id in rules:
                matched_delay = rules[fc_id]

        if via_bot and matched_delay is None:
            vb_username = via_bot.username or ""
            vb_id = str(via_bot.id)
            if vb_username in rules:
                matched_delay = rules[vb_username]
            elif vb_id in rules:
                matched_delay = rules[vb_id]
    else:
        username = sender.username or ""
        user_id_str = str(sender.id)

        if username and username in rules:
            matched_delay = rules[username]
        elif user_id_str in rules:
            matched_delay = rules[user_id_str]

    if matched_delay is not None:
        asyncio.create_task(delete_after(context, chat.id, msg.message_id, matched_delay))


async def delete_after(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    delay: int,
):
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info("Deleted message %d in chat %d after %ds", message_id, chat_id, delay)
    except TelegramError as e:
        logger.warning("Could not delete message %d in chat %d: %s", message_id, chat_id, e)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable not set!")
        return

    if OWNER_ID == 0:
        logger.error("OWNER_ID environment variable not set!")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    # General
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))

    # Bot management (owner + premium)
    app.add_handler(CommandHandler("setdelete", setdelete))
    app.add_handler(CommandHandler("listdeletes", listdeletes))
    app.add_handler(CommandHandler("removedelete", removedelete))
    app.add_handler(CommandHandler("cleardeletes", cleardeletes))
    app.add_handler(CommandHandler("status", status))

    # Premium management (owner only)
    app.add_handler(CommandHandler("addpremium", addpremium))
    app.add_handler(CommandHandler("removepremium", removepremium))
    app.add_handler(CommandHandler("listpremium", listpremium))

    app.add_handler(
        MessageHandler(
            filters.ALL & ~filters.COMMAND,
            handle_message,
        )
    )

    logger.info("Bot started successfully. Polling for updates...")

    async with app:
        await app.start()
        await app.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )
        # Run forever until interrupted
        await asyncio.Event().wait()
        await app.updater.stop()
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
