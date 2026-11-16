
# -*- coding: utf-8 -*-
"""
FIN-INTEL Telegram 新闻机器人

/start  显示帮助
/id     查看群ID和用户ID
/news   手动采集并推送新闻
/latest 查看最近一次采集结果

权限和运行规则全部读取 bot_config.json
两个群共享一个采集队列，避免同时运行采集程序。
"""

import asyncio
import io
import json
import logging

from contextlib import redirect_stdout
from pathlib import Path

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)


# ============================================================
# 路径
# ============================================================

BASE = Path(__file__).resolve().parent

TOKEN_FILE = BASE / "bot_token.txt"
CONFIG_FILE = BASE / "bot_config.json"
RESULT_FILE = BASE / "news_result.txt"

MAX_MESSAGE_LENGTH = 3500


# ============================================================
# 日志
# ============================================================

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# ============================================================
# 全局采集锁
# ============================================================

SCAN_LOCK = asyncio.Lock()


# ============================================================
# 读取配置
# ============================================================

def load_token():

    if not TOKEN_FILE.exists():
        raise FileNotFoundError(
            f"找不到机器人Token文件：{TOKEN_FILE}"
        )

    token = TOKEN_FILE.read_text(
        encoding="utf-8-sig"
    ).strip()

    if not token:
        raise ValueError(
            "bot_token.txt 是空文件"
        )

    return token


def load_config():

    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"找不到配置文件：{CONFIG_FILE}"
        )

    data = json.loads(
        CONFIG_FILE.read_text(
            encoding="utf-8-sig"
        )
    )

    config = {
        "allowed_chat_ids": [
            int(x)
            for x in data.get("allowed_chat_ids", [])
        ],

        "owner_user_ids": [
            int(x)
            for x in data.get("owner_user_ids", [])
        ],

        "group_member_access": data.get(
            "group_member_access",
            "all"
        ),

        "private_access": data.get(
            "private_access",
            "owner_only"
        ),

        "scan_mode": data.get(
            "scan_mode",
            "queue"
        ),
    }

    if config["group_member_access"] not in (
        "all",
        "admins_only",
    ):
        raise ValueError(
            "group_member_access 配置错误"
        )

    if config["private_access"] != "owner_only":
        raise ValueError(
            "private_access 目前只支持 owner_only"
        )

    if config["scan_mode"] != "queue":
        raise ValueError(
            "scan_mode 目前只支持 queue"
        )

    return config


# ============================================================
# 权限验证
# ============================================================

async def check_permission(update, context):

    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message

    if not chat or not user:
        return False

    config = load_config()

    # --------------------------------------------------------
    # 群聊
    # --------------------------------------------------------

    if chat.type in ("group", "supergroup"):

        if chat.id not in config["allowed_chat_ids"]:

            await message.reply_text(
                "该群尚未授权使用 FIN-INTEL 新闻机器人。"
            )

            return False

        # 默认允许群内所有成员
        if config["group_member_access"] == "all":
            return True

        # 可选：如果以后改成 admins_only，再验证管理员
        if config["group_member_access"] == "admins_only":

            try:

                member = await context.bot.get_chat_member(
                    chat.id,
                    user.id,
                )

                if member.status not in (
                    "administrator",
                    "creator",
                ):

                    await message.reply_text(
                        "只有群管理员可以执行此操作。"
                    )

                    return False

                return True

            except Exception:

                logger.exception(
                    "检查群管理员权限失败"
                )

                await message.reply_text(
                    "无法验证群管理员权限。"
                )

                return False

    # --------------------------------------------------------
    # 私聊
    # --------------------------------------------------------

    if chat.type == "private":

        if user.id not in config["owner_user_ids"]:

            await message.reply_text(
                "你没有私聊操作权限。"
            )

            return False

        return True

    return False


# ============================================================
# 消息拆分
# ============================================================

def split_messages(content):

    content = content.strip()

    if not content:
        return []

    blocks = content.split("\n\n")

    messages = []
    current = ""

    for block in blocks:

        if len(block) > MAX_MESSAGE_LENGTH:

            if current:
                messages.append(current)
                current = ""

            for start in range(
                0,
                len(block),
                MAX_MESSAGE_LENGTH,
            ):

                messages.append(
                    block[
                        start:start + MAX_MESSAGE_LENGTH
                    ]
                )

            continue

        candidate = (
            current + "\n\n" + block
            if current
            else block
        )

        if len(candidate) > MAX_MESSAGE_LENGTH:

            if current:
                messages.append(current)

            current = block

        else:

            current = candidate

    if current:
        messages.append(current)

    return messages


# ============================================================
# 推送新闻
# ============================================================

async def send_news(chat_id, context, content):

    messages = split_messages(content)

    if not messages:

        await context.bot.send_message(
            chat_id=chat_id,
            text="目前没有可发送的新闻。",
        )

        return

    total = len(messages)

    for index, message in enumerate(messages, 1):

        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            disable_web_page_preview=True,
        )

        logger.info(
            "群 %s：已发送第 %s/%s 批新闻",
            chat_id,
            index,
            total,
        )

        await asyncio.sleep(0.15)


# ============================================================
# 执行 FIN-INTEL 采集程序
# ============================================================

def run_scan():

    import main as fin_intel_main

    old_mtime = (
        RESULT_FILE.stat().st_mtime_ns
        if RESULT_FILE.exists()
        else None
    )

    log_buffer = io.StringIO()

    with redirect_stdout(log_buffer):

        fin_intel_main.main()

    log_text = log_buffer.getvalue()

    if not RESULT_FILE.exists():

        raise RuntimeError(
            "采集程序没有生成 news_result.txt。\n"
            + log_text[-1500:]
        )

    new_mtime = RESULT_FILE.stat().st_mtime_ns

    if old_mtime == new_mtime:

        raise RuntimeError(
            "本次运行没有更新新闻结果文件，"
            "可能是采集程序提前退出。\n"
            + log_text[-1500:]
        )

    result = RESULT_FILE.read_text(
        encoding="utf-8-sig"
    ).strip()

    return result


# ============================================================
# 后台采集任务
# ============================================================

async def process_news(chat_id, context):

    # 两个群共享同一把锁。
    # 一个群采集结束后，另一个群的任务自动继续。

    async with SCAN_LOCK:

        try:

            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "开始采集金融新闻，请稍候……\n"
                    "采集完成后会自动分批推送。"
                ),
            )

            logger.info(
                "开始为群 %s 执行新闻采集",
                chat_id,
            )

            result = await asyncio.to_thread(
                run_scan
            )

            if not result:
                result = "本次没有符合条件的新闻。"

            await send_news(
                chat_id,
                context,
                result,
            )

            await context.bot.send_message(
                chat_id=chat_id,
                text="本次新闻采集与推送完成。",
            )

            logger.info(
                "群 %s 新闻采集与推送完成",
                chat_id,
            )

        except Exception as exc:

            logger.exception(
                "群 %s 新闻采集失败",
                chat_id,
            )

            try:

                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "新闻采集失败：\n"
                        f"{type(exc).__name__}: {exc}"
                    )[:3500],
                )

            except Exception:

                logger.exception(
                    "发送错误通知失败"
                )


# ============================================================
# /start
# ============================================================

async def start_command(update, context):

    await update.effective_message.reply_text(
        "FIN-INTEL 金融新闻机器人\n\n"
        "/news  手动采集并推送最新新闻\n"
        "/latest 查看最近一次采集结果\n"
        "/id     查看群ID和用户ID\n\n"
        "两个授权群均可使用。\n"
        "多个采集请求自动排队。\n"
        "新闻采集不会自动定时运行。"
    )


# ============================================================
# /id
# ============================================================

async def id_command(update, context):

    chat = update.effective_chat
    user = update.effective_user

    await update.effective_message.reply_text(
        "当前Telegram信息：\n\n"
        f"群/聊天ID：{chat.id}\n"
        f"你的用户ID：{user.id}\n"
        f"聊天类型：{chat.type}\n\n"
        "请将需要授权的ID填入 bot_config.json。"
    )


# ============================================================
# /news
# ============================================================

async def news_command(update, context):

    if not await check_permission(update, context):
        return

    chat_id = update.effective_chat.id

    # 不再因为另一个群正在采集而拒绝请求。
    # 创建后台任务，等待全局采集锁自动排队。

    await update.effective_message.reply_text(
        "新闻采集请求已加入队列。\n"
        "轮到本群时将自动开始采集并推送结果。"
    )

    context.application.create_task(
        process_news(
            chat_id,
            context,
        ),
        update=update,
        name=f"FIN-INTEL-news-{chat_id}",
    )

    logger.info(
        "群 %s 已加入新闻采集队列",
        chat_id,
    )


# ============================================================
# /latest
# ============================================================

async def latest_command(update, context):

    if not await check_permission(update, context):
        return

    if not RESULT_FILE.exists():

        await update.effective_message.reply_text(
            "还没有新闻结果，请先执行 /news。"
        )

        return

    try:

        content = RESULT_FILE.read_text(
            encoding="utf-8-sig"
        ).strip()

        if not content:
            content = "最近一次采集没有符合条件的新闻。"

        await send_news(
            update.effective_chat.id,
            context,
            content,
        )

    except Exception as exc:

        logger.exception(
            "读取最近新闻失败"
        )

        await update.effective_message.reply_text(
            f"读取新闻失败：{type(exc).__name__}: {exc}"
        )


# ============================================================
# 主程序
# ============================================================

def main():

    token = load_token()

    # 启动前检查 JSON 配置
    config = load_config()

    if not config["allowed_chat_ids"]:
        raise ValueError(
            "bot_config.json 中没有配置授权群ID"
        )

    app: Application = (
        ApplicationBuilder()
        .token(token)
        .build()
    )

    app.add_handler(
        CommandHandler("start", start_command)
    )

    app.add_handler(
        CommandHandler("id", id_command)
    )

    app.add_handler(
        CommandHandler("news", news_command)
    )

    app.add_handler(
        CommandHandler("latest", latest_command)
    )

    print("=" * 48)
    print("FIN-INTEL Telegram 新闻机器人")
    print("=" * 48)
    print(f"授权群数量：{len(config['allowed_chat_ids'])}")
    print(f"私聊授权用户数量：{len(config['owner_user_ids'])}")
    print("群成员权限：", config["group_member_access"])
    print("采集模式：自动排队")
    print("机器人正在启动……")

    app.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


if __name__ == "__main__":
    main()
