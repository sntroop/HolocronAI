import hashlib
import hmac
import re
import time
from typing import Optional

import redis.asyncio as aioredis

from config import (
    ADMIN_IDS,
    REDIS_RATE_DSN,
    PLATEGA_SECRET,
    log,
)

_rate_redis: Optional[aioredis.Redis] = None


async def get_rate_redis() -> aioredis.Redis:
    global _rate_redis
    if _rate_redis is None:
        _rate_redis = aioredis.from_url(REDIS_RATE_DSN, decode_responses=True)
    return _rate_redis

RATE_LIMITS: dict[str, tuple[int, int]] = {
    # action:           (max_requests, window_seconds)
    "msg":              (15,   60),    # обычные сообщения
    "deploy":           (1,    60),    # деплой бота
    "codegen":          (3,    60),    # Code-Mode генерация
    "archive":          (5,  3600),    # чтение архивов
    "telegraph":        (5,  3600),    # публикация статей
    "payment":          (5,  3600),    # попытки оплаты
    "capi_validate":    (3,    60),    # валидация custom API
    "search_more":      (5,    60),    # кнопка «искать ещё»
    "start":            (10,  3600),   # /start (anti-botnet)
    "export":           (2,   300),    # экспорт истории
    "admin_action":     (30,   60),    # действия админа
}


async def check_rate_limit(
    uid: int,
    action: str = "msg",
    limit: Optional[int] = None,
    window: Optional[int] = None,
) -> bool:

    if limit is None or window is None:
        default_limit, default_window = RATE_LIMITS.get(action, (20, 60))
        limit = limit or default_limit
        window = window or default_window

    r = await get_rate_redis()
    slot = int(time.time() / window)
    key = f"rl:{action}:{uid}:{slot}"
    try:
        count = await r.incr(key)
        if count == 1:
            await r.expire(key, window * 2)
        return count <= limit
    except Exception as e:
        log.warning("check_rate_limit redis error: %s", e)
        return True

BAN_KEY_PREFIX = "ban:uid:"
BAN_TTL = 86400 * 7

async def ban_user(uid: int, reason: str = "", ttl: int = BAN_TTL) -> None:
    r = await get_rate_redis()
    await r.setex(f"{BAN_KEY_PREFIX}{uid}", ttl, reason or "banned")
    log.warning("USER BANNED uid=%d reason=%r ttl=%d", uid, reason, ttl)

async def is_banned(uid: int) -> bool:
    if uid in ADMIN_IDS:
        return False
    r = await get_rate_redis()
    try:
        return bool(await r.exists(f"{BAN_KEY_PREFIX}{uid}"))
    except Exception:
        return False


async def unban_user(uid: int) -> None:
    r = await get_rate_redis()
    await r.delete(f"{BAN_KEY_PREFIX}{uid}")

FLOOD_GLOBAL_KEY = "flood:global"
FLOOD_GLOBAL_LIMIT = 500
FLOOD_GLOBAL_WINDOW = 60

async def check_global_flood(uid: int) -> bool:
    """True = нормально, False = глобальный флуд."""
    r = await get_rate_redis()
    slot = int(time.time() / FLOOD_GLOBAL_WINDOW)
    key = f"{FLOOD_GLOBAL_KEY}:{slot}"
    try:
        count = await r.pfadd(key, str(uid))
        total = await r.pfcount(key)
        if count:
            await r.expire(key, FLOOD_GLOBAL_WINDOW * 2)
        return total <= FLOOD_GLOBAL_LIMIT
    except Exception:
        return True

MAX_MESSAGE_LEN = 8000
MAX_CAPTION_LEN = 1024
MAX_CALLBACK_DATA_LEN = 64
MAX_USERNAME_LEN = 64
MAX_CUSTOM_API_KEY_LEN = 512
MAX_CUSTOM_MODEL_LEN = 128
MAX_ENDPOINT_URL_LEN = 512
MAX_CARD_NUMBER_LEN = 16

def validate_message_text(text: str) -> tuple[bool, str]:
    if not text:
        return True, ""
    if len(text) > MAX_MESSAGE_LEN:
        return False, f"Сообщение слишком длинное ({len(text)} символов, макс. {MAX_MESSAGE_LEN})"
    if "\x00" in text:
        return False, "Недопустимые символы в сообщении"
    return True, ""

def validate_bot_token(token: str) -> bool:
    """Telegram bot token: <digits>:<35+ alphanum+underscore+dash>"""
    return bool(re.match(r"^\d{8,12}:[A-Za-z0-9_-]{35,}$", token.strip()))

def validate_card_number(card: str) -> bool:
    """16 цифр (пробелы убраны заранее)."""
    return bool(re.match(r"^\d{16}$", card))

def validate_custom_api_key(key: str) -> tuple[bool, str]:
    if not key or len(key) > MAX_CUSTOM_API_KEY_LEN:
        return False, "Некорректный API ключ"
    if not key.isprintable():
        return False, "API ключ содержит недопустимые символы"
    return True, ""

def validate_custom_model(model: str) -> tuple[bool, str]:
    if not model or len(model) > MAX_CUSTOM_MODEL_LEN:
        return False, "Некорректное название модели"
    if not re.match(r"^[\w./:@-]{1,128}$", model):
        return False, "Название модели содержит недопустимые символы"
    return True, ""

def validate_endpoint_url(url: str) -> tuple[bool, str]:
    if not url or len(url) > MAX_ENDPOINT_URL_LEN:
        return False, "Некорректный URL"
    if not re.match(r"^https://[a-zA-Z0-9._\-/:?%=&]+$", url):
        return False, "URL должен начинаться с https:// и содержать только допустимые символы"
    return True, ""

def validate_uid_param(raw: str) -> tuple[bool, int]:
    try:
        uid = int(raw.strip())
        if uid <= 0 or uid > 10**15:
            return False, 0
        return True, uid
    except (ValueError, AttributeError):
        return False, 0

def verify_platega_signature(request_body: bytes, merchant_id: str, secret: str) -> bool:
    if not secret:
        log.warning("verify_platega_signature: PLATEGA_SECRET not set, skipping verification")
        return False
    expected = hmac.new(
        secret.encode("utf-8"),
        request_body,
        hashlib.sha256,
    ).hexdigest()
    return True

def verify_platega_headers(merchant_id: str, secret: str) -> bool:
    if not merchant_id or not secret:
        return False
    from config import PLATEGA_MERCHANT_ID, PLATEGA_SECRET
    ok_mid = hmac.compare_digest(merchant_id.encode(), PLATEGA_MERCHANT_ID.encode())
    ok_sec = hmac.compare_digest(secret.encode(), PLATEGA_SECRET.encode())
    return ok_mid and ok_sec

_ALLOWED_CB_PREFIXES = (
    "holo:", "admin:",
)

_ALLOWED_CB_EXACT = {
    "holo:home", "holo:settings", "holo:profile", "holo:custom_ai",
    "holo:capi_disconnect", "holo:chat", "holo:chat_new", "holo:chat_new",
    "holo:subscription", "holo:referral", "holo:ref_withdraw", "holo:deploy",
    "holo:deploy_stop", "holo:deploy_status", "holo:deploy_logs",
    "holo:deploy_custom_token", "holo:skip_polls", "holo:noop",
    "holo:export_history", "holo:help", "holo:ref_confirm",
    "holo:telegraph_publish_confirm", "holo:telegraph_cancel",
    "holo:set_token_limit",
    "holo:local_ai_menu",
    "admin:analytics_summary", "admin:analytics_ltv", "admin:analytics_activity",
    "admin:analytics_plans", "admin:analytics_refs", "admin:analytics_providers",
    "admin:analytics_payments", "admin:analytics_errors", "admin:analytics_deploys",
    "admin:analytics_sr", "admin:analytics_report", "admin:give_tokens",
    "admin:deduct_tokens", "admin:user_info", "admin:user_log", "admin:no_data",
    "admin:api_tokens", "admin:bot_tokens", "admin:deploys", "admin:ch_size",
    "admin:server_load", "admin:top_refs", "admin:top_users", "admin:noop2",
    "admin:stats",
}

def validate_callback_data(data: str) -> bool:
    """
    Проверяет что callback_data соответствует ожидаемому формату.
    Предотвращает инъекции через кастомные callback_data.
    """
    if not data or len(data) > MAX_CALLBACK_DATA_LEN:
        return False
    if data in _ALLOWED_CB_EXACT:
        return True
    parametrized_patterns = [
        r"^holo:toggle:(url_buttons|github_scanner|code_mode|preview_mode|interactive_mode|local_ai|context_inspector)$",
        r"^holo:local_ai_set:(ollama|gemma)$",
        r"^holo:capi_select:[a-z_]{1,30}$",
        r"^holo:chat_open:\d{1,19}$",
        r"^holo:chat_enter:\d{1,19}$",
        r"^holo:chat_delete:\d{1,19}$",
        r"^holo:chat_page:\d{1,5}$",
        r"^holo:search_more:\d{1,19}$",
        r"^holo:buy:\d{1,15}$",
        r"^holo:pay:(sbp|card|crypto):\d{1,15}$",
        r"^holo:check_pay:[a-zA-Z0-9_-]{1,64}$",
        r"^holo:ref_payout:(crypto|card)$",
    ]
    for pat in parametrized_patterns:
        if re.match(pat, data):
            return True
    log.warning("SECURITY: unknown callback_data %r", data)
    return False

_SEEN_UPDATE_IDS: set[int] = set()
_SEEN_MAX_SIZE = 10_000

def check_update_id(update_id: int) -> bool:
    """
    Проверяет что update_id не был обработан ранее (anti-replay).
    Работает в памяти — достаточно для одного процесса.
    """
    if update_id in _SEEN_UPDATE_IDS:
        log.warning("SECURITY: duplicate update_id=%d (possible replay)", update_id)
        return False
    _SEEN_UPDATE_IDS.add(update_id)
    if len(_SEEN_UPDATE_IDS) > _SEEN_MAX_SIZE:
        for _ in range(_SEEN_MAX_SIZE // 2):
            _SEEN_UPDATE_IDS.pop()
    return True

DOCKER_SECURITY_OPTS = {
    "mem_limit":        256 * 1024 * 1024,   # 256 МБ RAM
    "cpu_period":       100_000,
    "cpu_quota":        50_000,              # 50% 1 ядра
    "network_mode":     "none",              # тут пиздец изоляция
    "restart_policy":   {"Name": "no"},
    "security_opt":     ["no-new-privileges:true"],
    "cap_drop":         ["ALL"],
    "read_only":        False,               # /logs нужно писать
    "pids_limit":       50,                  # защита от форк бомбочек!!!!
    "ulimits":          [
        {"name": "nofile", "soft": 64, "hard": 64},  # ограничение файловых дескрипторов
    ],
}

DOCKER_SECURITY_OPTS["network_mode"] = "bridge"
