import asyncio, json, logging, logging.handlers, os, re, shutil, time, io, tempfile
from pathlib import Path
from typing import Optional

import aiohttp, asyncpg, redis.asyncio as aioredis, clickhouse_connect
import docker as docker_lib
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatAction
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.types import BotCommand, BufferedInputFile, CallbackQuery, Message, PollAnswer, Voice, PhotoSize

from dotenv import load_dotenv
load_dotenv()

BOT_TOKEN         = os.getenv("BOT_TOKEN", "")
ADMIN_IDS         = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

CH_HOST           = os.getenv("CH_HOST", "127.0.0.1")
CH_PORT           = int(os.getenv("CH_PORT", "8123"))
CH_USER           = os.getenv("CH_USER", "default")
CH_PASS           = os.getenv("CH_PASS", "")
CH_DB             = os.getenv("CH_DB", "bot_db")
CH_TABLE          = os.getenv("CH_TABLE", "rag_documents")

OLLAMA_URL        = os.getenv("OLLAMA_URL", "http://localhost:11434")
LOCAL_AI_URL      = os.getenv("LOCAL_AI_URL", "http://127.0.0.1:8090/completion")
OLLAMA_EMB_MODEL  = os.getenv("OLLAMA_EMB_MODEL", "bge-m3")
EMB_DIM           = int(os.getenv("EMB_DIM", "1024"))

VSEGPT_KEY        = os.getenv("VSEGPT_KEY", "")
VSEGPT_URL        = os.getenv("VSEGPT_URL", "https://api.vsegpt.ru/v1/chat/completions")
VSEGPT_MODEL_CHAT = os.getenv("VSEGPT_MODEL_CHAT", "qwen/qwq-32b")
VSEGPT_MODEL_CODE = os.getenv("VSEGPT_MODEL_CODE", "deepseek/deepseek-chat-0324-alt")

GITHUB_TOKEN      = os.getenv("GITHUB_TOKEN", "")
GITHUB_API_BASE   = "https://api.github.com"

TELEGRAPH_TOKEN   = os.getenv("TELEGRAPH_TOKEN", "")
TELEGRAPH_URL     = "https://api.telegra.ph"

TOP_K             = int(os.getenv("TOP_K", "7"))

PG_DSN            = os.getenv("PG_DSN", "postgresql://user:password@localhost:5432/bot_db")
REDIS_DSN         = os.getenv("REDIS_DSN", "redis://localhost:6379/0")
REDIS_RATE_DSN    = os.getenv("REDIS_RATE_DSN", "redis://localhost:6379/1")
REDIS_SESSION_DSN = os.getenv("REDIS_SESSION_DSN", "redis://localhost:6379/2")

PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "")
PLATEGA_SECRET      = os.getenv("PLATEGA_SECRET", "")
PLATEGA_URL         = "https://app.platega.io"
PLATEGA_CALLBACK    = os.getenv("PLATEGA_CALLBACK", "https://yourdomain.com/payments/platega")

CRYPTOBOT_TOKEN     = os.getenv("CRYPTOBOT_TOKEN", "")
CRYPTOBOT_URL       = "https://pay.crypt.bot/api"

PLANS = [
    (80_000,       79,    0.95,  "80K"),
    (110_000,     100,    1.21,  "110K"),
    (180_000,     150,    1.81,  "180K"),
    (250_000,     200,    2.41,  "250K"),
    (400_000,     299,    3.61,  "400K"),
    (600_000,     399,    4.82,  "600K"),
    (1_000_000,   599,    7.23,  "1M"),
    (2_000_000,   999,   12.60,  "2M"),
    (4_500_000,  1990,   24.03,  "4.5M"),
    (10_000_000, 3990,   48.18,  "10M"),
    (30_000_000, 8990,  108.00,  "30M"),
    (100_000_000,24990, 301.73,  "100M"),
]
PLAN_BY_TOKENS     = {p[0]: p for p in PLANS}
REF_PERCENT        = 5
REF_MIN_PAYOUT     = 100
TOKENS_MIN_REQUEST = 500
TOKENS_MIN_CODE    = 1000

SANDBOX_ROOT     = Path(os.getenv("SANDBOX_ROOT", "/var/www/bot/sandbox"))
SANDBOX_BASE_URL = os.getenv("SANDBOX_BASE_URL", "https://yourdomain.com/sandbox")
SANDBOX_TTL      = 20 * 60
BANNER_PATH      = Path(__file__).parent / "star.png"

PREVIEW_TOKENS = [t.strip() for t in os.getenv("PREVIEW_TOKENS", "").split(",") if t.strip()]
DEPLOY_TTL       = 20 * 60
BOTS_DIR         = Path(os.getenv("BOTS_DIR", "/opt/bot/preview_bots"))
LOGS_DIR         = Path(os.getenv("LOGS_DIR", "/opt/bot/preview_logs"))
DOCKER_IMAGE     = "python:3.12-slim"
CONTAINER_PREFIX = "bot_"

ARCHIVE_EXTS         = {".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz"}
ARCHIVE_MAX_SIZE     = 50  * 1024 * 1024
ARCHIVE_MAX_UNPACKED = 100 * 1024 * 1024
ARCHIVE_MAX_FILES    = 500
ARCHIVE_MAX_FILE_SZ  = 5   * 1024 * 1024
ARCHIVE_MAX_DEPTH    = 10
ARCHIVE_MAX_READ     = 10
ARCHIVE_BLOCKED_EXT  = {
    ".exe", ".dll", ".so", ".dylib", ".bat", ".cmd", ".ps1", ".vbs",
    ".msi", ".bin", ".elf", ".pyc", ".pyo", ".com", ".scr", ".hta", ".jar",
}
ARCHIVE_TEXT_EXT = {
    ".txt", ".py", ".js", ".ts", ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg",
    ".md", ".rst", ".html", ".htm", ".css", ".xml", ".csv", ".sql", ".env",
    ".bash", ".zsh", ".log", ".conf", ".php", ".rb", ".go", ".rs", ".cpp",
    ".c", ".h", ".java", ".kt", ".swift", ".dart", ".r", ".scala", ".pl", ".sh",
}

E = {
    "robot":  "5372981976804366741", "gear":   "5373123633415723713",
    "search": "5372913502140766965", "star":   "5458799228719472718",
    "bolt":   "5370547013815376328", "bulb":   "5465143921912846619",
    "globe":  "5399898266265475100", "user":   "5373012449597335010",
    "chart":  "5472308992514464048", "tools":  "5373262021556967911",
    "check":  "5370900820336319679", "cross":  "5373123633415723713",
    "refresh":"5370869711888194012", "key":    "5472308992514464048",
    "bell":   "5370870691140737817", "code":   "5334882760735598374",
    "github": "5454219968948229067", "fire":   "5375271950287379933",
    "pin":    "5467480195143310096", "eyes":   "5372952977185184202",
    "preview":"5399898266265475100", "rocket": "5370869711888194012",
    "stop":   "5373123633415723713", "log":    "5334882760735598374",
    "note":   "5465143921912846619",
}

LOG_FILE = os.getenv("LOG_FILE", "/opt/bot/logs/bot.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_fh  = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=50*1024*1024, backupCount=5, encoding="utf-8")
_fh.setFormatter(_fmt)
_sh  = logging.StreamHandler()
_sh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_fh, _sh])
log = logging.getLogger("bot")

class S(StatesGroup):
    chat=State(); code_polling=State(); awaiting_custom_token=State()
    custom_api_key=State(); custom_api_model=State()
    awaiting_card=State()
    admin_give_uid=State(); admin_give_amount=State()
    admin_deduct_uid=State(); admin_deduct_amount=State()
    admin_check_uid=State()
    telegraph_confirm=State()
    set_token_limit=State()

def tg_e(name: str, fb: str = "●") -> str:
    eid = E.get(name, "")
    return f'<tg-emoji emoji-id="{eid}">{fb}</tg-emoji>' if eid else fb

PROVIDERS = {
    "openai":        {"name":"OpenAI",          "url":"https://api.openai.com/v1/chat/completions",                          "type":"openai"},
    "anthropic":     {"name":"Anthropic",        "url":"https://api.anthropic.com/v1/messages",                               "type":"anthropic",
                      "models":["claude-opus-4-5","claude-sonnet-4-5","claude-haiku-4-5"]},
    "deepseek":      {"name":"DeepSeek",         "url":"https://api.deepseek.com/v1/chat/completions",                        "type":"openai"},
    "groq":          {"name":"Groq",             "url":"https://api.groq.com/openai/v1/chat/completions",                     "type":"openai"},
    "mistral":       {"name":"Mistral",          "url":"https://api.mistral.ai/v1/chat/completions",                          "type":"openai"},
    "together":      {"name":"Together AI",      "url":"https://api.together.xyz/v1/chat/completions",                        "type":"openai"},
    "openrouter":    {"name":"OpenRouter",       "url":"https://openrouter.ai/api/v1/chat/completions",                       "type":"openai"},
    "vsegpt":        {"name":"VseGPT",           "url":"https://api.vsegpt.ru/v1/chat/completions",                           "type":"openai"},
    "perplexity":    {"name":"Perplexity",       "url":"https://api.perplexity.ai/chat/completions",                          "type":"openai",
                      "models":["llama-3.1-sonar-huge-128k-online","llama-3.1-sonar-large-128k-online"]},
    "gemini":        {"name":"Google Gemini",    "url":"https://generativelanguage.googleapis.com/v1beta/openai/chat/completions", "type":"openai"},
    "qwen_ali":      {"name":"Qwen (Alibaba)",   "url":"https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",  "type":"openai"},
    "qwen_or":       {"name":"Qwen (OpenRouter)","url":"https://openrouter.ai/api/v1/chat/completions",                       "type":"openai",
                      "models":["qwen/qwen-2.5-72b-instruct","qwen/qwq-32b"]},
    "huggingface":   {"name":"HuggingFace (Inference API)", "url":"https://api-inference.huggingface.co/v1/chat/completions", "type":"openai"},
    "huggingface_ep":{"name":"HuggingFace (Endpoints)",     "url":"", "type":"openai_ep", "note":"Enter your Inference Endpoint URL"},
}

PROVIDER_ORDER = ["openai","anthropic","deepseek","groq","mistral","together","openrouter",
                  "vsegpt","perplexity","gemini","qwen_ali","qwen_or","huggingface","huggingface_ep"]

SYSTEM_PROMPT = (
    "You are a helpful AI assistant. Answer concisely and to the point.\n"
    "Use Telegram HTML formatting:\n"
    "- Bold: <b>text</b>\n"
    "- Links: <a href=\"https://example.com\">link text</a>\n"
    "- Lists: each line starts with '- '\n"
    "- Code: `code` or ```block```\n\n"
    "Do NOT use markdown link syntax [text](url).\n"
    "Use only HTML tags <a href=\"url\">text</a> for links.\n\n"
    "If you don't know something, say so honestly."
)
