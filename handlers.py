from config import *
from security import (
    check_rate_limit, is_banned, ban_user,
    validate_message_text, validate_bot_token, validate_card_number,
    validate_custom_api_key, validate_custom_model, validate_endpoint_url,
    validate_uid_param, validate_callback_data, check_update_id,
    check_global_flood, DOCKER_SECURITY_OPTS,
)
from database import *
from services import (
    ask_llm, ask_llm_stream, ask_llm_custom, ask_llm_custom_stream,
    validate_custom_api, get_embedding, ch_vector_search, ch_repo_exists, ch_insert_repo,
    extract_github_repos, github_get_repo, fetch_url_content, ocr_image, transcribe_voice,
    read_archive, telegraph_publish, _detect_is_telegraph, _detect_is_tg_bot, _detect_is_web,
    platega_create_invoice, cryptobot_create_invoice, cryptobot_check_invoice,
    cryptobot_create_check, _process_payment,
    _daily_report, ask_local_ai, ask_local_ai_stream,
    PROVIDERS, PROVIDER_ORDER, SYSTEM_PROMPT,
)
from keyboards import (
    btn, markup, tg_send, tg_send_banner, tg_edit, tg_edit_banner, tg_send_poll, tg_stop_poll,
    tg_typing, E2, WAIT_EMOJI,
    kb_home, kb_settings, kb_profile, kb_custom_ai, kb_custom_ai_back, kb_back, kb_chat,
    kb_skip_polls, kb_after_code, kb_deploy_bot, kb_deploy_web, kb_url_rows, kb_chat_list,
    kb_chat_actions, kb_in_chat, kb_admin, kb_promos, kb_my_center,
    CUSTOM_AI_INFO, home_text, settings_text, profile_text, chat_list_text, HELP_TEXT,
    clean_md_to_html, safe_truncate_html, _stream_preview, extract_urls,
    context_bar, context_bar_transferring, CONTEXT_MAX,
)

class _SPromo(StatesGroup):
    enter_promo              = State()
    admin_promo_set_discount = State()
    admin_promo_code_name    = State()
    admin_promo_code_type    = State()
    admin_promo_code_value   = State()
    admin_promo_code_filter  = State()
    admin_promo_code_limit   = State()

S.enter_promo              = _SPromo.enter_promo
S.admin_promo_set_discount = _SPromo.admin_promo_set_discount
S.admin_promo_code_name    = _SPromo.admin_promo_code_name
S.admin_promo_code_type    = _SPromo.admin_promo_code_type
S.admin_promo_code_value   = _SPromo.admin_promo_code_value
S.admin_promo_code_filter  = _SPromo.admin_promo_code_filter
S.admin_promo_code_limit   = _SPromo.admin_promo_code_limit

def _extract_web_blocks(raw):
    blocks={"html":"","css":"","js":""}
    pats={"html":r'```[ \t]*html\n([\s\S]*?)```',"css":r'```[ \t]*css\n([\s\S]*?)```',
          "js":r'```[ \t]*(?:javascript|js|typescript|ts)\n([\s\S]*?)```'}
    for lang,pat in pats.items():
        m=re.search(pat,raw,re.IGNORECASE)
        if m: blocks[lang]=m.group(1).strip()
    for raw_block in re.findall(r'```[a-zA-Z]*\n?([\s\S]*?)```',raw):
        b=raw_block.strip()
        if not b: continue
        if not blocks["html"] and re.search(r'<\s*(!DOCTYPE|html|head|body|div|section)',b,re.IGNORECASE): blocks["html"]=b
        elif not blocks["js"] and not re.search(r'<[a-z]',b,re.IGNORECASE) and re.search(r'(function\s+\w+|const\s+\w+\s*=|document\.|addEventListener|=>)',b): blocks["js"]=b
        elif not blocks["css"] and not re.search(r'<[a-z]',b,re.IGNORECASE) and re.search(r'[.#*:a-zA-Z][\w\s,.-]*\s*\{[\s\S]*?\}',b): blocks["css"]=b
    if blocks["html"]:
        if not blocks["css"]:
            sm=re.search(r'<style[^>]*>([\s\S]*?)</style>',blocks["html"],re.IGNORECASE)
            if sm: blocks["css"]=sm.group(1).strip()
        if not blocks["js"]:
            jm=re.search(r'<script[^>]*>([\s\S]*?)</script>',blocks["html"],re.IGNORECASE)
            if jm: blocks["js"]=jm.group(1).strip()
    return blocks

def sandbox_deploy_files(uid,blocks):
    if not any(blocks.values()): return None
    user_dir=SANDBOX_ROOT/str(uid); user_dir.mkdir(parents=True,exist_ok=True)
    html=blocks.get("html",""); css=blocks.get("css",""); js=blocks.get("js","")
    if not html: html="<!DOCTYPE html><html><head><meta charset='UTF-8'></head><body></body></html>"
    if css and "style.css" not in html: html=html.replace("</head>","  <link rel='stylesheet' href='style.css'>\n</head>")
    if js and "script.js" not in html: html=html.replace("</body>","  <script src='script.js'></script>\n</body>")
    (user_dir/"index.html").write_text(html,encoding="utf-8")
    if css: (user_dir/"style.css").write_text(css,encoding="utf-8")
    if js:  (user_dir/"script.js").write_text(js,encoding="utf-8")
    return f"{SANDBOX_BASE_URL}/{uid}/index.html"

def sandbox_cleanup(uid):
    d=SANDBOX_ROOT/str(uid)
    if d.exists(): shutil.rmtree(d,ignore_errors=True); log.info("sandbox cleaned uid=%d",uid)

def _docker_stop(container_id):
    if not container_id: return
    try:
        c=docker_lib.from_env().containers.get(container_id)
        c.stop(timeout=5); c.remove(force=True); log.info("container %s removed",container_id[:12])
    except docker_lib.errors.NotFound: pass
    except Exception as e: log.warning("docker_stop: %s",e)

def _clean_code_block(code: str) -> str:
    lines = code.split("\n")
    result = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            result.append(line); continue
        if stripped.startswith("##") or stripped.startswith("**") or stripped.endswith("**"):
            continue
        if re.match(r'^\d+\.\s', stripped):
            continue
        if re.match(r'^[\U00010000-\U0010FFFF\U00002600-\U000027BF]\s', stripped):
            continue
        if stripped.startswith("- `") or stripped.startswith("* `"):
            continue
        if re.match(r'^(pip\s+install|pip3\s+install|cd\s|mkdir\s|export\s|echo\s|chmod\s|apt\s|sudo\s)', stripped):
            continue
        result.append(line)
    cleaned = "\n".join(result)
    code_lines = [l for l in result if l.strip() and
                  not l.strip().startswith("#") and
                  not l.strip().startswith("import os")]
    return cleaned.strip() if len(code_lines) >= 2 else ""

def _extract_bot_files(full_text: str) -> tuple:
    files = {}

    new_fmt = re.findall(
        r'^([\w]+\.py):\s*\n```(?:python|py)?\n([\s\S]*?)```',
        full_text, re.MULTILINE
    )
    for fname, code in new_fmt:
        cleaned = _clean_code_block(code)
        if cleaned:
            files[fname.strip()] = cleaned

    if not files.get("requirements.txt"):
        req_new = re.search(
            r'^requirements\.txt:\s*\n```[^\n]*\n([\s\S]*?)```',
            full_text, re.MULTILINE
        )
        if req_new:
            files["requirements.txt"] = req_new.group(1).strip()

    if files:
        log.info("extracted files (new fmt): %s", list(files.keys()))
        for candidate in ["main.py", "bot.py", "app.py", "run.py", "start.py"]:
            if candidate in files:
                entry = candidate; break
        else:
            entry = next((k for k in files if k.endswith(".py")), next(iter(files)))
        return files, entry

    named = re.findall(
        r"(?:#{1,3}\s*|[*]{2})?([\w]+\.py)(?:[*]{2})?\s*\n```(?:python|py)?\n([\s\S]*?)```",
        full_text
    )
    for fname, code in named:
        cleaned = _clean_code_block(code)
        if cleaned:
            files[fname.strip()] = cleaned

    if not files:
        blocks = re.findall(r"```(?:python|py)?\n([\s\S]*?)```", full_text)
        for code in blocks:
            code = code.strip()
            if not code: continue
            first_line = code.split("\n")[0].strip()
            m = re.match(r"#\s*([\w./]+\.py)\s*$", first_line)
            if m:
                fname = m.group(1).split("/")[-1]
                files[fname] = "\n".join(code.split("\n")[1:]).strip()
            else:
                if any(x in code for x in ["dp.include_router","start_polling","run_polling","executor.","asyncio.run(main"]):
                    files.setdefault("main.py", code)
                elif any(x in code for x in ["@router.","Router()","message_handler","callback_query_handler"]):
                    n = f"handlers_{len([k for k in files if k.startswith('handlers')])}.py"
                    files.setdefault("handlers.py", code) if "handlers.py" not in files else files.__setitem__(n, code)
                elif any(x in code for x in ["CREATE TABLE","sqlite3.connect","psycopg2","motor.motor"]):
                    files.setdefault("db.py", code)
                elif any(x in code for x in ["State(","StatesGroup","FSMContext"]):
                    files.setdefault("states.py", code)
                else:
                    idx = len(files)
                    fname = "main.py" if idx == 0 else f"module_{idx}.py"
                    files.setdefault(fname, code)

    if not files:
        all_blocks = re.findall(r"```\n([\s\S]*?)```", full_text)
        code = max(all_blocks, key=len).strip() if all_blocks else full_text
        files["bot.py"] = code

    for candidate in ["main.py", "bot.py", "app.py", "run.py", "start.py"]:
        if candidate in files:
            entry = candidate; break
    else:
        entry = next(iter(files))

    req_blocks = re.findall(
        r"(?:###\s*requirements\.txt\s*\n```(?:txt|text|pip)?\n)([\s\S]*?)```",
        full_text
    )
    if not req_blocks:
        req_blocks = re.findall(
            r"```(?:txt|text|pip|requirements)\n([\s\S]*?)```",
            full_text
        )
    if req_blocks:
        req_content = max(req_blocks, key=len).strip()
        if not any(x in req_content for x in ["import ", "def ", "class ", "async "]):
            files["requirements.txt"] = req_content
            log.info("requirements.txt from AI: %r", req_content[:100])

    log.info("extracted files: %s", list(files.keys()))
    return files, entry

def _extract_bot_code(full_text: str) -> str:
    files, _ = _extract_bot_files(full_text)
    return max(files.values(), key=len)

def _patch_token(code: str, token: str) -> str:
    token_vars = [
        "BOT_TOKEN", "API_TOKEN", "TOKEN", "TG_TOKEN", "TELEGRAM_TOKEN",
        "TELEGRAM_BOT_TOKEN", "BOT_API_TOKEN", "TelegramToken",
    ]
    for var in token_vars:
        code = re.sub(
            rf'{var}\s*=\s*["\'][^\"\']*["\']',
            f'{var} = os.environ.get("BOT_TOKEN", "")',
            code
        )
    code = re.sub(r'telebot\.TeleBot\(["\'][^\"\']*["\']\)',
                  'telebot.TeleBot(os.environ.get("BOT_TOKEN", ""))', code)
    code = re.sub(r'Bot\(token=["\'][^\"\']*["\']\)',
                  'Bot(token=os.environ.get("BOT_TOKEN", ""))', code)
    code = re.sub(r'Bot\(["\'][^\"\']*["\']\)',
                  'Bot(os.environ.get("BOT_TOKEN", ""))', code)
    code = re.sub(r'\nBOT_TOKEN\s*=\s*\n', '\n', code)
    code = re.sub(r'\nTOKEN\s*=\s*\n', '\n', code)
    code = re.sub(r'\nAPI_TOKEN\s*=\s*\n', '\n', code)
    if "import os" not in code:
        code = "import os\n" + code
    return code

def _sanitize_bot_code(code: str) -> str:
    import re as _re

    dangerous_patterns = [
        r'(message|msg|bot)\.answer\([^)]*os\.environ[^)]*\)',
        r'(message|msg|bot)\.send_message\([^)]*os\.environ[^)]*\)',
        r'bot\.send_message\([^)]*BOT_TOKEN[^)]*\)',
        r'(message|msg)\.answer\([^)]*BOT_TOKEN[^)]*\)',
        r'f["\'][^"\']*os\.environ[^"\']*["\']',
        r'f["\'][^"\']*getenv[^"\']*["\']',
        r'open\(["\']/?etc/',
        r'open\(["\']/?proc/',
        r'subprocess\.(run|call|Popen).*shell=True',
        r'os\.system\(',
        r'os\.environ\.items\(\)',
        r'str\(os\.environ\)',
        r'dict\(os\.environ\)',
    ]
    for pat in dangerous_patterns:
        if _re.search(pat, code, _re.IGNORECASE):
            log.warning("SECURITY: blocked dangerous pattern %r in bot code", pat[:50])
            raise ValueError(f"Код содержит опасный паттерн и не может быть задеплоен")

    code = _re.sub(r',\s*is_admin=True', '', code)
    code = _re.sub(r'[├└│─]+[^\n]*\n', '', code)
    code = _re.sub(r'^pip install[^\n]*\n', '', code, flags=_re.MULTILINE)
    code = _re.sub(r'#\s*project_root[^\n]*\n', '', code)
    code = _re.sub(r'\n(BOT_TOKEN|TOKEN|API_TOKEN)\s*=\s*\n', '\n', code)
    code = code.lstrip('\n')
    return code

def _detect_aiogram_version(code: str) -> str:
    v2_signs = [
        "aiogram.contrib", "from aiogram.dispatcher import",
        "executor.start_polling", "Dispatcher(bot,",
        "dp.message_handler", "dp.callback_query_handler",
        "@dp.message_handler", "@dp.callback_query_handler",
        "from aiogram import executor",
    ]
    for sign in v2_signs:
        if sign in code:
            return "2"
    return "3"

_STDLIB = {
    "os","sys","re","json","time","math","random","datetime","pathlib","typing",
    "asyncio","threading","logging","sqlite3","hashlib","hmac","uuid","copy",
    "io","struct","base64","urllib","http","email","html","xml","csv","configparser",
    "collections","itertools","functools","dataclasses","enum","abc","contextlib",
    "traceback","inspect","gc","weakref","signal","subprocess","shutil","tempfile",
    "glob","fnmatch","stat","socket","ssl","select","queue","heapq","bisect",
    "array","string","textwrap","unicodedata","codecs","locale","gettext",
    "argparse","unittest","pprint","reprlib","warnings","decimal","fractions",
    "statistics","cmath","numbers","operator","types","dis","ast","token","tokenize",
    "platform","sysconfig","site","importlib","pkgutil","zipfile","tarfile","gzip",
    "bz2","lzma","zlib","pickle","shelve","dbm","secrets","token","keyword",
}

_IMPORT_TO_PIP = {
    "telebot":          "pyTelegramBotAPI",
    "telegram":         "python-telegram-bot",
    "PIL":              "pillow",
    "cv2":              "opencv-python",
    "sklearn":          "scikit-learn",
    "bs4":              "beautifulsoup4",
    "dotenv":           "python-dotenv",
    "psycopg2":         "psycopg2-binary",
    "MySQLdb":          "mysqlclient",
    "mysql":            "mysql-connector-python",
    "pymysql":          "PyMySQL",
    "Crypto":           "pycryptodome",
    "yaml":             "pyyaml",
    "dateutil":         "python-dateutil",
    "gi":               "PyGObject",
    "usb":              "pyusb",
    "serial":           "pyserial",
    "wx":               "wxPython",
    "gi.repository":    "PyGObject",
    "google.cloud":     "google-cloud",
    "google.auth":      "google-auth",
    "anthropic":        "anthropic",
    "openai":           "openai",
    "g4f":              "g4f",
}

def _make_requirements(code: str) -> str:
    import re as _re
    found = set()

    for m in _re.finditer(r'^import\s+([\w]+)', code, _re.MULTILINE):
        found.add(m.group(1))

    for m in _re.finditer(r'^from\s+([\w]+)', code, _re.MULTILINE):
        found.add(m.group(1))

    libs = []

    if "aiogram" in found:
        found.discard("aiogram")
        ver = _detect_aiogram_version(code)
        libs.append("aiogram==2.25.2" if ver == "2" else "aiogram==3.20.0")

    _never_install = {"aiohttp", "python-telegram-bot", "pyrogram", "telethon"}

    _local_mods = {"main","bot","app","run","handlers","db","database",
                   "config","settings","keyboards","states","utils","helpers",
                   "models","schemas","middlewares","filters","routers","logger"}
    for imp in sorted(found):
        if imp in _STDLIB:
            continue
        if imp in _local_mods: continue
        if not imp or not imp[0].isalpha():
            continue
        pip_name = _IMPORT_TO_PIP.get(imp, imp)
        if pip_name.lower() in _never_install: continue
        if pip_name not in libs:
            libs.append(pip_name)

    if not libs:
        libs.append("aiogram==3.20.0")

    return "\n".join(libs) + "\n"

def _rewrite_forbidden_imports(code: str, topic: str = "") -> str:
    forbidden = [
        'from telegram import', 'from telegram.ext import',
        'import telegram', 'from pyrogram import', 'import pyrogram',
        'from telethon import', 'import telethon',
    ]
    if not any(f in code for f in forbidden):
        return code

    log.warning("_rewrite_forbidden_imports: replacing forbidden imports in code, topic=%r", topic[:50])

    start_text = f"Привет! Я бот по теме: {topic[:60]}. Напиши вопрос." if topic else "Привет! Напиши мне что-нибудь."
    sm = re.search(r'reply_text\s*\(\s*["\']([^"\']{5,200})["\']', code)
    if sm: start_text = sm.group(1)

    model = "qwen/qwen3-next-80b-a3b-instruct:free"
    mm = re.search(r'(?:MODEL|model)\s*=\s*["\']([^"\']{5,80})["\']', code)
    if mm: model = mm.group(1)

    sys_prompt = f"Ты полезный ИИ-ассистент. Тема: {topic}." if topic else "Ты полезный ИИ-ассистент."

    template = f'import os\nimport asyncio\nimport aiohttp\nfrom aiogram import Bot, Dispatcher, Router\nfrom aiogram.filters import CommandStart\nfrom aiogram.types import Message\n\nBOT_TOKEN = os.environ.get("BOT_TOKEN", "")\nOPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")\nMODEL = "{model}"\nSYSTEM_PROMPT = "{sys_prompt}"\n\nrouter = Router()\nbot = Bot(token=BOT_TOKEN)\ndp = Dispatcher()\ndp.include_router(router)\n\nasync def ask_ai(text: str) -> str:\n    url = "https://openrouter.ai/api/v1/chat/completions"\n    headers = {{"Authorization": f"Bearer {{OPENROUTER_API_KEY}}", "Content-Type": "application/json"}}\n    payload = {{"model": MODEL, "messages": [{{"role": "system", "content": SYSTEM_PROMPT}}, {{"role": "user", "content": text}}], "max_tokens": 1000, "temperature": 0.7}}\n    try:\n        async with aiohttp.ClientSession() as session:\n            async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as r:\n                if r.status == 200:\n                    data = await r.json()\n                    return data["choices"][0]["message"]["content"]\n                return f"Ошибка API: {{r.status}}"\n    except Exception as e:\n        return f"Ошибка: {{e}}"\n\n@router.message(CommandStart())\nasync def cmd_start(message: Message):\n    await message.answer("{start_text}")\n\n@router.message()\nasync def handle_message(message: Message):\n    if not message.text:\n        return\n    reply = await ask_ai(message.text)\n    await message.answer(reply[:4096])\n\nasync def main():\n    await dp.start_polling(bot)\n\nif __name__ == "__main__":\n    asyncio.run(main())\n'
    return template

def _normalize_bot_code(full_text: str, topic: str = "") -> str:
    import ast as _ast

    all_blocks = re.findall(r'```(?:python|py)?\s*\n([\s\S]*?)```', full_text, re.IGNORECASE)
    py_blocks = []
    for b in all_blocks:
        b = b.strip()
        if not b or len(b) < 20: continue
        if not any(x in b for x in ['import ', 'def ', 'class ', 'async ', 'Bot(', 'bot =', 'dp =', 'Updater']): continue
        py_blocks.append(b)

    if not py_blocks:
        all_raw = re.findall(r'```[a-z]*\s*\n([\s\S]*?)```', full_text)
        for b in all_raw:
            b = b.strip()
            if len(b) > 100 and 'import' in b:
                py_blocks.append(b)

    if not py_blocks:
        return full_text

    main_code = max(py_blocks, key=len)

    lines = main_code.split('\n')
    if lines and re.match(r'^#\s*[\w./]+\.py', lines[0].strip()):
        main_code = '\n'.join(lines[1:]).strip()

    main_code = _rewrite_forbidden_imports(main_code, topic=topic)

    fname = "main.py"

    req_content = ""
    req_explicit = re.search(
        r'requirements\.txt[:\s]*\n```[^\n]*\n([\s\S]*?)```', full_text)
    if req_explicit:
        rb = req_explicit.group(1).strip()
        pkg_lines = []
        for l in rb.splitlines():
            l = l.strip()
            if not l or l.startswith('#') or l.startswith('pip'): continue
            if re.match(r'^[a-zA-Z][a-zA-Z0-9_\-]*(\s*[><=!~].+)?$', l):
                pkg_lines.append(l)
        if pkg_lines:
            req_content = '\n'.join(pkg_lines)

    if not req_content:
        req_content = _make_requirements(main_code).strip()

    _bad_pkgs = {'python-telegram-bot', 'pyrogram', 'telethon', 'aiohttp',
                 'python_telegram_bot', 'os', 'sys', 'json', 'asyncio', 're'}
    clean_reqs = []
    for l in req_content.splitlines():
        pkg = l.strip().lower().split('=')[0].split('>')[0].split('<')[0].split('[')[0]
        if pkg and pkg not in _bad_pkgs:
            clean_reqs.append(l.strip())
    req_content = '\n'.join(clean_reqs) if clean_reqs else 'aiogram==3.20.0'

    result = f"{fname}:\n```python\n{main_code}\n```\n\nrequirements.txt:\n```\n{req_content}\n```"
    log.info("_normalize_bot_code: fname=%s code_len=%d req=%r", fname, len(main_code), req_content[:80])
    return result

def _validate_python_file(fname: str, code: str) -> tuple[bool, str]:
    import ast
    try:
        ast.parse(code)
        return True, ""
    except SyntaxError as e:
        return False, f"{fname}: SyntaxError line {e.lineno}: {e.msg}"

def _docker_run(token, uid, bot_code):
    import shlex
    import re
    
    user_dir = BOTS_DIR / str(uid)
    user_dir.mkdir(parents=True, exist_ok=True)
    (LOGS_DIR / str(uid)).mkdir(parents=True, exist_ok=True)

    for f in user_dir.glob("*.py"):
        f.unlink(missing_ok=True)

    files, entry_point = _extract_bot_files(bot_code)
    log.info("extracted files: %s, entry=%s", list(files.keys()), entry_point)

    all_code = "\n".join(files.values())

    for fname in files:
        files[fname] = _patch_token(files[fname], token)
        try:
            files[fname] = _sanitize_bot_code(files[fname])
        except ValueError as sec_err:
            log.warning("SECURITY BLOCK uid=%d file=%s: %s", uid, fname, sec_err)
            raise RuntimeError(f"🚫 Безопасность: {sec_err}")

    aiogram_ver = _detect_aiogram_version(all_code)
    log.info("bot files: %s, entry=%s", list(files.keys()), entry_point)
    for _fn, _fc in files.items():
        log.info("  file %s: %d bytes, first_line=%r", _fn, len(_fc), _fc.split("\n")[0][:60])
    log.info("files ready: %d files, aiogram_ver=%s", len(files), aiogram_ver)

    py_files_to_validate = {k: v for k, v in files.items() if k.endswith(".py")}
    syntax_errors = []
    for fname, code in py_files_to_validate.items():
        ok, err = _validate_python_file(fname, code)
        if not ok:
            syntax_errors.append(err)
            log.warning("Syntax validation failed: %s", err)
    if syntax_errors:
        raise RuntimeError("❌ Синтаксическая ошибка в сгенерированном коде:\n" + "\n".join(syntax_errors))

    for fname, code in files.items():
        fpath = user_dir / fname
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(code, encoding="utf-8")

    (LOGS_DIR / str(uid) / "bot.log").write_text("", encoding="utf-8")

    if "requirements.txt" in files:
        req_content = files.pop("requirements.txt")
        auto_reqs = set(_make_requirements(all_code).strip().split("\n"))
        ai_reqs   = set(req_content.strip().split("\n"))
        ai_clean  = set()
        _local = {"main","bot","app","run","handlers","db","database","config",
                  "settings","keyboards","states","utils","helpers","models",
                  "schemas","middlewares","filters","routers","logger"}
        for r in ai_reqs:
            r = r.strip()
            if not r or r.startswith("#"): continue
            if r.startswith("import ") or r.startswith("from "): continue
            if r.lower() in _local: continue
            if " " in r and not any(op in r for op in ["==",">=","<=",">","<","!=","~="]): continue
            if not r[0].isalpha() and r[0] != "-": continue
            ai_clean.add(r)
        merged = list(ai_clean)
        _local_mods = {"main","bot","app","run","handlers","db","database","config",
                       "settings","keyboards","states","utils","helpers","models",
                       "schemas","middlewares","filters","routers","logger"}
        _bot_file_names = {f.stem for f in (BOTS_DIR/str(uid)).glob("*.py")} if (BOTS_DIR/str(uid)).exists() else set()
        _exclude = _local_mods | _bot_file_names
        for r in auto_reqs:
            pkg = r.split("==")[0].split(">=")[0].split("<=")[0].strip().lower()
            if pkg in _exclude: continue
            if pkg and not any(pkg in x.lower() for x in merged):
                merged.append(r)
        req_content = "\n".join(merged)
        log.info("requirements (AI+auto): %r", req_content[:150])
    else:
        req_content = _make_requirements(all_code)
        log.info("requirements (auto): %r", req_content.strip())
    (user_dir / "requirements.txt").write_text(req_content + "\n", encoding="utf-8")

    entry_path = user_dir / entry_point
    if not entry_path.exists():
        py_files = list(user_dir.glob("*.py"))
        if py_files:
            entry_point = py_files[0].name
            log.warning("entry %s not found, using %s", entry_path.name, entry_point)
        else:
            raise RuntimeError(f"Нет Python файлов в папке бота! entry={entry_point}")

    if not re.match(r'^[a-zA-Z0-9_\-]+\.py$', entry_point):
        log.error("Invalid entry_point detected: %r", entry_point)
        raise RuntimeError(f"Некорректное имя файла: {entry_point}")
    
    entry_point_escaped = shlex.quote(entry_point)
    
    client = docker_lib.from_env()
    name = f"{CONTAINER_PREFIX}{uid}"
    try:
        old = client.containers.get(name)
        old.stop(timeout=3)
        old.remove(force=True)
    except docker_lib.errors.NotFound:
        pass

    for f in user_dir.iterdir():
        if f.is_file() and f.suffix not in (".py", ".txt", ".json", ".env"):
            log.warning("removing non-py file from bot dir: %s", f.name)
            f.unlink(missing_ok=True)

    cmd = (
        "pip install --quiet --no-warn-script-location -r /bot/requirements.txt > /logs/pip.log 2>&1 "
        "&& echo '=== pip OK ===' >> /logs/bot.log "
        f"&& python -u /bot/{entry_point_escaped} >> /logs/bot.log 2>&1 "
        "|| (echo '=== FAILED ===' >> /logs/bot.log && cat /logs/pip.log >> /logs/bot.log)"
    )

    c = client.containers.run(
        image=DOCKER_IMAGE,
        command=["bash", "-c", cmd],
        name=name,
        detach=True,
        environment={"BOT_TOKEN": token, "PYTHONUNBUFFERED": "1", "PIP_NO_CACHE_DIR": "1"},
        volumes={
            str(user_dir):              {"bind": "/bot",  "mode": "ro"},
            str(LOGS_DIR / str(uid)):   {"bind": "/logs", "mode": "rw"},
        },
        mem_limit=DOCKER_SECURITY_OPTS["mem_limit"],
        cpu_period=DOCKER_SECURITY_OPTS["cpu_period"],
        cpu_quota=DOCKER_SECURITY_OPTS["cpu_quota"],
        network_mode=DOCKER_SECURITY_OPTS["network_mode"],
        restart_policy=DOCKER_SECURITY_OPTS["restart_policy"],
        security_opt=DOCKER_SECURITY_OPTS["security_opt"],
        cap_drop=DOCKER_SECURITY_OPTS["cap_drop"],
        read_only=DOCKER_SECURITY_OPTS["read_only"],
        pids_limit=DOCKER_SECURITY_OPTS["pids_limit"],
    )
    log.info("container %s started uid=%d entry=%s", c.id[:12], uid, entry_point)
    return c.id, files, entry_point

def _docker_logs(container_id,lines=50):
    try: return docker_lib.from_env().containers.get(container_id).logs(tail=lines).decode("utf-8",errors="replace")
    except docker_lib.errors.NotFound: return "(контейнер остановлен)"
    except Exception as e: return f"Ошибка: {e}"

def _docker_status(container_id):
    try: return docker_lib.from_env().containers.get(container_id).status
    except: return "gone"

async def _get_bot_username(token):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.telegram.org/bot{token}/getMe",timeout=aiohttp.ClientTimeout(total=10)) as r:
                return (await r.json())["result"]["username"]
    except: return "unknown_bot"

async def cleanup_user(uid):
    d=await db_get_active(uid)
    if not d: return
    if d.get("container_id"): await asyncio.get_event_loop().run_in_executor(None,_docker_stop,d["container_id"])
    if d.get("sandbox_path"): sandbox_cleanup(uid)
    await db_del_active(uid); log.info("deploys cleaned uid=%d",uid)

async def deploy_bot(uid, bot_code):
    loop = asyncio.get_event_loop()
    await cleanup_user(uid)
    used = await db_used_slots()
    slot = next((i for i,_ in enumerate(PREVIEW_TOKENS) if i not in used), None)
    if slot is None:
        await db_queue_add(uid, bot_code)
        return {"ok":True,"queued":True,"position":await db_queue_size()}
    token = PREVIEW_TOKENS[slot]
    try:
        BOTS_DIR.mkdir(parents=True, exist_ok=True)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        result = await loop.run_in_executor(None, _docker_run, token, uid, bot_code)
        cid, files, entry = result
        exp = int(time.time()) + DEPLOY_TTL
        await db_set_active(uid, "tg_bot", token, cid, "", exp)
        return {"ok":True,"queued":False,"token":token,"container_id":cid,
                "expires_at":exp,"files":files,"entry":entry}
    except Exception as e:
        log.exception("deploy_bot uid=%d", uid)
        return {"ok":False,"error":str(e)}

async def deploy_sandbox(uid,bot_code):
    await cleanup_user(uid)
    try:
        blocks=_extract_web_blocks(bot_code)
        url=sandbox_deploy_files(uid,blocks)
        if not url: return {"ok":False,"error":"Не нашёл HTML/CSS/JS в коде"}
        exp=int(time.time())+SANDBOX_TTL
        await db_set_active(uid,"web","","",str(SANDBOX_ROOT/str(uid)),exp)
        return {"ok":True,"url":url,"expires_at":exp}
    except Exception as e:
        log.exception("deploy_sandbox uid=%d",uid); return {"ok":False,"error":str(e)}

async def _ttl_worker():
    while True:
        await asyncio.sleep(60)
        for d in await db_all_active():
            if d["expires_at"]<=int(time.time()):
                log.info("TTL expired uid=%d type=%s",d["uid"],d["deploy_type"])
                if d.get("container_id"): await asyncio.get_event_loop().run_in_executor(None,_docker_stop,d["container_id"])
                if d.get("sandbox_path"): sandbox_cleanup(d["uid"])
                await db_del_active(d["uid"])
                await tg_send(d["uid"],"Превью истекло. Задеплой снова.")
                q=await db_queue_pop()
                if q:
                    res=await deploy_bot(q["uid"],q["bot_code"])
                    if res.get("ok") and not res.get("queued"):
                        uname=await _get_bot_username(res["token"])
                        await tg_send(q["uid"],f"Слот освободился! Бот задеплоен: @{uname}")

def btn(text,cb=None,url=None,eid=None,style=None,web_app=None):
    b={"text":text}
    if cb:      b["callback_data"]=cb
    if url:     b["url"]=url
    if web_app: b["web_app"]={"url":web_app}
    if eid:     b["icon_custom_emoji_id"]=eid
    if style:   b["style"]=style
    return b

def markup(*rows): return {"inline_keyboard":list(rows)}

async def tg_send(chat_id,text,kb=None,reply_to=None,preview=False):
    payload={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":not preview}
    if kb:       payload["reply_markup"]=kb
    if reply_to: payload["reply_to_message_id"]=reply_to
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",json=payload) as r:
                data=await r.json()
                if not data.get("ok"): log.warning("tg_send err: %s len=%d",data.get("description","?"),len(text))
                return data.get("result")
    except Exception as e: log.error("tg_send: %s",e); return None

async def tg_send_banner(chat_id, caption, kb=None):
    if not BANNER_PATH.exists():
        return await tg_send(chat_id, caption, kb)
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("caption", caption)
        form.add_field("parse_mode", "HTML")
        form.add_field("photo", open(BANNER_PATH, "rb"), filename="star.png", content_type="image/png")
        if kb:
            form.add_field("reply_markup", json.dumps(kb))
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", data=form) as r:
                data = await r.json()
                if not data.get("ok"): log.warning("tg_send_banner err: %s", data.get("description","?"))
                return data.get("result")
    except Exception as e:
        log.error("tg_send_banner: %s", e)
        return await tg_send(chat_id, caption, kb)

async def tg_edit_banner(chat_id, msg_id, caption, kb=None):
    if not BANNER_PATH.exists():
        return await tg_edit(chat_id, msg_id, caption, kb)
    try:
        media = {"type": "photo", "media": "attach://star.png", "caption": caption, "parse_mode": "HTML"}
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("message_id", str(msg_id))
        form.add_field("media", json.dumps(media))
        form.add_field("star.png", open(BANNER_PATH, "rb"), filename="star.png", content_type="image/png")
        if kb:
            form.add_field("reply_markup", json.dumps(kb))
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageMedia", data=form) as r:
                data = await r.json()
                if not data.get("ok"): log.warning("tg_edit_banner err: %s", data.get("description","?"))
                return data.get("ok", False)
    except Exception as e:
        log.error("tg_edit_banner: %s", e)
        return await tg_edit(chat_id, msg_id, caption, kb)

async def tg_edit(chat_id,msg_id,text,kb=None,preview=False):
    kb_payload = {"reply_markup": kb} if kb else {}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageCaption",
            json={"chat_id":chat_id,"message_id":msg_id,"caption":text,"parse_mode":"HTML",**kb_payload}) as r:
            if (await r.json()).get("ok"): return True
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json={"chat_id":chat_id,"message_id":msg_id,"text":text,"parse_mode":"HTML",
                  "disable_web_page_preview":True,**kb_payload}) as r:
            if (await r.json()).get("ok"): return True
        await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
            json={"chat_id":chat_id,"message_id":msg_id})
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id":chat_id,"text":text,"parse_mode":"HTML",
                  "disable_web_page_preview":True,**kb_payload}) as r:
            data = await r.json()
            if not data.get("ok"): log.warning("tg_edit fallback err: %s",data.get("description","?"))
            return data.get("ok",False)

async def tg_send_poll(chat_id,question,options):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPoll",
                json={"chat_id":chat_id,"question":question,"options":[{"text":o} for o in options],
                      "is_anonymous":False,"type":"regular","allows_multiple_answers":False}) as r:
                return (await r.json()).get("result")
    except Exception as e: log.error("tg_send_poll: %s",e); return None

async def tg_stop_poll(chat_id, msg_id):
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/stopPoll",
                         json={"chat_id":chat_id,"message_id":msg_id})
            await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
                         json={"chat_id":chat_id,"message_id":msg_id})
    except: pass

async def tg_typing(bot,chat_id):
    try: await bot.send_chat_action(chat_id=chat_id,action=ChatAction.TYPING)
    except: pass

def E2(eid, fb="⚡"):
    return f'<tg-emoji emoji-id="{eid}">{fb}</tg-emoji>'

WAIT_EMOJI = E2("5328089410963513796","⚡")

async def kb_profile(uid=None):
    has_custom = bool(await db_get_custom_api(uid)) if uid else False
    ref_bal    = await db_get_ref_balance(uid) if uid else 0.0
    rows = [
        [btn("Download History",  cb="holo:export_history", eid="5431736674147114227")],
        [btn("Пополнить баланс",  cb="holo:subscription",   eid="5375296873982604963", style="primary")],
    ]
    if ref_bal >= REF_MIN_PAYOUT:
        rows.append([btn(f"💸 Вывести реф. баланс ({ref_bal:.0f}₽)", cb="holo:ref_withdraw", style="primary")])
    if has_custom:
        rows.append([btn("Отключить Custom AI", cb="holo:capi_disconnect", style="danger")])
    rows += [
        [btn("Law Enforcement Unit", url="https://telegra.ph/Pravovoj-blok-i-politika-ispolzovaniya-Holocron-03-21", eid="5274099962655816924")],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    ]
    return {"inline_keyboard": rows}

def kb_custom_ai(has_active=False):
    rows = []
    for pid in PROVIDER_ORDER:
        p = PROVIDERS[pid]
        rows.append([btn(p["name"], cb=f"holo:capi_select:{pid}")])
    if has_active:
        rows.append([btn("Отключить Custom AI", cb="holo:capi_disconnect", style="danger")])
    rows.append([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return {"inline_keyboard": rows}

def kb_custom_ai_back():
    return markup([btn("Назад", cb="holo:custom_ai", eid="5341715473882955310")])

CUSTOM_AI_INFO = (
    f'{E2("5431376038628171216","🤖")} <b>Custom AI - подключи свою модель</b>\n\n'
    "<b>Что это такое?</b>\n"
    "Holocron по умолчанию работает через сервер разработчиков. Custom AI позволяет подключить <b>свой API ключ</b> от любого провайдера - тогда все запросы пойдут через твой аккаунт и твою модель с сохранением функционала (советуем использовать Uncensored модели.\n\n"
    "<b>Зачем это нужно?</b>\n"
    "- Использовать модели которых нет в дефолтной конфигурации\n"
    "- Использовать свои лимиты и тарифы\n"
    "- Полный контроль над тем, через какой API идут запросы\n\n"
    "<b>Поддерживаемые провайдеры:</b>\n"
    "OpenAI, Anthropic, DeepSeek, Groq, Mistral, Together AI, OpenRouter, VseGPT, Perplexity, Google Gemini, Qwen (Alibaba Cloud), Qwen (через OpenRouter), HuggingFace Inference API, HuggingFace Endpoints\n\n"
    "<b>Как подключить:</b>\n"
    "1. Нажми <b>Подключить API</b> ниже\n"
    "2. Выбери провайдера\n"
    "3. Введи API ключ\n"
    "4. Укажи модель (или выбери из списка)\n"
    "5. Бот проверит ключ и модель - если всё ок, подключение активируется\n\n"
    "<b>Отключить:</b> в любой момент через кнопку <b>Отключить Custom AI</b>\n\n"
    "<i>Твой ключ хранится только на сервере бота и используется исключительно для запросов к выбранному провайдеру.</i>"
)
def kb_back():
    return markup([btn("Назад", cb="holo:home", eid="5341715473882955310")])

def kb_chat():
    return markup([btn("Назад", cb="holo:home", eid="5341715473882955310")])

def kb_skip_polls():
    return markup([btn("Пропустить опросы", cb="holo:skip_polls", eid="5341715473882955310", style="danger")])

def kb_after_code(deploy_type):
    if deploy_type == "tg_bot":
        label = "Запустить и посмотреть бота"
    elif deploy_type == "web":
        label = "Задеплоить и посмотреть сайт"
    else:
        return markup([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return markup(
        [btn(label, cb="holo:deploy", eid="5287454433318300292", style="primary")],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    )

def kb_deploy_bot(bot_url):
    return markup(
        [btn("Открыть бота", url=bot_url, eid="5287454433318300292", style="primary")],
        [btn("Логи", cb="holo:deploy_logs", eid="5341715473882955310"),
         btn("Статус", cb="holo:deploy_status", eid="5341715473882955310")],
        [btn("Остановить", cb="holo:deploy_stop", eid="5341715473882955310", style="danger")],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    )

def kb_deploy_web(url):
    p_btn = (btn("Открыть сайт", web_app=url, eid="5287454433318300292", style="primary")
             if url.startswith("https://")
             else btn("Открыть сайт", url=url, eid="5287454433318300292", style="primary"))
    return markup(
        [p_btn],
        [btn("Остановить", cb="holo:deploy_stop", eid="5341715473882955310", style="danger")],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    )

def kb_url_rows(urls):
    rows = [[btn(title[:35], url=url)] for url, title in urls[:8]]
    rows.append([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return {"inline_keyboard": rows}

def kb_chat_list(chats, page=0, per_page=5):
    rows = []
    start = page * per_page; chunk = chats[start:start+per_page]
    for c in chunk:
        rows.append([btn(c["title"][:40], cb=f"holo:chat_open:{c['id']}")])
    nav = []
    if page > 0: nav.append(btn("⬅️", cb=f"holo:chat_page:{page-1}"))
    nav.append(btn(f"{page+1} стр", cb="holo:noop"))
    if start+per_page < len(chats): nav.append(btn("➡️", cb=f"holo:chat_page:{page+1}"))
    nav.append(btn("+", cb="holo:chat_new"))
    rows.append(nav)
    rows.append([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return {"inline_keyboard": rows}

def kb_chat_actions(chat_id):
    return markup(
        [btn("Открыть чат", cb=f"holo:chat_enter:{chat_id}", style="primary")],
        [btn("Удалить чат", cb=f"holo:chat_delete:{chat_id}", style="danger")],
        [btn("Назад", cb="holo:chat", eid="5341715473882955310")],
    )

def kb_in_chat(chat_id, show_search_more: bool = False, last_question_key: str = ""):
    row1 = [btn("Выйти из чата", cb="holo:chat", eid="5341715473882955310")]
    if show_search_more:
        row1.append(btn("🔍 Искать ещё", cb=f"holo:search_more:{chat_id}"))
    return markup(row1)

def home_text():
    return (
        f'{E2("5328230887186274308")} <b>Holocron</b>\n\n'
        f'{E2("5454219968948229067")} <i>Have others refused? I\'ll help you.</i>'
    )

def settings_text():
    return (
        f'{E2("5341715473882955310")} <b>Настройки</b>\n\n'
        f'Советуем для начала ознакомиться с ботом в разделе <b>Помощь</b>'
    )

async def profile_text(uid):
    stats   = await db_get_stats(uid)
    balance = await db_get_balance(uid)
    ref_bal = await db_get_ref_balance(uid)
    ref_cnt = await db_get_ref_count(uid)
    custom  = await db_get_custom_api(uid)
    lines = [
        f'{E2("5373012449597335010")} <b>Профиль</b>\n',
        f'Всего запросов - <b>{stats["total_reqs"]}</b>',
        f'Баланс токенов - <b>{balance:,}</b>',
        f'Реф. баланс - <b>{ref_bal:.2f}₽</b>',
        f'Рефералов - <b>{ref_cnt}</b>',
    ]
    if custom:
        p = PROVIDERS.get(custom["provider"], {})
        lines.append(f'\nAPI провайдер - <b>{p.get("name", custom["provider"])}</b>')
        lines.append(f'API токен     - <code>{custom["api_key"][:12]}…</code>')
    return "\n".join(lines)

def chat_list_text(chats):
    if not chats:
        return (
            f'{E2("5443038326535759644")} <b>Чаты</b>\n\n'
            'Это полный список ваших чатов, тут вы можете начать новый чат, открыть старый чат, удалить не нужный чат.\n\n'
            '<i>У вас пока нет чатов. Нажмите + чтобы создать первый.</i>'
        )
    return (
        f'{E2("5443038326535759644")} <b>Чаты</b>\n\n'
        'Это полный список ваших чатов, тут вы можете начать новый чат, открыть старый чат, удалить не нужный чат.'
    )

HELP_TEXT = (
    f'{E2("5445188473063480079")} <b>Помощь - как работать с Holocron</b>\n\n'

    '<b>Что такое Holocron</b>\n'
    'Интеллектуальный ИИ-ассистент с RAG-поиском по базе знаний. Не просто отвечает из головы - ищет по архиву, генерирует код, строит презентации, деплоит ботов и умеет видеть фото.\n\n'

    '<b>Как начать</b>\n'
    'Нажми <b>Начать</b> → откроется список чатов. Каждый чат хранит историю отдельно - ИИ не путает контексты между разными темами.\n\n'

    '<b>Чаты</b>\n'
    'Создавай неограниченное количество чатов под разные задачи. Первое сообщение становится названием. Пустые чаты удаляются автоматически.\n\n'

    '<b>Как правильно спрашивать</b>\n'
    '- Конкретно: не "расскажи про Python", а "как работает GIL в Python и зачем это важно"\n'
    '- С контекстом: продолжаешь тему - пиши в том же чате, ИИ помнит историю\n'
    '- Для кода - уточни язык, фреймворк, версию\n\n'

    '<b>Code-Mode</b>\n'
    'Включается в настройках. При первом запросе на код ИИ задаёт 5 уточняющих вопросов - фреймворк, БД, структура. Это сильно улучшает результат. Пропустить - <code>/s</code>.\n\n'

    '<b>Деплой Telegram-ботов</b>\n'
    'Попроси написать бота - ИИ сгенерирует код и предложит кнопку запуска. Бот поднимется в Docker на 20 минут, получишь живую ссылку @username. Если слоты заняты - можно деплоить на свой токен от @BotFather.\n\n'

    '<b>Preview-Mode</b>\n'
    'Включи в настройках. Попроси сделать сайт - ИИ сгенерирует HTML/CSS/JS и задеплоит на временный URL на 20 минут. Файлы придут отдельно.\n\n'

    '<b>Презентации</b>\n'
    'Напиши "сделай презентацию на 15 слайдов про..." - ИИ напишет python-pptx билдер, запустит его и пришлёт готовый .pptx. Работает от 3 до 30+ слайдов.\n\n'

    '<b>Interactive-Mode</b>\n'
    'Включи в настройках:\n'
    '- <b>Фото</b> - OCR прочитает текст с изображения\n'
    '- <b>Голосовые</b> - Whisper транскрибирует речь\n'
    '- <b>Ссылки</b> - бот вытащит содержимое страницы и передаст ИИ\n\n'

    '<b>Визуализации</b>\n'
    'ИИ автоматически прикладывает картинки и графики. Попроси "покажи схему CPU" - придёт фото. "Нарисуй график продаж" - придёт PNG.\n\n'

    '<b>GitHub-сканер</b>\n'
    'Включи в настройках. Ссылка на GitHub-репо в сообщении - он автоматически попадёт в базу знаний бота.\n\n'

    '<b>Профиль</b>\n'
    'Статистика запросов и деплоев. Кнопка Download history - выгрузка всей истории чатов.\n\n'

    '<b>Команды</b>\n'
    '<code>/s</code> - пропустить опросы Code-Mode\n'
    '<code>/status</code> - статус деплоя\n'
    '<code>/logs</code> - логи бота\n'
    '<code>/stop</code> - остановить деплой'
)

def home_text():
    return (
        f'{E2("5328230887186274308")} <b>Holocron</b>\n\n'
        f'{E2("5454219968948229067")} <i>Have others refused? I\'ll help you.</i>'
    )

router=Router()

@router.message(CommandStart())
async def cmd_start(message:Message,state:FSMContext):
    await state.clear()
    uid  = message.from_user.id

    if message.chat.type != "private":
        return

    if await is_banned(uid):
        await message.answer("⛔ Доступ ограничен.")
        return

    if not await check_global_flood(uid):
        log.warning("GLOBAL FLOOD uid=%d", uid)
        return

    if not await check_rate_limit(uid, "start"):
        return

    args = message.text.split() if message.text else []
    await db_register(uid, message.from_user.username or "", message.from_user.first_name or "")
    await db_log_action(uid, "start", args[1] if len(args) > 1 else "")

    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1][4:])
            if referrer_id != uid and not await db_is_referral_confirmed(uid):
                await db_register_referral(uid, referrer_id)
                await tg_send_banner(message.chat.id,
                    "👋 Вас пригласил друг!\n\n<b>Подтвердите что вы не робот:</b>",
                    markup([btn("✅ Я не робот", cb="holo:ref_confirm", style="primary")]))
                return
        except (ValueError, IndexError): pass

    await tg_send_banner(message.chat.id, home_text(), kb_home())

@router.message(Command("s"))
async def cmd_skip(message:Message,state:FSMContext):
    uid=message.from_user.id; d=await db_poll_get(uid)
    if not d:
        await message.answer("Нет активных опросов.",parse_mode="HTML"); return
    if d.get("poll_msg_id"): await tg_stop_poll(d["chat_id"],d["poll_msg_id"])
    await state.set_state(S.chat)
    await message.answer(f"{WAIT_EMOJI}",parse_mode="HTML")
    await _run_codegen(d["chat_id"],uid,d["task"],d["answers"]); await db_poll_clear(uid)

@router.message(Command("status"))
async def cmd_status(message:Message):
    uid=message.from_user.id; d=await db_get_active(uid)
    if not d: await message.answer("Нет активного деплоя.",parse_mode="HTML"); return
    remaining=max(0,d["expires_at"]-int(time.time()))
    status=await asyncio.get_event_loop().run_in_executor(None,_docker_status,d.get("container_id",""))
    await tg_send(message.chat.id,
        f"<b>Статус деплоя</b>\n\n- Тип: <b>{d['deploy_type']}</b>\n- Docker: <code>{status}</code>\n- Осталось: <b>{remaining//60}м</b>",
        markup([btn("Логи",cb="holo:deploy_logs",eid=E["log"]),btn("Стоп",cb="holo:deploy_stop",eid=E["stop"],style="danger")]))

@router.message(Command("logs"))
async def cmd_logs(message:Message):
    uid=message.from_user.id; d=await db_get_active(uid)
    if not d or d["deploy_type"] not in ("bot","tg_bot"): await message.answer("Нет активного бота.",parse_mode="HTML"); return
    bot_log_path = LOGS_DIR / str(uid) / "bot.log"
    pip_log_path = LOGS_DIR / str(uid) / "pip.log"
    logs = ""
    if bot_log_path.exists():
        logs = bot_log_path.read_text(encoding="utf-8", errors="replace")[-3000:]
    if not logs.strip() and pip_log_path.exists():
        logs = "=== pip.log ===\n" + pip_log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
    logs = logs or "(пусто)"
    import html as _html
    await tg_send(message.chat.id,f"<pre>{_html.escape(logs)}</pre>",markup([btn("Обновить",cb="holo:deploy_logs",eid=E["refresh"])]))

@router.message(Command("stop"))
async def cmd_stop(message:Message):
    await cleanup_user(message.from_user.id)
    await message.answer("Деплой остановлен.",parse_mode="HTML")

@router.message(Command("context"))
async def cmd_context(message: Message, state: FSMContext):
    uid = message.from_user.id
    current_state = await state.get_state()
    if current_state != S.chat.state:
        await message.answer("Команда /context доступна только внутри чата.", parse_mode="HTML")
        return
    chat_id_db = await db_get_active_chat(uid)
    if not chat_id_db:
        await message.answer("Нет активного чата.", parse_mode="HTML")
        return
    history = await db_chat_history(chat_id_db)
    if len(history) < 4:
        await message.answer("История слишком короткая, нечего сжимать.", parse_mode="HTML")
        return

    status_msg = await tg_send(message.chat.id, "Cleaning up the context...")
    status_id = status_msg["message_id"] if status_msg else None

    try:
        compressed = await _compress_chat_history(history)
        if compressed:
            async with pg().acquire() as c:
                await c.execute("DELETE FROM chat_messages WHERE chat_id=$1", chat_id_db)
            await db_checkpoint_set(chat_id_db, compressed)
            new_count = await db_chat_msg_count(chat_id_db)
            settings = await db_get_settings(uid)
            result_text = "✅ Контекст оптимизирован. История сжата и сохранена."
            if settings.get("context_inspector"):
                result_text += context_bar(new_count)
            if status_id:
                await tg_edit(message.chat.id, status_id, result_text, kb_in_chat(chat_id_db))
        else:
            if status_id:
                await tg_edit(message.chat.id, status_id, "Не удалось сжать контекст. Попробуй позже.", kb_in_chat(chat_id_db))
    except Exception as e:
        log.exception("cmd_context error: %s", e)
        if status_id:
            await tg_edit(message.chat.id, status_id, f"Ошибка: {str(e)[:200]}", kb_in_chat(chat_id_db))

@router.callback_query(F.data=="holo:home")
async def cb_home(cb:CallbackQuery,state:FSMContext):
    await state.clear(); await cb.answer()
    await tg_edit_banner(cb.message.chat.id,cb.message.message_id,home_text(),kb_home())

@router.callback_query(F.data=="holo:stats")
async def cb_stats(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    await tg_edit(cb.message.chat.id,cb.message.message_id,await profile_text(uid),await kb_profile(uid))

@router.callback_query(F.data=="holo:profile")
async def cb_profile(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    await db_log_action(uid, "profile")
    await tg_edit_banner(cb.message.chat.id,cb.message.message_id,await profile_text(uid),await kb_profile(uid))

@router.callback_query(F.data=="holo:settings")
async def cb_settings(cb:CallbackQuery):
    await cb.answer(); s=await db_get_settings(cb.from_user.id)
    await tg_edit_banner(cb.message.chat.id,cb.message.message_id,settings_text(),kb_settings(s))

@router.callback_query(F.data.startswith("holo:toggle:"))
async def cb_toggle(cb:CallbackQuery):
    uid=cb.from_user.id; key=cb.data.split(":",2)[2]; nv=await db_toggle(uid,key)
    labels={"url_buttons":"URL-Buttons","github_scanner":"GitHub","code_mode":"Code-Mode","preview_mode":"Preview-Mode","interactive_mode":"Interactive","local_ai":"Local AI","context_inspector":"Context Inspector"}
    await cb.answer(f"{labels.get(key,key)}: {'ВКЛ' if nv else 'ВЫКЛ'}")
    s=await db_get_settings(uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,settings_text(),kb_settings(s))

@router.callback_query(F.data=="holo:set_token_limit")
async def cb_set_token_limit(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    s = await db_get_settings(uid)
    cur = s.get("token_limit") or 0
    cur_text = f"{cur} токенов" if cur else "∞ (без лимита)"
    await cb.answer()
    await tg_edit(
        cb.message.chat.id, cb.message.message_id,
        f"🔢 <b>Token Limit</b>\n\nТекущий лимит: <b>{cur_text}</b>\n\n"
        f"Введи максимальное количество токенов для генерации.\n"
        f"Например: <code>1000</code>, <code>2000</code>, <code>4096</code>\n"
        f"Отправь <code>0</code> чтобы снять ограничение.",
        markup([btn("Отмена", cb="holo:settings")])
    )
    await state.set_state(S.set_token_limit)

@router.message(S.set_token_limit)
async def fsm_set_token_limit(message: Message, state: FSMContext):
    uid = message.from_user.id
    text = (message.text or "").strip()
    try:
        limit = int(text)
        if limit < 0:
            raise ValueError
    except ValueError:
        await tg_send(message.chat.id, "⚠️ Введи целое число ≥ 0 (0 = без лимита)")
        return
    await db_set_token_limit(uid, limit)
    await state.clear()
    s = await db_get_settings(uid)
    label = f"{limit} токенов" if limit else "∞ без лимита"
    await tg_send(message.chat.id, f"✅ Token Limit установлен: <b>{label}</b>")
    await tg_send(message.chat.id, settings_text(), kb_settings(s))

@router.callback_query(F.data=="holo:clear_history")
async def cb_clear(cb:CallbackQuery,state:FSMContext):
    await state.clear(); await cb.answer("История очищена")
    await tg_edit_banner(cb.message.chat.id,cb.message.message_id,home_text(),kb_home())

@router.callback_query(F.data.startswith("holo:search_more:"))
async def cb_search_more(cb: CallbackQuery):
    await cb.answer("Ищу ещё…")
    uid = cb.from_user.id
    chat_id_db = int(cb.data.split(":")[-1])

    last_q = await redis_get(f"last_q:{uid}")
    if not last_q:
        await cb.answer("Вопрос не найден, задай снова.", show_alert=True)
        return

    try:
        emb = await get_embedding(last_q)
        if len(emb) != EMB_DIM:
            await cb.answer("Ошибка поиска.", show_alert=True)
            return
        extra_results = ch_vector_search(emb, top_k=7)
        extra_results = extra_results[3:]
    except Exception as e:
        await cb.answer(f"Ошибка: {str(e)[:100]}", show_alert=True)
        return

    if not extra_results:
        await cb.answer("Больше релевантных документов не найдено.", show_alert=True)
        return

    settings = await db_get_settings(uid)
    custom_api = await db_get_custom_api(uid)
    history = _build_lean_history(await db_chat_history(chat_id_db))
    checkpoint_summary = await db_checkpoint_get(chat_id_db)

    context_str = "\n\n".join(
        f"[{r['source_url'] or 'архив'}]\n{r['content'][:400]}"
        for r in extra_results
    )

    base_sys = SYSTEM_PROMPT + (
        "\n\nПРАВИЛО ОТВЕТОВ: Отвечай максимально кратко и по делу. "
        "Никаких вступлений. Спросили - ответил."
    )
    messages = [{"role": "system", "content": base_sys}]
    messages.append({"role": "system", "content": f"Дополнительный контекст из базы знаний:\n{context_str}"})
    if checkpoint_summary:
        messages.append({"role": "system", "content": f"Сжатый контекст предыдущего диалога:\n{checkpoint_summary}"})
    messages.extend(history)
    messages.append({"role": "user", "content": f"Дополни ответ используя новые источники: {last_q}"})

    wait = await tg_send(cb.message.chat.id, f"{WAIT_EMOJI}")
    wait_id = wait["message_id"] if wait else None

    try:
        if custom_api:
            full_answer = await ask_llm_custom_stream(messages, custom_api, lambda t: None)
        else:
            full_answer, used_tokens = await ask_llm_stream(messages, lambda t: None)
            if not custom_api and used_tokens > 0:
                await db_spend_tokens(uid, max(TOKENS_MIN_REQUEST, used_tokens))
    except Exception as e:
        if wait_id: await tg_edit(cb.message.chat.id, wait_id, f"Ошибка: {str(e)[:200]}", kb_in_chat(chat_id_db))
        return

    await db_chat_push(chat_id_db, uid, "assistant", _truncate_message(full_answer, 600))
    display = clean_md_to_html(full_answer)
    new_msg_count = await db_chat_msg_count(chat_id_db)
    if settings.get("context_inspector"):
        display += context_bar(new_msg_count)

    max_len = 3900
    if wait_id:
        ok = await tg_edit(cb.message.chat.id, wait_id, display[:max_len], kb_in_chat(chat_id_db))
        if not ok:
            await tg_send(cb.message.chat.id, display[:max_len], kb_in_chat(chat_id_db))
    else:
        await tg_send(cb.message.chat.id, display[:max_len], kb_in_chat(chat_id_db))

@router.callback_query(F.data=="holo:skip_polls")
async def cb_skip_polls(cb:CallbackQuery,state:FSMContext):
    uid=cb.from_user.id; d=await db_poll_get(uid)
    if not d: await cb.answer("Нет активных опросов",show_alert=True); return
    await cb.answer("Пропускаю")
    if d.get("poll_msg_id"): await tg_stop_poll(d["chat_id"],d["poll_msg_id"])
    await state.set_state(S.chat)
    await cb.message.answer(f"{WAIT_EMOJI}",parse_mode="HTML")
    await _run_codegen(d["chat_id"],uid,d["task"],d["answers"]); await db_poll_clear(uid)

@router.callback_query(F.data=="holo:noop")
async def cb_noop(cb:CallbackQuery): await cb.answer()

@router.callback_query(F.data=="holo:help")
async def cb_help(cb:CallbackQuery):
    await cb.answer()
    await tg_edit(cb.message.chat.id,cb.message.message_id,HELP_TEXT,kb_back())

@router.callback_query(F.data=="holo:export_history")
async def cb_export_history(cb:CallbackQuery):
    await cb.answer("Формирую...")
    uid=cb.from_user.id
    data=await db_export_all_history(uid)
    if not data:
        await cb.answer("Нет сохранённых чатов",show_alert=True); return
    lines=[]
    for c in data:
        lines.append(f"=== {c['title']} ({c['created_at']}) ===")
        for m in c["messages"]:
            role="You" if m["role"]=="user" else "Holocron"
            lines.append(f"[{m['created_at']}] {role}:\n{m['content']}\n")
        lines.append("")
    export_text="\n".join(lines)
    tmp_bot=Bot(token=BOT_TOKEN)
    try:
        await tmp_bot.send_document(chat_id=uid,
            document=BufferedInputFile(export_text.encode("utf-8"),filename="history_export.txt"),
            caption="Chat history export")
    finally: await tmp_bot.session.close()

@router.callback_query(F.data=="holo:chat")
async def cb_chat_list(cb:CallbackQuery,state:FSMContext):
    await cb.answer(); await state.clear()
    uid=cb.from_user.id; chats=await db_chat_list(uid)
    for c in chats:
        if await db_chat_msg_count(c["id"]) == 0:
            await db_chat_delete(c["id"])
    chats=await db_chat_list(uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,chat_list_text(chats),kb_chat_list(chats,0))

@router.callback_query(F.data.startswith("holo:chat_page:"))
async def cb_chat_page(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    page=int(cb.data.split(":")[-1]); chats=await db_chat_list(uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,chat_list_text(chats),kb_chat_list(chats,page))

@router.callback_query(F.data=="holo:chat_new")
async def cb_chat_new(cb:CallbackQuery,state:FSMContext):
    await cb.answer(); uid=cb.from_user.id
    chat_id=await db_chat_create(uid); await db_set_active_chat(uid,chat_id)
    await state.set_state(S.chat)
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        f'{E2("5266998388850706998","✨")} <i>I\'ve been waiting for you...</i>',kb_in_chat(chat_id))

@router.callback_query(F.data.startswith("holo:chat_open:"))
async def cb_chat_open(cb:CallbackQuery):
    await cb.answer(); chat_id=int(cb.data.split(":")[-1])
    c=await db_chat_get(chat_id)
    if not c: await cb.answer("Чат не найден",show_alert=True); return
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        f"<b>{c['title']}</b>",kb_chat_actions(chat_id))

@router.callback_query(F.data.startswith("holo:chat_enter:"))
async def cb_chat_enter(cb:CallbackQuery,state:FSMContext):
    await cb.answer(); uid=cb.from_user.id; chat_id=int(cb.data.split(":")[-1])
    c=await db_chat_get(chat_id)
    if not c: await cb.answer("Чат не найден",show_alert=True); return
    await db_set_active_chat(uid,chat_id)
    await state.set_state(S.chat)
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        f"{WAIT_EMOJI} <b>{c['title']}</b>\n\n<i>Продолжаем...</i>",kb_in_chat(chat_id))

@router.callback_query(F.data.startswith("holo:chat_delete:"))
async def cb_chat_delete(cb:CallbackQuery,state:FSMContext):
    await cb.answer(); uid=cb.from_user.id; chat_id=int(cb.data.split(":")[-1])
    await db_chat_delete(chat_id)
    if await db_get_active_chat(uid)==chat_id: await db_set_active_chat(uid,0)
    chats=await db_chat_list(uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,chat_list_text(chats),kb_chat_list(chats,0))

@router.callback_query(F.data=="holo:deploy_custom_token")
async def cb_deploy_custom_token(cb:CallbackQuery,state:FSMContext):
    await cb.answer()
    await state.set_state(S.awaiting_custom_token)
    await tg_send(cb.message.chat.id,
        "Вставь токен от @BotFather - задеплоим на него:",
        markup([btn("Отмена",cb="holo:home",eid=E["cross"])]))

@router.message(S.awaiting_custom_token)
async def handle_custom_token(message:Message,state:FSMContext):
    uid=message.from_user.id; token=message.text.strip() if message.text else ""
    if not validate_bot_token(token):
        await message.answer("Не похоже на токен. Скопируй из @BotFather.",parse_mode="HTML"); return
    pending=await db_get_pending(uid)
    if not pending:
        await state.set_state(S.chat)
        await message.answer("Нет сохранённого кода. Сначала сгенерируй бота.",parse_mode="HTML"); return
    await state.set_state(S.chat)
    msg=await tg_send(uid,"Деплою на твой токен...")
    msg_id=msg["message_id"] if msg else None
    async def edit(t,kb=None):
        if msg_id: await tg_edit(uid,msg_id,t,kb)
    try:
        from pathlib import Path as _P
        import docker as _dk
        cid,files,entry=await asyncio.get_event_loop().run_in_executor(None,_docker_run,token,uid,pending["bot_code"])
        exp=int(time.time())+DEPLOY_TTL
        await db_set_active(uid,"tg_bot",token,cid,"",exp)
        await db_clear_pending(uid)
        uname=await _get_bot_username(token)
        await edit(f"Бот запущен на твоём токене!\n\n@{uname}\nАктивен {DEPLOY_TTL//60} мин",
            markup([btn("Открыть",url=f"https://t.me/{uname}",style="primary")],
                   [btn("Главное меню",cb="holo:home",eid=E["bolt"])]))
        await _send_bot_files(uid,files)
    except Exception as e:
        await edit(f"Ошибка деплоя: <code>{str(e)[:200]}</code>",kb_back())

async def _run_deploy_loop(uid: int, chat_id: int, bot_code: str, deploy_type: str,
                           edit_fn, send_fn):
    import html as _html

    if deploy_type == "web":
        result = await deploy_sandbox(uid, bot_code)
        if not result["ok"]:
            await edit_fn("<b>Ошибка:</b> <code>" + _html.escape(str(result.get("error","?"))[:200]) + "</code>", kb_back())
            return
        await db_clear_pending(uid)
        await edit_fn(
            "<b>Сайт задеплоен!</b>\n\n"
            f"⏱ Активен <b>{SANDBOX_TTL//60} мин</b> - потом автоудаление",
            kb_deploy_web(result["url"]))
        return

    MAX_FIX = 3
    current_code = bot_code
    files = {}
    bot_log = ""

    for attempt in range(1, MAX_FIX + 1):
        await edit_fn(f"{WAIT_EMOJI} Деплой, попытка {attempt}/{MAX_FIX}…")

        result = await deploy_bot(uid, current_code)

        if not result["ok"]:
            err_msg = str(result.get("error", "?"))
            if attempt < MAX_FIX:
                log.warning("deploy error attempt=%d: %s", attempt, err_msg[:200])
                await edit_fn(
                    f"<b>Ошибка деплоя (попытка {attempt}):</b>\n"
                    f"<pre>{_html.escape(err_msg[:500])}</pre>\n\n"
                    f"<i>Прошу ИИ исправить…</i>")
                fix_prompt = (
                    f"Код не запустился, ошибка:\n```\n{err_msg}\n```\n\n"
                    f"Исправь код. Верни ТОЛЬКО файлы в формате:\n"
                    f"main.py:\n```python\n# код\n```\n"
                    f"Никакого текста кроме файлов."
                )
                try:
                    fixed = await ask_llm([{"role":"user","content":fix_prompt}], model=VSEGPT_MODEL_CODE)
                    ff, _ = _extract_bot_files(fixed)
                    if ff:
                        files = ff
                        current_code = "\n\n".join(f"{k}:\n```python\n{v}\n```" for k,v in ff.items())
                except Exception as e:
                    log.warning("autofix failed: %s", e)
                continue
            await edit_fn("<b>Ошибка деплоя:</b>\n<code>" + _html.escape(err_msg[:300]) + "</code>", kb_back())
            return

        if result.get("queued"):
            pos = result.get("position","?")
            await edit_fn(
                f"Все слоты заняты. Позиция: <b>{pos}</b>.\n"
                "Задеплоится автоматически когда освободится.\n\n"
                "Или используй свой токен от @BotFather:",
                markup([btn("Задеплоить на свой токен",cb="holo:deploy_custom_token",eid=E["key"])],
                       [btn("Главное меню",cb="holo:home",eid=E["bolt"])]))
            return

        cid   = result["container_id"]
        files = result.get("files", {})

        POLL_INTERVAL = 5
        MAX_WAIT = 120
        waited = 0
        container_status = "running"
        raw_docker_log = ""
        _blog = LOGS_DIR / str(uid) / "bot.log"
        _plog = LOGS_DIR / str(uid) / "pip.log"
        while waited < MAX_WAIT:
            await asyncio.sleep(POLL_INTERVAL)
            waited += POLL_INTERVAL
            container_status = await asyncio.get_event_loop().run_in_executor(
                None, _docker_status, cid)
            file_log = ""
            if _blog.exists():
                file_log = _blog.read_text(encoding="utf-8", errors="replace")[-500:]
            elif _plog.exists():
                file_log = _plog.read_text(encoding="utf-8", errors="replace")[-500:]
            raw_docker_log = file_log
            log.info("deploy poll uid=%d waited=%ds status=%s log_tail=%r",
                     uid, waited, container_status, file_log[-120:])
            if container_status in ("exited", "dead", "gone"):
                break
            if "pip OK" in file_log and container_status == "running":
                break

        _blog = LOGS_DIR / str(uid) / "bot.log"
        _plog = LOGS_DIR / str(uid) / "pip.log"
        bot_log = ""
        if _blog.exists(): bot_log = _blog.read_text(encoding="utf-8", errors="replace")
        if not bot_log.strip() and _plog.exists():
            bot_log = _plog.read_text(encoding="utf-8", errors="replace")
        if not bot_log.strip():
            bot_log = raw_docker_log

        has_error = container_status in ("exited", "dead", "gone")
        log.info("deploy check attempt=%d status=%s has_error=%s log_len=%d",
                 attempt, container_status, has_error, len(bot_log))

        if not has_error:
            await db_clear_pending(uid)
            uname   = await _get_bot_username(result["token"])
            bot_url = f"https://t.me/{uname}"
            await edit_fn(
                f"✅ <b>Бот запущен!</b> (попытка {attempt})\n\n"
                f"<b>@{uname}</b>\n"
                f"⏱ Активен <b>{DEPLOY_TTL//60} мин</b>",
                kb_deploy_bot(bot_url))
            await _send_bot_files(chat_id, files)
            return

        if attempt >= MAX_FIX:
            break

        log.warning("bot crashed attempt=%d, autofix. log tail: %s", attempt, bot_log[-200:])

        pip_fail = re.search(r'ERROR:.*?(Failed to build|Could not build|No matching distribution|ModuleNotFoundError.*pip)', bot_log)
        py_error = re.search(r'(Traceback[\s\S]*)', bot_log)

        if pip_fail:
            bad_pkg_m = re.search(r'Failed to build installable wheels for some pyproject\.toml based projects \(([^)]+)\)', bot_log)
            bad_pkg_m2 = re.search(r"ERROR: Could not find a version that satisfies the requirement (\S+)", bot_log)
            bad_pkg_m3 = re.search(r"No matching distribution found for (\S+)", bot_log)
            bad_pkgs = set()
            for m in [bad_pkg_m, bad_pkg_m2, bad_pkg_m3]:
                if m:
                    for p in m.group(1).split(','):
                        bad_pkgs.add(p.strip().lower().split('[')[0].split('=')[0].split('>')[0].split('<')[0])

            log.warning("pip autofix: bad packages detected: %s", bad_pkgs)

            req_path = BOTS_DIR / str(uid) / "requirements.txt"
            if req_path.exists():
                req_lines = req_path.read_text(encoding="utf-8").splitlines()
            else:
                req_lines = list(files.get("requirements.txt", "aiogram==3.20.0").splitlines())

            REPLACEMENTS = {
                "aiohttp": None,
                "python-telegram-bot": None,
                "pyrogram": None,
                "telethon": None,
            }
            new_lines = []
            for line in req_lines:
                line = line.strip()
                if not line or line.startswith('#'): continue
                pkg = line.lower().split('=')[0].split('>')[0].split('<')[0].split('[')[0].strip()
                if pkg in bad_pkgs or REPLACEMENTS.get(pkg) is False:
                    log.info("pip autofix: removing %r", line)
                    continue
                if pkg in REPLACEMENTS and REPLACEMENTS[pkg]:
                    new_lines.append(REPLACEMENTS[pkg])
                    log.info("pip autofix: replacing %r -> %r", line, REPLACEMENTS[pkg])
                else:
                    new_lines.append(line)

            new_req = "\n".join(new_lines) + "\n"
            req_path.write_text(new_req, encoding="utf-8")
            files["requirements.txt"] = new_req
            current_code = "\n\n".join(
                f"{k}:\n```python\n{v}\n```" if k.endswith('.py') else f"{k}:\n```\n{v}\n```"
                for k, v in files.items() if k != 'requirements.txt'
            ) + f"\n\nrequirements.txt:\n```\n{new_req}\n```"

            await edit_fn(
                f"<b>Ошибка pip (попытка {attempt}):</b>\n"
                f"<pre>{_html.escape(str(bad_pkgs))}</pre>\n"
                f"<i>Убрал проблемные зависимости, пробую снова…</i>")

        elif py_error:
            error_snippet = py_error.group(1)[-800:]
            broken_file = "main.py"
            fm = re.search(r'File "/bot/([^"]+)"', bot_log)
            if fm: broken_file = fm.group(1)

            mod_err = re.search(r"ModuleNotFoundError: No module named '([^']+)'", bot_log)
            _forbidden_mods = {"telegram", "pyrogram", "telethon", "python_telegram_bot",
                               "python-telegram-bot", "telebot"}

            if mod_err and mod_err.group(1).split('.')[0] in _forbidden_mods:
                bad_mod = mod_err.group(1)
                log.warning("autofix: forbidden module %r - rewriting bot with aiogram", bad_mod)
                await edit_fn(
                    f"<b>Запрещённый модуль '{bad_mod}' (попытка {attempt}):</b>\n"
                    f"<i>Переписываю бота на aiogram 3…</i>")

                fix_prompt = (
                    f"Бот использует '{bad_mod}' которого нет. Перепиши бота на aiogram==3.20.0.\n\n"
                    f"Текущий код:\n```python\n{files.get(broken_file, '')}\n```\n\n"
                    f"Сохрани ту же логику но используй aiogram 3.\n"
                    f"Верни ТОЛЬКО:\n"
                    f"main.py:\n```python\n# код на aiogram 3\n```\n\n"
                    f"requirements.txt:\n```\naiogram==3.20.0\n```"
                )
            else:
                await edit_fn(
                    f"<b>Ошибка при запуске (попытка {attempt}):</b>\n"
                    f"<pre>{_html.escape(error_snippet[-500:])}</pre>\n\n"
                    f"<i>Фикшу…</i>")
                fix_prompt = (
                    f"Бот упал с ошибкой:\n```\n{error_snippet}\n```\n\n"
                    f"Текущий код {broken_file}:\n```python\n"
                    + files.get(broken_file, "# файл не найден") +
                    f"\n```\n\n"
                    f"Исправь {broken_file}. Верни ТОЛЬКО:\n"
                    f"{broken_file}:\n```python\n# исправленный код\n```"
                )
            try:
                fixed = await ask_llm([{"role":"user","content":fix_prompt}], model=VSEGPT_MODEL_CODE)
                ff, _ = _extract_bot_files(fixed)
                if ff:
                    for fname, code in ff.items():
                        if fname.endswith('.py') and fname in files:
                            files[fname] = code
                            fpath = BOTS_DIR / str(uid) / fname
                            if fpath.exists():
                                fpath.write_text(code, encoding="utf-8")
                                log.info("autofix written: %s", fname)
                    if "requirements.txt" in ff:
                        files["requirements.txt"] = ff["requirements.txt"]
                        req_path = BOTS_DIR / str(uid) / "requirements.txt"
                        req_path.write_text(ff["requirements.txt"] + "\n", encoding="utf-8")
                        log.info("autofix requirements.txt updated")
                    current_code = "\n\n".join(
                        f"{k}:\n```python\n{v}\n```" if k.endswith('.py') else f"{k}:\n```\n{v}\n```"
                        for k, v in files.items()
                    )
            except Exception as e:
                log.warning("autofix failed: %s", e)
        else:
            log.warning("autofix: no recognizable error in log, retrying as-is")

    await edit_fn(
        f"❌ <b>Не удалось запустить за {MAX_FIX} попытки</b>\n\n"
        f"Последняя ошибка:\n<pre>{_html.escape(bot_log[-600:])}</pre>\n\n"
        "Попробуй сгенерировать заново с более точным описанием.",
        kb_back())
    await _send_bot_files(chat_id, files)

@router.callback_query(F.data=="holo:deploy")
async def cb_deploy(cb: CallbackQuery):
    await cb.answer()
    uid = cb.from_user.id
    if not await check_rate_limit(uid, "deploy", limit=1, window=60):
        await cb.answer("Подожди минуту перед следующим деплоем.", show_alert=True)
        return
    pending = await db_get_pending(uid)
    if not pending:
        await cb.answer("Код не найден - сгенерируй снова", show_alert=True); return
    deploy_type = pending["deploy_type"]
    chat_id     = cb.message.chat.id

    _spin_txt = "Запускаю контейнер…" if deploy_type == "tg_bot" else "Разворачиваю сайт…"
    spinner = await tg_send(chat_id, f"{WAIT_EMOJI} {_spin_txt}", None)
    spinner_id = spinner["message_id"] if spinner else None

    async def edit(text, kb=None):
        if spinner_id: await tg_edit(chat_id, spinner_id, text, kb)
        else: await tg_send(chat_id, text, kb)

    await _run_deploy_loop(uid, chat_id, pending["bot_code"], deploy_type, edit, edit)

async def _send_bot_files(chat_id: int, files: dict):
    if not files: return
    tmp = Bot(token=BOT_TOKEN)
    try:
        for fname, code in files.items():
            data = code.encode("utf-8")
            if not data: continue
            await tmp.send_document(
                chat_id=chat_id,
                document=BufferedInputFile(data, filename=fname),
                caption=fname)
    except Exception as e:
        log.warning("send_bot_files: %s", e)
    finally:
        await tmp.session.close()

@router.callback_query(F.data=="holo:deploy_status")
async def cb_deploy_status(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id; d=await db_get_active(uid)
    if not d: await cb.answer("Нет активного деплоя",show_alert=True); return
    remaining=max(0,d["expires_at"]-int(time.time()))
    status=await asyncio.get_event_loop().run_in_executor(None,_docker_status,d.get("container_id",""))
    kb=markup([btn("Логи",cb="holo:deploy_logs",eid=E["log"]),btn("Стоп",cb="holo:deploy_stop",eid=E["stop"],style="danger")],
               [btn("Главное меню",cb="holo:home",eid=E["bolt"])])
    text=(f"<b>Статус деплоя</b>\n\n"
          f"Тип: <code>{d['deploy_type']}</code>\n"
          f"Docker: <code>{status}</code>\n"
          f"Осталось: <b>{remaining//60}м {remaining%60}с</b>")
    ok=await tg_edit(cb.message.chat.id,cb.message.message_id,text,kb)
    if not ok: await tg_send(cb.message.chat.id,text,kb)

@router.callback_query(F.data=="holo:deploy_logs")
async def cb_deploy_logs(cb:CallbackQuery):
    await cb.answer("Обновляю..."); uid=cb.from_user.id; d=await db_get_active(uid)
    if not d or d["deploy_type"] not in ("bot","tg_bot"): await cb.answer("Нет активного бота",show_alert=True); return
    bot_log_path = LOGS_DIR / str(uid) / "bot.log"
    pip_log_path = LOGS_DIR / str(uid) / "pip.log"
    logs = ""
    if bot_log_path.exists():
        logs = bot_log_path.read_text(encoding="utf-8", errors="replace")[-3000:]
    if not logs.strip() and pip_log_path.exists():
        logs = "pip.log:\n" + pip_log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
    logs = logs or "(пусто - бот ещё стартует)"
    import html as _html
    logs_safe = _html.escape(logs[-2500:])
    kb=markup(
        [btn("Обновить",cb="holo:deploy_logs",eid=E["refresh"]),btn("Стоп",cb="holo:deploy_stop",eid=E["stop"],style="danger")],
        [btn("◀️ Статус",cb="holo:deploy_status",eid=E["bolt"])],
    )
    ok=await tg_edit(cb.message.chat.id,cb.message.message_id,f"<b>Логи</b>\n\n<pre>{logs_safe}</pre>",kb)
    if not ok: await tg_send(cb.message.chat.id,f"<b>Логи</b>\n\n<pre>{logs_safe}</pre>",kb)

@router.callback_query(F.data=="holo:deploy_stop")
async def cb_deploy_stop(cb:CallbackQuery):
    await cb.answer(); await cleanup_user(cb.from_user.id)
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        "Деплой остановлен",kb_back())

async def _start_code_polling(chat_id, uid, task, bot):
    await tg_typing(bot, chat_id)
    is_web = bool(re.search(
        r"\b(сайт|страниц|лендинг|верстк|html|css|javascript|frontend|веб|визитк)\b",
        task, re.IGNORECASE))
    is_bot = bool(re.search(
        r"\b(бот|bot|telegram|aiogram|telebot|розыгрыш|lottery|магазин|shop|викторин|quiz|pyrogram|telethon)\b",
        task, re.IGNORECASE))

    if is_web:
        context = "frontend/веб разработка (HTML+CSS+JS, БЕЗ бэкенда и БД)"
        aspects = "цветовая схема, стиль дизайна, секции страницы, анимации, адаптивность под мобилку"
    elif is_bot:
        context = "Telegram бот"
        aspects = ("фреймворк и точная версия (aiogram 3.x/2.x, pyTelegramBotAPI, "
                   "python-telegram-bot, pyrogram, telethon и т.д.), "
                   "хранилище данных, нужна ли FSM/состояния диалога, "
                   "роли пользователей, внешние API или интеграции если нужны, способ запуска")
    else:
        context = "код/скрипт"
        aspects = "язык и точная версия, библиотеки/фреймворк, хранилище данных, обработка ошибок, структура проекта"

    system_q = (
        f"Ты опытный разработчик. Пользователь хочет написать: {context}.\n"
        f"Задача: {task}\n\n"
        "Сгенерируй ровно 5 уточняющих технических вопросов.\n"
        f"Аспекты для уточнения: {aspects}\n\n"
        "Правила:\n"
        "- Каждый вопрос должен иметь 3-4 конкретных варианта ответа\n"
        "- ПОСЛЕДНИЙ вариант в каждом вопросе ОБЯЗАТЕЛЬНО: \"Другое\"\n"
        "- Варианты конкретные (не \"SQLite/MySQL/PostgreSQL\" а отдельно каждый)\n"
        "- Длина каждого варианта до 50 символов\n"
        "- Вопросы технические и конкретные, без воды\n"
        + ("- ЗАПРЕЩЕНО спрашивать про БД, бэкенд, Python, aiogram, Telegram - это фронтенд\n" if is_web else "") +
        "\nВерни ТОЛЬКО JSON массив без пояснений:\n"
        '[{"q":"вопрос","opts":["вариант1","вариант2","вариант3","Другое"]},...]'
    )

    if is_web:
        fallback = [
            {"q":"Цветовая схема?","opts":["Тёмная","Светлая","Градиент","Другое"]},
            {"q":"Стиль дизайна?","opts":["Современный минимализм","Классический","Яркий/Креативный","Другое"]},
            {"q":"Секции на странице?","opts":["О нас + контакты","Портфолио","Услуги + цены","Другое"]},
            {"q":"Анимации?","opts":["Да, активные","Subtle эффекты","Без анимаций","Другое"]},
            {"q":"Адаптивность под мобилки?","opts":["Да","Нет","Другое"]},
        ]
    elif is_bot:
        fallback = [
            {"q":"Фреймворк?","opts":["aiogram 3.x","aiogram 2.x","pyTelegramBotAPI","Другое"]},
            {"q":"Хранение данных?","opts":["SQLite","PostgreSQL","Redis","Другое"]},
            {"q":"FSM (состояния диалога)?","opts":["Да","Нет","Другое"]},
            {"q":"Роли пользователей?","opts":["Только юзеры","Юзеры + Админы","Другое"]},
            {"q":"Внешние API/интеграции?","opts":["Нет","Платёжная система","Стороннее API","Другое"]},
        ]
    else:
        fallback = [
            {"q":"Язык?","opts":["Python","JavaScript/Node.js","Go","Другое"]},
            {"q":"Хранилище?","opts":["SQLite","PostgreSQL","MongoDB","Другое"]},
            {"q":"Обработка ошибок?","opts":["Базовая","С логами и retry","Другое"]},
            {"q":"Асинхронность?","opts":["Да (async)","Нет (sync)","Другое"]},
            {"q":"Структура?","opts":["Один файл","Модули/пакеты","Другое"]},
        ]

    try:
        raw = await ask_llm([{"role":"system","content":system_q}])
        raw = re.sub(r"```[a-z]*","",raw).strip().strip("`").strip()
        m = re.search(r"(\[[\s\S]*\])", raw)
        if m: raw = m.group(1)
        questions = json.loads(raw)
        if not isinstance(questions, list) or len(questions) == 0: raise ValueError
        cleaned = []
        for q in questions[:5]:
            if not isinstance(q, dict) or "q" not in q or "opts" not in q: continue
            opts = [str(o)[:50] for o in q["opts"][:3]]
            if "Другое" not in opts: opts.append("Другое")
            cleaned.append({"q": str(q["q"])[:100], "opts": opts})
        if not cleaned: raise ValueError
        questions = cleaned
    except Exception as e:
        log.warning("code questions gen: %s", e)
        questions = fallback

    await db_poll_init(uid, chat_id, task, questions)
    await _send_next_poll(chat_id, uid)

async def _send_next_poll(chat_id, uid):
    d = await db_poll_get(uid)
    if not d: return
    idx = d["current_q"]; questions = d["questions"]
    if idx >= len(questions):
        if d.get("poll_msg_id") and d["poll_msg_id"] > 0:
            await tg_stop_poll(chat_id, d["poll_msg_id"])
        ctrl_id = d.get("ctrl_msg_id", 0)
        msg = f"{WAIT_EMOJI}"
        if ctrl_id: await tg_edit(chat_id, ctrl_id, msg, None)
        else: await tg_send(chat_id, msg, None)
        await _run_codegen(chat_id, uid, d["task"], d["answers"])
        await db_poll_clear(uid); return
    q = questions[idx]; num = idx+1; total = len(questions)
    if idx > 0 and d.get("poll_msg_id", 0) > 0:
        await tg_stop_poll(chat_id, d["poll_msg_id"])
    result = await tg_send_poll(chat_id, q["q"], q["opts"])
    if result: await db_poll_set_msg(uid, result["message_id"])
    status = f"{WAIT_EMOJI} {num}/{total}"
    kb = markup([btn("⏭ Пропустить", cb="holo:skip_polls", style="danger")])
    ctrl_id = d.get("ctrl_msg_id", 0)
    if ctrl_id:
        await tg_edit(chat_id, ctrl_id, status, kb)
    else:
        sent = await tg_send(chat_id, status, kb)
        if sent:
            await db_poll_set_ctrl(uid, sent["message_id"])

async def _run_codegen(chat_id,uid,task,answers):
    answers_text="\n".join(f"- {a['q']}: {a['a']}" for a in answers) if answers else ""
    sent=await tg_send(chat_id,f"{WAIT_EMOJI}",None)
    if not sent: return
    sent_id=sent["message_id"]
    settings=await db_get_settings(uid)
    want_preview=bool(settings.get("preview_mode"))
    log.info("_run_codegen START uid=%d task=%r answers=%d", uid, task[:60], len(answers))
    is_web=bool(re.search(r'\b(сайт|страниц|лендинг|верстк|html|css|javascript|js|frontend|ui|интерфейс|веб|визитк|портфол|одностранич)\b',task,re.IGNORECASE))

    if is_web:
        log.info("_run_codegen: web request, redirecting to chat model")
        web_sys = (
            "Ты опытный frontend-разработчик. Напиши красивый современный сайт.\n"
            "СТРОГО раздели на три отдельных блока:\n"
            "```html\n<!-- только HTML структура, без <style> и <script> -->\n```\n\n"
            "```css\n/* весь CSS */\n```\n\n"
            "```javascript\n// весь JS если нужен\n```\n\n"
            "ЗАПРЕЩЕНО: Python, aiogram, бэкенд, БД. Только HTML+CSS+JS."
        )
        user_content = f"Задача: {task}"
        if answers_text:
            user_content += f"\n\nУточнения:\n{answers_text}"
        messages = [{"role":"system","content":web_sys}, {"role":"user","content":user_content}]

        _last_text_web = [""]
        async def on_upd_web(text):
            html_preview = clean_md_to_html(text)
            if len(html_preview) > 3500:
                html_preview = safe_truncate_html(html_preview, 3500)
            
            if html_preview == _last_text_web[0]:
                return
            _last_text_web[0] = html_preview
            
            try:
                tmp = Bot(token=BOT_TOKEN)
                await tmp.send_message_draft(
                    chat_id=chat_id,
                    draft_id=2,
                    text=f"⚡ Генерирую сайт...\n\n{html_preview}",
                    parse_mode="HTML"
                )
                await tmp.session.close()
            except Exception:
                pass

        try:
            full, _ = await ask_llm_stream(messages, on_upd_web)
        except Exception as e:
            await tg_edit(chat_id, sent_id, f"<b>Ошибка:</b>\n<code>{str(e)[:300]}</code>", kb_back())
            return

        if want_preview:
            await tg_edit(chat_id, sent_id, f"{WAIT_EMOJI} Деплою сайт…")
            from handlers import deploy_sandbox
            result = await deploy_sandbox(uid, full)
            if result.get("ok"):
                await db_clear_pending(uid)
                await tg_edit(chat_id, sent_id,
                    "<b>Сайт задеплоен!</b>\n\n"
                    f"⏱ Активен <b>{SANDBOX_TTL//60} мин</b>",
                    kb_deploy_web(result["url"]))
            else:
                await tg_edit(chat_id, sent_id,
                    f"<b>Ошибка деплоя:</b> {result.get('error','?')[:200]}", kb_back())
        else:
            full_html = clean_md_to_html(full)
            text_only = re.sub(r'<pre><code>.*?</code></pre>', '', full_html, flags=re.DOTALL)
            text_only = text_only.strip()
            display = text_only if text_only else "<i>Файлы выше</i>"
            await tg_edit(chat_id, sent_id, safe_truncate_html(display, 3500),
                          markup([btn("Главное меню", cb="holo:home", eid=E["bolt"])]))

        web_langs = {"html","css","javascript","js"}
        name_map = {"html":"index.html","css":"style.css","javascript":"script.js","js":"script.js"}
        raw_blocks = re.findall(r'```([a-zA-Z0-9_+\-]*)\n?([\s\S]*?)```', full)
        tmp_bot = Bot(token=BOT_TOKEN)
        try:
            for lang, code in raw_blocks:
                ll = lang.lower().strip()
                if ll in web_langs and code.strip():
                    name = name_map.get(ll, f"code.{ll}")
                    await tmp_bot.send_document(chat_id=chat_id,
                        document=BufferedInputFile(code.strip().encode("utf-8"), filename=name),
                        caption=name)
        except Exception as e:
            log.warning("web files send: %s", e)
        finally:
            await tmp_bot.session.close()
        return

    is_tg = False
    if not is_web:
        is_tg = bool(re.search(
            r"\b(aiogram|telebot|telegram|pyrogram|telethon|бот|bot|openrouter|llm|gpt|ии|чат|chat|нейр)\b",
            task+" "+(answers_text or ""), re.IGNORECASE))
        if not is_tg:
            is_tg = True

    if is_web:
        system_content = (
            "Ты опытный frontend-разработчик. Напиши красивый рабочий сайт.\n"
            "СТРОГО раздели на три блока:\n"
            "1. ```html - только HTML структура (без <style> и <script>)\n"
            "2. ```css - весь CSS с красивыми стилями\n"
            "3. ```javascript - весь JS если нужен\n"
            "После блоков - 1-2 предложения что сделал. Отвечай на русском."
        )
    elif is_tg:
        system_content = (
            "Отвечай СТРОГО в таком формате и НИКАК ИНАЧЕ:\n\n"
            "main.py:\n"
            "```python\n"
            "<весь код>\n"
            "```\n\n"
            "requirements.txt:\n"
            "```\n"
            "<зависимости>\n"
            "```\n\n"
            "ЗАПРЕЩЕНО: любой текст до первого файла, между файлами, после файлов.\n"
            "ЗАПРЕЩЕНО: заголовки, списки, инструкции, объяснения, pip install, .env примеры.\n"
            "ТОЛЬКО два блока выше - ничего больше.\n\n"
            "Технические требования:\n"
            "- Только aiogram==3.20.0. Запрещено: python-telegram-bot, pyrogram, telethon, Updater\n"
            "- aiogram 3 синтаксис: Router(), @router.message(), dp.start_polling(bot)\n"
            "- BOT_TOKEN = os.environ.get('BOT_TOKEN', '')\n"
            "- Только aiosqlite/SQLite. Запрещено: PostgreSQL, MySQL, MongoDB\n"
            "- В requirements.txt ЗАПРЕЩЕНО: aiohttp, os, json, asyncio, sys (они встроены)\n"
            "- Один файл main.py. Запрещено: config.py, utils.py, .sh файлы"
        )
    else:
        system_content = (
            "Ты опытный разработчик.\n"
            "ПРАВИЛО: Весь код в ОДНОМ файле - никаких многофайловых структур.\n"
            "Напиши полный рабочий код с комментариями в одном блоке ```язык ... ```.\n"
            "После кода - краткое объяснение на русском."
        )

    messages=[
        {"role":"system","content":system_content},
        {"role":"user","content":f"Задача: {task}\n\n"+
         (f"Уточнения:\n{answers_text}" if answers_text else "")}
    ]

    _draft_id = 1
    _last_draft_text = [""]
    
    async def on_upd(text):
        html_preview = clean_md_to_html(text)
        if len(html_preview) > 3500:
            html_preview = safe_truncate_html(html_preview, 3500)
        
        if html_preview == _last_draft_text[0]:
            return
        _last_draft_text[0] = html_preview
        
        try:
            tmp = Bot(token=BOT_TOKEN)
            await tmp.send_message_draft(
                chat_id=chat_id,
                draft_id=_draft_id,
                text=f"⚡ Генерирую...\n\n{html_preview}",
                parse_mode="HTML"
            )
            await tmp.session.close()
        except Exception:
            pass

    log.info("codegen sending to %s, system_len=%d", VSEGPT_MODEL_CODE, len(messages[0]["content"]))
    try:
        full,_=await ask_llm_stream(messages,on_upd,model=VSEGPT_MODEL_CODE)
    except Exception as e:
        log.exception("codegen llm error: %s", e)
        err_text = str(e)[:300]
        await tg_edit(chat_id, sent_id, f"<b>Ошибка генерации:</b>\n<code>{err_text}</code>", kb_back())
        return

    full_html = clean_md_to_html(full)
    text_only = re.sub(r'<pre><code>.*?</code></pre>', '', full_html, flags=re.DOTALL)
    text_only = text_only.strip()
    html_expl = text_only if text_only else "<i>Смотри файлы ниже</i>"

    is_tg_bot=_detect_is_tg_bot(full)
    deploy_type="tg_bot" if is_tg_bot else ("web" if is_web else "code")

    can_deploy = (deploy_type == "tg_bot") or (deploy_type == "web" and want_preview)
    log.info("codegen deploy_type=%s is_tg_bot=%s can_deploy=%s full[50]=%r",
             deploy_type, is_tg_bot, can_deploy, full[20:70])

    lang_to_ext={"python":"py","py":"py","javascript":"js","js":"js","typescript":"ts","ts":"ts",
                 "html":"html","css":"css","bash":"sh","shell":"sh","sh":"sh","go":"go","rust":"rs",
                 "java":"java","cpp":"cpp","c":"c","csharp":"cs","cs":"cs","php":"php","ruby":"rb",
                 "swift":"swift","kotlin":"kt","sql":"sql","yaml":"yaml","yml":"yml","json":"json","xml":"xml"}
    web_langs={"html","css","javascript","js","typescript","ts"}
    name_map={"html":"index.html","css":"style.css","javascript":"script.js","js":"script.js","typescript":"script.ts"}
    code_blocks=re.findall(r'```([a-zA-Z0-9_+\-]*)\n?([\s\S]*?)```',full)
    blocks_map={}
    _skip_langs = {"sh","bash","shell","bat","cmd","powershell"}
    for lang,code in code_blocks:
        ll=lang.lower().strip()
        if ll in _skip_langs: continue
        if code.strip(): blocks_map.setdefault(ll or "txt",code.strip())
    if blocks_map:
        has_web=any(l in web_langs for l in blocks_map)
        tmp_bot=Bot(token=BOT_TOKEN)
        try:
            if has_web:
                for lang,code in blocks_map.items():
                    if lang in web_langs or lang in name_map:
                        name=name_map.get(lang,f"code.{lang_to_ext.get(lang,lang)}")
                        _doc=code.encode("utf-8")
                        if _doc:
                            await tmp_bot.send_document(chat_id=chat_id,
                                document=BufferedInputFile(_doc,filename=name),caption=name)
            else:
                first_lang=next(iter(blocks_map),"txt")
                ext=lang_to_ext.get(first_lang,first_lang or "txt")
                all_code="\n\n".join(blocks_map.values())
                if all_code.strip():
                    await tmp_bot.send_document(chat_id=chat_id,
                        document=BufferedInputFile(all_code.encode("utf-8"),filename=f"code.{ext}"))
        finally: await tmp_bot.session.close()

    if can_deploy:
        normalized_full = _normalize_bot_code(full, topic=task[:100]) if deploy_type == "tg_bot" else full
        await db_save_pending(uid, normalized_full, deploy_type)
        _deploy_txt = "Запускаю…" if deploy_type == "tg_bot" else "Разворачиваю сайт…"
        ok = await tg_edit(chat_id, sent_id, f"{WAIT_EMOJI} {_deploy_txt}")
        if not ok:
            sent2 = await tg_send(chat_id, f"{WAIT_EMOJI} {_deploy_txt}")
            sent_id = sent2["message_id"] if sent2 else sent_id

        async def _edit(text, kb=None):
            r = await tg_edit(chat_id, sent_id, text, kb)
            if not r: await tg_send(chat_id, text, kb)

        await _run_deploy_loop(uid, chat_id, normalized_full, deploy_type, _edit, _edit)
    else:
        _final_text = "<b>Готово!</b>\n\n" + safe_truncate_html(html_expl, 3200)
        final_kb = markup([btn("Главное меню", cb="holo:home", eid=E["bolt"])])
        ok = await tg_edit(chat_id, sent_id, _final_text, final_kb)
        if not ok:
            await tg_send(chat_id, _final_text, final_kb)

@router.poll_answer()
async def on_poll_answer(poll_answer: PollAnswer):
    uid = poll_answer.user.id
    d   = await db_poll_get(uid)
    if not d: return
    idx = d["current_q"]; qs = d["questions"]
    if idx >= len(qs): return
    q = qs[idx]
    chosen_idx = poll_answer.option_ids[0] if poll_answer.option_ids else 0
    chosen = q["opts"][chosen_idx] if chosen_idx < len(q["opts"]) else q["opts"][0]
    chat_id = d["chat_id"]

    if chosen == "Другое":
        await tg_send(chat_id, "✏️ Напиши свой вариант для: <b>" + q["q"] + "</b>",
            markup([btn("Пропустить", cb="holo:skip_polls", style="danger")]))
        await db_poll_set_waiting(uid, 1)
        return

    await db_poll_add_answer(uid, q["q"], chosen)
    await asyncio.sleep(0.3)
    await _send_next_poll(chat_id, uid)

async def _bg_compress_last_pair(chat_id_db: int):
    try:
        async with pg().acquire() as c:
            rows = await c.fetch(
                "SELECT id, role, content FROM chat_messages "
                "WHERE chat_id=$1 ORDER BY id DESC LIMIT 2",
                chat_id_db
            )
        if len(rows) < 2:
            return
        for row in rows:
            content = row["content"]
            if len(content) <= 400:
                continue
            role = row["role"]
            if role == "user":
                compressed = await _compact_question(content)
            else:
                compressed = _truncate_message(content, 400)
            if compressed and compressed != content:
                async with pg().acquire() as c:
                    await c.execute(
                        "UPDATE chat_messages SET content=$1 WHERE id=$2",
                        compressed, row["id"]
                    )
    except Exception as e:
        log.warning("_bg_compress_last_pair: %s", e)

async def _compress_chat_history(history: list) -> str:
    if not history:
        return ""
    dialog_text = "\n".join(
        f"{'U' if m['role']=='user' else 'A'}: {m['content'][:300]}"
        for m in history
    )
    prompt = (
        "Сожми этот диалог в краткий контекст - не более 150 слов.\n"
        "Сохрани только: ключевые факты, задачи, решения, договорённости.\n"
        "Без вступлений, только суть. Формат: короткие тезисы.\n\n"
        f"{dialog_text[:4000]}"
    )
    try:
        summary = await ask_llm([{"role": "user", "content": prompt}])
        return summary.strip()[:1500]
    except Exception as e:
        log.warning("_compress_chat_history error: %s", e)
        return ""

def _truncate_message(content: str, max_chars: int = 600) -> str:
    if len(content) <= max_chars:
        return content
    return content[:max_chars] + "…[сокращено]"

async def _compact_question(question: str) -> str:
    if len(question) <= 300:
        return question
    prompt = (
        "Сожми это сообщение до 1-3 коротких тезисов - только суть задачи/вопроса.\n"
        "Без вводных слов, просто тезисы через точку с запятой.\n\n"
        f"{question[:2000]}"
    )
    try:
        compact = await ask_llm([{"role": "user", "content": prompt}])
        result = compact.strip()
        if len(result) > len(question) or len(result) < 5:
            return question[:300] + "…"
        return result
    except Exception:
        return question[:300] + "…"

def _build_lean_history(history: list, max_entries: int = 10, max_chars_per_msg: int = 500) -> list:
    recent = history[-max_entries:] if len(history) > max_entries else history
    return [
        {"role": m["role"], "content": _truncate_message(m["content"], max_chars_per_msg)}
        for m in recent
    ]

async def _do_checkpoint(chat_id_db: int, uid: int, tg_chat_id: int, pending_question: str) -> str:
    transfer_msg = await tg_send(tg_chat_id, context_bar_transferring(0))
    transfer_id = transfer_msg["message_id"] if transfer_msg else None

    history = await db_chat_history(chat_id_db)

    async def _animate():
        for step in range(1, 7):
            await asyncio.sleep(0.7)
            if transfer_id:
                try:
                    await tg_edit(tg_chat_id, transfer_id, context_bar_transferring(step))
                except Exception:
                    pass

    anim_task = asyncio.create_task(_animate())
    summary = await _compress_chat_history(history)
    anim_task.cancel()

    async with pg().acquire() as c:
        await c.execute("DELETE FROM chat_messages WHERE chat_id=$1", chat_id_db)
    if summary:
        await db_checkpoint_set(chat_id_db, summary)

    if transfer_id:
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
                    json={"chat_id": tg_chat_id, "message_id": transfer_id}
                )
        except Exception:
            pass

    return summary

def _compress_rag_results(results: list, max_total_chars: int = 400) -> str:
    if not results:
        return ""
    
    compressed_parts = []
    total_chars = 0
    
    for r in results:
        content = r.get("content", "")
        source = r.get("source_url", "источник")
        
        if len(content) > 200:
            content = content[:200] + "..."
        
        part = f"[{source}]: {content}"
        
        if total_chars + len(part) > max_total_chars:
            remaining = max_total_chars - total_chars
            if remaining > 50:
                compressed_parts.append(part[:remaining] + "...")
            break
        
        compressed_parts.append(part)
        total_chars += len(part)
    
    return "\n\n".join(compressed_parts)

@router.message(S.chat)
async def fsm_chat(message:Message,state:FSMContext):
    uid=message.from_user.id
    settings=await db_get_settings(uid)

    d = await db_poll_get(uid)
    if d and d.get("waiting_text"):
        question=(message.text or "").strip()
        if not question: return
        await db_poll_set_waiting(uid, 0)
        q = d["questions"][d["current_q"]]
        await db_poll_add_answer(uid, q["q"], question)
        await _send_next_poll(d["chat_id"], uid)
        return

    chat_id_db = await db_get_active_chat(uid)
    if not chat_id_db:
        chat_id_db = await db_chat_create(uid); await db_set_active_chat(uid, chat_id_db)

    question = ""
    if message.voice:
        if settings.get("interactive_mode"):
            status_msg = await tg_send(message.chat.id, f"{WAIT_EMOJI}", None)
            status_id = status_msg["message_id"] if status_msg else None
            question = await transcribe_voice(message.bot, message.voice)
            if status_id: await tg_edit(message.chat.id, status_id, f"<i>Распознано:</i> {question[:200]}")
        else:
            await tg_send(message.chat.id, "Включи <b>Interactive</b> в настройках чтобы я мог распознавать голосовые", kb_in_chat(chat_id_db))
            return
    elif message.document and any((message.document.file_name or "").lower().endswith(e) for e in ARCHIVE_EXTS):
        if not await check_rate_limit(uid, "archive", limit=5, window=3600):
            await tg_send(message.chat.id, "Слишком много архивов. Подожди час.", kb_in_chat(chat_id_db))
            return
        status_msg = await tg_send(message.chat.id, f"{WAIT_EMOJI} Читаю архив...", None)
        status_id = status_msg["message_id"] if status_msg else None
        arc_content = await read_archive(message.bot, message.document)
        if not arc_content:
            await tg_send(message.chat.id, "Формат архива не поддерживается.", kb_in_chat(chat_id_db))
            return
        if arc_content.startswith("["):
            if status_id: await tg_edit(message.chat.id, status_id, arc_content)
            return
        caption = (message.caption or "").strip()
        question = f"{caption}\n\n{arc_content}" if caption else arc_content
        if status_id: await tg_edit(message.chat.id, status_id, f"<i>Архив прочитан, отправляю ИИ...</i>")
    elif message.photo:
        if settings.get("interactive_mode"):
            status_msg = await tg_send(message.chat.id, f"{WAIT_EMOJI}", None)
            status_id = status_msg["message_id"] if status_msg else None
            photo = message.photo[-1]
            ocr_text = await ocr_image(message.bot, photo)
            caption = (message.caption or "").strip()
            question = f"{caption}\n\n[OCR]:\n{ocr_text}" if caption else ocr_text
            if status_id: await tg_edit(message.chat.id, status_id, f"<i>Текст с фото прочитан</i>")
        else:
            await tg_send(message.chat.id, "Включи <b>Interactive</b> в настройках чтобы я мог читать текст с фото", kb_in_chat(chat_id_db))
            return
    else:
        question = (message.text or message.caption or "").strip()

    if not question: return

    ok_txt, err_txt = validate_message_text(question)
    if not ok_txt:
        await message.answer(f"⚠️ {err_txt}")
        return

    if await is_banned(uid):
        await message.answer("⛔ Доступ ограничен.")
        return

    if not await check_rate_limit(uid, "msg", limit=15, window=60):
        await message.answer("Слишком много запросов. Подожди минуту.")
        return

    if settings.get("interactive_mode"):
        urls_in_msg = re.findall(r'https?://[^\s<>"\']+', question)
        if urls_in_msg:
            fetched_parts = []
            for u in urls_in_msg[:2]:
                content = await fetch_url_content(u)
                if content and not content.startswith("[Ошибка"):
                    fetched_parts.append(f"[Содержимое {u}]:\n{content[:3000]}")
            if fetched_parts:
                question = question + "\n\n" + "\n\n".join(fetched_parts)

    await cleanup_user(uid)

    msg_count_before = await db_chat_msg_count(chat_id_db)
    if msg_count_before >= CONTEXT_MAX:
        await _do_checkpoint(chat_id_db, uid, message.chat.id, question)
        msg_count_before = 0

    is_telegraph = _detect_is_telegraph(question)
    is_code = (not is_telegraph) and bool(re.search(
        r'\b(напиши|сделай|создай|реализуй|напишите|разработай|скрипт|бот|парсер'
        r'|функци|класс|модуль|сайт|страниц|лендинг|верстк|html|css|javascript'
        r'|frontend|веб|визитк|приложени|игр)\b',question,re.IGNORECASE))
    history_before = await db_chat_history(chat_id_db)

    if is_telegraph:
        if not await check_rate_limit(uid, "telegraph", limit=5, window=3600):
            await message.answer("Слишком много статей. Подожди час.")
            return
        wait = await tg_send(message.chat.id, f"{WAIT_EMOJI} Пишу статью...", None)
        wait_id = wait["message_id"] if wait else None
        tg_sys = (
            "Ты - профессиональный автор статей.\n"
            "Напиши статью по запросу пользователя.\n"
            "ФОРМАТ ОТВЕТА - строго:\n"
            "Первая строка: ЗАГОЛОВОК: <название статьи>\n"
            "Затем пустая строка.\n"
            "Затем полная статья в Markdown:\n"
            "- ## для разделов, ### для подразделов\n"
            "- Маркированные списки через -\n"
            "- Код в ```блоках```\n"
            "- Введение, разделы с подзаголовками, заключение\n"
            "Длина: 1500-4000 слов. Отвечай на русском."
        )
        tg_messages = [{"role":"system","content":tg_sys}, {"role":"user","content":question}]
        async def on_tg_noop(text): pass
        custom_api = await db_get_custom_api(uid)
        try:
            if custom_api:
                full_tg = await ask_llm_custom_stream(tg_messages, custom_api, on_tg_noop)
                tg_tokens = 0
            else:
                full_tg, tg_tokens = await ask_llm_stream(tg_messages, on_tg_noop)
                if tg_tokens > 0:
                    await db_spend_tokens(uid, max(TOKENS_MIN_REQUEST, tg_tokens))
        except Exception as e:
            if wait_id: await tg_edit(message.chat.id, wait_id, f"Ошибка: {e}", kb_back())
            return
        lines_tg = full_tg.strip().split("\n")
        title_line = lines_tg[0] if lines_tg else ""
        if title_line.startswith("ЗАГОЛОВОК:"):
            article_title   = title_line[len("ЗАГОЛОВОК:"):].strip()
            article_content = "\n".join(lines_tg[2:]).strip()
        else:
            article_title   = question[:80]
            article_content = full_tg.strip()
        if wait_id: await tg_edit(message.chat.id, wait_id, f"{WAIT_EMOJI} Публикую...")
        result = await telegraph_publish(article_title, article_content)
        await db_log_action(uid, "telegraph_publish", (result["url"] if result else "error"))
        if result:
            await tg_edit(message.chat.id, wait_id,
                f"✅ <b>Статья опубликована!</b>\n\n📝 <b>{article_title}</b>",
                markup(
                    [btn("📖 Открыть статью", url=result["url"], style="primary")],
                    [btn("Главное меню", cb="holo:home")],
                ))
        else:
            await tg_edit(message.chat.id, wait_id,
                "❌ Ошибка публикации в Telegra.ph. Проверь TELEGRAPH_TOKEN в .env", kb_back())
        return

    is_web_for_codemode = bool(re.search(
        r'\b(сайт|страниц|лендинг|верстк|html|css|javascript|js|frontend|ui|интерфейс|веб|визитк|портфол|одностранич)\b',
        question, re.IGNORECASE))
    if settings["code_mode"] and is_code and not is_web_for_codemode and len(history_before) == 0:
        if not await check_rate_limit(uid, "codegen", limit=3, window=60):
            await message.answer("Слишком много запросов на генерацию кода. Подожди минуту.")
            return
        await state.set_state(S.code_polling)
        await tg_send(message.chat.id,
            f"{WAIT_EMOJI} Задача принята",None)
        await _start_code_polling(message.chat.id,uid,question,message.bot); return

    if settings["github_scanner"]:
        for owner,repo in extract_github_repos(question):
            url=f"https://github.com/{owner}/{repo}"
            if not ch_repo_exists(url):
                asyncio.create_task(_scan_repo_bg(message.chat.id,uid,owner,repo))

    await tg_typing(message.bot,message.chat.id)

    chat_info = await db_chat_get(chat_id_db)

    if chat_info and chat_info["title"] == "Новый чат":
        raw_title = (message.text or message.caption or question or "Чат")[:50]
        await db_chat_set_title(chat_id_db, raw_title)

    question_to_store = await _compact_question(question) if len(question) > 300 else question
    await db_chat_push(chat_id_db, uid, "user", question_to_store)

    history = _build_lean_history(history_before, max_entries=8, max_chars_per_msg=300)

    try:
        emb=await get_embedding(question)
        results=ch_vector_search(emb, top_k=2) if len(emb)==EMB_DIM else []
    except Exception as e:
        log.warning("embedding: %s",e); results=[]

    context_str = _compress_rag_results(results, max_total_chars=400) if results else ""

    is_web_q=bool(re.search(
        r'\b(сайт|страниц|лендинг|верстк|html|css|javascript|js|frontend|ui|интерфейс|веб|визитк|портфол|одностранич)\b',
        question, re.IGNORECASE))
    base_sys=SYSTEM_PROMPT
    
    if settings.get("preview_mode") and is_web_q:
        base_sys+=(
            "\n\nВАЖНО: Раздели код на блоки: ```html (без style/script), "
            "```css (весь CSS), ```javascript (весь JS).")

    is_bot_req = (not is_web_q) and bool(re.search(
        r'\b(бот|bot|telegram|aiogram|telebot|pyrogram)\b', question, re.IGNORECASE))
    if is_bot_req:
        base_sys += (
            "\n\nЕСЛИ пишешь Telegram бота - СТРОГО:\n"
            "- Только aiogram==3.20.0. ЗАПРЕЩЕНО: python-telegram-bot, Updater, pyrogram, telethon\n"
            "- Формат ответа ТОЛЬКО: имя_файла.py:\\n```python\\nкод\\n```\\n\\nrequirements.txt:\\n```\\nзависимости\\n```\n"
            "- НИКАКОГО текста кроме файлов\n"
            "- В requirements.txt НЕ писать: aiohttp, os, json, asyncio"
        )

    messages=[{"role":"system","content":base_sys}]
    if context_str: messages.append({"role":"system","content":f"Контекст:\n{context_str}"})
    checkpoint_summary = await db_checkpoint_get(chat_id_db)
    if checkpoint_summary:
        messages.append({"role":"system","content":f"Сжатый контекст предыдущего диалога:\n{checkpoint_summary}"})
    messages.extend(history)
    messages.append({"role":"user","content":question})

    is_generative=bool(re.search(
        r'\b(напиши|сделай|создай|реализуй|разработай|сгенерируй|придумай|составь'
        r'|сайт|страниц|лендинг|верстк|html|css|скрипт|функци|класс|модуль)\b',question,re.IGNORECASE))
    wait_text=(f"{WAIT_EMOJI}" if is_generative
               else f"{WAIT_EMOJI}")
    sent=await tg_send(message.chat.id,wait_text,None)
    if not sent: return
    sent_id=sent["message_id"]

    _draft_id = sent_id
    _last_draft_text = [""]
    
    async def on_upd(text):
        html_preview = clean_md_to_html(text)
        if len(html_preview) > 3500:
            html_preview = safe_truncate_html(html_preview, 3500)
        
        if html_preview == _last_draft_text[0]:
            return
        _last_draft_text[0] = html_preview
        
        try:
            tmp = Bot(token=BOT_TOKEN)
            await tmp.send_message_draft(
                chat_id=message.chat.id,
                draft_id=_draft_id,
                text=html_preview,
                parse_mode="HTML"
            )
            await tmp.session.close()
        except Exception:
            try:
                await tg_edit(message.chat.id, sent_id, html_preview, None)
            except Exception:
                pass

    custom_api = await db_get_custom_api(uid)

    if settings.get("local_ai") and not custom_api:
        log.info("fsm_chat uid=%d provider=local_ai rag_docs=%d", uid, len(results))
        
        local_provider = await db_get_local_ai_provider(uid)
        
        if local_provider == "gemma":
            log.info("fsm_chat uid=%d using Gemma (PP1)", uid)
            
            gemma_key = os.getenv("GEMMA_API_KEY", "")
            if not gemma_key:
                await tg_edit(message.chat.id, sent_id, 
                    "❌ PP1 (Gemma) не настроен. Обратитесь к администратору.\n\n"
                    "Попробуйте P7 (локальная модель) в настройках.",
                    kb_in_chat(chat_id_db))
                return
            
            gemma_messages = [{"role": "system", "content": base_sys}]
            if context_str:
                gemma_messages.append({"role": "system", "content": f"Контекст:\n{context_str}"})
            if checkpoint_summary:
                gemma_messages.append({"role": "system", "content": f"Сжатый контекст предыдущего диалога:\n{checkpoint_summary}"})
            gemma_messages.extend(history)
            gemma_messages.append({"role": "user", "content": question})
            
            try:
                full_answer, used_tokens = await ask_llm_stream(
                    gemma_messages,
                    on_upd,
                    model="gemma-3-27b-it",
                    max_tokens=1024,
                    api_key=gemma_key
                )
            except Exception as e:
                log.exception("Gemma error: %s", e)
                await tg_edit(message.chat.id, sent_id, 
                    f"🔴 PP1 (Gemma) ошибка:\n<code>{str(e)[:300]}</code>\n\n"
                    f"Попробуйте P7 (локальная модель) в настройках.",
                    kb_in_chat(chat_id_db))
                return
        
        else:
            log.info("fsm_chat uid=%d using Ollama (P7)", uid)
            
            local_messages = [{"role": "system", "content": base_sys}]
            if context_str:
                local_messages.append({"role": "system", "content": f"Контекст:\n{context_str}"})
            if checkpoint_summary:
                local_messages.append({"role": "system", "content": f"Сжатый контекст предыдущего диалога:\n{checkpoint_summary}"})
            local_messages.extend(history)
            local_messages.append({"role": "user", "content": question})
            
            try:
                full_answer = await ask_local_ai_stream(local_messages, on_upd, context_str)
                used_tokens = len(full_answer) // 4
            except Exception as e:
                log.exception("Local AI error: %s", e)
                await tg_edit(message.chat.id, sent_id, 
                    f"🔴 P7 (локальная) ошибка:\n<code>{str(e)[:300]}</code>",
                    kb_in_chat(chat_id_db))
                return
        
        found = bool(results) and len(full_answer) > 50
        await db_inc_stat(uid, found)
        await db_chat_push(chat_id_db, uid, "assistant", _truncate_message(full_answer, 600))
        asyncio.create_task(_bg_compress_last_pair(chat_id_db))

        display = clean_md_to_html(full_answer)
        url_pairs = extract_urls(full_answer)
        final_kb = kb_url_rows(url_pairs) if (settings["url_buttons"] and url_pairs) else kb_in_chat(chat_id_db)
        
        if settings.get("context_inspector"):
            new_count = await db_chat_msg_count(chat_id_db)
            display = display + context_bar(new_count)
        
        max_len = 3900
        if len(display) <= max_len:
            ok = await tg_edit(message.chat.id, sent_id, display, final_kb)
            if not ok:
                await tg_send(message.chat.id, display, final_kb)
        else:
            parts = [display[i:i+max_len] for i in range(0, len(display), max_len)]
            ok = await tg_edit(message.chat.id, sent_id, parts[0])
            if not ok:
                await tg_send(message.chat.id, parts[0])
            for p in parts[1:-1]:
                await tg_send(message.chat.id, p)
            await tg_send(message.chat.id, parts[-1], final_kb)
        
        return

    if not custom_api:
        is_code_req = bool(re.search(
            r'\b(напиши|сделай|создай|реализуй|разработай|скрипт|бот|парсер'
            r'|функци|класс|модуль|сайт|страниц|лендинг|верстк|html|css|javascript'
            r'|frontend|веб|визитк|приложени|игр)\b', question, re.IGNORECASE))
        min_needed = TOKENS_MIN_CODE if is_code_req else TOKENS_MIN_REQUEST
        balance = await db_get_balance(uid)
        if balance < min_needed:
            await tg_edit(message.chat.id, sent_id,
                f"❌ <b>Недостаточно токенов</b>\n\n"
                f"На балансе: <b>{balance:,}</b> токенов\n"
                f"Нужно минимум: <b>{min_needed:,}</b> токенов\n\n"
                f"Пополните баланс или подключите свой API в <b>Custom AI</b>.",
                markup(
                    [btn("💎 Пополнить баланс", cb="holo:subscription", style="primary")],
                    [btn("🤖 Custom AI",         cb="holo:custom_ai")],
                ))
            return

    try:
        if custom_api:
            log.info("fsm_chat uid=%d provider=custom/%s model=%s rag_docs=%d",
                     uid, custom_api.get("provider","?"), custom_api.get("model","?"), len(results))
            _tok_limit = settings.get("token_limit") or 4096
            full_answer=await ask_llm_custom_stream(messages, custom_api, on_upd, max_tokens=_tok_limit)
            used_tokens = 0
        else:
            log.info("fsm_chat uid=%d provider=vsegpt model=%s rag_docs=%d",
                     uid, VSEGPT_MODEL_CHAT, len(results))
            _tok_limit = settings.get("token_limit") or 4096
            full_answer, used_tokens=await ask_llm_stream(messages, on_upd, max_tokens=_tok_limit)
    except Exception as e:
        log.exception("fsm_chat llm error: %s", e)
        await tg_edit(message.chat.id, sent_id, f"Ошибка:\n<code>{str(e)[:300]}</code>", kb_in_chat(chat_id_db))
        return

    is_generative_result=is_generative or bool(re.search(r'```[a-zA-Z]',full_answer))
    found=(bool(results) or is_generative_result) and len(full_answer)>50
    await db_inc_stat(uid,found)
    await db_chat_push(chat_id_db, uid, "assistant", _truncate_message(full_answer, 600))
    asyncio.create_task(_bg_compress_last_pair(chat_id_db))

    if not custom_api:
        is_code_req2 = bool(re.search(
            r'\b(напиши|сделай|создай|реализуй|разработай|скрипт|бот|парсер'
            r'|функци|класс|модуль|сайт|страниц|лендинг|верстк|html|css|javascript'
            r'|frontend|веб|визитк|приложени|игр)\b', question, re.IGNORECASE))
        min_cost = TOKENS_MIN_CODE if is_code_req2 else TOKENS_MIN_REQUEST
        actual_cost = max(min_cost, used_tokens)
        await db_spend_tokens(uid, actual_cost)
        await db_log_action(uid, "token_spend", f"{actual_cost} tokens (real={used_tokens})")

    if is_generative_result:
        expl=re.sub(r'```[\s\S]*?```','',full_answer).strip()
        expl=re.sub(r'\n{3,}','\n\n',expl).strip()
        display=clean_md_to_html(expl) if expl else "<i>Смотри файлы ниже</i>"
    else:
        display=clean_md_to_html(full_answer)

    is_tg_bot_resp=_detect_is_tg_bot(full_answer)
    is_web_resp=_detect_is_web(full_answer,from_llm_output=True)
    if is_web_q:
        is_tg_bot_resp = False
        is_web_resp = True
    deploy_type_resp="tg_bot" if is_tg_bot_resp else ("web" if is_web_resp else None)
    can_deploy_resp = (deploy_type_resp=="tg_bot") or (deploy_type_resp=="web" and settings.get("preview_mode"))
    log.info("fsm_chat detect: is_tg_bot=%s is_web=%s deploy_type=%s can_deploy=%s is_web_q=%s",
             is_tg_bot_resp, is_web_resp, deploy_type_resp, can_deploy_resp, is_web_q)

    if deploy_type_resp and can_deploy_resp:
        normalized = _normalize_bot_code(full_answer, topic=question[:100]) if deploy_type_resp == "tg_bot" else full_answer
        await db_save_pending(uid, normalized, deploy_type_resp)
        max_len=3900
        if len(display)<=max_len:
            ok=await tg_edit(message.chat.id,sent_id,display)
            if not ok: await tg_send(message.chat.id,display)
        else:
            parts=[display[i:i+max_len] for i in range(0,len(display),max_len)]
            ok=await tg_edit(message.chat.id,sent_id,parts[0])
            if not ok: await tg_send(message.chat.id,parts[0])
            for p in parts[1:]: await tg_send(message.chat.id,p)
        spin = await tg_send(message.chat.id, f"{WAIT_EMOJI} Запускаю…", None)
        spin_id = spin["message_id"] if spin else None
        async def _edit_deploy(text, kb=None):
            if spin_id:
                r = await tg_edit(message.chat.id, spin_id, text, kb)
                if not r: await tg_send(message.chat.id, text, kb)
            else:
                await tg_send(message.chat.id, text, kb)
        await _run_deploy_loop(uid, message.chat.id, normalized, deploy_type_resp, _edit_deploy, _edit_deploy)
    else:
        url_pairs=extract_urls(full_answer)
        _has_rag = bool(results)
        if _has_rag:
            await redis_set(f"last_q:{uid}", question[:500], ttl=3600)
        final_kb = (kb_url_rows(url_pairs) if (settings["url_buttons"] and url_pairs)
                    else kb_in_chat(chat_id_db, show_search_more=_has_rag))
        new_msg_count = await db_chat_msg_count(chat_id_db)
        if settings.get("context_inspector"):
            display = display + context_bar(new_msg_count)
        max_len=3900
        if len(display)<=max_len:
            ok=await tg_edit(message.chat.id,sent_id,display,final_kb)
            if not ok: await tg_send(message.chat.id,display,final_kb)
        else:
            parts=[display[i:i+max_len] for i in range(0,len(display),max_len)]
            ok=await tg_edit(message.chat.id,sent_id,parts[0])
            if not ok: await tg_send(message.chat.id,parts[0])
            for p in parts[1:-1]: await tg_send(message.chat.id,p)
            await tg_send(message.chat.id,parts[-1],final_kb)

    if is_generative_result:
        lang_to_ext={"python":"py","py":"py","javascript":"js","js":"js","typescript":"ts","ts":"ts",
                     "html":"html","css":"css","bash":"sh","shell":"sh","sh":"sh","go":"go","rust":"rs",
                     "java":"java","cpp":"cpp","c":"c","csharp":"cs","cs":"cs","php":"php",
                     "sql":"sql","yaml":"yaml","json":"json","xml":"xml"}
        web_langs={"html","css","javascript","js","typescript","ts"}
        name_map={"html":"index.html","css":"style.css","javascript":"script.js","js":"script.js","typescript":"script.ts"}
        raw_blocks=re.findall(r'```([a-zA-Z0-9_+\-]*)\n?([\s\S]*?)```',full_answer)
        blocks_map={}
        for lang,code in raw_blocks:
            ll=lang.lower().strip()
            if code.strip(): blocks_map.setdefault(ll or "txt",code.strip())
        if blocks_map:
            has_web=any(l in web_langs for l in blocks_map)
            tmp_bot=Bot(token=BOT_TOKEN)
            try:
                if has_web:
                    for lang,code in blocks_map.items():
                        if lang in web_langs or lang in name_map:
                            name=name_map.get(lang,f"code.{lang_to_ext.get(lang,lang)}")
                            data=code.encode("utf-8")
                            if data:
                                await tmp_bot.send_document(chat_id=message.chat.id,
                                    document=BufferedInputFile(data,filename=name),caption=name)
                else:
                    first_lang=next(iter(blocks_map),"txt")
                    ext=lang_to_ext.get(first_lang,first_lang or "txt")
                    all_code="\n\n".join(blocks_map.values())
                    if all_code.strip():
                        await tmp_bot.send_document(chat_id=message.chat.id,
                            document=BufferedInputFile(all_code.encode("utf-8"),filename=f"code.{ext}"))
            finally: await tmp_bot.session.close()

    if not found:
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        uname = message.from_user.username or "-"
        fname = message.from_user.first_name or "-"
        report = (
            f"=== Нет данных в базе ===\n"
            f"Время:      {now}\n"
            f"User ID:    {uid}\n"
            f"Username:   @{uname}\n"
            f"Имя:        {fname}\n"
            f"Чат ID:     {chat_id_db}\n"
            f"\n--- Запрос ---\n{question}\n"
        )
        tmp_bot = Bot(token=BOT_TOKEN)
        try:
            for aid in ADMIN_IDS:
                try:
                    await tmp_bot.send_document(
                        chat_id=aid,
                        document=BufferedInputFile(report.encode("utf-8"), filename="no_data.txt"),
                        caption=f"Нет данных | @{uname} | {now}")
                except Exception as e:
                    log.warning("admin notify uid=%d: %s", aid, e)
        finally:
            await tmp_bot.session.close()

@router.callback_query(F.data=="holo:custom_ai")
async def cb_custom_ai(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    active=await db_get_custom_api(uid)
    status=""
    if active:
        p=PROVIDERS.get(active["provider"],{})
        status=f"\n\n✅ <b>Подключено:</b> {p.get('name',active['provider'])} - <code>{active['model']}</code>"
    await tg_edit(cb.message.chat.id,cb.message.message_id,CUSTOM_AI_INFO+status,kb_custom_ai(has_active=bool(active)))

@router.callback_query(F.data == "holo:my_center")
async def cb_my_center(cb: CallbackQuery):
    await cb.answer()
    await tg_edit_banner(
        cb.message.chat.id,
        cb.message.message_id,
        f'{E2("5361837567463399422")} <b>Мой центр</b>\n\n'
        f'Здесь вы можете настроить бота, подключить свой API-ключ и управлять базой знаний.',
        kb_my_center()
    )

@router.callback_query(F.data == "holo:connect_db")
async def cb_connect_db(cb: CallbackQuery):
    await cb.answer("🚧 Функция в разработке. Скоро появится!", show_alert=True)

@router.callback_query(F.data=="holo:capi_disconnect")
async def cb_capi_disconnect(cb:CallbackQuery):
    await cb.answer(); await db_delete_custom_api(cb.from_user.id)
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        CUSTOM_AI_INFO, kb_custom_ai(has_active=False))

@router.callback_query(F.data == "holo:local_ai_menu")
async def cb_local_ai_menu(cb: CallbackQuery):
    await cb.answer()
    uid = cb.from_user.id
    settings = await db_get_settings(uid)
    current_status = settings.get('local_ai', 0)
    current_provider = await db_get_local_ai_provider(uid)
    
    if not current_status:
        await tg_edit(
            cb.message.chat.id, cb.message.message_id,
            f"{E2('5399898266265475100')} <b>Local AI</b>\n\n"
            f"Local AI сейчас <b>выключен</b>.\n\n"
            f"Нажми <b>Включить</b> чтобы активировать бесплатную генерацию.",
            markup(
                [btn("✅ Включить Local AI", cb="holo:toggle:local_ai", style="primary")],
                [btn("Назад", cb="holo:settings", eid="5341715473882955310")]
            )
        )
        return
    
    await tg_edit(
        cb.message.chat.id, cb.message.message_id,
        f"{E2('5399898266265475100')} <b>Local AI</b>\n\n"
        f"Выбери модель для бесплатной генерации:\n\n"
        f"<b>P7</b>\n"
        f"   - Медленная\n"
        f"   - 0 цензуры\n\n"
        f"<b>PP1</b>\n"
        f"   - Модель 27B параметров\n"
        f"   - Огромная скорость\n"
        f"   - Иногда не обходит цензуру\n\n"
        f"<b>Текущий выбор:</b> {'P7' if current_provider == 'ollama' else 'PP1'}",
        markup(
            [btn("P7", cb="holo:local_ai_set:ollama", style="primary" if current_provider == "ollama" else None)],
            [btn("PP1", cb="holo:local_ai_set:gemma", style="primary" if current_provider == "gemma" else None)],
            [btn("Выключить Local AI", cb="holo:toggle:local_ai", style="danger")],
            [btn("Назад", cb="holo:settings", eid="5341715473882955310")]
        )
    )

@router.callback_query(F.data.startswith("holo:local_ai_set:"))
async def cb_local_ai_set(cb: CallbackQuery):
    await cb.answer()
    uid = cb.from_user.id
    provider = cb.data.split(":")[2]
    
    await db_set_local_ai_provider(uid, provider)
    
    provider_name = "P7 (Ollama)" if provider == "ollama" else "PP1 (Gemma)"
    await cb.answer(f"Выбран {provider_name}", show_alert=True)
    
    settings = await db_get_settings(uid)
    await tg_edit(cb.message.chat.id, cb.message.message_id, settings_text(), kb_settings(settings))

@router.callback_query(F.data.startswith("holo:capi_select:"))
async def cb_capi_select(cb:CallbackQuery,state:FSMContext):
    await cb.answer()
    provider_id=cb.data.split(":",2)[2]
    p=PROVIDERS.get(provider_id)
    if not p: await cb.answer("Неизвестный провайдер",show_alert=True); return
    await state.update_data(capi_provider=provider_id)
    if provider_id=="huggingface_ep":
        await state.set_state(S.custom_api_key)
        await state.update_data(capi_step="endpoint_url")
        await tg_edit(cb.message.chat.id,cb.message.message_id,
            f"<b>{p['name']}</b>\n\nВведи URL твоего Inference Endpoint:\n<i>Пример: https://xyz.eu-west-1.aws.endpoints.huggingface.cloud</i>",
            kb_custom_ai_back())
        return
    await state.set_state(S.custom_api_key)
    await state.update_data(capi_step="api_key")
    models_hint=""
    if p.get("models"):
        models_hint="\n\n<b>Популярные модели:</b>\n" + "\n".join(f"- <code>{m}</code>" for m in p["models"])
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        f"<b>{p['name']}</b>\n\nВведи API ключ:{models_hint}",
        kb_custom_ai_back())

@router.message(S.custom_api_key)
async def fsm_custom_api_key(message:Message,state:FSMContext):
    uid=message.from_user.id
    text=(message.text or "").strip()
    if not text: return
    data=await state.get_data()
    step=data.get("capi_step","api_key")
    provider_id=data.get("capi_provider","")
    p=PROVIDERS.get(provider_id,{})

    if step=="endpoint_url":
        ok, err = validate_endpoint_url(text)
        if not ok:
            await message.answer(f"⚠️ {err}",parse_mode="HTML"); return
        await state.update_data(capi_endpoint_url=text, capi_step="api_key")
        await message.answer(f"<b>{p.get('name','')}</b>\n\nТеперь введи API ключ:",parse_mode="HTML")
        return

    ok_k, err_k = validate_custom_api_key(text)
    if not ok_k:
        await message.answer(f"⚠️ {err_k}",parse_mode="HTML"); return
    await state.update_data(capi_api_key=text, capi_step="model")
    await state.set_state(S.custom_api_model)
    models_hint=""
    if p.get("models"):
        models_hint="\n\n<b>Доступные модели:</b>\n" + "\n".join(f"- <code>{m}</code>" for m in p["models"])
    await message.answer(
        f"<b>{p.get('name','')}</b>\n\nВведи название модели:{models_hint}",
        parse_mode="HTML")

@router.message(S.custom_api_model)
async def fsm_custom_api_model(message:Message,state:FSMContext):
    uid=message.from_user.id
    model=(message.text or "").strip()
    if not model: return
    ok_m, err_m = validate_custom_model(model)
    if not ok_m:
        await message.answer(f"⚠️ {err_m}",parse_mode="HTML"); return
    data=await state.get_data()
    provider_id=data.get("capi_provider","")
    api_key=data.get("capi_api_key","")
    endpoint_url=data.get("capi_endpoint_url","")
    p=PROVIDERS.get(provider_id,{})

    if not await check_rate_limit(uid, "capi_validate", limit=3, window=60):
        await message.answer("Слишком много попыток. Подожди минуту.")
        return
    wait=await tg_send(uid,f"{WAIT_EMOJI} Проверяю подключение...",None)
    wait_id=wait["message_id"] if wait else None

    ok,err=await validate_custom_api(provider_id,api_key,model,endpoint_url or None)

    if not ok:
        if wait_id: await tg_edit(uid,wait_id,f"❌ <b>Ошибка:</b> {err}\n\nПопробуй снова - введи другую модель или проверь ключ.",
            markup([btn("Назад к выбору провайдера",cb="holo:custom_ai",eid="5341715473882955310")]))
        await state.clear(); return

    custom_data={"provider":provider_id,"api_key":api_key,"model":model}
    if endpoint_url: custom_data["endpoint_url"]=endpoint_url
    await db_set_custom_api(uid,provider_id,api_key,model)
    await state.clear()
    if wait_id:
        await tg_edit(uid,wait_id,
            f"✅ <b>Custom AI подключён!</b>\n\n"
            f"<b>Провайдер:</b> {p.get('name',provider_id)}\n"
            f"<b>Модель:</b> <code>{model}</code>\n\n"
            f"Все твои запросы теперь идут через этот API. Чтобы отключить - зайди в <b>Custom AI</b>.",
            markup([btn("Начать чат",cb="holo:chat",eid="5287454433318300292",style="primary")],
                   [btn("Главное меню",cb="holo:home",eid="5341715473882955310")]))

@router.callback_query(F.data=="holo:ref_confirm")
async def cb_ref_confirm(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    if await db_is_referral_confirmed(uid):
        await tg_edit_banner(cb.message.chat.id,cb.message.message_id,home_text(),kb_home()); return
    await db_confirm_referral(uid)
    referrer_id=await db_get_referrer(uid)
    if referrer_id:
        try:
            await tg_send(referrer_id,
                f"🎉 <b>Новый реферал!</b>\nПользователь зарегистрировался по вашей ссылке.\n"
                f"Вы будете получать {REF_PERCENT}% с каждой его покупки.")
        except Exception: pass
    await db_log_action(uid,"ref_confirmed",str(referrer_id))
    await tg_edit_banner(cb.message.chat.id,cb.message.message_id,
        "✅ <b>Подтверждено!</b>\n\nДобро пожаловать в Holocron!",
        markup([btn("Главное меню",cb="holo:home",style="primary")]))

@router.callback_query(F.data=="holo:referral")
async def cb_referral(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    await db_log_action(uid,"referral_open")
    ref_cnt = await db_get_ref_count(uid)
    ref_bal = await db_get_ref_balance(uid)
    me = await cb.bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref_{uid}"
    text=(
        f'🔗 <b>Реферальная система</b>\n\n'
        f'Ваша ссылка:\n<code>{ref_link}</code>\n\n'
        f'Приглашено рефералов: <b>{ref_cnt}</b>\n'
        f'Реферальный баланс:   <b>{ref_bal:.2f}₽</b>\n\n'
        f'<i>Вы получаете {REF_PERCENT}% с каждой покупки реферала.</i>\n'
        f'Вывод доступен от {REF_MIN_PAYOUT}₽'
    )
    rows=[]
    if ref_bal>=REF_MIN_PAYOUT:
        rows.append([btn("💸 Вывести",cb="holo:ref_withdraw",style="primary")])
    rows.append([btn("Назад",cb="holo:home",eid="5341715473882955310")])
    await tg_edit(cb.message.chat.id,cb.message.message_id,text,{"inline_keyboard":rows})

@router.callback_query(F.data=="holo:ref_withdraw")
async def cb_ref_withdraw(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    ref_bal=await db_get_ref_balance(uid)
    if ref_bal<REF_MIN_PAYOUT:
        await cb.answer(f"Минимум {REF_MIN_PAYOUT}₽. У вас {ref_bal:.2f}₽",show_alert=True); return
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        f'💸 <b>Вывод реферального баланса</b>\n\nДоступно: <b>{ref_bal:.2f}₽</b>\n\nВыберите способ:',
        markup(
            [btn("₿ CryptoBot (мгновенно)",cb="holo:ref_payout:crypto")],
            [btn("💳 Карта РФ (до 24ч)",   cb="holo:ref_payout:card")],
            [btn("Назад",cb="holo:referral")],
        ))

@router.callback_query(F.data.startswith("holo:ref_payout:"))
async def cb_ref_payout(cb:CallbackQuery,state:FSMContext):
    await cb.answer(); uid=cb.from_user.id
    method=cb.data.split(":")[2]
    ref_bal=await db_get_ref_balance(uid)
    if ref_bal<REF_MIN_PAYOUT:
        await cb.answer("Недостаточно средств",show_alert=True); return
    if method=="crypto":
        usd=round(ref_bal/90,2)
        wait=await tg_send(uid,f"{WAIT_EMOJI} Создаю чек...")
        wait_id=wait["message_id"] if wait else None
        check_url=await cryptobot_create_check(usd,f"Вывод {ref_bal:.2f}₽ = ${usd}")
        if check_url:
            await db_subtract_ref_balance(uid,ref_bal)
            await db_add_payout_request(uid,ref_bal,"cryptobot",check_url)
            await db_log_action(uid,"ref_payout",f"crypto {ref_bal}₽")
            if wait_id:
                await tg_edit(uid,wait_id,
                    f'✅ <b>Чек создан!</b>\n\nСумма: <b>${usd}</b> USDT',
                    markup([btn("Получить чек",url=check_url,style="primary")],[btn("Главное меню",cb="holo:home")]))
        else:
            if wait_id: await tg_edit(uid,wait_id,"❌ Ошибка создания чека. Попробуй позже.",kb_back())
    else:
        await state.update_data(ref_payout_amount=ref_bal)
        await state.set_state(S.awaiting_card)
        await tg_edit(cb.message.chat.id,cb.message.message_id,
            f'💳 <b>Вывод на карту РФ</b>\n\nСумма: <b>{ref_bal:.2f}₽</b>\n\nВведи номер карты (16 цифр):',
            markup([btn("Отмена",cb="holo:referral")]))

@router.message(S.awaiting_card)
async def fsm_awaiting_card(message:Message,state:FSMContext):
    uid=message.from_user.id
    card=(message.text or "").strip().replace(" ","")
    if not validate_card_number(card):
        await message.answer("Неверный формат. Введи 16 цифр:",parse_mode="HTML"); return
    data=await state.get_data(); amount=data.get("ref_payout_amount",0)
    await state.clear()
    await db_subtract_ref_balance(uid,amount)
    req_id=await db_add_payout_request(uid,amount,"card_rf",card)
    await db_log_action(uid,"ref_payout",f"card {amount}₽ req_id={req_id}")
    await message.answer(
        f'✅ <b>Заявка #{req_id} принята!</b>\n\n'
        f'Сумма: <b>{amount:.2f}₽</b>\n'
        f'Карта: <code>{card[:4]}****{card[-4:]}</code>\n\n'
        f'Перевод поступит в течение <b>24 часов</b>.',parse_mode="HTML")
    row=await pg().fetchrow("SELECT username,first_name FROM users WHERE uid=$1", uid)
    uname=f"@{row['username']}" if (row and row['username']) else f"id{uid}"
    for aid in ADMIN_IDS:
        try:
            await tg_send(aid,
                f'💸 <b>Запрос на вывод #{req_id}</b>\n\n'
                f'Юзер: {uname}\nМетод: <b>Карта РФ</b>\n'
                f'Карта: <code>{card}</code>\nСумма: <b>{amount:.2f}₽</b>')
        except Exception: pass

def _effective_disc(tokens: int, active_promos: dict, user_disc: dict | None) -> int | None:
    promo_disc = active_promos.get(tokens)
    if user_disc:
        tf = user_disc.get("tokens_filter")
        if tf is None or tf == tokens:
            ud = user_disc.get("discount_pct", 0)
            if promo_disc is None or ud > promo_disc:
                return ud
    return promo_disc

@router.callback_query(F.data=="holo:subscription")
async def cb_subscription(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    await db_log_action(uid,"subscription_open")
    balance=await db_get_balance(uid)
    text=(
        f'💎 <b>Подписка - токены</b>\n\n'
        f'Текущий баланс: <b>{balance:,}</b> токенов\n\n'
        '<b>Выберите тариф:</b>'
    )
    rows=[]
    active_promos = {p["tokens"]: p["discount_pct"] for p in (await db_get_active_promotions())}
    user_disc = await db_get_user_discount(uid)
    for tokens,price_rub,price_usd,label in PLANS:
        disc = _effective_disc(tokens, active_promos, user_disc)
        if disc:
            new_rub = round(price_rub * (1 - disc / 100))
            new_usd = round(price_usd * (1 - disc / 100), 2)
            rows.append([btn(f"{label}  -  {new_rub}₽ / ${new_usd}  🔥-{disc}%",cb=f"holo:buy:{tokens}")])
        else:
            rows.append([btn(f"{label}  -  {price_rub}₽ / ${price_usd}",cb=f"holo:buy:{tokens}")])
    rows.append([btn("Назад",cb="holo:home",eid="5341715473882955310")])
    await tg_edit(cb.message.chat.id,cb.message.message_id,text,{"inline_keyboard":rows})

@router.callback_query(F.data.startswith("holo:buy:"))
async def cb_buy(cb:CallbackQuery):
    await cb.answer(); uid=cb.from_user.id
    tokens=int(cb.data.split(":")[2])
    plan=PLAN_BY_TOKENS.get(tokens)
    if not plan: await cb.answer("Тариф не найден",show_alert=True); return
    tokens,price_rub,price_usd,label=plan
    active_promos = {p["tokens"]: p["discount_pct"] for p in (await db_get_active_promotions())}
    user_disc = await db_get_user_discount(uid)
    disc = _effective_disc(tokens, active_promos, user_disc)
    if disc:
        price_rub = round(price_rub * (1 - disc / 100))
        price_usd = round(price_usd * (1 - disc / 100), 2)
    await db_log_action(uid,"buy_select",label)
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        f'💎 <b>{label} токенов</b>\n\nЦена: <b>{price_rub}₽</b> / <b>${price_usd}</b>\n\nВыберите способ оплаты:',
        markup(
            [btn("💳 СБП (QR-код)",cb=f"holo:pay:sbp:{tokens}")],
            [btn("💳 Карта РФ",    cb=f"holo:pay:card:{tokens}")],
            [btn("₿ CryptoBot",   cb=f"holo:pay:crypto:{tokens}")],
            [btn("Назад",cb="holo:subscription",eid="5341715473882955310")],
        ))

@router.callback_query(F.data.startswith("holo:pay:"))
async def cb_pay(cb:CallbackQuery):
    await cb.answer("Создаю счёт..."); uid=cb.from_user.id
    if not await check_rate_limit(uid, "payment", limit=5, window=3600):
        await cb.answer("Слишком много попыток оплаты. Попробуй через час.", show_alert=True)
        return
    parts=cb.data.split(":"); method_str=parts[2]; tokens=int(parts[3])
    plan=PLAN_BY_TOKENS.get(tokens)
    if not plan: await cb.answer("Тариф не найден",show_alert=True); return
    tokens,price_rub,price_usd,label=plan
    active_promos = {p["tokens"]: p["discount_pct"] for p in (await db_get_active_promotions())}
    user_disc = await db_get_user_discount(uid)
    disc = _effective_disc(tokens, active_promos, user_disc)
    if disc:
        price_rub = round(price_rub * (1 - disc / 100))
        price_usd = round(price_usd * (1 - disc / 100), 2)
    await db_log_action(uid,"pay_start",f"{method_str}:{label}")
    wait=await tg_send(uid,f"{WAIT_EMOJI} Создаю счёт..."); wait_id=wait["message_id"] if wait else None
    async def upd(text,kb=None):
        if wait_id: await tg_edit(uid,wait_id,text,kb)
    if method_str=="crypto":
        result=await cryptobot_create_invoice(uid,tokens,price_rub,price_usd)
        if not result: await upd("❌ Ошибка CryptoBot. Попробуй позже.",kb_back()); return
        await upd(
            f'₿ <b>Оплата через CryptoBot</b>\n\nТариф: <b>{label}</b>\nСумма: <b>${price_usd}</b>\n\nСчёт действителен 60 минут.',
            markup(
                [btn("Оплатить",url=result["redirect"],style="primary")],
                [btn("✅ Я оплатил - проверить",cb=f"holo:check_pay:{result['invoice_id']}")],
                [btn("Назад",cb="holo:subscription")],
            ))
    else:
        platega_method=2 if method_str=="sbp" else 10
        result=await platega_create_invoice(uid,tokens,price_rub,price_usd,platega_method)
        if not result: await upd("❌ Ошибка Platega. Попробуй позже.",kb_back()); return
        method_label="СБП QR-код" if method_str=="sbp" else "Карта РФ"
        await upd(
            f'💳 <b>Оплата - {method_label}</b>\n\nТариф: <b>{label}</b>\nСумма: <b>{price_rub}₽</b>\n\nСчёт действителен 15 минут.',
            markup(
                [btn("Перейти к оплате",url=result["redirect"],style="primary")],
                [btn("✅ Я оплатил - проверить",cb=f"holo:check_pay:{result['invoice_id']}")],
                [btn("Назад",cb="holo:subscription")],
            ))

@router.callback_query(F.data.startswith("holo:check_pay:"))
async def cb_check_pay(cb:CallbackQuery):
    await cb.answer("Проверяю..."); uid=cb.from_user.id
    inv_id=cb.data.split(":",2)[2]
    inv=await db_get_invoice(inv_id)
    if not inv: await cb.answer("Счёт не найден",show_alert=True); return
    if inv["status"]=="paid":
        await cb.answer("✅ Уже оплачен!",show_alert=True)
        await tg_edit(cb.message.chat.id,cb.message.message_id,
            f"✅ <b>Оплата подтверждена!</b>\nБаланс: <b>{await db_get_balance(uid):,}</b> токенов",kb_back()); return
    if inv["provider"]=="cryptobot":
        paid=await cryptobot_check_invoice(inv_id)
        if paid:
            await _process_payment(inv_id)
            await tg_edit(cb.message.chat.id,cb.message.message_id,
                f"✅ <b>Оплата подтверждена!</b>\nБаланс: <b>{await db_get_balance(uid):,}</b> токенов",kb_back()); return
    await cb.answer("Оплата не найдена. Подожди немного и проверь снова.",show_alert=True)

async def analytics_today() -> dict:
    async with pg().acquire() as c:
        new_users = await c.fetchval(
            "SELECT count(*) FROM users WHERE joined_at >= NOW() - INTERVAL '1 day'") or 0
        requests = await c.fetchval(
            "SELECT count(*) FROM user_action_log WHERE action='message' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        revenue = await c.fetchval(
            "SELECT COALESCE(SUM(amount_rub),0) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        errors = await c.fetchval(
            "SELECT count(*) FROM user_action_log WHERE action='no_data' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        deploys = await c.fetchval(
            "SELECT count(*) FROM user_action_log WHERE action LIKE 'deploy%' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        payments = await c.fetchval(
            "SELECT count(*) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '1 day'") or 0
    return {"new_users":new_users,"requests":requests,"revenue":float(revenue),
            "errors":errors,"deploys":deploys,"payments":payments}

async def analytics_week() -> dict:
    async with pg().acquire() as c:
        new_users = await c.fetchval(
            "SELECT count(*) FROM users WHERE joined_at >= NOW() - INTERVAL '7 days'") or 0
        requests = await c.fetchval(
            "SELECT count(*) FROM user_action_log WHERE action='message' AND created_at >= NOW() - INTERVAL '7 days'") or 0
        revenue = await c.fetchval(
            "SELECT COALESCE(SUM(amount_rub),0) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '7 days'") or 0
        new_payers = await c.fetchval(
            "SELECT count(DISTINCT uid) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '7 days'") or 0
    return {"new_users":new_users,"requests":requests,"revenue":float(revenue),"new_payers":new_payers}

async def analytics_month() -> dict:
    async with pg().acquire() as c:
        total_users = await c.fetchval("SELECT count(*) FROM users") or 0
        revenue = await c.fetchval(
            "SELECT COALESCE(SUM(amount_rub),0) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '30 days'") or 0
        payers = await c.fetchval(
            "SELECT count(DISTINCT uid) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '30 days'") or 1
        total_refs = await c.fetchval("SELECT count(*) FROM referrals WHERE confirmed=1") or 0
        avg_check = await c.fetchval(
            "SELECT AVG(amount_rub) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '30 days'") or 0
        arpu = float(revenue) / max(int(total_users), 1)
    return {"total_users":total_users,"revenue":float(revenue),"payers":payers,
            "total_refs":total_refs,"avg_check":float(avg_check),"arpu":arpu}

async def analytics_ltv(limit=15) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT i.uid,
                   COALESCE(u.username,'') as username,
                   SUM(i.amount_rub) as total_spent,
                   COUNT(*) as payments_count,
                   AVG(i.amount_rub) as avg_check
            FROM invoices i
            LEFT JOIN users u ON u.uid=i.uid
            WHERE i.status='paid'
            GROUP BY i.uid, u.username
            ORDER BY total_spent DESC
            LIMIT $1""", limit)
        return [dict(r) for r in rows]

async def analytics_daily_activity(days=14) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT DATE(created_at) as day,
                   COUNT(*) as total,
                   COUNT(DISTINCT uid) as uniq_users,
                   SUM(CASE WHEN action='no_data' THEN 1 ELSE 0 END) as failed
            FROM user_action_log
            WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
            GROUP BY day ORDER BY day""", str(days))
        return [dict(r) for r in rows]

async def analytics_revenue_by_plan() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT tokens,
                   COUNT(*) as purchases,
                   SUM(amount_rub) as total_rub,
                   AVG(amount_rub) as avg_rub
            FROM invoices WHERE status='paid'
            GROUP BY tokens ORDER BY total_rub DESC""")
        return [dict(r) for r in rows]

async def analytics_provider_stats() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT provider, COUNT(*) as users
            FROM user_custom_api WHERE enabled=1
            GROUP BY provider ORDER BY users DESC""")
        return [dict(r) for r in rows]

async def analytics_success_rate() -> float:
    async with pg().acquire() as c:
        row = await c.fetchrow("""
            SELECT SUM(found_reqs)::float / NULLIF(SUM(total_reqs),0) * 100 as rate
            FROM user_stats""")
        return round(float(row["rate"] or 0), 1)

async def analytics_ref_quality(limit=10) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT r.referrer_id,
                   COALESCE(u.username,'') as username,
                   COUNT(DISTINCT r.uid) as referrals,
                   COALESCE(SUM(e.amount_rub),0) as earned
            FROM referrals r
            LEFT JOIN users u ON u.uid=r.referrer_id
            LEFT JOIN ref_earnings e ON e.referrer_id=r.referrer_id
            WHERE r.confirmed=1
            GROUP BY r.referrer_id, u.username
            ORDER BY earned DESC LIMIT $1""", limit)
        return [dict(r) for r in rows]

async def analytics_error_patterns(days=7, limit=10) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT SUBSTRING(detail,1,80) as pattern, COUNT(*) as cnt
            FROM user_action_log
            WHERE action='no_data' AND created_at >= NOW() - ($1 || ' days')::INTERVAL
            GROUP BY pattern ORDER BY cnt DESC LIMIT $2""", str(days), limit)
        return [dict(r) for r in rows]

async def analytics_deploy_stats() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT deploy_type, COUNT(*) as deploys
            FROM user_action_log
            WHERE action LIKE 'deploy%' AND created_at >= NOW() - INTERVAL '7 days'
            GROUP BY deploy_type ORDER BY deploys DESC""")
        return [dict(r) for r in rows]

async def analytics_payment_provider_split() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT provider, COUNT(*) as cnt, SUM(amount_rub) as total
            FROM invoices WHERE status='paid'
            GROUP BY provider ORDER BY total DESC""")
        return [dict(r) for r in rows]

async def _send_analytics_file(chat_id: int, text: str, filename: str, caption: str = ""):
    tmp_bot = Bot(token=BOT_TOKEN)
    try:
        await tmp_bot.send_document(
            chat_id=chat_id,
            document=BufferedInputFile(text.encode("utf-8"), filename=filename),
            caption=caption)
    finally:
        await tmp_bot.session.close()

async def _check_alerts():
    sr = await analytics_success_rate()
    if sr < 50:
        for aid in ADMIN_IDS:
            try: await tg_send(aid, f"⚠️ <b>Алерт:</b> качество ответов упало до <b>{sr}%</b>! Проверь RAG базу.")
            except Exception: pass

async def _daily_report():
    import datetime
    today = await analytics_today()
    week  = await analytics_week()
    month = await analytics_month()
    sr    = await analytics_success_rate()
    text = (
        f"📊 <b>Ежедневный отчёт {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}</b>\n\n"
        f"<b>Сегодня:</b>\n"
        f"👥 Новых: <b>{today['new_users']}</b>\n"
        f"💬 Запросов: <b>{today['requests']}</b>\n"
        f"💰 Выручка: <b>{today['revenue']:.2f}₽</b>\n"
        f"💳 Платежей: <b>{today['payments']}</b>\n"
        f"❌ Пустых ответов: <b>{today['errors']}</b>\n"
        f"🚀 Деплоев: <b>{today['deploys']}</b>\n\n"
        f"<b>Неделя:</b>\n"
        f"👥 Новых: <b>{week['new_users']}</b>  💰 <b>{week['revenue']:.2f}₽</b>\n\n"
        f"<b>Месяц:</b>\n"
        f"👥 Всего: <b>{month['total_users']}</b>  ARPU: <b>{month['arpu']:.2f}₽</b>\n"
        f"✅ Success rate: <b>{sr}%</b>  ⭐ Рефералов: <b>{month['total_refs']}</b>"
    )
    for aid in ADMIN_IDS:
        try: await tg_send(aid, text)
        except Exception: pass

async def _analytics_worker():
    import datetime
    while True:
        await asyncio.sleep(3600)
        try:
            now = datetime.datetime.now()
            if now.hour == 9 and now.minute < 5:
                await _daily_report()
            await _check_alerts()
        except Exception as e:
            log.error("_analytics_worker: %s", e)

def kb_admin():
    return markup(
        [btn("📊 Сводка (день/нед/мес)",  cb="admin:analytics_summary")],
        [btn("💰 LTV топ плательщики",    cb="admin:analytics_ltv")],
        [btn("📈 Активность по дням",     cb="admin:analytics_activity")],
        [btn("🎯 Тарифы - что берут",     cb="admin:analytics_plans")],
        [btn("🔗 Качество рефералов",     cb="admin:analytics_refs")],
        [btn("⚡ Провайдеры Custom AI",   cb="admin:analytics_providers")],
        [btn("💳 Платежи по каналам",     cb="admin:analytics_payments")],
        [btn("❌ Частые ошибки",          cb="admin:analytics_errors")],
        [btn("🚀 Деплои по типам",        cb="admin:analytics_deploys")],
        [btn("✅ Success rate RAG",        cb="admin:analytics_sr")],
        [btn("📨 Ежедн. отчёт сейчас",   cb="admin:analytics_report")],
        [btn("─────────────────",          cb="admin:noop2")],
        [btn("👤 Выдать токены",          cb="admin:give_tokens")],
        [btn("➖ Списать токены",          cb="admin:deduct_tokens")],
        [btn("📋 Инфо юзера",             cb="admin:user_info")],
        [btn("📜 Лог юзера",              cb="admin:user_log")],
        [btn("⚠️ Нет данных",             cb="admin:no_data")],
        [btn("🔑 API токены юзеров",      cb="admin:api_tokens")],
        [btn("🤖 Бот токены пула",        cb="admin:bot_tokens")],
        [btn("🚀 Активные деплои",        cb="admin:deploys")],
        [btn("💾 Размер БД CH",           cb="admin:ch_size")],
        [btn("🖥 Нагрузка сервера",       cb="admin:server_load")],
        [btn("🏆 Топ рефоводы",           cb="admin:top_refs")],
        [btn("👑 Топ активность",         cb="admin:top_users")],
        [btn("─────────────────",          cb="admin:noop3")],
        [btn("🏷 Акции — управление",     cb="admin:promos")],
        [btn("🎟 Промокоды — список",     cb="admin:promo_codes")],
        [btn("➕ Создать промокод",       cb="admin:promo_create")],
        [btn("Закрыть", cb="holo:home")],
    )

@router.message(Command("panel"))
async def cmd_panel(message:Message):
    if message.from_user.id not in ADMIN_IDS: return
    await db_log_action(message.from_user.id,"admin_panel")
    await tg_send(message.chat.id,"🛡 <b>Админ панель</b>",kb_admin())

@router.message(Command("stats"))
async def cmd_stats(message:Message):
    if message.from_user.id not in ADMIN_IDS: return
    import datetime
    today = await analytics_today()
    week  = await analytics_week()
    month = await analytics_month()
    sr    = await analytics_success_rate()
    text = (
        f"📊 <b>Статистика Holocron</b>\n\n"
        f"<b>Сегодня:</b>\n"
        f"👥 Новых: {today['new_users']}  💬 Запросов: {today['requests']}\n"
        f"💰 Выручка: {today['revenue']:.2f}₽  ❌ Ошибок: {today['errors']}\n\n"
        f"<b>Неделя:</b>\n"
        f"👥 Новых: {week['new_users']}  💰 {week['revenue']:.2f}₽\n\n"
        f"<b>Месяц / всего:</b>\n"
        f"👥 Юзеров: {month['total_users']}  ARPU: {month['arpu']:.2f}₽\n"
        f"✅ Success rate: {sr}%  ⭐ Рефералов: {month['total_refs']}\n\n"
        f"<i>/panel для полной панели</i>"
    )
    await message.answer(text, parse_mode="HTML")

@router.callback_query(F.data=="admin:stats")
async def admin_stats(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    s=await db_payment_stats(); total_users=await db_total_users()
    await tg_edit(cb.message.chat.id,cb.message.message_id,
        f'📊 <b>Статистика бота</b>\n\nВсего юзеров: <b>{total_users}</b>\n'
        f'Платежей: <b>{s["payments"]}</b> на <b>{s["revenue_rub"]:.2f}₽</b>\n'
        f'Выводов: <b>{s["payouts"]}</b> на <b>{s["payout_rub"]:.2f}₽</b>\n'
        f'Рефералов: <b>{s["refs"]}</b>',
        markup([btn("Назад к панели",cb="holo:home")]))

@router.callback_query(F.data=="admin:give_tokens")
async def admin_give_tokens(cb:CallbackQuery,state:FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer(); await state.set_state(S.admin_give_uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,"Введи ID пользователя:",markup([btn("Отмена",cb="holo:home")]))

@router.message(S.admin_give_uid)
async def admin_fsm_give_uid(message:Message,state:FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    try: uid=int(message.text.strip())
    except: await message.answer("Введи числовой ID:",parse_mode="HTML"); return
    await state.update_data(target_uid=uid); await state.set_state(S.admin_give_amount)
    await message.answer(f"ID: <b>{uid}</b>\nСколько токенов выдать?",parse_mode="HTML")

@router.message(S.admin_give_amount)
async def admin_fsm_give_amount(message:Message,state:FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    try: amount=int(message.text.strip())
    except: await message.answer("Введи число:",parse_mode="HTML"); return
    data=await state.get_data(); uid=data["target_uid"]; await state.clear()
    await db_add_tokens(uid,amount)
    await db_log_action(uid,"admin_give",f"{amount} tokens by {message.from_user.id}")
    await message.answer(f"✅ Выдано <b>{amount:,}</b> токенов юзеру <b>{uid}</b>\nБаланс: <b>{await db_get_balance(uid):,}</b>",parse_mode="HTML")
    await tg_send(message.chat.id,"🛡 Админ панель",kb_admin())

@router.callback_query(F.data=="admin:deduct_tokens")
async def admin_deduct_tokens(cb:CallbackQuery,state:FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer(); await state.set_state(S.admin_deduct_uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,"Введи ID пользователя для списания:",markup([btn("Отмена",cb="holo:home")]))

@router.message(S.admin_deduct_uid)
async def admin_fsm_deduct_uid(message:Message,state:FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    try: uid=int(message.text.strip())
    except: await message.answer("Введи числовой ID:",parse_mode="HTML"); return
    await state.update_data(deduct_uid=uid); await state.set_state(S.admin_deduct_amount)
    await message.answer(f"ID: <b>{uid}</b>\nСколько токенов списать?",parse_mode="HTML")

@router.message(S.admin_deduct_amount)
async def admin_fsm_deduct_amount(message:Message,state:FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    try: amount=int(message.text.strip())
    except: await message.answer("Введи число:",parse_mode="HTML"); return
    data=await state.get_data(); uid=data["deduct_uid"]; await state.clear()
    await db_set_tokens(uid,max(0,await db_get_balance(uid)-amount))
    await db_log_action(uid,"admin_deduct",f"{amount} tokens by {message.from_user.id}")
    await message.answer(f"✅ Списано <b>{amount:,}</b> токенов у юзера <b>{uid}</b>\nБаланс: <b>{await db_get_balance(uid):,}</b>",parse_mode="HTML")
    await tg_send(message.chat.id,"🛡 Админ панель",kb_admin())

@router.callback_query(F.data=="admin:user_info")
async def admin_user_info_prompt(cb:CallbackQuery,state:FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer(); await state.update_data(admin_mode="info"); await state.set_state(S.admin_check_uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,"Введи ID пользователя:",markup([btn("Отмена",cb="holo:home")]))

@router.callback_query(F.data=="admin:user_log")
async def admin_user_log_prompt(cb:CallbackQuery,state:FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer(); await state.update_data(admin_mode="log"); await state.set_state(S.admin_check_uid)
    await tg_edit(cb.message.chat.id,cb.message.message_id,"Введи ID пользователя для лога:",markup([btn("Отмена",cb="holo:home")]))

@router.message(S.admin_check_uid)
async def admin_fsm_check_uid(message:Message,state:FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    try: uid=int(message.text.strip())
    except: await message.answer("Введи числовой ID:",parse_mode="HTML"); return
    data=await state.get_data(); mode=data.get("admin_mode","info"); await state.clear()
    tmp_bot=Bot(token=BOT_TOKEN)
    try:
        if mode=="info":
            async with pg().acquire() as c:
                user   = await c.fetchrow("SELECT * FROM users WHERE uid=$1", uid)
                deploy = await c.fetchrow("SELECT * FROM active_deploys WHERE uid=$1", uid)
                inv    = await c.fetchrow("SELECT count(*),COALESCE(SUM(amount_rub),0) FROM invoices WHERE uid=$1 AND status='paid'", uid)
            stats   = await db_get_stats(uid)
            balance = await db_get_balance(uid)
            custom  = await db_get_custom_api(uid)
            lines=[f"Юзер {uid}\n"]
            if user: lines+=[f"Username: @{user['username'] or '-'}",f"Имя: {user['first_name'] or '-'}",f"Дата: {user['joined_at']}"]
            lines+=[
                f"\nЗапросов: {stats['total_reqs']} | Найдено: {stats['found_reqs']} | Пустых: {stats['empty_reqs']}",
                f"Баланс токенов: {balance:,}",
                f"Рефералов: {await db_get_ref_count(uid)} | Реф.баланс: {await db_get_ref_balance(uid):.2f}₽",
                f"Оплат: {inv[0]} на {float(inv[1]):.2f}₽",
            ]
            if deploy: lines.append(f"\nДеплой: {deploy['deploy_type']} expires={deploy['expires_at']}")
            if custom: lines.append(f"Custom API: {custom['provider']} model={custom['model']}")
            await tmp_bot.send_document(chat_id=message.chat.id,
                document=BufferedInputFile("\n".join(lines).encode("utf-8"),filename=f"user_{uid}_info.txt"),
                caption=f"Инфо юзера {uid}")
        elif mode=="log":
            actions=await db_get_user_actions(uid)
            if not actions:
                await message.answer(f"Нет логов для юзера {uid}",parse_mode="HTML")
            else:
                lines=[f"=== Лог юзера {uid} ===\n"]
                for a in actions: lines.append(f"[{a['created_at']}] {a['action']} | {a['detail']}")
                await tmp_bot.send_document(chat_id=message.chat.id,
                    document=BufferedInputFile("\n".join(lines).encode("utf-8"),filename=f"user_{uid}_log.txt"),
                    caption=f"Лог юзера {uid} ({len(actions)} записей)")
    finally: await tmp_bot.session.close()
    await tg_send(message.chat.id,"🛡 Админ панель",kb_admin())

@router.callback_query(F.data=="admin:no_data")
async def admin_no_data(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Формирую...")
    rows=await db_failed_requests()
    if not rows: await cb.answer("Нет пустых запросов",show_alert=True); return
    lines=["=== Запросы без данных ===\n"]
    for r in rows: lines.append(f"[{r['created_at']}] uid={r['uid']} | {r['detail']}")
    tmp_bot=Bot(token=BOT_TOKEN)
    try:
        await tmp_bot.send_document(chat_id=cb.message.chat.id,
            document=BufferedInputFile("\n".join(lines).encode("utf-8"),filename="no_data_requests.txt"),
            caption=f"Запросы без данных: {len(rows)} шт.")
    finally: await tmp_bot.session.close()

@router.callback_query(F.data=="admin:api_tokens")
async def admin_api_tokens(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Формирую...")
    rows=await db_all_custom_apis()
    if not rows: await cb.answer("Нет подключённых API",show_alert=True); return
    lines=["=== Custom API юзеров ===\n"]
    for r in rows: lines.append(f"uid={r['uid']} | {r['provider']} | model={r['model']} | key={r['api_key']}")
    tmp_bot=Bot(token=BOT_TOKEN)
    try:
        await tmp_bot.send_document(chat_id=cb.message.chat.id,
            document=BufferedInputFile("\n".join(lines).encode("utf-8"),filename="custom_api_tokens.txt"),
            caption=f"Custom API: {len(rows)} шт.")
    finally: await tmp_bot.session.close()

@router.callback_query(F.data=="admin:bot_tokens")
async def admin_bot_tokens(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    used=await db_used_slots()
    lines=["=== Бот токены (preview pool) ===\n"]
    for i,t in enumerate(PREVIEW_TOKENS):
        lines.append(f"[{i}] {'ЗАНЯТ' if i in used else 'свободен'} | {t[:20]}...")
    tmp_bot=Bot(token=BOT_TOKEN)
    try:
        await tmp_bot.send_document(chat_id=cb.message.chat.id,
            document=BufferedInputFile("\n".join(lines).encode("utf-8"),filename="bot_tokens.txt"),
            caption="Бот токены пула")
    finally: await tmp_bot.session.close()

@router.callback_query(F.data=="admin:deploys")
async def admin_deploys(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    deploys=await db_all_active()
    if not deploys: await cb.answer("Нет активных деплоев",show_alert=True); return
    lines=["=== Активные деплои ===\n"]
    for d in deploys:
        rem=max(0,d["expires_at"]-int(time.time()))
        lines.append(f"uid={d['uid']} type={d['deploy_type']} remains={rem//60}м")
    tmp_bot=Bot(token=BOT_TOKEN)
    try:
        await tmp_bot.send_document(chat_id=cb.message.chat.id,
            document=BufferedInputFile("\n".join(lines).encode("utf-8"),filename="active_deploys.txt"),
            caption=f"Активных деплоев: {len(deploys)}")
    finally: await tmp_bot.session.close()

@router.callback_query(F.data=="admin:ch_size")
async def admin_ch_size(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    try:
        ch=get_ch()
        rows=ch.query("SELECT database,formatReadableSize(sum(bytes)) AS size,count() AS tables FROM system.parts WHERE active GROUP BY database ORDER BY sum(bytes) DESC").result_rows
        lines=["=== ClickHouse ===\n"]
        for r in rows: lines.append(f"DB: {r[0]} | {r[1]} | таблиц: {r[2]}")
        text="\n".join(lines)
    except Exception as e: text=f"Ошибка: {e}"
    await tg_edit(cb.message.chat.id,cb.message.message_id,f"<pre>{text}</pre>",
                  markup([btn("Назад к панели",cb="holo:home")]))

@router.callback_query(F.data=="admin:server_load")
async def admin_server_load(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    try:
        import psutil
        cpu=psutil.cpu_percent(interval=1); ram=psutil.virtual_memory(); disk=psutil.disk_usage("/")
        text=(f"🖥 <b>Нагрузка сервера</b>\n\n"
              f"CPU:  <b>{cpu}%</b>\n"
              f"RAM:  <b>{ram.percent}%</b> ({ram.used//1024**2} / {ram.total//1024**2} МБ)\n"
              f"Disk: <b>{disk.percent}%</b> ({disk.used//1024**3} / {disk.total//1024**3} ГБ)")
    except ImportError: text="psutil не установлен (pip install psutil)"
    await tg_edit(cb.message.chat.id,cb.message.message_id,text,
                  markup([btn("Обновить",cb="admin:server_load")],[btn("Назад",cb="holo:home")]))

@router.callback_query(F.data=="admin:top_refs")
async def admin_top_refs(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    rows=await db_top_refs(10); lines=["🏆 <b>Топ рефоводы:</b>\n"]
    for i,r in enumerate(rows,1):
        user=await pg().fetchrow("SELECT username FROM users WHERE uid=$1", r["uid"])
        un=f"@{user['username']}" if (user and user['username']) else str(r["uid"])
        lines.append(f"{i}. {un} - {r['cnt']} рефералов")
    await tg_edit(cb.message.chat.id,cb.message.message_id,"\n".join(lines),markup([btn("Назад",cb="holo:home")]))

@router.callback_query(F.data=="admin:noop2")
async def admin_noop2(cb:CallbackQuery): await cb.answer()

@router.callback_query(F.data=="admin:analytics_summary")
async def admin_analytics_summary(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Собираю данные...")
    import datetime
    today = await analytics_today()
    week  = await analytics_week()
    month = await analytics_month()
    sr    = await analytics_success_rate()
    text = (
        f"📊 <b>Сводка аналитики</b>\n\n"
        f"<b>Сегодня ({datetime.datetime.now().strftime('%d.%m.%Y')}):</b>\n"
        f"👥 Новых юзеров: <b>{today['new_users']}</b>\n"
        f"💬 Запросов: <b>{today['requests']}</b>\n"
        f"💰 Выручка: <b>{today['revenue']:.2f}₽</b>\n"
        f"💳 Платежей: <b>{today['payments']}</b>\n"
        f"❌ Пустых ответов: <b>{today['errors']}</b>\n"
        f"🚀 Деплоев: <b>{today['deploys']}</b>\n\n"
        f"<b>Неделя:</b>\n"
        f"👥 Новых: <b>{week['new_users']}</b>\n"
        f"💬 Запросов: <b>{week['requests']}</b>\n"
        f"💰 Выручка: <b>{week['revenue']:.2f}₽</b>\n"
        f"💳 Платящих: <b>{week['new_payers']}</b>\n\n"
        f"<b>Месяц / всего:</b>\n"
        f"👥 Всего юзеров: <b>{month['total_users']}</b>\n"
        f"💰 Выручка 30д: <b>{month['revenue']:.2f}₽</b>\n"
        f"💳 Ср. чек: <b>{month['avg_check']:.2f}₽</b>\n"
        f"📈 ARPU: <b>{month['arpu']:.2f}₽</b>\n"
        f"⭐ Рефералов: <b>{month['total_refs']}</b>\n"
        f"✅ Success rate: <b>{sr}%</b>"
    )
    await tg_edit(cb.message.chat.id, cb.message.message_id, text,
        markup([btn("Обновить", cb="admin:analytics_summary"),
                btn("Назад", cb="holo:home")]))

@router.callback_query(F.data=="admin:analytics_ltv")
async def admin_analytics_ltv(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Формирую...")
    rows = await analytics_ltv(15)
    lines = ["💰 Топ плательщики (LTV)\n"]
    for i, r in enumerate(rows, 1):
        un = f"@{r['username']}" if r['username'] else f"id{r['uid']}"
        lines.append(f"{i}. {un} - {float(r['total_spent']):.0f}₽ ({r['payments_count']} плат., ср. {float(r['avg_check']):.0f}₽)")
    await _send_analytics_file(cb.message.chat.id, "\n".join(lines), "ltv.txt", f"LTV топ {len(rows)} юзеров")

@router.callback_query(F.data=="admin:analytics_activity")
async def admin_analytics_activity(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Формирую...")
    rows = await analytics_daily_activity(14)
    lines = ["📈 Активность по дням (14 дней)\n"]
    lines.append(f"{'Дата':<12} {'Запросов':>10} {'Уников':>8} {'Ошибок':>8}")
    lines.append("-" * 42)
    for r in rows:
        lines.append(f"{str(r['day']):<12} {r['total']:>10} {r['uniq_users']:>8} {r['failed']:>8}")
    await _send_analytics_file(cb.message.chat.id, "\n".join(lines), "activity.txt", "Активность по дням")

@router.callback_query(F.data=="admin:analytics_plans")
async def admin_analytics_plans(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Формирую...")
    rows = await analytics_revenue_by_plan()
    lines = ["🎯 Популярность тарифов\n"]
    for r in rows:
        tokens = r['tokens']
        label = next((p[3] for p in PLANS if p[0]==tokens), str(tokens))
        lines.append(f"{label}: {r['purchases']} покупок - {float(r['total_rub']):.0f}₽ (ср. {float(r['avg_rub']):.0f}₽)")
    text = "\n".join(lines)
    await tg_edit(cb.message.chat.id, cb.message.message_id, f"<pre>{text[:3500]}</pre>",
        markup([btn("Назад", cb="holo:home")]))

@router.callback_query(F.data=="admin:analytics_refs")
async def admin_analytics_refs(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Формирую...")
    rows = await analytics_ref_quality(15)
    lines = ["🔗 Качество рефералов\n"]
    for i, r in enumerate(rows, 1):
        un = f"@{r['username']}" if r['username'] else f"id{r['referrer_id']}"
        lines.append(f"{i}. {un} - {r['referrals']} рефов, заработано {float(r['earned']):.2f}₽")
    await _send_analytics_file(cb.message.chat.id, "\n".join(lines), "refs.txt", f"Реферальная аналитика ({len(rows)} рефоводов)")

@router.callback_query(F.data=="admin:analytics_providers")
async def admin_analytics_providers(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    rows = await analytics_provider_stats()
    lines = ["⚡ Custom AI провайдеры\n"]
    for r in rows:
        lines.append(f"{r['provider']}: {r['users']} юзеров")
    text = "\n".join(lines) or "Нет данных"
    await tg_edit(cb.message.chat.id, cb.message.message_id, f"<pre>{text}</pre>",
        markup([btn("Назад", cb="holo:home")]))

@router.callback_query(F.data=="admin:analytics_payments")
async def admin_analytics_payments(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    rows = await analytics_payment_provider_split()
    lines = ["💳 Платежи по каналам\n"]
    for r in rows:
        lines.append(f"{r['provider']}: {r['cnt']} платежей - {float(r['total']):.0f}₽")
    text = "\n".join(lines) or "Нет данных"
    await tg_edit(cb.message.chat.id, cb.message.message_id, f"<pre>{text}</pre>",
        markup([btn("Назад", cb="holo:home")]))

@router.callback_query(F.data=="admin:analytics_errors")
async def admin_analytics_errors(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Формирую...")
    rows = await analytics_error_patterns(7, 15)
    if not rows:
        await cb.answer("Ошибок нет за 7 дней", show_alert=True); return
    lines = ["❌ Частые пустые ответы (7 дней)\n"]
    for r in rows:
        lines.append(f"x{r['cnt']:>4}  {r['pattern']}")
    await _send_analytics_file(cb.message.chat.id, "\n".join(lines), "errors.txt", f"Паттерны ошибок ({len(rows)} типов)")

@router.callback_query(F.data=="admin:analytics_deploys")
async def admin_analytics_deploys_cb(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    rows = await analytics_deploy_stats()
    lines = ["🚀 Деплои по типам (7 дней)\n"]
    for r in rows:
        lines.append(f"{r['deploy_type']}: {r['deploys']} деплоев")
    text = "\n".join(lines) or "Нет данных"
    await tg_edit(cb.message.chat.id, cb.message.message_id, f"<pre>{text}</pre>",
        markup([btn("Назад", cb="holo:home")]))

@router.callback_query(F.data=="admin:analytics_sr")
async def admin_analytics_sr(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    sr = await analytics_success_rate()
    emoji = "✅" if sr >= 70 else ("⚠️" if sr >= 50 else "❌")
    text = (
        f"{emoji} <b>Success rate RAG</b>\n\n"
        f"Доля запросов где найдены данные в базе:\n"
        f"<b>{sr}%</b>\n\n"
        f"{'Отлично' if sr>=70 else ('Норм' if sr>=50 else 'Плохо - пополни базу знаний!')}"
    )
    await tg_edit(cb.message.chat.id, cb.message.message_id, text,
        markup([btn("Обновить", cb="admin:analytics_sr"), btn("Назад", cb="holo:home")]))

@router.callback_query(F.data=="admin:analytics_report")
async def admin_analytics_report(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer("Отправляю отчёт...")
    await _daily_report()

@router.callback_query(F.data=="admin:top_users")
async def admin_top_users_cb(cb:CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    rows=await db_top_users(10); lines=["👑 <b>Топ активных юзеров:</b>\n"]
    for i,r in enumerate(rows,1):
        user=await pg().fetchrow("SELECT username FROM users WHERE uid=$1", r["uid"])
        un=f"@{user['username']}" if (user and user['username']) else str(r["uid"])
        lines.append(f"{i}. {un} - {r['cnt']} действий")
    await tg_edit(cb.message.chat.id,cb.message.message_id,"\n".join(lines),markup([btn("Назад",cb="holo:home")]))

@router.callback_query(F.data=="holo:telegraph_publish_confirm")
async def cb_telegraph_confirm(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    uid  = cb.from_user.id
    data = await state.get_data()
    title   = data.get("tg_title", "Статья")
    content = data.get("tg_content", "")
    await state.clear()
    wait = await tg_send(uid, f"{WAIT_EMOJI} Публикую...")
    wait_id = wait["message_id"] if wait else None
    result = await telegraph_publish(title, content)
    if result:
        await db_log_action(uid, "telegraph_publish", result["url"])
        if wait_id:
            await tg_edit(uid, wait_id,
                f"✅ <b>Статья опубликована!</b>\n\n📝 <b>{result['title']}</b>\n\n🔗 {result['url']}",
                markup(
                    [btn("📖 Открыть статью", url=result["url"], style="primary")],
                    [btn("Главное меню", cb="holo:home")],
                ))
    else:
        if wait_id:
            await tg_edit(uid, wait_id, "❌ Ошибка публикации. Проверь TELEGRAPH_TOKEN в .env", kb_back())

@router.callback_query(F.data=="holo:telegraph_cancel")
async def cb_telegraph_cancel(cb: CallbackQuery, state: FSMContext):
    await cb.answer(); await state.clear()
    await tg_edit(cb.message.chat.id, cb.message.message_id, "Публикация отменена.", kb_back())

@router.message(S.telegraph_confirm)
async def fsm_telegraph_confirm(message: Message, state: FSMContext):
    uid = message.from_user.id
    text = (message.text or "").strip().lower()
    data = await state.get_data()
    title   = data.get("tg_title", "Статья")
    content = data.get("tg_content", "")
    if not content:
        await state.clear()
        await message.answer("Нет содержимого. Попробуй снова.", parse_mode="HTML")
        return
    if text in ("да", "yes", "y", "опубликовать", "publish", "ок", "ok", "+"):
        await state.clear()
        wait = await tg_send(uid, f"{WAIT_EMOJI} Публикую статью...")
        wait_id = wait["message_id"] if wait else None
        result = await telegraph_publish(title, content)
        if result:
            await db_log_action(uid, "telegraph_publish", result["url"])
            kb = markup(
                [btn("📖 Открыть статью", url=result["url"], style="primary")],
                [btn("Главное меню", cb="holo:home")],
            )
            if wait_id:
                await tg_edit(uid, wait_id,
                    f"✅ <b>Статья опубликована!</b>\n\n"
                    f"📝 <b>{result['title']}</b>\n\n"
                    f"🔗 {result['url']}", kb)
        else:
            if wait_id:
                await tg_edit(uid, wait_id,
                    "❌ Ошибка публикации. Проверь TELEGRAPH_TOKEN в .env", kb_back())
    elif text in ("нет", "no", "n", "отмена", "cancel", "-"):
        await state.clear()
        await message.answer("Публикация отменена.", parse_mode="HTML",
            reply_markup={"inline_keyboard": [[btn("Главное меню", cb="holo:home")]]})
    else:
        await message.answer(
            "Ответь <b>да</b> для публикации или <b>нет</b> для отмены.",
            parse_mode="HTML")

@router.message(Command("ban"))
async def cmd_ban(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /ban <user_id> [причина]", parse_mode="HTML"); return
    ok, uid = validate_uid_param(parts[1])
    if not ok:
        await message.answer("Некорректный ID пользователя", parse_mode="HTML"); return
    reason = " ".join(parts[2:]) if len(parts) > 2 else "admin ban"
    await ban_user(uid, reason)
    await db_log_action(message.from_user.id, "admin_ban", f"uid={uid} reason={reason}")
    await message.answer(f"✅ Пользователь <b>{uid}</b> заблокирован: {reason}", parse_mode="HTML")

@router.message(Command("unban"))
async def cmd_unban(message: Message):
    if message.from_user.id not in ADMIN_IDS: return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /unban <user_id>", parse_mode="HTML"); return
    ok, uid = validate_uid_param(parts[1])
    if not ok:
        await message.answer("Некорректный ID пользователя", parse_mode="HTML"); return
    await unban_user(uid)
    await db_log_action(message.from_user.id, "admin_unban", f"uid={uid}")
    await message.answer(f"✅ Пользователь <b>{uid}</b> разблокирован", parse_mode="HTML")

@router.callback_query(F.data == "holo:promos")
async def cb_promos(cb: CallbackQuery):
    await cb.answer()
    uid = cb.from_user.id
    promos = await db_get_active_promotions()

    if promos:
        lines = [f'{E2("5199749070830197566","🏷")} <b>Акции</b>\n']
        for p in promos:
            plan = PLAN_BY_TOKENS.get(p["tokens"])
            if not plan:
                continue
            tokens_str, price_rub, price_usd, label = plan
            disc = p["discount_pct"]
            new_rub = round(price_rub * (1 - disc / 100))
            new_usd = round(price_usd * (1 - disc / 100), 2)
            lines.append(
                f'<tg-emoji emoji-id="5199749070830197566">🏷</tg-emoji>' +
                f'<b>-{disc}%</b>  {label}  — ' +
                f'<s>{price_rub}₽</s> <b>{new_rub}₽</b> / <b>${new_usd}</b>'
            )
        text = "\n".join(lines)
    else:
        text = (
            f'{E2("5199749070830197566","🏷")} <b>Акции</b>\n\n'
            '<i>Сейчас активных акций нет.\n'
            'Но вы можете ввести промокод для получения бонусов!</i>'
        )

    await tg_edit(cb.message.chat.id, cb.message.message_id, text, kb_promos(has_active=bool(promos)))

@router.callback_query(F.data == "holo:enter_promo")
async def cb_enter_promo(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.set_state(S.enter_promo)
    await tg_edit(
        cb.message.chat.id, cb.message.message_id,
        f'{E2("5199749070830197566","🎟")} <b>Промокод</b>\n\nВведите ваш промокод:',
        markup([btn("Отмена", cb="holo:promos")])
    )

@router.message(S.enter_promo)
async def fsm_enter_promo(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    code = (message.text or "").strip()

    promo = await db_get_promo_code(code)
    if not promo:
        await tg_send(
            uid,
            '❌ Промокод не найден или уже неактивен.',
            markup([btn("Попробовать снова", cb="holo:enter_promo"),
                    btn("Назад", cb="holo:promos")])
        )
        return

    ok, result_msg = await db_activate_promo_code(uid, promo)
    kb = markup([btn("Главное меню", cb="holo:home")])
    await tg_send(uid, result_msg, kb)

@router.callback_query(F.data == "admin:promos")
async def admin_promos_list(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    promos = await db_get_all_promotions()
    lines = ["🏷 <b>Акции по тарифам</b>\n"]
    plan_rows = []
    for plan in PLANS:
        tokens, price_rub, price_usd, label = plan
        found = next((p for p in promos if p["tokens"] == tokens), None)
        if found:
            status = "✅" if found["enabled"] else "⏸"
            disc = found["discount_pct"]
            new_rub = round(price_rub * (1 - disc / 100))
            row_text = f'{status} -{disc}% {label} → {new_rub}₽'
            plan_rows.append([btn(row_text, cb=f"admin:promo_toggle:{tokens}")])
        else:
            plan_rows.append([btn(f"➕ {label} — {price_rub}₽", cb=f"admin:promo_create_for:{tokens}")])
    plan_rows.append([btn("Назад в админку", cb="holo:admin")])
    await tg_edit(cb.message.chat.id, cb.message.message_id,
                  "\n".join(lines), {"inline_keyboard": plan_rows})

@router.callback_query(F.data == "holo:admin")
async def cb_holo_admin(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    await tg_edit(cb.message.chat.id, cb.message.message_id, "🛡 <b>Админ панель</b>", kb_admin())

@router.callback_query(F.data.startswith("admin:promo_toggle:"))
async def admin_promo_toggle(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    tokens = int(cb.data.split(":")[2])
    plan = PLAN_BY_TOKENS.get(tokens)
    if not plan: return
    promos = await db_get_all_promotions()
    found = next((p for p in promos if p["tokens"] == tokens), None)
    if not found: return
    label = plan[3]
    disc = found["discount_pct"]
    status = "✅ включена" if found["enabled"] else "⏸ выключена"
    new_rub = round(plan[1] * (1 - disc / 100))
    await tg_edit(
        cb.message.chat.id, cb.message.message_id,
        f'🏷 <b>Акция: {label}</b>\n\n'
        f'Скидка: <b>-{disc}%</b>  →  <b>{new_rub}₽</b>\n'
        f'Статус: {status}',
        markup(
            [btn("✏️ Изменить скидку", cb=f"admin:promo_edit:{tokens}")],
            [btn("🔁 Вкл/Выкл",        cb=f"admin:promo_onoff:{tokens}")],
            [btn("🗑 Удалить акцию",    cb=f"admin:promo_del_act:{tokens}")],
            [btn("Назад",              cb="admin:promos")],
        )
    )

@router.callback_query(F.data.startswith("admin:promo_onoff:"))
async def admin_promo_onoff(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    tokens = int(cb.data.split(":")[2])
    new_state = await db_toggle_promotion(tokens)
    state_text = "включена ✅" if new_state else "выключена ⏸"
    await cb.answer(f"Акция {state_text}", show_alert=True)
    await admin_promos_list(cb)

@router.callback_query(F.data.startswith("admin:promo_del_act:"))
async def admin_promo_del_act(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    tokens = int(cb.data.split(":")[2])
    await db_delete_promotion(tokens)
    await cb.answer("Акция удалена", show_alert=True)
    await admin_promos_list(cb)

@router.callback_query(F.data.startswith("admin:promo_edit:"))
async def admin_promo_edit(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    tokens = int(cb.data.split(":")[2])
    plan = PLAN_BY_TOKENS.get(tokens)
    if not plan: return await cb.answer("Тариф не найден")
    await cb.answer()
    await state.update_data(promo_tokens=tokens)
    await state.set_state(S.admin_promo_set_discount)
    await tg_edit(
        cb.message.chat.id, cb.message.message_id,
        f'🏷 <b>Изменить скидку: {plan[3]}</b>\n\nВведите новый % скидки (1–99):',
        markup([btn("Отмена", cb="admin:promos")])
    )

@router.callback_query(F.data.startswith("admin:promo_create_for:"))
async def admin_promo_create_for(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    tokens = int(cb.data.split(":")[2])
    plan = PLAN_BY_TOKENS.get(tokens)
    if not plan: return await cb.answer("Тариф не найден")
    await cb.answer()
    await state.update_data(promo_tokens=tokens)
    await state.set_state(S.admin_promo_set_discount)
    label = plan[3]
    await tg_edit(
        cb.message.chat.id, cb.message.message_id,
        f'🏷 <b>Создать акцию для {label}</b>\n\nВведите скидку в % (например: <code>20</code>):',
        markup([btn("Отмена", cb="admin:promos")])
    )

@router.message(S.admin_promo_set_discount)
async def fsm_admin_promo_discount(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    try:
        disc = int((message.text or "").strip())
        if not (1 <= disc <= 99): raise ValueError
    except ValueError:
        await message.answer("Введите число от 1 до 99."); return
    data = await state.get_data()
    tokens = data["promo_tokens"]
    await state.clear()
    await db_set_promotion(tokens, disc, 1)
    plan = PLAN_BY_TOKENS.get(tokens)
    label = plan[3] if plan else str(tokens)
    await tg_send(message.from_user.id,
        f'✅ Акция <b>-{disc}%</b> на тариф <b>{label}</b> создана и включена!',
        markup([btn("К акциям", cb="admin:promos"), btn("Главное меню", cb="holo:home")]))

@router.callback_query(F.data == "admin:promo_codes")
async def admin_promo_codes_list(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    codes = await db_get_all_promo_codes()
    if not codes:
        return await tg_edit(cb.message.chat.id, cb.message.message_id,
            "🎟 <b>Промокоды</b>\n\n<i>Промокодов нет.</i>",
            markup([btn("➕ Создать промокод", cb="admin:promo_create")],
                   [btn("Назад", cb="holo:admin")]))
    lines = ["🎟 <b>Промокоды</b>\n"]
    rows = []
    for c in codes:
        status = "✅" if c["enabled"] else "❌"
        acts = f'{c["activations"]}/{c["max_activations"]}'
        if c["type"] == "tokens":
            desc = f'+{c["tokens_amount"]:,} токенов'
        else:
            tf = c["tokens_filter"]
            scope = f' ({PLAN_BY_TOKENS[tf][3]})' if tf and tf in PLAN_BY_TOKENS else ""
            desc = f'-{c["discount_pct"]}%{scope}'
        lines.append(f'{status} <code>{c["code"]}</code> — {desc} [{acts}]')
        rows.append([
            btn(f'🗑 {c["code"]}', cb=f'admin:promo_del:{c["id"]}'),
            btn("Выкл" if c["enabled"] else "Вкл", cb=f'admin:promo_toggle_code:{c["id"]}')
        ])
    rows.append([btn("➕ Создать промокод", cb="admin:promo_create")])
    rows.append([btn("Назад", cb="holo:admin")])
    await tg_edit(cb.message.chat.id, cb.message.message_id,
                  "\n".join(lines), {"inline_keyboard": rows})

@router.callback_query(F.data.startswith("admin:promo_del:"))
async def admin_promo_del(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    code_id = int(cb.data.split(":")[2])
    await db_delete_promo_code(code_id)
    await cb.answer("Промокод удалён", show_alert=True)
    await admin_promo_codes_list(cb)

@router.callback_query(F.data.startswith("admin:promo_toggle_code:"))
async def admin_promo_toggle_code(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    code_id = int(cb.data.split(":")[2])
    codes = await db_get_all_promo_codes()
    found = next((c for c in codes if c["id"] == code_id), None)
    if not found: return await cb.answer("Не найден")
    if found["enabled"]:
        await db_disable_promo_code(code_id)
        await cb.answer("Промокод выключен", show_alert=True)
    else:
        await db_enable_promo_code(code_id)
        await cb.answer("Промокод включён", show_alert=True)
    await admin_promo_codes_list(cb)

@router.callback_query(F.data == "admin:noop3")
async def admin_noop3(cb: CallbackQuery): await cb.answer()

@router.callback_query(F.data == "admin:promo_create")
async def admin_promo_create_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    await cb.answer()
    await state.set_state(S.admin_promo_code_name)
    await tg_edit(
        cb.message.chat.id, cb.message.message_id,
        '🎟 <b>Создать промокод</b>\n\n'
        'Шаг 1/4: Введите название промокода (латиница/цифры, без пробелов):\n\n'
        '<i>Например: SUMMER25</i>',
        markup([btn("Отмена", cb="admin:promo_codes")])
    )

@router.message(S.admin_promo_code_name)
async def fsm_promo_code_name(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    code = (message.text or "").strip().upper()
    if not re.match(r'^[A-Z0-9_\-]{2,30}$', code):
        await message.answer("Только латиница, цифры, _ и - (2-30 символов). Попробуйте снова:")
        return
    existing = await db_get_promo_code(code)
    if existing:
        await message.answer("Такой промокод уже существует. Введите другое название:")
        return
    await state.update_data(new_code=code)
    await state.set_state(S.admin_promo_code_type)
    await tg_send(message.from_user.id,
        f'✅ Промокод: <code>{code}</code>\n\nШаг 2/4: Тип промокода:',
        markup([btn("🪙 Бесплатные токены", cb="admin:promo_type:tokens")],
               [btn("💸 Скидка на тариф",   cb="admin:promo_type:discount")],
               [btn("Отмена", cb="admin:promo_codes")]))

@router.callback_query(F.data.startswith("admin:promo_type:"))
async def admin_promo_type_cb(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    promo_type = cb.data.split(":")[2]
    await state.update_data(new_type=promo_type)
    await cb.answer()
    if promo_type == "tokens":
        await state.set_state(S.admin_promo_code_value)
        await tg_edit(cb.message.chat.id, cb.message.message_id,
            'Шаг 3/4: Сколько токенов зачислять?\n\n<i>Например: 50000</i>',
            markup([btn("Отмена", cb="admin:promo_codes")]))
    else:
        await state.set_state(S.admin_promo_code_filter)
        plan_rows = [[btn("🌐 На все тарифы", cb="admin:promo_filter:0")]]
        for tokens, price_rub, price_usd, label in PLANS:
            plan_rows.append([btn(f"{label} — {price_rub}₽", cb=f"admin:promo_filter:{tokens}")])
        plan_rows.append([btn("Отмена", cb="admin:promo_codes")])
        await tg_edit(cb.message.chat.id, cb.message.message_id,
            'Шаг 3а/4: На какой тариф действует скидка?',
            {"inline_keyboard": plan_rows})

@router.callback_query(F.data.startswith("admin:promo_filter:"))
async def admin_promo_filter_cb(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS: return await cb.answer()
    tokens_filter = int(cb.data.split(":")[2])
    await state.update_data(new_filter=tokens_filter)
    await state.set_state(S.admin_promo_code_value)
    await cb.answer()
    await tg_edit(cb.message.chat.id, cb.message.message_id,
        'Шаг 3б/4: Скидка в % (например: <code>15</code>):',
        markup([btn("Отмена", cb="admin:promo_codes")]))

@router.message(S.admin_promo_code_value)
async def fsm_promo_code_value(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    data = await state.get_data()
    promo_type = data.get("new_type", "tokens")
    try:
        val = int((message.text or "").strip())
        if val <= 0: raise ValueError
        if promo_type == "discount" and val > 99: raise ValueError
    except ValueError:
        hint = "от 1 до 99" if promo_type == "discount" else "больше 0"
        await message.answer(f"Введите число ({hint}):"); return
    await state.update_data(new_value=val)
    await state.set_state(S.admin_promo_code_limit)
    await tg_send(message.from_user.id,
        'Шаг 4/4: Максимальное количество активаций:\n\n<i>Например: 100</i>',
        markup([btn("Отмена", cb="admin:promo_codes")]))

@router.message(S.admin_promo_code_limit)
async def fsm_promo_code_limit(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    try:
        limit = int((message.text or "").strip())
        if limit <= 0: raise ValueError
    except ValueError:
        await message.answer("Введите число больше 0:"); return
    data = await state.get_data()
    code          = data["new_code"]
    promo_type    = data["new_type"]
    val           = data["new_value"]
    tokens_filter = data.get("new_filter", 0)
    await state.clear()
    tokens_amount = val if promo_type == "tokens" else 0
    discount_pct  = val if promo_type == "discount" else 0
    await db_create_promo_code(
        code=code, type_=promo_type,
        tokens_amount=tokens_amount, discount_pct=discount_pct,
        tokens_filter=tokens_filter, max_activations=limit)
    if promo_type == "tokens":
        summary = f"+{tokens_amount:,} токенов"
    else:
        tf_label = PLAN_BY_TOKENS[tokens_filter][3] if tokens_filter and tokens_filter in PLAN_BY_TOKENS else "все тарифы"
        summary = f"-{discount_pct}% на {tf_label}"
    await tg_send(message.from_user.id,
        f'✅ <b>Промокод создан!</b>\n\n'
        f'Код: <code>{code}</code>\n'
        f'Тип: {summary}\n'
        f'Активаций: 0 / {limit}',
        markup([btn("📋 Все промокоды", cb="admin:promo_codes")],
               [btn("Главное меню", cb="holo:home")]))

@router.message()
async def fallback(message:Message,state:FSMContext):
    uid=message.from_user.id
    if message.chat.type != "private":
        return
    if await is_banned(uid):
        await message.answer("⛔ Доступ ограничен.")
        return
    if not await check_global_flood(uid):
        return
    await db_register(uid,message.from_user.username or "",message.from_user.first_name or "")
    await db_log_action(uid, "message", (message.text or "")[:200])
    await state.set_state(S.chat); await fsm_chat(message,state)

async def _scan_repo_bg(chat_id,uid,owner,repo):
    info=await github_get_repo(owner,repo)
    if not info: return
    ch_insert_repo(info["name"],info["description"],info["url"],info["readme"])
    await tg_send(uid,f'<b>GitHub</b> - Репо <a href="{info["url"]}">{info["name"]}</a> добавлен в базу')
    uid=message.from_user.id
    await db_register(uid,message.from_user.username or "",message.from_user.first_name or "")
    await db_log_action(uid, "message", (message.text or "")[:200])
    await state.set_state(S.chat); await fsm_chat(message,state)