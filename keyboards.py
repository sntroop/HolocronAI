from config import *
from database import db_get_custom_api, db_get_ref_balance
from services import PROVIDERS, PROVIDER_ORDER

def btn(text, cb=None, url=None, eid=None, style=None, web_app=None):
    b = {"text": text}
    if cb:      b["callback_data"] = cb
    if url:     b["url"] = url
    if web_app: b["web_app"] = {"url": web_app}
    if eid:     b["icon_custom_emoji_id"] = eid
    if style:   b["style"] = style
    return b

def markup(*rows): return {"inline_keyboard": list(rows)}

async def tg_send(chat_id, text, kb=None, reply_to=None, preview=False):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": not preview}
    if kb:       payload["reply_markup"] = kb
    if reply_to: payload["reply_to_message_id"] = reply_to
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload) as r:
                data = await r.json()
                if not data.get("ok"): log.warning("tg_send err: %s len=%d", data.get("description","?"), len(text))
                return data.get("result")
    except Exception as e:
        log.error("tg_send: %s", e); return None

async def tg_send_banner(chat_id, caption, kb=None):
    if not BANNER_PATH.exists(): return await tg_send(chat_id, caption, kb)
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("caption", caption)
        form.add_field("parse_mode", "HTML")
        form.add_field("photo", open(BANNER_PATH, "rb"), filename="star.png", content_type="image/png")
        if kb: form.add_field("reply_markup", json.dumps(kb))
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", data=form) as r:
                data = await r.json()
                if not data.get("ok"): log.warning("tg_send_banner err: %s", data.get("description","?"))
                return data.get("result")
    except Exception as e:
        log.error("tg_send_banner: %s", e); return await tg_send(chat_id, caption, kb)

async def tg_edit_banner(chat_id, msg_id, caption, kb=None):
    if not BANNER_PATH.exists(): return await tg_edit(chat_id, msg_id, caption, kb)
    try:
        media = {"type": "photo", "media": "attach://star.png", "caption": caption, "parse_mode": "HTML"}
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("message_id", str(msg_id))
        form.add_field("media", json.dumps(media))
        form.add_field("star.png", open(BANNER_PATH, "rb"), filename="star.png", content_type="image/png")
        if kb: form.add_field("reply_markup", json.dumps(kb))
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageMedia", data=form) as r:
                data = await r.json()
                if not data.get("ok"): log.warning("tg_edit_banner err: %s", data.get("description","?"))
                return data.get("ok", False)
    except Exception as e:
        log.error("tg_edit_banner: %s", e); return await tg_edit(chat_id, msg_id, caption, kb)

async def tg_edit(chat_id, msg_id, text, kb=None, preview=False):
    kb_payload = {"reply_markup": kb} if kb else {}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageCaption",
            json={"chat_id":chat_id,"message_id":msg_id,"caption":text,"parse_mode":"HTML",**kb_payload}) as r:
            if (await r.json()).get("ok"): return True
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json={"chat_id":chat_id,"message_id":msg_id,"text":text,"parse_mode":"HTML",
                  "disable_web_page_preview":True,**kb_payload}) as r:
            if (await r.json()).get("ok"): return True
        plain = re.sub(r'<[^>]+>', '', text)
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json={"chat_id":chat_id,"message_id":msg_id,"text":plain,
                  "disable_web_page_preview":True,**kb_payload}) as r:
            if (await r.json()).get("ok"): return True
        await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage", json={"chat_id":chat_id,"message_id":msg_id})
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id":chat_id,"text":plain,"disable_web_page_preview":True,**kb_payload}) as r:
            data = await r.json()
            return data.get("ok", False)

async def tg_send_poll(chat_id, question, options):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPoll",
                json={"chat_id":chat_id,"question":question,"options":[{"text":o} for o in options],
                      "is_anonymous":False,"type":"regular","allows_multiple_answers":False}) as r:
                return (await r.json()).get("result")
    except Exception as e:
        log.error("tg_send_poll: %s", e); return None

async def tg_stop_poll(chat_id, msg_id):
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/stopPoll", json={"chat_id":chat_id,"message_id":msg_id})
            await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage", json={"chat_id":chat_id,"message_id":msg_id})
    except: pass

async def tg_typing(bot, chat_id):
    try: await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    except: pass

def E2(eid, fb="⚡"): return f'<tg-emoji emoji-id="{eid}">{fb}</tg-emoji>'

WAIT_EMOJI = E2("5328089410963513796", "⚡")

def kb_home():
    return markup(
        [btn("Начать", cb="holo:chat", eid="5287454433318300292", style="primary")],
        
        [btn("Поддержка", url="https://t.me/sntroop", eid="5818727130325847330"),
         btn("Помощь", url=os.getenv("HELP_URL", "#"), eid="5445188473063480079")],
        [btn("Профиль", cb="holo:profile", eid="5373012449597335010")],
        
        [btn("Мой центр", cb="holo:my_center", eid="5361837567463399422")],
        [btn("Акции", cb="holo:promos", eid="5199749070830197566")],
        [btn("Подписка", cb="holo:subscription", eid="5375296873982604963"),
         btn("Реферальная система", cb="holo:referral", eid="5372926953978341366")],
    )

def kb_settings(s):
    def oo(v): return "ВКЛ" if v else "ВЫКЛ"

    local_ai_status = s.get('local_ai', 0)
    local_ai_provider = s.get('local_ai_provider', 'ollama')
    
    if local_ai_status:
        provider_label = "P7 (Ollama)" if local_ai_provider == "ollama" else "PP1 (Gemma)"
        local_ai_text = f"Local AI - {oo(1)} [{provider_label}]"
    else:
        local_ai_text = f"Local AI - {oo(0)}"
    
    return markup(
        [btn(f"URL-Buttons - {oo(s['url_buttons'])}", cb="holo:toggle:url_buttons", style="success" if s["url_buttons"] else None)],
        [btn(f"GitHub - {oo(s['github_scanner'])}", cb="holo:toggle:github_scanner", style="success" if s["github_scanner"] else None)],
        [btn(f"Code-Mode - {oo(s['code_mode'])}", cb="holo:toggle:code_mode", style="success" if s["code_mode"] else None)],
        [btn(f"Preview-Mode - {oo(s.get('preview_mode',0))}", cb="holo:toggle:preview_mode", style="success" if s.get("preview_mode") else None)],
        [btn(f"Interactive - {oo(s.get('interactive_mode',0))}", cb="holo:toggle:interactive_mode", style="success" if s.get("interactive_mode") else None)],
        [btn(local_ai_text, cb="holo:local_ai_menu", style="success" if s.get("local_ai") else None)],
        [btn(f"Context Inspector - {oo(s.get('context_inspector',0))}", cb="holo:toggle:context_inspector", style="success" if s.get("context_inspector") else None)],
        [btn(
            f"Token Limit - {'∞ без лимита' if not s.get('token_limit') else str(s.get('token_limit')) + ' токенов'}",
            cb="holo:set_token_limit"
        )],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    )

CONTEXT_MAX = 30  # это типо сколько максимум сообщений, если у вас лркальная ИИ хорошая ебашьте сколько хотите

def context_bar(msg_count: int) -> str:
    cells = 6
    filled_cells = round(min(msg_count, CONTEXT_MAX) / CONTEXT_MAX * cells)
    bar = "■" * filled_cells + "▢" * (cells - filled_cells)
    return f"\n\n<code>Context: {bar}</code>"

def context_bar_transferring(step: int) -> str:
    cells = 6
    filled = min(step, cells)
    bar = "▬ " + ("▭ " * (cells - 1))
    parts = []
    for i in range(cells):
        parts.append("▬" if i < filled else "▭")
    bar = " ".join(parts)
    return f"<code>{bar}  Transfer...</code>"

async def kb_profile(uid=None):
    has_custom = bool(await db_get_custom_api(uid)) if uid else False
    ref_bal    = await db_get_ref_balance(uid) if uid else 0.0
    rows = [
        [btn("Download History", cb="holo:export_history", eid="5431736674147114227")],
        [btn("Пополнить баланс", cb="holo:subscription",   eid="5375296873982604963", style="primary")],
    ]
    if ref_bal >= REF_MIN_PAYOUT:
        rows.append([btn(f"💸 Вывести реф. баланс ({ref_bal:.0f}₽)", cb="holo:ref_withdraw", style="primary")])
    if has_custom:
        rows.append([btn("Отключить Custom AI", cb="holo:capi_disconnect", style="danger")])
    rows += [
        [btn("Law Enforcement Unit", url=os.getenv("TERMS_URL", "#"), eid="5274099962655816924")],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    ]
    return {"inline_keyboard": rows}

def kb_custom_ai(has_active=False):
    rows = [[btn(PROVIDERS[pid]["name"], cb=f"holo:capi_select:{pid}")] for pid in PROVIDER_ORDER]
    if has_active: rows.append([btn("Отключить Custom AI", cb="holo:capi_disconnect", style="danger")])
    rows.append([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return {"inline_keyboard": rows}

def kb_custom_ai_back():
    return markup([btn("Назад", cb="holo:custom_ai", eid="5341715473882955310")])

def kb_back():
    return markup([btn("Назад", cb="holo:home", eid="5341715473882955310")])

def kb_promos(has_active: bool = False):
    rows = []
    if has_active:
        rows.append([btn("🎟 Ввести промокод", cb="holo:enter_promo", style="primary")])
    else:
        rows.append([btn("🎟 Ввести промокод", cb="holo:enter_promo")])
    rows.append([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return {"inline_keyboard": rows}

def kb_chat():
    return markup([btn("Назад", cb="holo:home", eid="5341715473882955310")])

def kb_skip_polls():
    return markup([btn("Пропустить опросы", cb="holo:skip_polls", eid="5341715473882955310", style="danger")])

def kb_after_code(deploy_type):
    if deploy_type == "tg_bot":   label = "Запустить и посмотреть бота"
    elif deploy_type == "web":    label = "Задеплоить и посмотреть сайт"
    else: return markup([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return markup(
        [btn(label, cb="holo:deploy", eid="5287454433318300292", style="primary")],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    )

def kb_deploy_bot(bot_url):
    return markup(
        [btn("Открыть бота", url=bot_url, eid="5287454433318300292", style="primary")],
        [btn("Логи", cb="holo:deploy_logs"), btn("Статус", cb="holo:deploy_status")],
        [btn("Остановить", cb="holo:deploy_stop", style="danger")],
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    )

def kb_deploy_web(url):
    p_btn = (btn("Открыть сайт", web_app=url, style="primary") if url.startswith("https://")
             else btn("Открыть сайт", url=url, style="primary"))
    return markup([p_btn], [btn("Остановить", cb="holo:deploy_stop", style="danger")],
                  [btn("Назад", cb="holo:home", eid="5341715473882955310")])

def kb_url_rows(urls):
    rows = [[btn(title[:35], url=url)] for url, title in urls[:8]]
    rows.append([btn("Назад", cb="holo:home", eid="5341715473882955310")])
    return {"inline_keyboard": rows}

def kb_chat_list(chats, page=0, per_page=5):
    rows = []
    start = page * per_page; chunk = chats[start:start+per_page]
    for c in chunk: rows.append([btn(c["title"][:40], cb=f"holo:chat_open:{c['id']}")])
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

def kb_in_chat(chat_id):
    return markup([btn("Выйти из чата", cb="holo:chat", eid="5341715473882955310")])

def kb_admin():
    return markup(
        [btn("📊 Сводка (день/нед/мес)",  cb="admin:analytics_summary")],
        [btn("💰 LTV топ плательщики",    cb="admin:analytics_ltv")],
        [btn("📈 Активность по дням",     cb="admin:analytics_activity")],
        [btn("🎯 Тарифы — что берут",     cb="admin:analytics_plans")],
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

CUSTOM_AI_INFO = (
    f'{E2("5431376038628171216","🤖")} <b>Custom AI - подключи свою модель</b>\n\n'
    "<b>Что это такое?</b>\n"
    "Holocron по умолчанию работает через сервер разработчиков. Custom AI позволяет подключить <b>свой API ключ</b>.\n\n"
    "<b>Поддерживаемые провайдеры:</b>\n"
    "OpenAI, Anthropic, DeepSeek, Groq, Mistral, Together AI, OpenRouter, VseGPT, Perplexity, Gemini, Qwen, HuggingFace\n\n"
    "<b>Как подключить:</b>\n"
    "1. Нажми провайдера ниже\n2. Введи API ключ\n3. Укажи модель\n\n"
    "<i>Если подключён Custom AI — токены с баланса не списываются.</i>"
)

def home_text():
    return (
        f'{E2("5328230887186274308")} <b>Holocron</b>\n\n'
        f'{E2("5454219968948229067")} <i>Have others refused? I\'ll help you.</i>'
    )

def settings_text():
    return f'{E2("5341715473882955310")} <b>Настройки</b>\n\nСоветуем для начала ознакомиться с ботом в разделе <b>Помощь</b>'

async def profile_text(uid):
    from database import db_get_stats, db_get_balance, db_get_ref_balance, db_get_ref_count, db_get_custom_api
    stats   = await db_get_stats(uid)
    balance = await db_get_balance(uid)
    ref_bal = await db_get_ref_balance(uid)
    ref_cnt = await db_get_ref_count(uid)
    custom  = await db_get_custom_api(uid)
    lines = [
        f'{E2("5373012449597335010")} <b>Профиль</b>\n',
        f'Всего запросов   — <b>{stats["total_reqs"]}</b>',
        f'Баланс токенов   — <b>{balance:,}</b>',
        f'Реф. баланс      — <b>{ref_bal:.2f}₽</b>',
        f'Рефералов        — <b>{ref_cnt}</b>',
    ]
    if custom:
        p = PROVIDERS.get(custom["provider"], {})
        lines.append(f'\nAPI провайдер — <b>{p.get("name", custom["provider"])}</b>')
        lines.append(f'API токен     — <code>{custom["api_key"][:12]}…</code>')
    return "\n".join(lines)

def chat_list_text(chats):
    if not chats:
        return (f'{E2("5443038326535759644")} <b>Чаты</b>\n\n'
                '<i>У вас пока нет чатов. Нажмите + чтобы создать первый.</i>')
    return f'{E2("5443038326535759644")} <b>Чаты</b>\n\nОткрой чат или создай новый.'

HELP_TEXT = (
    f'{E2("5445188473063480079")} <b>Помощь — как работать с Holocron</b>\n\n'
    '<b>Начало работы:</b>\nНажми <b>Начать</b> → создай чат → задай вопрос или попроси написать код/статью.\n\n'
    '<b>Возможности:</b>\n'
    '- 💬 Чат с RAG-поиском по базе знаний\n'
    '- 🤖 Генерация кода и деплой Telegram-ботов\n'
    '- 🌐 Генерация сайтов (Preview-Mode)\n'
    '- ✍️ Публикация статей в Telegra.ph\n'
    '- 📦 Чтение архивов (zip/rar/7z)\n'
    '- 🖼 OCR фото, 🎤 распознавание голоса (Interactive-Mode)\n\n'
    '<b>Команды:</b>\n'
    '<code>/s</code> — пропустить опросы Code-Mode\n'
    '<code>/context</code> — оптимизировать контекст чата (сжать историю)\n'
    '<code>/status</code> — статус деплоя\n'
    '<code>/logs</code> — логи бота\n'
    '<code>/stop</code> — остановить деплой'
)

def kb_my_center():
    """Клавиатура для меню 'Мой центр'"""
    return markup(
        [btn("Настройки", cb="holo:settings", eid="5341715473882955310")],
        [btn("Custom AI", cb="holo:custom_ai", eid="5431376038628171216")],
        [btn("🔌 Подключить свою базу", cb="holo:connect_db", eid="5399898266265475100")],  # заглушка
        [btn("Назад", cb="holo:home", eid="5341715473882955310")],
    )

def _fix_url(url: str) -> str:
    url = url.strip().rstrip(".,;)")
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    import re as _re
    if not _re.search(r'https?://[a-zA-Z0-9._-]+\.[a-zA-Z]{2,}', url):
        return ""
    return url

def clean_md_to_html(text):
    import html as _h
    
    text = re.sub(r'Context:\s*[■▢]+\s*\n?', '', text, flags=re.MULTILINE)
    
    def fix_raw_a(m):
        href = _fix_url(m.group(1))
        label = m.group(2).strip()
        if not href or not label:
            return label or ""
        return f'[{label}]({href})'
    text = re.sub(r'<a\s+href=["\']([^"\']+)["\']>([^<]*)</a>', fix_raw_a, text, flags=re.IGNORECASE)
    text = re.sub(r'<a\s[^>]*>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'</a>', '', text, flags=re.IGNORECASE)

    text = re.sub(r'\[\d+\]', '', text)
    text = re.sub(r'(Найдено в контексте|Источник[и]?|В контексте)[:\s\[\d\]]*\.?', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\n{3,}', '\n\n', text)

    code_blocks = []
    def save_code_block(m):
        inner = m.group(1); lines = inner.split('\n')
        if lines and re.match(r'^[a-zA-Z0-9_+\-]{1,20}$', lines[0].strip()): inner = '\n'.join(lines[1:])
        code_blocks.append(f'<pre><code>{_h.escape(inner.strip())}</code></pre>')
        return f'\x00CODE{len(code_blocks)-1}\x00'
    text = re.sub(r'```([\s\S]*?)```', save_code_block, text)

    inline_codes = []
    def save_inline_code(m):
        inline_codes.append(f'<code>{_h.escape(m.group(1))}</code>')
        return f'\x00INLINE{len(inline_codes)-1}\x00'
    text = re.sub(r'`([^`\n]+)`', save_inline_code, text)

    links = []
    def save_md_link(m):
        href = _fix_url(m.group(2))
        if not href:
            return m.group(1)
        links.append(f'<a href="{href}">{_h.escape(m.group(1))}</a>')
        return f'\x00LINK{len(links)-1}\x00'
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', save_md_link, text)

    def save_bare_link(m):
        u = m.group(0).rstrip(".,;)>")
        label = re.sub(r'https?://(www\.)?', '', u)[:35]
        u = _fix_url(u)
        if not u:
            return m.group(0)
        links.append(f'<a href="{u}">{_h.escape(label)}</a>')
        return f'\x00LINK{len(links)-1}\x00'
    text = re.sub(r'(?<!\x00)https?://[^\s<>"\'\x00]+', save_bare_link, text)

    def save_bare_domain(m):
        raw = m.group(0).rstrip(".,;)>")
        u = _fix_url(raw)
        if not u:
            return m.group(0)
        label = re.sub(r'https?://(www\.)?', '', raw)[:40]
        links.append(f'<a href="{u}">{_h.escape(label)}</a>')
        return f'\x00LINK{len(links)-1}\x00'
    
    text = re.sub(
        r'(?<![\w/@])([a-zA-Z0-9][a-zA-Z0-9_-]{1,63}\.(?:ru|com|org|net|gov|io|ai|app|dev|me|su)(?:/[^\s<>"\']*)?)',
        save_bare_domain, text
    )

    text = _h.escape(text, quote=False)

    text = re.sub(r'^#{1,6}\s+(.+)$', r'<b>\1</b>', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    text = re.sub(r'^[\-\*]\s+', '- ', text, flags=re.MULTILINE)

    for i, link in enumerate(links):
        text = text.replace(_h.escape(f'\x00LINK{i}\x00'), link)
        text = text.replace(f'\x00LINK{i}\x00', link)
    for i, code in enumerate(inline_codes):
        text = text.replace(_h.escape(f'\x00INLINE{i}\x00'), code)
        text = text.replace(f'\x00INLINE{i}\x00', code)
    for i, code in enumerate(code_blocks):
        text = text.replace(_h.escape(f'\x00CODE{i}\x00'), code)
        text = text.replace(f'\x00CODE{i}\x00', code)

    return text.strip()

def safe_truncate_html(text: str, max_len: int) -> str:
    if len(text) <= max_len: return text
    truncated = text[:max_len]
    last_open = truncated.rfind('<'); last_close = truncated.rfind('>')
    if last_open > last_close: truncated = truncated[:last_open]
    TAGS = ('b','i','code','pre','a'); stack = []
    for m in re.finditer(r'<(/?)(' + '|'.join(TAGS) + r')[^>]*>', truncated):
        closing, tag = m.group(1), m.group(2)
        if closing:
            for j in range(len(stack)-1,-1,-1):
                if stack[j] == tag: stack.pop(j); break
        else: stack.append(tag)
    for tag in reversed(stack): truncated += f'</{tag}>'
    return truncated + "…"

def _stream_preview(text):
    if text.count('```') % 2 != 0: text = text + '\n```'
    return clean_md_to_html(text)

def extract_urls(raw_text):
    found = re.findall(r'href="([^"]+)"[^>]*>([^<]+)<', raw_text)
    seen, result = set(), []
    for url, title in found:
        if url.startswith("http") and url not in seen:
            seen.add(url); result.append((url, title.strip()[:35]))
    for u in re.findall(r'https?://[^\s<>"\')\]]+', raw_text):
        u = u.rstrip(".,;)")
        if u not in seen: seen.add(u); result.append((u, re.sub(r'https?://(www\.)?','',u)[:35]))
    return result