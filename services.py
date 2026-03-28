from config import *
from security import verify_platega_headers
from database import *

_ch = None
def get_ch():
    global _ch
    if _ch is None:
        _ch = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB)
    return _ch

def ch_total():
    try: return get_ch().query(f"SELECT count() FROM {CH_TABLE}").result_rows[0][0]
    except: return 0

RAG_DIST_THRESHOLD = float(os.getenv("RAG_DIST_THRESHOLD", "0.35"))

def ch_vector_search(emb, top_k: int = 3):
    emb_str = "[" + ",".join(map(str, emb)) + "]"
    result = get_ch().query(
        f"SELECT content,source_url,cosineDistance(embedding,{emb_str}) AS dist "
        f"FROM {CH_TABLE} WHERE length(embedding)={EMB_DIM} ORDER BY dist ASC LIMIT {top_k}")
    rows = [{"content": r[0], "source_url": r[1], "dist": r[2]} for r in result.result_rows]
    relevant = [r for r in rows if r["dist"] <= RAG_DIST_THRESHOLD]
    log.info("RAG search: %d docs found, %d relevant (threshold=%.2f, best_dist=%.3f, top_k=%d)",
             len(rows), len(relevant), RAG_DIST_THRESHOLD,
             rows[0]["dist"] if rows else 1.0, top_k)
    return relevant

def ch_repo_exists(url):
    try: return get_ch().query(f"SELECT count() FROM {CH_TABLE} WHERE source_url=%(u)s", parameters={"u": url}).result_rows[0][0] > 0
    except: return False

def ch_insert_repo(name, description, url, readme):
    try:
        content = f"GitHub: {name}\n{description}\n{url}\n\n{readme[:3000]}"
        get_ch().insert(CH_TABLE, [[content, [0.0]*EMB_DIM, 0, "url", url]],
                        column_names=["content", "embedding", "is_premium", "source_type", "source_url"])
    except Exception as e:
        log.warning("ch_insert_repo: %s", e)

async def get_embedding(text):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{OLLAMA_URL}/api/embeddings", json={"model": OLLAMA_EMB_MODEL, "prompt": text},
                          timeout=aiohttp.ClientTimeout(total=120)) as r:
            if r.status != 200: raise RuntimeError(f"Ollama {r.status}")
            return (await r.json()).get("embedding", [])

async def ask_gemma_stream(messages: list, on_update, api_key: str, model: str = "gemma-3-27b-it", max_tokens: int = 4096):
    full = ""
    last_edit = time.time()
    
    prompt_parts = []
    for m in messages:
        role = "User" if m["role"] == "user" else "Assistant"
        prompt_parts.append(f"{role}: {m['content']}")
    
    prompt_parts.append("Assistant: ")
    full_prompt = "\n\n".join(prompt_parts)
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:streamGenerateContent"
    
    payload = {
        "contents": [{
            "parts": [{"text": full_prompt}]
        }],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": max_tokens,
            "topP": 0.95
        }
    }
    
    timeout = aiohttp.ClientTimeout(total=120, connect=30, sock_read=60)
    
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{url}?alt=sse&key={api_key}",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=timeout
            ) as r:
                if r.status != 200:
                    body = await r.text()
                    raise RuntimeError(f"Gemma API {r.status}: {body[:200]}")
                
                async for line in r.content:
                    line = line.decode("utf-8", errors="ignore").strip()
                    if not line.startswith("data: "):
                        continue
                    
                    data_str = line[6:]
                    if not data_str or data_str == "[DONE]":
                        continue
                    
                    try:
                        chunk = json.loads(data_str)
                        if "candidates" in chunk and chunk["candidates"]:
                            content = chunk["candidates"][0].get("content", {}).get("parts", [{}])[0].get("text", "")
                            if content:
                                full += content
                                if time.time() - last_edit >= 1.5:
                                    await on_update(full)
                                    last_edit = time.time()
                    except json.JSONDecodeError:
                        continue
                        
    except asyncio.TimeoutError:
        if not full:
            raise RuntimeError("Gemma API timeout")
    except Exception as e:
        raise RuntimeError(f"Gemma API error: {e}")
    
    await on_update(full)
    
    total_tokens = max(1, len(full) // 4) + max(1, len(full_prompt) // 4)
    
    return full, total_tokens

async def ask_local_ai(prompt: str, system_prompt: str = SYSTEM_PROMPT) -> str:
    payload = {
        "prompt": f"{system_prompt}\n\nUser: {prompt}\n\nAssistant:",
        "n_predict": 1024,
        "temperature": 0.7,
        "stop": ["User:"],
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(LOCAL_AI_URL, json=payload,
                              timeout=aiohttp.ClientTimeout(total=120)) as r:
                if r.status != 200:
                    raise RuntimeError(f"LocalAI HTTP {r.status}")
                data = await r.json()
                return data.get("content", "").strip() or "Пустой ответ от модели."
    except asyncio.TimeoutError:
        return "⏱ Таймаут — локальная модель не ответила за 120 секунд."
    except Exception as e:
        return f"🔴 Сервер ИИ недоступен: {e}"

_LOCAL_AI_SEM = asyncio.Semaphore(2)
_LOCAL_AI_QUEUE_COUNT = 0

async def ask_local_ai_stream(messages: list, on_update, context_str: str = "") -> str:
    global _LOCAL_AI_QUEUE_COUNT

    if _LOCAL_AI_SEM.locked():
        _LOCAL_AI_QUEUE_COUNT += 1
        log.info("local_ai queue: позиция %d", _LOCAL_AI_QUEUE_COUNT)
        await on_update("⏳ <b>Бесплатная генерация занимает время...</b>")

    async with _LOCAL_AI_SEM:
        _LOCAL_AI_QUEUE_COUNT = max(0, _LOCAL_AI_QUEUE_COUNT - 1)

        sys_parts = [m["content"] for m in messages if m["role"] == "system"]
    history_parts = []
    for m in messages:
        if m["role"] == "user":
            history_parts.append(f"User: {m['content']}")
        elif m["role"] == "assistant":
            history_parts.append(f"Assistant: {m['content']}")

    system_block = "\n".join(sys_parts)
    if context_str:
        system_block += f"\n\nКонтекст из базы знаний:\n{context_str}"

    prompt = system_block + "\n\n" + "\n".join(history_parts) + "\nAssistant:"

    payload = {
        "prompt": prompt,
        "n_predict": 1024,
        "temperature": 0.7,
        "stop": ["User:"],
        "stream": True,
    }

    log.info("local_ai_stream START prompt_len=%d context_docs=%d",
             len(prompt), len([x for x in context_str.split("\n\n") if x.strip()]) if context_str else 0)
    t_start = time.time()
    full = ""
    last_edit = time.time()
    try:
        timeout = aiohttp.ClientTimeout(total=240, connect=10, sock_read=60)
        async with aiohttp.ClientSession() as s:
            async with s.post(LOCAL_AI_URL, json=payload, timeout=timeout) as r:
                if r.status != 200:
                    body = await r.text()
                    return f"🔴 LocalAI HTTP {r.status}: {body[:200]}"
                async for raw in r.content:
                    line = raw.decode("utf-8", errors="ignore").strip()
                    if not line.startswith("data:"):
                        continue
                    payload_str = line[5:].strip()
                    try:
                        chunk = json.loads(payload_str)
                        content = chunk.get("content", "")
                        if content:
                            full += content
                            if time.time() - last_edit >= 1.5:
                                await on_update(full)
                                last_edit = time.time()
                        if chunk.get("stop"):
                            break
                    except Exception:
                        pass
    except asyncio.TimeoutError:
        if full:
            pass
        else:
            return "⏱ Таймаут — локальная модель не ответила."
    except Exception as e:
        if full:
            pass
        else:
            return f"🔴 Сервер ИИ недоступен: {e}"

    full = md_to_tg_html(full.strip())
    await on_update(full)
    log.info("local_ai_stream DONE tokens≈%d elapsed=%.1fs", len(full) // 4, time.time() - t_start)
    return full or "Пустой ответ от модели."

SYSTEM_PROMPT = (
    "Ты Голокрон - ИИ-ассистент. Отвечай кратко, только по делу, на русском.\n"
    "СТРОГО используй только Telegram HTML-разметку:\n"
    "  Жирный: <b>текст</b>\n"
    "  Ссылка: <a href=\"https://example.com\">текст</a>\n"
    "  Список: строки начинающиеся с '- '\n"
    "ЗАПРЕЩЕНО использовать: **markdown**, [текст](url), ```блоки```, # заголовки.\n"
    "Без вступлений и лишних слов."
)

def md_to_tg_html(text: str) -> str:
    def fix_raw_a(m):
        url = m.group(1).strip()
        label = m.group(2).strip()
        if not url or not label:
            return label or ""
        safe_url = re.sub(r'[^a-zA-Z0-9_\-./:?=&%#@]', '', url)
        return f'[{label}]({safe_url})'
    
    text = re.sub(r'<a\s+href=["\']([^"\']+)["\']>([^<]*)</a>', fix_raw_a, text, flags=re.IGNORECASE)

    text = re.sub(r'<[^>]+>', '', text)
    lines = text.split("\n")
    result = []
    for line in lines:
        line = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', line)
        line = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<i>\1</i>', line)
        line = re.sub(r'`([^`]+)`', r'<code>\1</code>', line)
        line = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', line)
        line = re.sub(r'^#{1,3}\s+(.+)', r'<b>\1</b>', line)
        result.append(line)
    
    joined = "\n".join(result)
    joined = re.sub(r'```[a-z]*\n?', '', joined)
    
    return joined

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
    "huggingface_ep":{"name":"HuggingFace (Endpoints)",     "url":"", "type":"openai_ep", "note":"Введи URL своего Inference Endpoint"},
}
PROVIDER_ORDER = ["openai","anthropic","deepseek","groq","mistral","together","openrouter",
                  "vsegpt","perplexity","gemini","qwen_ali","qwen_or","huggingface","huggingface_ep"]

async def validate_custom_api(provider_id, api_key, model, custom_url=None):
    p = PROVIDERS.get(provider_id)
    if not p: return False, "Неизвестный провайдер"
    url = custom_url or p["url"]
    ptype = p["type"]
    if provider_id == "huggingface_ep":
        if not custom_url: return False, "Укажи URL Endpoint"
        url = custom_url.rstrip("/") + "/v1/chat/completions"
    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession() as s:
            if ptype == "anthropic":
                headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
                async with s.post(p["url"], json={"model":model,"max_tokens":5,"messages":[{"role":"user","content":"hi"}]}, headers=headers, timeout=timeout) as r:
                    if r.status == 401: return False, "Неверный API ключ"
                    if r.status == 404: return False, f"Модель {model!r} не найдена"
                    if r.status in (200, 400, 529): return True, "OK"
                    return False, f"HTTP {r.status}: {(await r.text())[:150]}"
            if provider_id == "huggingface":
                url = f"https://api-inference.huggingface.co/models/{model}/v1/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            async with s.post(url, json={"model":model,"max_tokens":5,"messages":[{"role":"user","content":"hi"}]}, headers=headers, timeout=timeout) as r:
                if r.status == 401: return False, "Неверный API ключ"
                if r.status == 404: return False, f"Модель {model!r} не найдена"
                if r.status in (200, 400, 402, 429, 503): return True, "OK"
                body = await r.text()
                if "model_not_found" in body or "does not exist" in body: return False, f"Модель {model!r} не найдена"
                return False, f"HTTP {r.status}: {body[:150]}"
    except asyncio.TimeoutError: return False, "Таймаут"
    except Exception as e: return False, f"Ошибка: {str(e)[:100]}"

async def ask_llm(messages, model=None):
    model = model or VSEGPT_MODEL_CHAT
    async with aiohttp.ClientSession() as s:
        async with s.post(VSEGPT_URL,
            headers={"Authorization": f"Bearer {VSEGPT_KEY}", "Content-Type": "application/json"},
            json={"model": model, "messages": messages, "max_tokens": 4096, "temperature": 0.2},
            timeout=aiohttp.ClientTimeout(total=180)) as r:
            if r.status != 200: raise RuntimeError(f"VseGPT {r.status}: {await r.text()}")
            return (await r.json())["choices"][0]["message"]["content"]

async def ask_llm_stream(messages, on_update, model=None, max_tokens=4096, api_key=None):
    if api_key and model and "gemma" in model.lower():
        return await ask_gemma_stream(messages, on_update, api_key, model, max_tokens)
    
    model = model or VSEGPT_MODEL_CHAT
    full = ""
    last_edit = time.time()
    chunks = 0
    prompt_tokens = 0
    completion_tokens = 0
    t_start = time.time()
    timeout = aiohttp.ClientTimeout(total=240, connect=30, sock_read=60)
    
    async with aiohttp.ClientSession() as s:
        async with s.post(VSEGPT_URL,
            headers={"Authorization": f"Bearer {VSEGPT_KEY}", "Content-Type": "application/json"},
            json={"model": model, "messages": messages, "max_tokens": max_tokens, "temperature": 0.2,
                  "stream": True, "stream_options": {"include_usage": True}},
            timeout=timeout) as r:
            if r.status != 200:
                body = await r.text()
                raise RuntimeError(f"VseGPT {r.status}: {body[:200]}")
            async for raw in r.content:
                line = raw.decode("utf-8", errors="ignore").strip()
                if not line.startswith("data:"): continue
                payload = line[5:].strip()
                if payload == "[DONE]": break
                try:
                    chunk = json.loads(payload)
                    if chunk.get("usage"):
                        prompt_tokens     = chunk["usage"].get("prompt_tokens", 0)
                        completion_tokens = chunk["usage"].get("completion_tokens", 0)
                    delta = chunk["choices"][0]["delta"].get("content", "") if chunk.get("choices") else ""
                    if delta:
                        full += delta; chunks += 1
                        if time.time() - last_edit >= 1.5:
                            await on_update(full); last_edit = time.time()
                except: pass
    if not prompt_tokens:
        input_text = " ".join(m.get("content","") for m in messages)
        prompt_tokens     = max(1, len(input_text) // 4)
        completion_tokens = max(1, len(full) // 4)
    total_tokens = prompt_tokens + completion_tokens
    log.info("llm_stream DONE model=%s tokens=%d elapsed=%.1fs", model, total_tokens, time.time()-t_start)
    await on_update(full)
    return full, total_tokens

async def ask_llm_custom(messages, custom):
    p = PROVIDERS.get(custom["provider"], {}); ptype = p.get("type","openai")
    api_key = custom["api_key"]; model = custom["model"]
    timeout = aiohttp.ClientTimeout(total=180)
    async with aiohttp.ClientSession() as s:
        if ptype == "anthropic":
            sys_msgs  = [m["content"] for m in messages if m["role"]=="system"]
            user_msgs = [m for m in messages if m["role"]!="system"]
            headers = {"x-api-key":api_key,"anthropic-version":"2023-06-01","content-type":"application/json"}
            async with s.post(p["url"], json={"model":model,"max_tokens":4096,"system":"\n".join(sys_msgs),"messages":user_msgs}, headers=headers, timeout=timeout) as r:
                if r.status!=200: raise RuntimeError(f"Anthropic {r.status}")
                return (await r.json())["content"][0]["text"]
        url = p.get("url","")
        if custom["provider"] == "huggingface": url = f"https://api-inference.huggingface.co/models/{model}/v1/chat/completions"
        elif custom["provider"] == "huggingface_ep": url = custom.get("endpoint_url","").rstrip("/") + "/v1/chat/completions"
        headers = {"Authorization":f"Bearer {api_key}","Content-Type":"application/json"}
        async with s.post(url, json={"model":model,"messages":messages,"max_tokens":4096,"temperature":0.2}, headers=headers, timeout=timeout) as r:
            if r.status!=200: raise RuntimeError(f"{custom['provider']} {r.status}")
            return (await r.json())["choices"][0]["message"]["content"]

async def ask_llm_custom_stream(messages, custom, on_update, max_tokens=4096):
    p = PROVIDERS.get(custom["provider"], {}); ptype = p.get("type","openai")
    api_key = custom["api_key"]; model = custom["model"]
    full = ""; last_edit = time.time()
    timeout = aiohttp.ClientTimeout(total=240, connect=30, sock_read=60)
    async with aiohttp.ClientSession() as s:
        if ptype == "anthropic":
            sys_msgs  = [m["content"] for m in messages if m["role"]=="system"]
            user_msgs = [m for m in messages if m["role"]!="system"]
            headers = {"x-api-key":api_key,"anthropic-version":"2023-06-01","content-type":"application/json"}
            async with s.post(p["url"], json={"model":model,"max_tokens":max_tokens,"system":"\n".join(sys_msgs),"messages":user_msgs,"stream":True}, headers=headers, timeout=timeout) as r:
                if r.status!=200: raise RuntimeError(f"Anthropic {r.status}")
                async for raw in r.content:
                    line = raw.decode("utf-8","ignore").strip()
                    if not line.startswith("data:"): continue
                    ps = line[5:].strip()
                    if ps == "[DONE]": break
                    try:
                        delta = json.loads(ps).get("delta",{}).get("text","")
                        if delta:
                            full += delta
                            if time.time()-last_edit >= 1.5: await on_update(full); last_edit=time.time()
                    except: pass
        else:
            url = p.get("url","")
            if custom["provider"] == "huggingface": url = f"https://api-inference.huggingface.co/models/{model}/v1/chat/completions"
            elif custom["provider"] == "huggingface_ep": url = custom.get("endpoint_url","").rstrip("/") + "/v1/chat/completions"
            headers = {"Authorization":f"Bearer {api_key}","Content-Type":"application/json"}
            async with s.post(url, json={"model":model,"messages":messages,"max_tokens":max_tokens,"temperature":0.2,"stream":True}, headers=headers, timeout=timeout) as r:
                if r.status!=200: raise RuntimeError(f"{custom['provider']} {r.status}")
                async for raw in r.content:
                    line = raw.decode("utf-8","ignore").strip()
                    if not line.startswith("data:"): continue
                    ps = line[5:].strip()
                    if ps == "[DONE]": break
                    try:
                        delta = json.loads(ps)["choices"][0]["delta"].get("content","")
                        if delta:
                            full += delta
                            if time.time()-last_edit >= 1.5: await on_update(full); last_edit=time.time()
                    except: pass
    await on_update(full)
    return full

async def github_get_repo(owner, repo):
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{GITHUB_API_BASE}/repos/{owner}/{repo}", headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200: return None
                info = await r.json()
            readme = ""
            async with s.get(f"{GITHUB_API_BASE}/repos/{owner}/{repo}/readme",
                             headers={**headers, "Accept": "application/vnd.github.v3.raw"}, timeout=aiohttp.ClientTimeout(total=15)) as r2:
                if r2.status == 200: readme = await r2.text()
        return {"name": info.get("full_name",""), "description": info.get("description",""),
                "url": info.get("html_url",""), "stars": info.get("stargazers_count",0),
                "language": info.get("language",""), "readme": readme}
    except Exception as e:
        log.warning("github: %s", e); return None

def extract_github_repos(text):
    return list(set(re.findall(r'github\.com/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)', text)))[:3]

async def fetch_url_content(url: str) -> str:
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; HolocronBot/1.0)"}
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15), ssl=False) as r:
                if r.status != 200: return f"[HTTP {r.status}]"
                raw = await r.text(errors="ignore")
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(raw, "html.parser")
            for tag in soup(["script","style","nav","footer","head"]): tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            return re.sub(r'\n{3,}', '\n\n', text).strip()[:6000]
        except ImportError:
            return re.sub(r'\n{3,}', '\n\n', re.sub(r'<[^>]+>', '', raw)).strip()[:6000]
    except Exception as e:
        return f"[Ошибка загрузки: {e}]"

async def ocr_image(bot: Bot, photo) -> str:
    try:
        f = await bot.get_file(photo.file_id)
        buf = io.BytesIO(); await bot.download_file(f.file_path, buf); buf.seek(0)
        data = aiohttp.FormData()
        data.add_field("apikey", "K81453423888957")
        data.add_field("language", "rus")
        data.add_field("isOverlayRequired", "false")
        data.add_field("OCREngine", "2")
        data.add_field("file", buf, filename="photo.jpg", content_type="image/jpeg")
        async with aiohttp.ClientSession() as s:
            async with s.post("https://api.ocr.space/parse/image", data=data, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200: return f"[OCR ошибка: HTTP {r.status}]"
                result = await r.json()
                if result.get("IsErroredOnProcessing"): return f"[OCR ошибка: {result.get('ErrorMessage','?')}]"
                text = "\n".join(p.get("ParsedText","") for p in result.get("ParsedResults",[])).strip()
                return text or "[Текст не распознан]"
    except Exception as e:
        return f"[Ошибка OCR: {e}]"

async def transcribe_voice(bot: Bot, voice: Voice) -> str:
    try:
        f = await bot.get_file(voice.file_id)
        buf = io.BytesIO(); await bot.download_file(f.file_path, buf); buf.seek(0)
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp.write(buf.read()); tmp_path = tmp.name
        try:
            from faster_whisper import WhisperModel
            model = WhisperModel("small", device="cpu", compute_type="int8")
            segments, _ = model.transcribe(tmp_path, language="ru")
            text = " ".join(s.text for s in segments).strip()
        except ImportError:
            try:
                import whisper
                result = whisper.load_model("small").transcribe(tmp_path, language="ru")
                text = result["text"].strip()
            except ImportError:
                text = "[Whisper не установлен: pip install faster-whisper]"
        os.unlink(tmp_path)
        return text or "[Голос не распознан]"
    except Exception as e:
        return f"[Ошибка распознавания: {e}]"

async def read_archive(bot: Bot, document) -> str:
    import zipfile, tarfile, os as _os
    try: import rarfile; HAS_RAR = True
    except ImportError: HAS_RAR = False
    try: import py7zr; HAS_7Z = True
    except ImportError: HAS_7Z = False

    fname = (document.file_name or "archive").lower()
    if document.file_size and document.file_size > ARCHIVE_MAX_SIZE:
        return f"[Архив слишком большой: {document.file_size//1024//1024} МБ. Макс. 50 МБ]"

    f = await bot.get_file(document.file_id)
    buf = io.BytesIO(); await bot.download_file(f.file_path, buf); buf.seek(0)

    import shutil as _shutil
    tmpdir = tempfile.mkdtemp(prefix="holo_arc_")
    try:
        arc_path = _os.path.join(tmpdir, "archive")
        with open(arc_path, "wb") as fh: fh.write(buf.read())
        extract_dir = _os.path.join(tmpdir, "ex"); _os.makedirs(extract_dir)
        total_size = 0

        def safe_path(base, target):
            real = _os.path.realpath(_os.path.join(base, target))
            base_real = _os.path.realpath(base)
            if not real.startswith(base_real + _os.sep) and real != base_real:
                raise ValueError(f"Zip slip: {target}")
            return real

        def depth(p): return len([x for x in p.replace("\\","/").split("/") if x])

        if fname.endswith(".zip"):
            with zipfile.ZipFile(arc_path) as zf:
                members = zf.infolist()
                if len(members) > ARCHIVE_MAX_FILES: return f"[Слишком много файлов: {len(members)}]"
                for m in members:
                    ext = _os.path.splitext(m.filename.lower())[1]
                    if ext in ARCHIVE_BLOCKED_EXT: return f"[Заблокированный файл: {m.filename}]"
                    if m.file_size > ARCHIVE_MAX_FILE_SZ: continue
                    total_size += m.file_size
                    if total_size > ARCHIVE_MAX_UNPACKED: return "[Архив-бомба: размер > 100 МБ]"
                    if depth(m.filename) > ARCHIVE_MAX_DEPTH: continue
                    safe_path(extract_dir, m.filename); zf.extract(m, extract_dir)
        elif fname.endswith((".tar",".tar.gz",".tgz",".tar.bz2",".tar.xz")):
            with tarfile.open(arc_path) as tf:
                members = tf.getmembers()
                if len(members) > ARCHIVE_MAX_FILES: return f"[Слишком много файлов: {len(members)}]"
                for m in members:
                    if m.issym() or m.islnk(): continue
                    ext = _os.path.splitext(m.name.lower())[1]
                    if ext in ARCHIVE_BLOCKED_EXT: return f"[Заблокированный файл: {m.name}]"
                    if m.size > ARCHIVE_MAX_FILE_SZ: continue
                    total_size += m.size
                    if total_size > ARCHIVE_MAX_UNPACKED: return "[Архив-бомба: размер > 100 МБ]"
                    if depth(m.name) > ARCHIVE_MAX_DEPTH: continue
                    safe_path(extract_dir, m.name); tf.extract(m, extract_dir, set_attrs=False)
        elif fname.endswith(".rar"):
            if not HAS_RAR: return "[RAR: pip install rarfile + apt install unrar]"
            with rarfile.RarFile(arc_path) as rf:
                members = rf.infolist()
                if len(members) > ARCHIVE_MAX_FILES: return f"[Слишком много файлов: {len(members)}]"
                for m in members:
                    ext = _os.path.splitext(m.filename.lower())[1]
                    if ext in ARCHIVE_BLOCKED_EXT: return f"[Заблокированный файл: {m.filename}]"
                    if m.file_size > ARCHIVE_MAX_FILE_SZ: continue
                    total_size += m.file_size
                    if total_size > ARCHIVE_MAX_UNPACKED: return "[Архив-бомба: размер > 100 МБ]"
                    safe_path(extract_dir, m.filename); rf.extract(m, extract_dir)
        elif fname.endswith(".7z"):
            if not HAS_7Z: return "[7z: pip install py7zr]"
            with py7zr.SevenZipFile(arc_path, mode="r") as sz:
                members = sz.list()
                if len(members) > ARCHIVE_MAX_FILES: return f"[Слишком много файлов: {len(members)}]"
                for m in members:
                    ext = _os.path.splitext(m.filename.lower())[1]
                    if ext in ARCHIVE_BLOCKED_EXT: return f"[Заблокированный файл: {m.filename}]"
                    if hasattr(m,"uncompressed") and m.uncompressed: total_size += m.uncompressed
                    if total_size > ARCHIVE_MAX_UNPACKED: return "[Архив-бомба: размер > 100 МБ]"
                sz.extractall(path=extract_dir)
        else: return ""

        structure, results, read_count = [], [], 0
        for root, dirs, files in _os.walk(extract_dir):
            cur_depth = root.replace(extract_dir,"").count(_os.sep)
            if cur_depth > ARCHIVE_MAX_DEPTH: dirs.clear(); continue
            rel_root = _os.path.relpath(root, extract_dir)
            for fn in sorted(files):
                rel_path = fn if rel_root == "." else _os.path.join(rel_root, fn)
                structure.append(rel_path)
                if read_count >= ARCHIVE_MAX_READ: continue
                ext = _os.path.splitext(fn.lower())[1]
                if ext not in ARCHIVE_TEXT_EXT: continue
                full_path = _os.path.join(root, fn)
                try:
                    sz = _os.path.getsize(full_path)
                    if sz > ARCHIVE_MAX_FILE_SZ: results.append(f"=== {rel_path} (пропущен: {sz//1024} КБ) ==="); continue
                    with open(full_path, "r", encoding="utf-8", errors="replace") as fh: content = fh.read(8000)
                    results.append(f"=== {rel_path} ===\n{content}"); read_count += 1
                except Exception: results.append(f"=== {rel_path} [ошибка чтения] ===")

        out = f"[Архив: {_os.path.basename(fname)}, файлов: {len(structure)}]\n\nСтруктура:\n" + "\n".join(structure[:100])
        if results: out += f"\n\n{'='*40}\nСодержимое ({read_count} файлов):\n\n" + "\n\n".join(results)
        skipped = max(0, len([s for s in structure if _os.path.splitext(s.lower())[1] in ARCHIVE_TEXT_EXT]) - ARCHIVE_MAX_READ)
        if skipped > 0: out += f"\n\n[...ещё {skipped} файлов не показаны]"
        return out[:12000]
    except ValueError as e: return f"[Безопасность: {e}]"
    except Exception as e: log.error("read_archive: %s", e); return f"[Ошибка чтения архива: {e}]"
    finally: _shutil.rmtree(tmpdir, ignore_errors=True)

def _detect_is_tg_bot(text: str) -> bool:
    markers = [
        "import telebot","from telebot","import aiogram","from aiogram",
        "from telegram import","from telegram.ext","import telegram",
        "telebot.TeleBot","TeleBot(","Application.builder","ApplicationBuilder",
        "bot.polling(","bot.infinity_polling","dp.start_polling","dp.run_polling",
        "executor.start_polling","Dispatcher(","from pyrogram","import pyrogram",
        "from telethon","import telethon","TelegramClient(",
        "Updater(","updater.start_polling","updater.idle","dispatcher.add_handler",
        "MessageHandler","CommandHandler","CallbackQueryHandler",
        "bot_token","BOT_TOKEN","getUpdates","sendMessage","reply_text(",
        "telegram.Bot(","Bot(token=",
    ]
    return any(m in text for m in markers)

def _detect_is_web(text: str, from_llm_output: bool = False) -> bool:
    if from_llm_output:
        return bool(re.search(r"```\s*(html|css)", text, re.IGNORECASE))
    return bool(re.search(r"\b(сайт|страниц|лендинг|верстк|html|css|javascript|frontend|веб|визитк)\b", text, re.IGNORECASE))

def _detect_is_telegraph(text: str) -> bool:
    if re.search(r'telegra\.?ph|telegraph', text, re.IGNORECASE):
        return True
    if re.search(r'\b(напиши|сделай|создай|опубликуй|написать|сделать)\b', text, re.IGNORECASE) and \
       re.search(r'\b(статью|статья|гайд|туториал|инструкц|руководств|лонгрид|пост)\b', text, re.IGNORECASE):
        return True
    return False

def _md_to_telegraph_nodes(text: str) -> list:
    nodes = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            i += 1
            continue
        if stripped.startswith("### "):
            nodes.append({"tag":"h4","children":[stripped[4:]]})
        elif stripped.startswith("## "):
            nodes.append({"tag":"h3","children":[stripped[3:]]})
        elif stripped.startswith("# "):
            nodes.append({"tag":"h4","children":[stripped[2:]]})
        elif stripped.startswith("```"):
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            nodes.append({"tag":"pre","children":[{"tag":"code","children":["\n".join(code_lines)]}]})
        elif re.match(r'^[-*]\s+', stripped):
            items = []
            while i < len(lines) and re.match(r'^[-*]\s+', lines[i].strip()):
                items.append({"tag":"li","children":[re.sub(r'^[-*]\s+','',lines[i].strip())]})
                i += 1
            nodes.append({"tag":"ul","children":items})
            continue
        else:
            nodes.append({"tag":"p","children":[stripped]})
        i += 1
    return nodes

async def telegraph_publish(title: str, content_text: str, author_name: str = "Holocron AI") -> dict | None:
    if not TELEGRAPH_TOKEN:
        return None
    nodes = _md_to_telegraph_nodes(content_text)
    if not nodes:
        return None
    payload = {
        "access_token": TELEGRAPH_TOKEN,
        "title": title[:256],
        "author_name": author_name,
        "author_url": os.getenv("BOT_URL", ""),
        "content": json.dumps(nodes),
        "return_content": False,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{TELEGRAPH_URL}/createPage", json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if data.get("ok"):
                    return {"url": "https://telegra.ph/" + data["result"]["path"], "title": data["result"]["title"]}
                log.warning("telegraph_publish error: %s", data)
                return None
    except Exception as e:
        log.error("telegraph_publish: %s", e); return None

async def telegraph_edit(page_path: str, title: str, content_text: str) -> dict | None:
    if not TELEGRAPH_TOKEN: return None
    nodes = _md_to_telegraph_nodes(content_text)
    payload = {
        "access_token": TELEGRAPH_TOKEN,
        "path": page_path,
        "title": title[:256],
        "content": json.dumps(nodes),
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{TELEGRAPH_URL}/editPage", json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if data.get("ok"):
                    return {"url": "https://telegra.ph/" + data["result"]["path"]}
                return None
    except Exception as e:
        log.error("telegraph_edit: %s", e); return None

async def platega_create_invoice(uid, tokens, amount_rub, amount_usd, method=2):
    import uuid as _uuid
    body = {
        "paymentMethod": method,
        "paymentDetails": {"amount": float(amount_rub), "currency": "RUB"},
        "description": f"{tokens:,} tokens",
        "return": os.getenv("BOT_URL", ""),
        "failedUrl": os.getenv("BOT_URL", ""),
        "payload": f"uid={uid};tokens={tokens}",
    }
    headers = {"X-MerchantId": PLATEGA_MERCHANT_ID, "X-Secret": PLATEGA_SECRET, "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{PLATEGA_URL}/transaction/process", json=body, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if r.status != 200: log.warning("platega create error: %s", data); return None
                invoice_id = data.get("transactionId") or str(_uuid.uuid4())
                await db_save_invoice(invoice_id, uid, tokens, amount_rub, amount_usd, "platega")
                return {"invoice_id": invoice_id, "redirect": data.get("redirect", "")}
    except Exception as e:
        log.error("platega_create_invoice: %s", e); return None

async def cryptobot_create_invoice(uid, tokens, amount_rub, amount_usd):
    body = {"currency_type":"fiat","fiat":"USD","amount":str(round(amount_usd,2)),
            "description":f"{tokens:,} tokens","payload":f"uid={uid};tokens={tokens}",
            "paid_btn_name":"openBot","paid_btn_url":os.getenv("BOT_URL", "")}
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN, "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CRYPTOBOT_URL}/createInvoice", json=body, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if not data.get("ok"): log.warning("cryptobot error: %s", data); return None
                result = data["result"]
                invoice_id = str(result["invoice_id"])
                pay_url = result.get("bot_invoice_url") or result.get("pay_url","")
                await db_save_invoice(invoice_id, uid, tokens, amount_rub, amount_usd, "cryptobot")
                return {"invoice_id": invoice_id, "redirect": pay_url}
    except Exception as e:
        log.error("cryptobot_create_invoice: %s", e); return None

async def cryptobot_check_invoice(invoice_id: str) -> bool:
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{CRYPTOBOT_URL}/getInvoices", params={"invoice_ids": invoice_id},
                             headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                items = data.get("result",{}).get("items",[])
                return bool(items and items[0].get("status") == "paid")
    except Exception as e:
        log.error("cryptobot_check_invoice: %s", e); return False

async def cryptobot_create_check(amount_usd: float, comment: str = ""):
    body = {"asset":"USDT","amount":str(round(amount_usd,2)),"description":comment or "Вывод реферального баланса Holocron"}
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN, "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{CRYPTOBOT_URL}/createCheck", json=body, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if data.get("ok"):
                    return data["result"].get("bot_check_url") or data["result"].get("check_url")
    except Exception as e:
        log.error("cryptobot_create_check: %s", e)
    return None

async def _process_payment(invoice_id: str):
    from keyboards import tg_send
    inv = await db_confirm_invoice(invoice_id)
    if not inv: return
    uid = inv["uid"]; tokens = inv["tokens"]
    amount_rub = inv["amount_rub"]; provider = inv["provider"]
    await db_add_tokens(uid, tokens)
    await db_log_action(uid, "payment", f"{tokens} tokens, {amount_rub}₽, {provider}")
    referrer_id = await db_get_referrer(uid)
    if referrer_id:
        bonus = round(float(amount_rub) * REF_PERCENT / 100, 2)
        await db_add_ref_balance(referrer_id, bonus)
        async with pg().acquire() as c:
            await c.execute("INSERT INTO ref_earnings(referrer_id,ref_uid,amount_rub) VALUES($1,$2,$3)", referrer_id, uid, bonus)
        try: await tg_send(referrer_id, f"💸 Реферальный бонус <b>+{bonus}₽</b>\nПользователь купил токены на {float(amount_rub):.2f}₽")
        except Exception: pass
    await tg_send(uid,
        f"✅ <b>Оплата прошла!</b>\n\n"
        f"Зачислено: <b>{tokens:,}</b> токенов\n"
        f"Провайдер: {provider}\n\n"
        f"Ваш баланс: <b>{await db_get_balance(uid):,}</b> токенов")
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT username,first_name FROM users WHERE uid=$1", uid)
    uname = f"@{row['username']}" if (row and row['username']) else f"id{uid}"
    fname = row['first_name'] if row else ""
    for aid in ADMIN_IDS:
        try: await tg_send(aid, f"💰 <b>Новая оплата</b>\n\nЮзер: {uname} ({fname})\nПровайдер: <b>{provider}</b>\nТокенов: <b>{tokens:,}</b>\nСумма: <b>{float(amount_rub):.2f}₽</b>")
        except Exception: pass

async def platega_webhook(request):
    from aiohttp import web as _web
    mid = request.headers.get("X-MerchantId",""); secret = request.headers.get("X-Secret","")
    if not verify_platega_headers(mid, secret):
        log.warning("platega_webhook: invalid auth mid=%r", mid[:10] if mid else "empty")
        return _web.Response(status=401)
    try:
        data = await request.json()
        invoice_id = data.get("id",""); status = data.get("status","")
        if status == "CONFIRMED" and invoice_id: await _process_payment(invoice_id)
    except Exception as e: log.error("platega_webhook: %s", e)
    return _web.Response(status=200)

async def health_handler(request):
    from aiohttp import web as _web
    try: pg_ok = await pg().fetchval("SELECT 1") == 1
    except Exception: pg_ok = False
    try: redis_ok = await rdb().ping()
    except Exception: redis_ok = False
    try: ch_ok = ch_total() >= 0
    except Exception: ch_ok = False
    return _web.json_response({"postgres": pg_ok, "redis": redis_ok, "clickhouse": ch_ok,
                               "status": "ok" if (pg_ok and redis_ok) else "degraded"})

async def _check_expired_invoices():
    while True:
        await asyncio.sleep(60)
        try:
            async with pg().acquire() as c:
                rows = await c.fetch("SELECT id,uid,provider FROM invoices WHERE status='pending'")
            for row in rows:
                if row[2] == "cryptobot":
                    paid = await cryptobot_check_invoice(row[0])
                    if paid: await _process_payment(row[0])
        except Exception as e:
            log.error("_check_expired_invoices: %s", e)

async def _check_alerts():
    from keyboards import tg_send
    sr = await analytics_success_rate()
    if sr < 50:
        for aid in ADMIN_IDS:
            try: await tg_send(aid, f"⚠️ <b>Алерт:</b> success rate упал до <b>{sr}%</b>! Проверь RAG базу.")
            except Exception: pass

async def _daily_report():
    import datetime
    from keyboards import tg_send
    today = await analytics_today(); week = await analytics_week(); month = await analytics_month()
    sr = await analytics_success_rate()
    text = (
        f"📊 <b>Ежедневный отчёт {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}</b>\n\n"
        f"<b>Сегодня:</b>\n"
        f"👥 Новых: <b>{today['new_users']}</b>  💬 Запросов: <b>{today['requests']}</b>\n"
        f"💰 Выручка: <b>{today['revenue']:.2f}₽</b>  💳 Платежей: <b>{today['payments']}</b>\n"
        f"❌ Пустых ответов: <b>{today['errors']}</b>  🚀 Деплоев: <b>{today['deploys']}</b>\n\n"
        f"<b>Неделя:</b>  👥 {week['new_users']}  💰 {week['revenue']:.2f}₽\n\n"
        f"<b>Месяц:</b>  👥 {month['total_users']}  ARPU: {month['arpu']:.2f}₽\n"
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
            if now.hour == 9 and now.minute < 5: await _daily_report()
            await _check_alerts()
        except Exception as e:
            log.error("_analytics_worker: %s", e)