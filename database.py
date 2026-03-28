from config import *

_pg: asyncpg.Pool | None = None
_redis: aioredis.Redis | None = None
_redis_rate: aioredis.Redis | None = None

def pg() -> asyncpg.Pool:
    assert _pg is not None, "PostgreSQL pool not initialized"
    return _pg

def rdb() -> aioredis.Redis:
    assert _redis is not None, "Redis not initialized"
    return _redis

async def init_db():
    global _pg, _redis, _redis_rate
    _pg         = await asyncpg.create_pool(PG_DSN, min_size=5, max_size=20, statement_cache_size=0)
    _redis      = aioredis.from_url(REDIS_DSN,      decode_responses=True)
    _redis_rate = aioredis.from_url(REDIS_RATE_DSN, decode_responses=True)
    await _create_tables()
    await db_init_promo_tables()
    log.info("✅ постгресик и редиска работают все заебок")

async def _create_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS users(uid BIGINT PRIMARY KEY, username TEXT, first_name TEXT, joined_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS user_settings(uid BIGINT PRIMARY KEY, url_buttons INT DEFAULT 0, github_scanner INT DEFAULT 0, code_mode INT DEFAULT 0, preview_mode INT DEFAULT 0, interactive_mode INT DEFAULT 0, local_ai INT DEFAULT 0, context_inspector INT DEFAULT 0);
    ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS local_ai INT DEFAULT 0;
    ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS context_inspector INT DEFAULT 0;
    CREATE TABLE IF NOT EXISTS user_stats(uid BIGINT PRIMARY KEY, total_reqs INT DEFAULT 0, found_reqs INT DEFAULT 0, empty_reqs INT DEFAULT 0, last_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS user_custom_api(uid BIGINT PRIMARY KEY, provider TEXT, api_key TEXT, model TEXT, endpoint_url TEXT, enabled INT DEFAULT 1, connected_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS user_balance(uid BIGINT PRIMARY KEY, tokens BIGINT DEFAULT 0, ref_balance_rub NUMERIC(12,2) DEFAULT 0);
    CREATE TABLE IF NOT EXISTS referrals(uid BIGINT PRIMARY KEY, referrer_id BIGINT DEFAULT 0, confirmed INT DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS ref_earnings(id BIGSERIAL PRIMARY KEY, referrer_id BIGINT, ref_uid BIGINT, amount_rub NUMERIC(12,2), created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS invoices(id TEXT PRIMARY KEY, uid BIGINT, tokens BIGINT, amount_rub NUMERIC(12,2), amount_usd NUMERIC(12,4), provider TEXT, status TEXT DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT NOW(), paid_at TIMESTAMPTZ);
    CREATE TABLE IF NOT EXISTS payout_requests(id BIGSERIAL PRIMARY KEY, uid BIGINT, amount_rub NUMERIC(12,2), method TEXT, details TEXT, status TEXT DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS user_action_log(id BIGSERIAL PRIMARY KEY, uid BIGINT, action TEXT, detail TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS chats(id BIGSERIAL PRIMARY KEY, uid BIGINT, title TEXT DEFAULT 'Новый чат', created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS chat_messages(id BIGSERIAL PRIMARY KEY, chat_id BIGINT, uid BIGINT, role TEXT, content TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS user_state(uid BIGINT PRIMARY KEY, active_chat_id BIGINT DEFAULT 0);
    CREATE TABLE IF NOT EXISTS chat_checkpoints(chat_id BIGINT PRIMARY KEY, summary TEXT DEFAULT '', updated_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS code_polls(uid BIGINT PRIMARY KEY, chat_id BIGINT DEFAULT 0, task TEXT, questions TEXT, answers TEXT DEFAULT '[]', current_q INT DEFAULT 0, poll_msg_id BIGINT DEFAULT 0, ctrl_msg_id BIGINT DEFAULT 0, waiting_text INT DEFAULT 0);
    CREATE TABLE IF NOT EXISTS pending_deploys(uid BIGINT PRIMARY KEY, bot_code TEXT, deploy_type TEXT, saved_at BIGINT);
    CREATE TABLE IF NOT EXISTS active_deploys(uid BIGINT PRIMARY KEY, deploy_type TEXT, token TEXT, container_id TEXT, sandbox_path TEXT, expires_at BIGINT, deployed_at BIGINT);
    CREATE TABLE IF NOT EXISTS deploy_queue(id BIGSERIAL PRIMARY KEY, uid BIGINT, bot_code TEXT, queued_at BIGINT);
    CREATE INDEX IF NOT EXISTS idx_chat_messages_chat ON chat_messages(chat_id);
    CREATE INDEX IF NOT EXISTS idx_action_log_uid ON user_action_log(uid);
    CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id);
    CREATE INDEX IF NOT EXISTS idx_invoices_uid ON invoices(uid);
    CREATE INDEX IF NOT EXISTS idx_chats_uid ON chats(uid);
    ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS local_ai_provider TEXT DEFAULT 'ollama';
    """
    async with pg().acquire() as c:
        await c.execute(sql)

async def db_get_local_ai_provider(uid: int) -> str:
    """Возвращает 'ollama' или 'gemma'"""
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT local_ai_provider FROM user_settings WHERE uid=$1", uid)
        return row["local_ai_provider"] if row else "ollama"

async def db_set_local_ai_provider(uid: int, provider: str):
    """Устанавливает провайдера: 'ollama' или 'gemma'"""
    if provider not in ("ollama", "gemma"):
        raise ValueError("provider must be 'ollama' or 'gemma'")
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_settings(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        await c.execute("UPDATE user_settings SET local_ai_provider=$1 WHERE uid=$2", provider, uid)

async def db_register(uid, un, fn):
    async with pg().acquire() as c:
        await c.execute("INSERT INTO users(uid,username,first_name) VALUES($1,$2,$3) ON CONFLICT(uid) DO NOTHING", uid, un, fn)
        await c.execute("INSERT INTO user_settings(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        await c.execute("INSERT INTO user_stats(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)

async def db_get_settings(uid) -> dict:
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_settings(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        row = await c.fetchrow("SELECT * FROM user_settings WHERE uid=$1", uid)
        return dict(row)

async def db_toggle(uid, key) -> bool:
    _TOGGLE_COLS = {
        "url_buttons":       "url_buttons",
        "github_scanner":    "github_scanner",
        "code_mode":         "code_mode",
        "preview_mode":      "preview_mode",
        "interactive_mode":  "interactive_mode",
        "local_ai":          "local_ai",          
        "context_inspector": "context_inspector",
    }
    if key not in _TOGGLE_COLS:
        raise ValueError(f"Unknown setting: {key}")
    col = _TOGGLE_COLS[key]  
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_settings(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        
        
        row = await c.fetchrow(
            f"SELECT {col} FROM user_settings WHERE uid=$1",
            uid,
        )
        nv = 0 if (row and row[col]) else 1
        await c.execute(
            f"UPDATE user_settings SET {col}=$1 WHERE uid=$2",
            nv, uid,
        )
        return bool(nv)

async def db_set_token_limit(uid, limit: int):
    """Set per-user token limit (0 = unlimited). Stored in user_settings.token_limit."""
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_settings(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        await c.execute("UPDATE user_settings SET token_limit=$1 WHERE uid=$2", limit, uid)

async def db_inc_stat(uid, found):
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_stats(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        if found:
            await c.execute("UPDATE user_stats SET total_reqs=total_reqs+1,found_reqs=found_reqs+1,last_at=NOW() WHERE uid=$1", uid)
        else:
            await c.execute("UPDATE user_stats SET total_reqs=total_reqs+1,empty_reqs=empty_reqs+1,last_at=NOW() WHERE uid=$1", uid)

async def db_get_stats(uid) -> dict:
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT * FROM user_stats WHERE uid=$1", uid)
        return dict(row) if row else {"total_reqs":0,"found_reqs":0,"empty_reqs":0,"last_at":"-"}

async def db_total_users() -> int:
    async with pg().acquire() as c:
        return await c.fetchval("SELECT count(*) FROM users")

async def db_set_custom_api(uid, provider, api_key, model, endpoint_url=None):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO user_custom_api(uid,provider,api_key,model,endpoint_url,enabled,connected_at) "
            "VALUES($1,$2,$3,$4,$5,1,NOW()) ON CONFLICT(uid) DO UPDATE SET "
            "provider=$2,api_key=$3,model=$4,endpoint_url=$5,enabled=1,connected_at=NOW()",
            uid, provider, api_key, model, endpoint_url)

async def db_get_custom_api(uid):
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT * FROM user_custom_api WHERE uid=$1 AND enabled=1", uid)
        return dict(row) if row else None

async def db_delete_custom_api(uid):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM user_custom_api WHERE uid=$1", uid)

async def db_get_balance(uid) -> int:
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_balance(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        return await c.fetchval("SELECT tokens FROM user_balance WHERE uid=$1", uid) or 0

async def db_get_ref_balance(uid) -> float:
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_balance(uid) VALUES($1) ON CONFLICT DO NOTHING", uid)
        val = await c.fetchval("SELECT ref_balance_rub FROM user_balance WHERE uid=$1", uid)
        return float(val or 0)

async def db_add_tokens(uid, tokens: int):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO user_balance(uid,tokens) VALUES($1,$2) ON CONFLICT(uid) DO UPDATE SET tokens=user_balance.tokens+$2",
            uid, tokens)

async def db_spend_tokens(uid, tokens: int) -> bool:
    async with pg().acquire() as c:
        row = await c.fetchrow(
            "UPDATE user_balance SET tokens=GREATEST(0,tokens-$1) WHERE uid=$2 AND tokens>=$1 RETURNING tokens",
            tokens, uid)
        return row is not None

async def db_set_tokens(uid, tokens: int):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO user_balance(uid,tokens) VALUES($1,$2) ON CONFLICT(uid) DO UPDATE SET tokens=$2",
            uid, tokens)

async def db_add_ref_balance(uid, amount_rub: float):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO user_balance(uid,ref_balance_rub) VALUES($1,$2) ON CONFLICT(uid) DO UPDATE SET ref_balance_rub=user_balance.ref_balance_rub+$2",
            uid, amount_rub)

async def db_subtract_ref_balance(uid, amount_rub: float):
    async with pg().acquire() as c:
        await c.execute("UPDATE user_balance SET ref_balance_rub=GREATEST(0,ref_balance_rub-$1) WHERE uid=$2", amount_rub, uid)

async def db_get_ref_count(uid) -> int:
    async with pg().acquire() as c:
        return await c.fetchval("SELECT count(*) FROM referrals WHERE referrer_id=$1 AND confirmed=1", uid) or 0

async def db_get_referrer(uid) -> int:
    async with pg().acquire() as c:
        return await c.fetchval("SELECT referrer_id FROM referrals WHERE uid=$1", uid) or 0

async def db_register_referral(uid, referrer_id):
    async with pg().acquire() as c:
        await c.execute("INSERT INTO referrals(uid,referrer_id,confirmed) VALUES($1,$2,0) ON CONFLICT DO NOTHING", uid, referrer_id)

async def db_confirm_referral(uid):
    async with pg().acquire() as c:
        await c.execute("UPDATE referrals SET confirmed=1 WHERE uid=$1 AND confirmed=0", uid)

async def db_is_referral_confirmed(uid) -> bool:
    async with pg().acquire() as c:
        val = await c.fetchval("SELECT confirmed FROM referrals WHERE uid=$1", uid)
        return bool(val)

async def db_save_invoice(invoice_id, uid, tokens, amount_rub, amount_usd, provider):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO invoices(id,uid,tokens,amount_rub,amount_usd,provider,status) "
            "VALUES($1,$2,$3,$4,$5,$6,'pending') ON CONFLICT(id) DO NOTHING",
            invoice_id, uid, tokens, amount_rub, amount_usd, provider)

async def db_confirm_invoice(invoice_id):
    async with pg().acquire() as c:
        row = await c.fetchrow(
            "UPDATE invoices SET status='paid',paid_at=NOW() WHERE id=$1 AND status='pending' RETURNING *",
            invoice_id)
        return dict(row) if row else None

async def db_get_invoice(invoice_id):
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT * FROM invoices WHERE id=$1", invoice_id)
        return dict(row) if row else None

async def db_add_payout_request(uid, amount_rub, method, details) -> int:
    async with pg().acquire() as c:
        return await c.fetchval(
            "INSERT INTO payout_requests(uid,amount_rub,method,details) VALUES($1,$2,$3,$4) RETURNING id",
            uid, amount_rub, method, details)

async def db_log_action(uid, action: str, detail: str = ""):
    async with pg().acquire() as c:
        await c.execute("INSERT INTO user_action_log(uid,action,detail) VALUES($1,$2,$3)", uid, action, detail)

async def db_get_user_actions(uid, limit=200) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT action,detail,created_at FROM user_action_log WHERE uid=$1 ORDER BY id DESC LIMIT $2", uid, limit)
        return [dict(r) for r in rows]

async def db_top_users(limit=10) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT uid, count(*) as cnt FROM user_action_log GROUP BY uid ORDER BY cnt DESC LIMIT $1", limit)
        return [dict(r) for r in rows]

async def db_top_refs(limit=10) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT referrer_id as uid, count(*) as cnt FROM referrals WHERE confirmed=1 GROUP BY referrer_id ORDER BY cnt DESC LIMIT $1", limit)
        return [dict(r) for r in rows]

async def db_payment_stats() -> dict:
    async with pg().acquire() as c:
        paid    = await c.fetchrow("SELECT count(*),COALESCE(SUM(amount_rub),0) FROM invoices WHERE status='paid'")
        payouts = await c.fetchrow("SELECT count(*),COALESCE(SUM(amount_rub),0) FROM payout_requests WHERE status='done'")
        refs    = await c.fetchval("SELECT count(*) FROM referrals WHERE confirmed=1")
        return {"payments": paid[0], "revenue_rub": float(paid[1]),
                "payouts": payouts[0], "payout_rub": float(payouts[1]), "refs": refs}

async def db_all_custom_apis() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT uid,provider,api_key,model FROM user_custom_api WHERE enabled=1")
        return [dict(r) for r in rows]

async def db_failed_requests(limit=500) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT uid,detail,created_at FROM user_action_log WHERE action='no_data' ORDER BY id DESC LIMIT $1", limit)
        return [dict(r) for r in rows]

async def db_chat_create(uid) -> int:
    async with pg().acquire() as c:
        return await c.fetchval("INSERT INTO chats(uid,title) VALUES($1,'New chat') RETURNING id", uid)

async def db_chat_set_title(chat_id, title):
    async with pg().acquire() as c:
        await c.execute("UPDATE chats SET title=$1 WHERE id=$2", title[:50], chat_id)

async def db_chat_delete(chat_id):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM chat_messages WHERE chat_id=$1", chat_id)
        await c.execute("DELETE FROM chats WHERE id=$1", chat_id)

async def db_chat_list(uid) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT id,title,created_at FROM chats WHERE uid=$1 ORDER BY id DESC", uid)
        return [dict(r) for r in rows]

async def db_chat_msg_count(chat_id) -> int:
    async with pg().acquire() as c:
        return await c.fetchval("SELECT count(*) FROM chat_messages WHERE chat_id=$1", chat_id) or 0

async def db_chat_push(chat_id, uid, role, content):
    async with pg().acquire() as c:
        await c.execute("INSERT INTO chat_messages(chat_id,uid,role,content) VALUES($1,$2,$3,$4)", chat_id, uid, role, content)
        await c.execute(
            "DELETE FROM chat_messages WHERE chat_id=$1 AND id NOT IN "
            "(SELECT id FROM chat_messages WHERE chat_id=$1 ORDER BY id DESC LIMIT 30)",
            chat_id)

async def db_chat_history(chat_id) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT role,content FROM chat_messages WHERE chat_id=$1 ORDER BY id ASC", chat_id)
        return [{"role": r["role"], "content": r["content"]} for r in rows]

async def db_chat_get(chat_id):
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT * FROM chats WHERE id=$1", chat_id)
        return dict(row) if row else None

async def db_get_active_chat(uid) -> int:
    async with pg().acquire() as c:
        val = await c.fetchval("SELECT active_chat_id FROM user_state WHERE uid=$1", uid)
        return val or 0

async def db_set_active_chat(uid, chat_id):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO user_state(uid,active_chat_id) VALUES($1,$2) ON CONFLICT(uid) DO UPDATE SET active_chat_id=$2",
            uid, chat_id)

async def db_export_all_history(uid) -> list:
    async with pg().acquire() as c:
        chats = await c.fetch("SELECT id,title,created_at FROM chats WHERE uid=$1 ORDER BY id ASC", uid)
        result = []
        for ch in chats:
            msgs = await c.fetch("SELECT role,content,created_at FROM chat_messages WHERE chat_id=$1 ORDER BY id ASC", ch["id"])
            result.append({"id": ch["id"], "title": ch["title"], "created_at": str(ch["created_at"]),
                           "messages": [dict(m) for m in msgs]})
        return result

async def db_poll_init(uid, chat_id, task, questions):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO code_polls(uid,chat_id,task,questions,answers,current_q,poll_msg_id,ctrl_msg_id,waiting_text) "
            "VALUES($1,$2,$3,$4,'[]',0,0,0,0) ON CONFLICT(uid) DO UPDATE SET "
            "chat_id=$2,task=$3,questions=$4,answers='[]',current_q=0,poll_msg_id=0,ctrl_msg_id=0,waiting_text=0",
            uid, chat_id, task, json.dumps(questions, ensure_ascii=False))

async def db_poll_get(uid):
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT * FROM code_polls WHERE uid=$1", uid)
        if not row: return None
        d = dict(row)
        d["questions"] = json.loads(d["questions"])
        d["answers"]   = json.loads(d["answers"])
        return d

async def db_poll_add_answer(uid, question, answer):
    d = await db_poll_get(uid)
    if not d: return
    answers = d["answers"]; answers.append({"q": question, "a": answer})
    async with pg().acquire() as c:
        await c.execute("UPDATE code_polls SET answers=$1,current_q=current_q+1 WHERE uid=$2",
                        json.dumps(answers, ensure_ascii=False), uid)

async def db_poll_set_msg(uid, msg_id):
    async with pg().acquire() as c:
        await c.execute("UPDATE code_polls SET poll_msg_id=$1 WHERE uid=$2", msg_id, uid)

async def db_poll_clear(uid):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM code_polls WHERE uid=$1", uid)

async def db_poll_set_waiting(uid, val: int):
    async with pg().acquire() as c:
        await c.execute("UPDATE code_polls SET waiting_text=$1 WHERE uid=$2", val, uid)

async def db_poll_set_ctrl(uid, ctrl_id: int):
    async with pg().acquire() as c:
        await c.execute("UPDATE code_polls SET ctrl_msg_id=$1 WHERE uid=$2", ctrl_id, uid)

async def db_save_pending(uid, bot_code, deploy_type):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO pending_deploys(uid,bot_code,deploy_type,saved_at) VALUES($1,$2,$3,$4) "
            "ON CONFLICT(uid) DO UPDATE SET bot_code=$2,deploy_type=$3,saved_at=$4",
            uid, bot_code, deploy_type, int(time.time()))

async def db_get_pending(uid):
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT * FROM pending_deploys WHERE uid=$1", uid)
        return dict(row) if row else None

async def db_clear_pending(uid):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM pending_deploys WHERE uid=$1", uid)

async def db_set_active(uid, deploy_type, token, container_id, sandbox_path, expires_at):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO active_deploys(uid,deploy_type,token,container_id,sandbox_path,expires_at,deployed_at) "
            "VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(uid) DO UPDATE SET "
            "deploy_type=$2,token=$3,container_id=$4,sandbox_path=$5,expires_at=$6,deployed_at=$7",
            uid, deploy_type, token, container_id, sandbox_path, expires_at, int(time.time()))

async def db_get_active(uid):
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT * FROM active_deploys WHERE uid=$1", uid)
        return dict(row) if row else None

async def db_del_active(uid):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM active_deploys WHERE uid=$1", uid)

async def db_all_active() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT * FROM active_deploys")
        return [dict(r) for r in rows]

async def db_used_slots() -> set:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT token FROM active_deploys WHERE deploy_type IN ('bot','tg_bot')")
        used = set()
        for r in rows:
            for i, t in enumerate(PREVIEW_TOKENS):
                if t == r["token"]: used.add(i)
        return used

async def db_queue_add(uid, code):
    async with pg().acquire() as c:
        await c.execute("INSERT INTO deploy_queue(uid,bot_code,queued_at) VALUES($1,$2,$3)", uid, code, int(time.time()))

async def db_queue_pop():
    async with pg().acquire() as c:
        row = await c.fetchrow(
            "DELETE FROM deploy_queue WHERE id=(SELECT id FROM deploy_queue ORDER BY queued_at ASC LIMIT 1) RETURNING *")
        return dict(row) if row else None

async def db_queue_size() -> int:
    async with pg().acquire() as c:
        return await c.fetchval("SELECT count(*) FROM deploy_queue") or 0

async def redis_get(key: str):
    val = await rdb().get(key)
    return json.loads(val) if val else None

async def redis_set(key: str, val, ttl: int = 0):
    s = json.dumps(val, ensure_ascii=False)
    if ttl: await rdb().setex(key, ttl, s)
    else:   await rdb().set(key, s)

async def redis_del(key: str):
    await rdb().delete(key)

async def check_rate_limit(uid: int, action: str = "msg", limit: int = 10, window: int = 60) -> bool:
    key = f"rate:{action}:{uid}:{int(time.time() / window)}"
    count = await rdb().incr(key)
    if count == 1: await rdb().expire(key, window)
    return count <= limit

async def analytics_today() -> dict:
    async with pg().acquire() as c:
        new_users = await c.fetchval("SELECT count(*) FROM users WHERE joined_at >= NOW() - INTERVAL '1 day'") or 0
        requests  = await c.fetchval("SELECT count(*) FROM user_action_log WHERE action='message' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        revenue   = await c.fetchval("SELECT COALESCE(SUM(amount_rub),0) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        errors    = await c.fetchval("SELECT count(*) FROM user_action_log WHERE action='no_data' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        deploys   = await c.fetchval("SELECT count(*) FROM user_action_log WHERE action LIKE 'deploy%' AND created_at >= NOW() - INTERVAL '1 day'") or 0
        payments  = await c.fetchval("SELECT count(*) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '1 day'") or 0
    return {"new_users":new_users,"requests":requests,"revenue":float(revenue),"errors":errors,"deploys":deploys,"payments":payments}

async def analytics_week() -> dict:
    async with pg().acquire() as c:
        new_users  = await c.fetchval("SELECT count(*) FROM users WHERE joined_at >= NOW() - INTERVAL '7 days'") or 0
        requests   = await c.fetchval("SELECT count(*) FROM user_action_log WHERE action='message' AND created_at >= NOW() - INTERVAL '7 days'") or 0
        revenue    = await c.fetchval("SELECT COALESCE(SUM(amount_rub),0) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '7 days'") or 0
        new_payers = await c.fetchval("SELECT count(DISTINCT uid) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '7 days'") or 0
    return {"new_users":new_users,"requests":requests,"revenue":float(revenue),"new_payers":new_payers}

async def analytics_month() -> dict:
    async with pg().acquire() as c:
        total_users = await c.fetchval("SELECT count(*) FROM users") or 0
        revenue     = await c.fetchval("SELECT COALESCE(SUM(amount_rub),0) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '30 days'") or 0
        total_refs  = await c.fetchval("SELECT count(*) FROM referrals WHERE confirmed=1") or 0
        avg_check   = await c.fetchval("SELECT AVG(amount_rub) FROM invoices WHERE status='paid' AND created_at >= NOW() - INTERVAL '30 days'") or 0
        arpu = float(revenue) / max(int(total_users), 1)
    return {"total_users":total_users,"revenue":float(revenue),"total_refs":total_refs,"avg_check":float(avg_check),"arpu":arpu}

async def analytics_ltv(limit=15) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT i.uid, COALESCE(u.username,'') as username,
                   SUM(i.amount_rub) as total_spent, COUNT(*) as payments_count, AVG(i.amount_rub) as avg_check
            FROM invoices i LEFT JOIN users u ON u.uid=i.uid
            WHERE i.status='paid' GROUP BY i.uid, u.username ORDER BY total_spent DESC LIMIT $1""", limit)
        return [dict(r) for r in rows]

async def analytics_daily_activity(days=14) -> list:
    try:
        days = max(1, min(int(days), 365))
    except (TypeError, ValueError):
        days = 14
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT DATE(created_at) as day, COUNT(*) as total,
                   COUNT(DISTINCT uid) as uniq_users,
                   SUM(CASE WHEN action='no_data' THEN 1 ELSE 0 END) as failed
            FROM user_action_log WHERE created_at >= NOW() - ($1 * INTERVAL '1 day')
            GROUP BY day ORDER BY day""", days)
        return [dict(r) for r in rows]

async def analytics_revenue_by_plan() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT tokens, COUNT(*) as purchases, SUM(amount_rub) as total_rub, AVG(amount_rub) as avg_rub
            FROM invoices WHERE status='paid' GROUP BY tokens ORDER BY total_rub DESC""")
        return [dict(r) for r in rows]

async def analytics_provider_stats() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT provider, COUNT(*) as users FROM user_custom_api WHERE enabled=1 GROUP BY provider ORDER BY users DESC")
        return [dict(r) for r in rows]

async def analytics_success_rate() -> float:
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT SUM(found_reqs)::float / NULLIF(SUM(total_reqs),0) * 100 as rate FROM user_stats")
        return round(float(row["rate"] or 0), 1)

async def analytics_ref_quality(limit=10) -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT r.referrer_id, COALESCE(u.username,'') as username,
                   COUNT(DISTINCT r.uid) as referrals, COALESCE(SUM(e.amount_rub),0) as earned
            FROM referrals r LEFT JOIN users u ON u.uid=r.referrer_id
            LEFT JOIN ref_earnings e ON e.referrer_id=r.referrer_id
            WHERE r.confirmed=1 GROUP BY r.referrer_id, u.username ORDER BY earned DESC LIMIT $1""", limit)
        return [dict(r) for r in rows]

async def analytics_error_patterns(days=7, limit=10) -> list:
    try:
        days = max(1, min(int(days), 365))
        limit = max(1, min(int(limit), 100))
    except (TypeError, ValueError):
        days, limit = 7, 10
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT SUBSTRING(detail,1,80) as pattern, COUNT(*) as cnt
            FROM user_action_log WHERE action='no_data' AND created_at >= NOW() - ($1 * INTERVAL '1 day')
            GROUP BY pattern ORDER BY cnt DESC LIMIT $2""", days, limit)
        return [dict(r) for r in rows]

async def analytics_deploy_stats() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("""
            SELECT deploy_type, COUNT(*) as deploys FROM user_action_log
            WHERE action LIKE 'deploy%' AND created_at >= NOW() - INTERVAL '7 days'
            GROUP BY deploy_type ORDER BY deploys DESC""")
        return [dict(r) for r in rows]

async def analytics_payment_provider_split() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT provider, COUNT(*) as cnt, SUM(amount_rub) as total FROM invoices WHERE status='paid' GROUP BY provider ORDER BY total DESC")
        return [dict(r) for r in rows]

async def db_checkpoint_get(chat_id) -> str:
    async with pg().acquire() as c:
        val = await c.fetchval("SELECT summary FROM chat_checkpoints WHERE chat_id=$1", chat_id)
        return val or ""

async def db_checkpoint_set(chat_id, summary: str):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO chat_checkpoints(chat_id,summary,updated_at) VALUES($1,$2,NOW()) "
            "ON CONFLICT(chat_id) DO UPDATE SET summary=$2,updated_at=NOW()",
            chat_id, summary)

async def db_checkpoint_delete(chat_id):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM chat_checkpoints WHERE chat_id=$1", chat_id)

async def db_init_promo_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS promotions(
        id BIGSERIAL PRIMARY KEY,
        tokens BIGINT NOT NULL,
        discount_pct INT NOT NULL DEFAULT 0,
        enabled INT NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS promo_codes(
        id BIGSERIAL PRIMARY KEY,
        code TEXT UNIQUE NOT NULL,
        type TEXT NOT NULL DEFAULT 'tokens',
        tokens_amount BIGINT DEFAULT 0,
        discount_pct INT DEFAULT 0,
        tokens_filter BIGINT DEFAULT 0,
        max_activations INT NOT NULL DEFAULT 1,
        activations INT NOT NULL DEFAULT 0,
        enabled INT NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS promo_activations(
        id BIGSERIAL PRIMARY KEY,
        uid BIGINT NOT NULL,
        promo_code_id BIGINT NOT NULL,
        activated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(uid, promo_code_id)
    );
    """
    async with pg().acquire() as c:
        await c.execute(sql)

async def db_get_all_promotions() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT * FROM promotions ORDER BY tokens ASC")
        return [dict(r) for r in rows]

async def db_get_active_promotions() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT * FROM promotions WHERE enabled=1 ORDER BY tokens ASC")
        return [dict(r) for r in rows]

async def db_set_promotion(tokens: int, discount_pct: int, enabled: int):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO promotions(tokens, discount_pct, enabled) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            tokens, discount_pct, enabled)
        await c.execute(
            "UPDATE promotions SET discount_pct=$1, enabled=$2 WHERE tokens=$3",
            discount_pct, enabled, tokens)

async def db_toggle_promotion(tokens: int) -> bool:
    async with pg().acquire() as c:
        row = await c.fetchrow("SELECT enabled FROM promotions WHERE tokens=$1", tokens)
        if not row: return False
        new_val = 0 if row["enabled"] else 1
        await c.execute("UPDATE promotions SET enabled=$1 WHERE tokens=$2", new_val, tokens)
        return bool(new_val)

async def db_delete_promotion(tokens: int):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM promotions WHERE tokens=$1", tokens)

async def db_create_promo_code(code: str, type_: str, tokens_amount: int,
                                discount_pct: int, tokens_filter: int, max_activations: int):
    async with pg().acquire() as c:
        await c.execute(
            "INSERT INTO promo_codes(code,type,tokens_amount,discount_pct,tokens_filter,max_activations,activations,enabled) "
            "VALUES($1,$2,$3,$4,$5,$6,0,1)",
            code.upper().strip(), type_, tokens_amount, discount_pct, tokens_filter, max_activations)

async def db_get_all_promo_codes() -> list:
    async with pg().acquire() as c:
        rows = await c.fetch("SELECT * FROM promo_codes ORDER BY created_at DESC")
        return [dict(r) for r in rows]

async def db_get_promo_code(code: str):
    async with pg().acquire() as c:
        row = await c.fetchrow(
            "SELECT * FROM promo_codes WHERE code=$1 AND enabled=1",
            code.upper().strip())
        return dict(row) if row else None

async def db_delete_promo_code(code_id: int):
    async with pg().acquire() as c:
        await c.execute("DELETE FROM promo_codes WHERE id=$1", code_id)

async def db_disable_promo_code(code_id: int):
    async with pg().acquire() as c:
        await c.execute("UPDATE promo_codes SET enabled=0 WHERE id=$1", code_id)

async def db_enable_promo_code(code_id: int):
    async with pg().acquire() as c:
        await c.execute("UPDATE promo_codes SET enabled=1 WHERE id=$1", code_id)

async def db_activate_promo_code(uid: int, promo: dict) -> tuple:
    async with pg().acquire() as c:
        used = await c.fetchval(
            "SELECT 1 FROM promo_activations WHERE uid=$1 AND promo_code_id=$2",
            uid, promo["id"])
        if used:
            return False, "Вы уже активировали этот промокод."
        if promo["activations"] >= promo["max_activations"]:
            return False, "Промокод исчерпал лимит активаций."
        await c.execute(
            "INSERT INTO promo_activations(uid, promo_code_id) VALUES($1,$2)",
            uid, promo["id"])
        await c.execute(
            "UPDATE promo_codes SET activations=activations+1 WHERE id=$1",
            promo["id"])
        if promo["type"] == "tokens":
            await c.execute(
                "INSERT INTO user_balance(uid,tokens) VALUES($1,$2) "
                "ON CONFLICT(uid) DO UPDATE SET tokens=user_balance.tokens+$2",
                uid, promo["tokens_amount"])
            return True, f"✅ На ваш баланс зачислено <b>{promo['tokens_amount']:,}</b> токенов!"
        else:
            import json as _json
            import redis.asyncio as _aioredis
            _r = _aioredis.from_url(REDIS_DSN, decode_responses=True)
            discount_data = _json.dumps({
                "discount_pct": promo["discount_pct"],
                "tokens_filter": promo["tokens_filter"],
                "code_id": promo["id"],
                "code": promo["code"],
            })
            await _r.set(f"promo_discount:{uid}", discount_data, ex=86400 * 30)
            tf = promo["tokens_filter"]
            scope = f"на тариф {tf:,} токенов" if tf else "на все тарифы"
            return True, (
                f"✅ Промокод активирован! Скидка <b>{promo['discount_pct']}%</b> "
                f"{scope} применена к вашему аккаунту на 30 дней."
            )

async def db_get_user_discount(uid: int) -> dict | None:
    import json as _json
    import redis.asyncio as _aioredis
    try:
        _r = _aioredis.from_url(REDIS_DSN, decode_responses=True)
        raw = await _r.get(f"promo_discount:{uid}")
        return _json.loads(raw) if raw else None
    except Exception:
        return None