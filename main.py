from config import *
from database import init_db
from services import platega_webhook, health_handler, _check_expired_invoices, _analytics_worker
from handlers import router, _ttl_worker
from keyboards import tg_send

async def main():
    log.info("пошла возня")
    await init_db()
    BOTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    bot     = Bot(token=BOT_TOKEN)
    storage = RedisStorage.from_url(REDIS_SESSION_DSN)
    dp      = Dispatcher(storage=storage)
    dp.include_router(router)

    try:
        await bot.set_my_commands([
            BotCommand(command="start",   description="Главное меню"),
            BotCommand(command="s",       description="Пропустить опросы CodeMode"),
            BotCommand(command="context", description="Оптимизировать контекст чата"),
            BotCommand(command="status",  description="Статус деплоя"),
            BotCommand(command="logs",    description="Логи задеплоенного бота"),
            BotCommand(command="stop",    description="Остановить деплой"),
            BotCommand(command="panel",   description="Админ панель"),
            BotCommand(command="stats",   description="Быстрая статистика"),
        ])
    except Exception as e:
        log.warning("set_my_commands skipped: %s", e)

    asyncio.create_task(_ttl_worker())
    asyncio.create_task(_check_expired_invoices())
    asyncio.create_task(_analytics_worker())

    from aiohttp import web as _web
    _app = _web.Application()
    _app.router.add_post("/payments/platega", platega_webhook)
    _app.router.add_get("/health", health_handler)
    _runner = _web.AppRunner(_app)
    await _runner.setup()
    site = _web.TCPSite(_runner, "0.0.0.0", 8080, reuse_port=True)
    await site.start()
    log.info("Webhook + health on :8080")

    await dp.start_polling(bot, allowed_updates=["message", "callback_query", "poll_answer"])

if __name__ == "__main__":
    asyncio.run(main())
