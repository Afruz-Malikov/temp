import logging
from fastapi import FastAPI
from dotenv import load_dotenv
import os
from routes.greenapi import greenapi_router
from routes.chatwoot import chatwoot_router
from apscheduler.schedulers.background import BackgroundScheduler
import httpx
from services.cron_schedule_service import process_items_cron

logger = logging.getLogger("uvicorn.webhook")
logging.basicConfig(level=logging.INFO)
load_dotenv()
app = FastAPI()
app.include_router(greenapi_router)
app.include_router(chatwoot_router)

def my_cron_job():
    try:
        logger.info("🚀 Запуск часовой крон задачи")
        process_items_cron()
        logger.info("✅ Часовая крон задача завершена")
    except Exception as e:
        logger.error(f"Ошибка при выполнении крон задачи: {e}")

scheduler = BackgroundScheduler()   
scheduler.add_job(my_cron_job, 'cron', hour='*')      

scheduler.start()

@app.get("/")
def root():
    return {"message": "✅ Chatwoot x GreenAPI интеграция готова"}