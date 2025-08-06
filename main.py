import logging
from fastapi import FastAPI
from dotenv import load_dotenv
import os
from routes.greenapi import greenapi_router
from routes.chatwoot import chatwoot_router
from apscheduler.schedulers.background import BackgroundScheduler
import httpx
from services.cron_schedule_service import process_items_cron
from services.cron_schedule_service import cleanup_old_messages
from db import Base, engine
from models.sended_message import SendedMessage

Base.metadata.create_all(bind=engine)
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
scheduler.add_job(my_cron_job, 'cron', second='*/30')      
scheduler.add_job(cleanup_old_messages, 'cron', hour=7, minute=0) 
scheduler.start()
@app.get("/")
def root():
    return {"message": "✅ Chatwoot x GreenAPI интеграция готова"}