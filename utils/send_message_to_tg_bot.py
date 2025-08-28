import os
import httpx
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") 
TELEGRAM_CHAT_ID = "752697899"
async def send_message_to_tg_bot(text: str):
    """
    Отправляет сообщение в Telegram-бота на указанный чат.
    """
    if not TELEGRAM_BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN не задан в переменных окружения")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text[:4096],
        "parse_mode": "HTML"
    }
    async with httpx.AsyncClient() as client:
        await client.post(url, data=payload)