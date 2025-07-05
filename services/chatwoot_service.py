import logging
import httpx
import os
import re
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger("uvicorn.webhook")
logging.basicConfig(level=logging.INFO)
GREENAPI_ID = os.getenv("GREENAPI_ID")
GREENAPI_TOKEN = os.getenv("GREENAPI_TOKEN")
CHATWOOT_API_KEY = os.getenv("CHATWOOT_API_KEY")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL")

def normalize_chat_id(chat_id: str) -> str:
    if chat_id.endswith('@g.us'):
        return chat_id
    if re.match(r'^\+\d{10,15}@c\.us$', chat_id):
        return chat_id
    phone = str(chat_id)
    if not phone.startswith('+'):
        phone.replace('+', '')
    return f'{phone}@c.us'

async def process_chatwoot_webhook(request):
    processed_messages = set()
    body = await request.json()
    logger.info("Получен вебхук от Chatwoot: %s", body)
    
    if body.get("event") != "message_created":
        return {"status": "ignored"}
    
    message = body.get("content")
    sender = body.get("sender", {})
    sender_type = sender.get("type")
    chat_id = normalize_chat_id( body.get("conversation", {}).get("contact_inbox", {}).get("source_id"))
    print( body.get("conversation", {}).get("contact_inbox", {}))
    message_id = body.get("id")

    if not all([message, sender_type, chat_id, message_id]):
        return {"status": "missing data"}

    if sender_type.lower() != "user":
        return {"status": "ignored"}

    if message_id in processed_messages:
        return {"status": "duplicate"}
    processed_messages.add(message_id)

    greenapi_url = f"https://api.green-api.com/waInstance{GREENAPI_ID}/SendMessage/{GREENAPI_TOKEN}"
    payload = {
        "chatId": chat_id,
        "message": message
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(greenapi_url, json=payload)
        print(payload)
        logger.info("Ответ от GreenAPI: %s", response.text)            
    return {"status": "sent"} 