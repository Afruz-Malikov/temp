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
async def process_chatwoot_webhook(request):
    processed_messages = set()
    body = await request.json()
    if body.get("event") != "message_created":
        return {"status": "ignored"}
    message = body.get("content")
    sender = body.get("sender", {})
    sender_type = sender.get("type")
    chat_id = body.get("conversation", {}).get("contact_inbox", {}).get("source_id")
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
        "message": message,
        "linkPreview": False
    }
    async with httpx.AsyncClient() as client:
        await client.post(greenapi_url, json=payload)    
    return {"status": "sent"} 