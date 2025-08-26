import logging
import httpx
import os
import re
from dotenv import load_dotenv
from constant.matchers import instance_by_inbox_id
load_dotenv()
logger = logging.getLogger("uvicorn.webhook")
logging.basicConfig(level=logging.INFO)
async def process_chatwoot_webhook(request):
    processed_messages = set()
    body = await request.json()
    if body.get("event") != "message_created":
        return {"status": "ignored"}
    message = body.get("content")
    sender = body.get("sender", {})
    sender_type = sender.get("type")
    inbox_id = str(body.get("conversation", {}).get("inbox_id"))
    instance_info = instance_by_inbox_id.get(inbox_id) or {}
    chat_id = body.get("conversation", {}).get("contact_inbox", {}).get("source_id")
    message_id = body.get("id")
    if not all([message, sender_type, chat_id, message_id]):
        return {"status": "missing data"}

    if sender_type.lower() != "user":
        return {"status": "ignored"}

    if message_id in processed_messages:
        return {"status": "duplicate"}
    processed_messages.add(message_id)

    greenapi_url = f"https://api.green-api.com/waInstance{instance_info.get('id')}/SendMessage/{instance_info.get('token')}"
    payload = {
        "chatId": chat_id,
        "message": message,
        "linkPreview": False
    }
    async with httpx.AsyncClient() as client:
       resp =  await client.post(greenapi_url, json=payload)    
       logger.info(f"Sent to GreenAPI: {resp.status_code}, {resp.json()}")
    return {"status": "sent"} 