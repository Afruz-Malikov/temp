from fastapi import FastAPI, Request
import httpx
from dotenv import load_dotenv
import os
load_dotenv()

app = FastAPI()

# 🔐 Переменные окружения
GREENAPI_ID = os.getenv("GREENAPI_ID")
GREENAPI_TOKEN = os.getenv("GREENAPI_TOKEN")
CHATWOOT_API_KEY = os.getenv("CHATWOOT_API_KEY")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL")

@app.get("/")
def root():
    return {"message": "✅ Chatwoot x GreenAPI интеграция готова"}

@app.post("/greenapi/webhook")
async def greenapi_webhook(request: Request):
    body = await request.json() 
    print(body)
    if body.get("typeWebhook") != "incomingMessageReceived":
        return {"status": "ignored"}

    message = body.get("body", {}).get("messageData", {}).get("textMessageData", {}).get("textMessage", "")
    sender_chat_id = body.get("body", {}).get("senderData", {}).get("chatId", "")
    sender_name = body.get("body", {}).get("senderData", {}).get("senderName", "")

    if not message or not sender_chat_id:
        return {"status": "no content"}

    payload = {
        "source_id": sender_chat_id,
        "inbox_id": int(CHATWOOT_INBOX_ID),
        "contact": {
            "name": sender_name,
            "phone_number": sender_chat_id.replace("@c.us", "")
        },
        "messages": [
            {
                "content": message,
                "message_type": "incoming"
            }
        ]
    }

    async with httpx.AsyncClient() as client:
        await client.post(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations",
            json=payload,
            headers={"api_access_token": CHATWOOT_API_KEY}
        )

    return {"status": "ok"}

@app.post("/chatwoot/webhook")
async def chatwoot_webhook(request: Request):
    body = await request.json()
    print(body)
    message = body.get("message")
    phone_number = body.get("phone_number")

    if not message or not phone_number:
        return {"status": "missing data"}

    chat_id = phone_number + "@c.us"

    payload = {
        "chatId": chat_id,
        "message": message
    }

    greenapi_url = f"https://api.green-api.com/waInstance{GREENAPI_ID}/SendMessage/{GREENAPI_TOKEN}"

    async with httpx.AsyncClient() as client:
        await client.post(greenapi_url, json=payload)

    return {"status": "sent"}
