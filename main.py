import logging
from fastapi import FastAPI, Request
import httpx
from dotenv import load_dotenv
import os

logger = logging.getLogger("uvicorn.webhook")
logging.basicConfig(level=logging.INFO)
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
    logger.info("Получен вебхук: %s", body)

    if body.get("typeWebhook") != "incomingMessageReceived":
        logger.info("Пропущен вебхук не того типа")
        return {"status": "ignored"}

    message = body.get("body", {}).get("messageData", {}).get("textMessageData", {}).get("textMessage", "")
    sender_chat_id = body.get("body", {}).get("senderData", {}).get("chatId", "")
    sender_name = body.get("body", {}).get("senderData", {}).get("senderName", "")

    if not message or not sender_chat_id:
        logger.warning("Нет текста или sender_chat_id")
        return {"status": "no content"}

    phone = sender_chat_id.replace("@c.us", "")
    formatted_phone = f"+{phone}"

    try:
        async with httpx.AsyncClient() as client:
            # 1. Получить список всех контактов
            contacts_resp = await client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                headers={"api_access_token": CHATWOOT_API_KEY}
            )
            contacts_resp.raise_for_status()
            contacts = contacts_resp.json().get("payload", [])
            contact = next((c for c in contacts if c["phone_number"] == formatted_phone), None)

            if not contact:
                # 2. Контакт не найден — создаём
                contact_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                    json={"name": sender_name, "phone_number": formatted_phone},
                    headers={"api_access_token": CHATWOOT_API_KEY}
                )
                contact_resp.raise_for_status()
                contact_json = contact_resp.json()
                logger.info("Создан контакт: %s", contact_json)

                # Получить ID контакта
                contact_id = (
                    contact_json.get("id")
                    or contact_json.get("payload", {}).get("contact", {}).get("id")
                    or contact_json.get("contact", {}).get("id")
                )
                if not contact_id:
                    raise Exception(f"Не удалось определить contact_id: {contact_json}")
            else:
                contact_id = contact["id"]

            # 3. Получить разговоры контакта
            convs_resp = await client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                headers={"api_access_token": CHATWOOT_API_KEY}
            )
            convs_resp.raise_for_status()
            conversations = convs_resp.json().get("payload", [])

            if conversations:
                # 4. Разговор найден — отправляем сообщение
                conversation_id = conversations[0]["id"]
                msg_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": message, "message_type": "incoming"},
                    headers={"api_access_token": CHATWOOT_API_KEY}
                )
                msg_resp.raise_for_status()
                logger.info("Отправлено сообщение в существующий разговор %s: %s", conversation_id, msg_resp.text)
            else:
                # 5. Нет разговора — создаём корректно
                conv_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations",
                    json={
                        "inbox_id": int(CHATWOOT_INBOX_ID),
                        "contact_id": contact_id,
                        "source_id": sender_chat_id,
                        "additional_attributes": {},
                        "status": "open"
                    },
                    headers={"api_access_token": CHATWOOT_API_KEY}
                )
                conv_resp.raise_for_status()
                new_conv = conv_resp.json()
                logger.info("Создан новый разговор: %s", new_conv)

                conversation_id = new_conv.get("id")
                if conversation_id:
                    msg_resp = await client.post(
                        f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                        json={"content": message, "message_type": "incoming"},
                        headers={"api_access_token": CHATWOOT_API_KEY}
                    )
                    msg_resp.raise_for_status()
                    logger.info("Сообщение добавлено в новый разговор %s", conversation_id)
                else:
                    logger.warning("Не удалось получить ID созданного разговора.")

    except Exception as e:
        logger.exception("Ошибка: %s", e)
        return {"status": "error", "detail": str(e)}

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


@app.post("/test/send-to-chatwoot")
async def test_send_to_chatwoot():
    test_sender_chat_id = "79998887766@c.us"
    test_sender_name = "Тестовый Клиент"
    test_message = "Привет! Это тестовое сообщение от GreenAPI → Chatwoot"

    payload = {
        "source_id": test_sender_chat_id,
        "inbox_id": int(CHATWOOT_INBOX_ID),
        "contact": {
            "name": test_sender_name,
            "phone_number": test_sender_chat_id.replace("@c.us", "")
        },
        "messages": [
            {
                "content": test_message,
                "message_type": "incoming"
            }
        ]
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations",
            json=payload,
            headers={"Authorization": f"Bearer {CHATWOOT_API_KEY}"}
        )
        print(response)

    return {
        "status": "test sent",
        "chatwoot_response": response.status_code,
        "chatwoot_response_body": response.json()
    }