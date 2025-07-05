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

async def process_greenapi_webhook(request):
    body = await request.json()
    logger.info("Получен вебхук: %s", body)

    if body.get("typeWebhook") != "incomingMessageReceived":
        logger.info("Пропущен вебхук не того типа")
        return {"status": "ignored"}
    message = body.get("messageData", {}).get("textMessageData", {}).get("textMessage", "")
    sender_chat_id = body.get("senderData", {}).get("chatId", "")
    sender_name = body.get("senderData", {}).get("senderName", "")

    if not message or not sender_chat_id:
        logger.warning("Нет текста или sender_chat_id")
        return {"status": "no content"}

    phone = sender_chat_id.replace("@c.us", "")
    formatted_phone = f"+{phone}"

    try:
        async with httpx.AsyncClient() as client:
            contacts_resp = await client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                headers={"api_access_token": CHATWOOT_API_KEY}
            )
            contacts_resp.raise_for_status()
            contacts = contacts_resp.json().get("payload", [])
            contact = next((c for c in contacts if c["phone_number"] == formatted_phone), None)

            if not contact:
                print(sender_name, formatted_phone)
                contact_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                    json={"name": sender_name, "phone_number": formatted_phone},
                    headers={"api_access_token": CHATWOOT_API_KEY}
                )
                contact_resp.raise_for_status()
                contact_json = contact_resp.json()
                logger.info("Создан контакт: %s", contact_json)
                contact_id = (
                    contact_json.get("id")
                    or contact_json.get("payload", {}).get("contact", {}).get("id")
                    or contact_json.get("contact", {}).get("id")
                )
                if not contact_id:
                    raise Exception(f"Не удалось определить contact_id: {contact_json}")
            else:
                contact_id = contact["id"]

            convs_resp = await client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                headers={"api_access_token": CHATWOOT_API_KEY}
            )
            convs_resp.raise_for_status()
            conversations = convs_resp.json().get("payload", [])

            if conversations:
                conversation_id = conversations[0]["id"]
                msg_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": message, "message_type": "incoming"},
                    headers={"api_access_token": CHATWOOT_API_KEY}
                )
                msg_resp.raise_for_status()
                logger.info("Отправлено сообщение в существующий разговор %s: %s", conversation_id, msg_resp.text)
            else:
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