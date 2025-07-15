import logging
import httpx
import os
import re
from dotenv import load_dotenv
import openai
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json

load_dotenv()

logger = logging.getLogger("uvicorn.webhook")
logging.basicConfig(level=logging.INFO)

GREENAPI_ID = os.getenv("GREENAPI_ID")
OPEN_API_KEY = os.getenv("OPENAI_API_KEY")
GREENAPI_TOKEN = os.getenv("GREENAPI_TOKEN")
CHATWOOT_API_KEY = os.getenv("CHATWOOT_API_KEY")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL")

def get_greenapi_chat_history(chat_id, count=20):
    """
    Получить историю сообщений из GreenAPI по chat_id
    """
    url = f"https://api.green-api.com/waInstance{GREENAPI_ID}/GetChatHistory/{GREENAPI_TOKEN}"
    payload = {"chatId": chat_id, "count": count}
    try:
        resp = httpx.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()  # список сообщений
    except Exception as e:
        logger.error(f"Ошибка при получении истории из GreenAPI: {e}")
        return []

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
                # --- AI обработка ---
                greenapi_history = get_greenapi_chat_history(sender_chat_id)
                system_prompt = fetch_google_doc_text() or "You are a helpful assistant."
                gpt_messages = [{"role": "system", "content": system_prompt}]
                for msg in reversed(greenapi_history):
                    if msg.get("type") == "incoming":
                        gpt_messages.append({"role": "user", "content": msg.get("textMessage", "")})
                    elif msg.get("type") == "outgoing":
                        gpt_messages.append({"role": "assistant", "content": msg.get("textMessage", "")})
                if not any(m.get("content") == message for m in gpt_messages if m["role"] == "user"):
                    gpt_messages.append({"role": "user", "content": message})
                ai_reply = await call_ai_service(gpt_messages)
                # ai_reply = {}
                # --- Проверка на operator_connect ---
                operator_connect = False
                operator_message = None
                try:
                    parsed = json.loads(ai_reply)
                    if isinstance(parsed, dict) and parsed.get("type") == "operator_connect":
                        operator_connect = True
                        operator_message = parsed.get("message") or "Клиенту требуется оператор."
                except Exception:
                    pass
                if operator_connect:
                    # Отправить сообщение всем операторам (в чат)
                    notify_text = f"{operator_message}"
                    await client.post(
                        f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                        json={"content": notify_text, "message_type": "outgoing"},
                        headers={"api_access_token": CHATWOOT_API_KEY}
                    )
                    logger.info(f"Оповещение операторов: {notify_text}")
                    await unassign_conversation(phone)
                else:
                    if ai_reply:
                        print("Ai reply:",ai_reply)
                        ai_msg_resp = await client.post(
                            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                            json={"content": ai_reply, "message_type": "outgoing"},
                            headers={"api_access_token": CHATWOOT_API_KEY}
                        )
                        ai_msg_resp.raise_for_status()
                        logger.info("AI ответ отправлен в разговор %s", conversation_id)
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
                    # --- AI обработка ---
                    # Истории нет, только system prompt и текущее сообщение
                    system_prompt = fetch_google_doc_text() or "Ты полезный ИИ ассистент помогающим пациентом МРТ клиники"
                    gpt_messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": message}
                    ]
                    ai_reply = await call_ai_service(gpt_messages)
                    # --- Проверка на operator_connect ---
                    operator_connect = False
                    operator_message = None
                    try:
                        parsed = json.loads(ai_reply)
                        if isinstance(parsed, dict) and parsed.get("type") == "operator_connect":
                            operator_connect = True
                            operator_message = parsed.get("message") or "Клиенту требуется оператор."
                    except Exception:
                        pass
                    if operator_connect:
                        # Отправить сообщение всем операторам (в чат)
                        notify_text = f"{operator_message}"
                        await client.post(
                            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                            json={"content": notify_text, "message_type": "outgoing"},
                            headers={"api_access_token": CHATWOOT_API_KEY}
                        )
                        logger.info(f"Оповещение операторов: {notify_text}")
                        await unassign_conversation(phone)
                        # Отправить сообщение в GreenAPI
                        if notify_text:
                            send_greenapi_message(f"{phone}@c.us", notify_text)
                    else:
                        if ai_reply:
                            print("Ai reply:",ai_reply)
                            ai_msg_resp = await client.post(
                                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                                json={"content": ai_reply, "message_type": "outgoing"},
                                headers={"api_access_token": CHATWOOT_API_KEY}
                            )
                            ai_msg_resp.raise_for_status()
                            logger.info("AI ответ отправлен в разговор %s", conversation_id)
                else:
                    logger.warning("Не удалось получить ID созданного разговора.")
    except Exception as e:
        logger.exception("Ошибка: %s", e)
        return {"status": "error", "detail": str(e)}
    return {"status": "ok"}

async def call_ai_service(messages) -> str:
    """
    Отправляет messages в OpenAI и возвращает ответ.
    """
    if not OPEN_API_KEY:
        return "[Ошибка: не задан OPEN_API_KEY]"
    client = openai.AsyncOpenAI(api_key=OPEN_API_KEY)
    try:
        response = await client.chat.completions.create(
            model="gpt-4.5-turbo",
            messages=messages,
            temperature=0.7,
            max_tokens=512
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.exception("Ошибка OpenAI: %s", e)
        return f"[Ошибка OpenAI: {e}]"

async def unassign_conversation(phone):
    async with httpx.AsyncClient() as client:
        # Найти контакт
        contacts_resp = await client.get(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
            headers={"api_access_token": CHATWOOT_API_KEY}
        )
        contacts = contacts_resp.json().get("payload", [])
        contact = next((c for c in contacts if c["phone_number"] == f'+{phone}'), None)
        if not contact:
            return
        contact_id = contact["id"]
        # Найти conversation
        convs_resp = await client.get(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
            headers={"api_access_token": CHATWOOT_API_KEY}
        )
        conversations = convs_resp.json().get("payload", [])
        if not conversations:
            return
        conversation_id = conversations[0]["id"]
        # Снять назначение
        await client.patch(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}",
            json={"assignee_id": None, "status": "open"},
            headers={"api_access_token": CHATWOOT_API_KEY}
        )

def fetch_google_doc_text():
    """
    Получить текст Google Docs по захардкоженному doc_id и GOOGLE_API_DOCS_SECRET
    """
    doc_id = "1aREZDEdWBRt0N9Fxree5sZww9v47xhlXo5ZfJEK_Hac"
    try:
        SCOPES = ['https://www.googleapis.com/auth/documents.readonly']
        credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
        service = build('docs', 'v1', credentials=credentials)
        doc = service.documents().get(documentId=doc_id).execute()
        text = ''
        for content in doc.get('body', {}).get('content', []):
            if 'paragraph' in content:
                for elem in content['paragraph'].get('elements', []):
                    text += elem.get('textRun', {}).get('content', '')
        return text
    except Exception as e:
        logger.error(f"Ошибка при получении Google Docs: {e}")
        return None 