import logging
import httpx
import os
import re
from dotenv import load_dotenv
import openai
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
from dateutil import parser

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
APPOINTMENTS_API_KEY = os.getenv("APPOINTMENTS_API_KEY") or 'CNvy6w6CRR1QLY2V6eq6gDQT'

CITY_IDS = [
    "0f2f2d09-8e7a-4356-bd4d-0b055d802e7b",
    "5f290be7-14ff-4ccd-8bc8-2871a9ca9d5f"
]
CLINICS_API_URL = "https://apitest.mrtexpert.ru/api/v3/clinics"
APPOINTMENTS_API_URL_V3 = "https://apitest.mrtexpert.ru/api/v3/appointments"

def extract_scheduled_at(message: str) -> str:
    """
    Извлекает дату и время appointment из текста уведомления.
    Возвращает строку в формате 'YYYY-MM-DD HH:MM' или None, если не найдено.
    """
    import re
    from datetime import datetime, date

    # 1. Поиск формата "на YYYY-MM-DD HH:MM"
    match = re.search(r"на (\d{4}-\d{2}-\d{2} \d{2}:\d{2})", message)
    if match:
        return match.group(1)
    # 2. Поиск формата "на DD.MM.YYYY в HH:MM"
    match = re.search(r"на (\d{2}\.\d{2}\.\d{4}) в (\d{2}:\d{2})", message)
    if match:
        # Преобразуем в ISO формат
        dt = datetime.strptime(f"{match.group(1)} {match.group(2)}", "%d.%m.%Y %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M")
    # 3. Поиск "сегодня в HH:MM"
    match = re.search(r"сегодня в (\d{2}:\d{2})", message)
    if match:
        today = date.today().strftime("%Y-%m-%d")
        return f"{today} {match.group(1)}"
    return None

def normalize_dt(dt_str):
    """
    Приводит строку даты-времени к формату 'YYYY-MM-DD HH:MM'.
    """
    dt = parser.parse(dt_str)
    return dt.strftime("%Y-%m-%d %H:%M")

async def find_item_id_by_scheduled_at(scheduled_at: str, token: str) -> str:
    """
    Ищет item_id по scheduled_at среди всех appointments.
    Возвращает id найденного item или None.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(APPOINTMENTS_API_URL_V3, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        resp.raise_for_status()
        appointments = resp.json().get("items", [])
        for appt in appointments:
            for item in appt.get("items", []):
                item_dt = normalize_dt(item.get("scheduled_at", ""))
                if item_dt == scheduled_at:
                    return item.get("id")
    return None

async def confirm_appointment_by_message(message: str):
    """
    Находит item_id по сообщению и подтверждает appointment (PATCH status=confirmed).
    Теперь отправляет весь объект appointment с обновлённым статусом item.
    """
    scheduled_at = extract_scheduled_at(message)
    if not scheduled_at:
        print("Не удалось извлечь дату/время из сообщения")
        return
    # Получить все appointment-ы по нужным городам
    appointments = get_all_appointments()
    found = None
    for appt in appointments:
        for item in appt.get("items", []):
            
            item_dt = normalize_dt(item.get("scheduled_at", ""))
            print(item_dt, scheduled_at)
            if item_dt == scheduled_at:
                
                found = (appt, item)
                break
        if found:
            break
    if not found:
        print("Не найден item с таким scheduled_at")
        return
    appt, item = found
    # Обновить статус только нужного item
    new_items = []
    for it in appt["items"]:
        if normalize_dt(it.get("scheduled_at", "")) == scheduled_at:
            it = {**it, "status": "confirmed"}
        new_items.append(it)
    # Собрать новое тело appointment только с нужными полями
    # patient: только firstname, lastname, middlename, birthdate, sex, phone, email, snils, email_confirm (если есть)
    patient = appt.get("patient", {})
    patch_patient = {
        "firstname": patient.get("firstname", ""),
        "lastname": patient.get("lastname", ""),
        "middlename": patient.get("middlename", ""),
        "birthdate": patient.get("birthdate", ""),
        "sex": patient.get("sex", ""),
        "phone": patient.get("phone", ""),
        "email": patient.get("email", ""),
        "snils": patient.get("snils", "")
    }
    if "email_confirm" in patient:
        patch_patient["email_confirm"] = patient["email_confirm"]
    # items: только нужные поля
    patch_items = []
    for it in new_items:
        provider_id = it.get("provider_id") or (it.get("provider") or {}).get("id")
        if provider_id == "00000000-0000-0000-0000-000000000000":
            provider_id = ""
        patch_item = {
            "service_id": it.get("service_id") or (it.get("service") or {}).get("id"),
            "scheduled_at": it.get("scheduled_at"),
            "status": it.get("status"),
            "provider_id": provider_id,
            "refdoctor_id": it.get("refdoctor_id") or (it.get("refdoctor") or {}).get("id"),
            "doctor_id": it.get("doctor_id") or (it.get("doctor") or {}).get("id"),
            "partners_finances": it.get("partners_finances", False)
        }
        patch_items.append(patch_item)
    patch_body = {
        "clinic_id": appt.get("clinic").get("id"),
        "patient_id": appt.get("patient").get("id") or "",
        "patient": patch_patient,
        "items": patch_items
    }
    patch_url = f"{APPOINTMENTS_API_URL_V3}/{appt.get('id')}"
    print(patch_url, patch_body)
    async with httpx.AsyncClient() as client:
        patch_resp = await client.patch(
            patch_url,
            json=patch_body,
            headers={"Authorization": f"Bearer {APPOINTMENTS_API_KEY}", "Content-Type": "application/json"}
        )
        patch_resp.raise_for_status()
        print(f"Item {item.get('id')} подтвержден (appointment обновлён)")

def get_greenapi_chat_history(chat_id, count=20):
    """
    Получить историю сообщений из GreenAPI по chat_id
    """
    url = f"https://api.green-api.com/waInstance{GREENAPI_ID}/GetChatHistory/{GREENAPI_TOKEN}"
    payload = {"chatId": chat_id, "count": count}
    try:
        resp = httpx.post(url, json=payload, timeout=10, headers={"Content-Type": "application/json"})
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
    message = ""
    msg_data = body.get("messageData", {})
    if msg_data.get("typeMessage") == "textMessage":
        message = msg_data.get("textMessageData", {}).get("textMessage", "")
    elif msg_data.get("typeMessage") == "extendedTextMessage":
        message = msg_data.get("extendedTextMessageData", {}).get("text", "")

    sender_chat_id = body.get("senderData", {}).get("chatId", "")
    sender_name = body.get("senderData", {}).get("senderName", "")

    if not message or not sender_chat_id:
        logger.warning("Нет текста или sender_chat_id")
        return {"status": "no content"}
    phone = sender_chat_id.replace("@c.us", "")
    formatted_phone = f"+{phone}"

    try:
        async with httpx.AsyncClient() as client:
            # --- Используем новую функцию для получения всех контактов ---
            contacts = await get_all_chatwoot_contacts(client, CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID, CHATWOOT_API_KEY)
            contact = next((c for c in contacts if c["phone_number"] == formatted_phone), None)

            if not contact:
                print({"name": sender_name, "phone_number": formatted_phone})
                contact_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                    json={"name": "afruz" or sender_name, "phone_number": formatted_phone},
                    headers={"api_access_token": CHATWOOT_API_KEY,"Content-Type": "application/json" }
                )
                if contact_resp.status_code == 422 and "Phone number has already been taken" in contact_resp.text:
                    contacts = await get_all_chatwoot_contacts(client, CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID, CHATWOOT_API_KEY)
                    contact = next((c for c in contacts if c["phone_number"] == formatted_phone), None)
                    if contact:
                        contact_id = contact["id"]
                    else:
                        raise Exception("Контакт с этим номером уже есть, но не найден в списке.")
                else:
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
                headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
            )
            convs_resp.raise_for_status()
            conversations = convs_resp.json().get("payload", [])

            if conversations:
                conversation_id = conversations[0]["id"]
                # Назначить оператора 3 на conversation
                await client.patch(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}",
                    json={"assignee_id": 3},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                msg_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": message, "message_type": "incoming"},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
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
                
                operator_connect = False
                operator_message = None
                try:
                    parsed = json.loads(ai_reply)
                    print(parsed)
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
                        headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                    )
                    logger.info(f"Оповещение операторов: {notify_text}")
                    await unassign_conversation(phone)
                else:
                    if ai_reply:
                        print("Ai reply:", ai_reply)
                        # --- Обработка подтверждения ---
                        try:
                            parsed = json.loads(ai_reply)
                            if isinstance(parsed, dict) and parsed.get("type") == "confirm":
                                # 1. Отправить благодарность в Chatwoot
                                thank_you_msg = "Спасибо за потверждение записи.\nЖдем вас за 15 минут до приема"
                                ai_msg_resp = await client.post(
                                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                                    json={"content": thank_you_msg, "message_type": "outgoing"},
                                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                                )
                                ai_msg_resp.raise_for_status()
                                logger.info("Благодарность отправлена в разговор %s", conversation_id)
                                # 2. Подтвердить appointment через API
                                from os import getenv
                                token = getenv("APPOINTMENTS_API_KEY")
                                if token:
                                    await confirm_appointment_by_message(parsed.get("message", ""))
                                else:
                                    logger.warning("APPOINTMENTS_API_KEY не задан, не могу подтвердить appointment")
                                return  # Не отправлять ai_reply как обычный ответ
                        except Exception as e:
                            logger.warning(f"Ошибка при обработке подтверждения: {e}")
                        # --- Обычный AI ответ ---
                        # Только если не confirm
                        if not (isinstance(ai_reply, str) and ai_reply.strip().startswith('{')):
                            ai_msg_resp = await client.post(
                                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                                json={"content": ai_reply, "message_type": "outgoing"},
                        headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
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
                        "status": "open",
                        "assignee_id": 3
                    },
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                conv_resp.raise_for_status()
                new_conv = conv_resp.json()
                logger.info("Создан новый разговор: %s", new_conv)
                conversation_id = new_conv.get("id")
                if conversation_id:
                    # 1. Синхронизируем историю из GreenAPI в Chatwoot
                    greenapi_history = get_greenapi_chat_history(sender_chat_id, count=50)
                    print(greenapi_history)
                    for msg in reversed(greenapi_history):
                        content = msg.get("textMessage", "")
                        if not content:
                            continue
                        msg_type = "incoming" if msg.get("type") == "incoming" else "outgoing"
                        await client.post(
                            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                            json={"content": content, "message_type": msg_type},
                            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                        )
                    # 2. Добавляем текущее входящее сообщение пользователя
                    msg_resp = await client.post(
                        f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                        json={"content": message, "message_type": "incoming"},
                        headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
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
                            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
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
                                headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
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
            model="gpt-4-turbo",
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
            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
        )
        contacts = contacts_resp.json().get("payload", [])
        contact = next((c for c in contacts if c["phone_number"] == f'+{phone}'), None)
        if not contact:
            return
        contact_id = contact["id"]
        # Найти conversation
        convs_resp = await client.get(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
        )
        conversations = convs_resp.json().get("payload", [])
        if not conversations:
            return
        conversation_id = conversations[0]["id"]
        # Снять назначение
        await client.patch(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}",
            json={"assignee_id": None, "status": "open"},
            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
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

def get_all_appointments():
    """
    Получает все заявки по всем клиникам из городов из CITY_IDS.
    Возвращает объединённый массив всех appointment-ов.
    """
    auth_header = {"Authorization": f"Bearer {APPOINTMENTS_API_KEY}", "Content-Type": "application/json"}
    # Получить все клиники одним запросом
    try:
        resp = httpx.get(CLINICS_API_URL, timeout=20, headers=auth_header)
        resp.raise_for_status()
        all_clinics = resp.json().get("result", [])
    except Exception as e:
        logger.error(f"Ошибка при получении всех клиник: {e}")
        all_clinics = []
    # Оставить только нужные города
    clinics = [c for c in all_clinics if c.get("city_id") in CITY_IDS]
    all_appointments = []
    for clinic in clinics:
        cid = clinic.get("id")
        if not cid:
            continue
        try:
            app_url = f"{APPOINTMENTS_API_URL_V3}?clinic_id={cid}"
            app_resp = httpx.get(app_url, timeout=20, headers=auth_header)
            app_resp.raise_for_status()
            appointments = app_resp.json().get("result", [])
            all_appointments.extend(appointments)
        except Exception as e:
            logger.error(f"Ошибка при получении заявок для клиники {cid}: {e}")
    return all_appointments 

# --- ДОБАВИТЬ: Получение всех контактов с пагинацией ---
async def get_all_chatwoot_contacts(client, base_url, account_id, api_key):
    contacts = []
    page = 1
    while True:
        resp = await client.get(
            f"{base_url}/api/v1/accounts/{account_id}/contacts",
            params={"page": page},
            headers={"api_access_token": api_key, "Content-Type": "application/json"}
        )
        resp.raise_for_status()
        data = resp.json()
        payload = data.get("payload", [])
        if not payload:
            break
        contacts.extend(payload)
        # Если меньше 15 на странице — это последняя страница
        if len(payload) < 15:
            break
        page += 1
    return contacts 