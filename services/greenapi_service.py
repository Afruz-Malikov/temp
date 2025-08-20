import logging
import httpx
import os,asyncio
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import re
from dotenv import load_dotenv
import openai
from db import SessionLocal
from models.sended_message import SendedMessage
from google.oauth2 import service_account
from datetime import datetime ,timezone, timedelta
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
APPOINTMENTS_API_KEY = os.getenv("APPOINTMENTS_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or "1aO1sI0cGAZAvr96unecOoVEkJ9upNbO8NfFDe3psFOg"
SHEET_NAME = os.getenv("SHEET_NAME", "Лист1")
GOOGLE_SA_FILE = os.getenv("GOOGLE_SA_FILE") 
CITY_IDS = [
    "0f2f2d09-8e7a-4356-bd4d-0b055d802e7b",
    "5f290be7-14ff-4ccd-8bc8-2871a9ca9d5f"
]
APPOINTMENTS_API_URL_V3 = "https://api.mrtexpert.ru/api/v3/appointments"
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
def _get_sheets_service():
    if not GOOGLE_SA_FILE:
        raise RuntimeError("GOOGLE_SA_FILE не задан")
    creds = Credentials.from_service_account_file('credentials.json', scopes=_SCOPES)
    print("Тип объекта:", type(creds))
    print("Email сервисного аккаунта:", creds.service_account_email)
    print("Scopes:", creds.scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)
def _append_row_sync(date_str: str, phone: str, decision: str,clinic_name:str):
    """Синхронная запись в Google Sheets (вызываем из отдельного потока)."""
    service = _get_sheets_service()
    body = {"values": [[clinic_name,date_str, phone, decision]]}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:C",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()
async def append_to_google_sheet(date_str: str, phone: str, decision: str, clinic_name: str):
    """Не блокируем event loop — пишем в шит в отдельном потоке."""
    try:
        await asyncio.to_thread(_append_row_sync, date_str, phone, decision,clinic_name)
    except Exception as e:
        print(f"⚠️ Не удалось записать в Google Sheets: {e} | {date_str=} {phone=} {decision=}")
def extract_scheduled_at(message: str) -> str | None:
    """
    Ищет дату/время визита в тексте и возвращает 'YYYY-MM-DD HH:MM' или None.
    Поддерживаемые варианты:
      - 'на 2025-08-20 14:00'
      - 'на 20.08.2025 в 14:00' (также 20-08-2025 / 20/08/2025)
      - 'на 20.08 в 14:00'  (год подставим текущий)
      - 'на 20 августа 2025 в 14:00' / 'на 20 августа в 14:00'
      - 'сегодня в 14:00' / 'завтра в 09:30'
      - время может быть '14:00' или '14.00'
    """
    import re
    from datetime import datetime, date, timedelta

    MONTHS_RU = {
        "январ": 1, "феврал": 2, "март": 3, "апрел": 4, "ма": 5,    # май/мая
        "июн": 6, "июл": 7, "август": 8, "сентябр": 9,
        "октябр": 10, "ноябр": 11, "декабр": 12,
    }

    def clean(s: str) -> str:
        s = re.sub(r"[–—−]+", "-", s)     # все длинные дефисы -> '-'
        s = re.sub(r"\s+", " ", s)        # схлопываем пробелы
        return s.strip()

    text = clean(message)

    # Попробуем анализировать часть после "на ...", т.к. обычно сразу там дата/время
    m_after = re.search(r"\bна\b(.+)", text, flags=re.IGNORECASE)
    scope = m_after.group(1).strip() if m_after else text

    now = datetime.now()
    this_year = now.year

    def build_and_return(y: int, m: int, d: int, hh: int, mm: int) -> str | None:
        try:
            dt = datetime(y, m, d, hh, mm)
        except ValueError:
            return None
        return dt.strftime("%Y-%m-%d %H:%M")

    # 0) 'сегодня/завтра в HH:MM'
    m = re.search(r"\b(сегодня|завтра)\b\s*(?:в\s*)?(?P<h>[01]?\d|2[0-3])[:.](?P<min>[0-5]\d)", text, re.IGNORECASE)
    if m:
        base_date = date.today() + (timedelta(days=1) if m.group(1).lower() == "завтра" else timedelta(days=0))
        return build_and_return(base_date.year, base_date.month, base_date.day, int(m.group("h")), int(m.group("min")))

    # 1) ISO-подобный: 'YYYY-MM-DD HH:MM' (или с . / /)
    m = re.search(
        r"(?P<y>\d{4})[.\-\/](?P<mo>0?[1-9]|1[0-2])[.\-\/](?P<d>0?[1-9]|[12]\d|3[01])\s+(?P<h>[01]?\d|2[0-3])[:.](?P<min>[0-5]\d)",
        scope
    )
    if m:
        return build_and_return(int(m.group("y")), int(m.group("mo")), int(m.group("d")), int(m.group("h")), int(m.group("min")))

    # 2) Числовая дата с годом + время: 'DD.MM.YYYY в HH:MM' (точки/дефисы/слеши)
    m = re.search(
        r"(?P<d>0?[1-9]|[12]\d|3[01])[.\-\/](?P<mo>0?[1-9]|1[0-2])[.\-\/](?P<y>\d{4}).{0,20}?(?:в\s*)?(?P<h>[01]?\d|2[0-3])[:.](?P<min>[0-5]\d)",
        scope, re.IGNORECASE
    )
    if m:
        return build_and_return(int(m.group("y")), int(m.group("mo")), int(m.group("d")), int(m.group("h")), int(m.group("min")))

    # 3) Текстовая дата (+/- год) + время: '20 августа (2025) в 14:00'
    m = re.search(
        r"(?P<d>0?[1-9]|[12]\d|3[01])\s+"
        r"(?P<mon>январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])"
        r"(?:\s+(?P<y>\d{4}))?.{0,20}?(?:в\s*)?(?P<h>[01]?\d|2[0-3])[:.](?P<min>[0-5]\d)",
        scope, re.IGNORECASE
    )
    if m:
        mon_raw = m.group("mon").lower()
        month = None
        for key, val in MONTHS_RU.items():
            if mon_raw.startswith(key):
                month = val
                break
        if month:
            year = int(m.group("y")) if m.group("y") else this_year
            return build_and_return(year, month, int(m.group("d")), int(m.group("h")), int(m.group("min")))

    # 4) Числовая дата без года + время: 'DD.MM в HH:MM' -> подставим текущий год
    m = re.search(
        r"(?P<d>0?[1-9]|[12]\d|3[01])[.\-\/](?P<mo>0?[1-9]|1[0-2]).{0,20}?(?:в\s*)?(?P<h>[01]?\d|2[0-3])[:.](?P<min>[0-5]\d)",
        scope, re.IGNORECASE
    )
    if m:
        return build_and_return(this_year, int(m.group("mo")), int(m.group("d")), int(m.group("h")), int(m.group("min")))

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

async def change_appointment_by_message(message: str, phone_number: str, status: str):
    """
    Подтверждает ВСЕ items во всех appointments, сохранённых в appointment_json (список).
    Для каждого appointment формируется отдельный PATCH-запрос.
    По каждому item создаётся запись type="confirm".
    """
    db = None
    try: 
        scheduled_at_str = extract_scheduled_at(message)
        if not scheduled_at_str:
            print("❌ Не удалось извлечь дату/время из сообщения")
            return

        moscow_tz = timezone(timedelta(hours=3))
        scheduled_at = datetime.fromisoformat(scheduled_at_str).replace(tzinfo=moscow_tz)

        db = SessionLocal()
        record = db.query(SendedMessage).filter(
            SendedMessage.phone_number == phone_number,
            SendedMessage.scheduled_at == scheduled_at
        ).first()
        if not record:
            print(f"❌ Не найдено уведомление с телефоном {phone_number} и временем {scheduled_at}")
            return
        
        appts_list = record.appointment_json or []
        if not isinstance(appts_list, list) or not appts_list:
            print("❌ В записи нет валидного appointment_json (ожидался список)")
            return
        async with httpx.AsyncClient(timeout=30) as client:
            total_patched = 0
            for appt in appts_list:
                appointment_id = appt.get("id")
                if not appointment_id:
                    print("⚠️ Пропуск: у одного из объектов нет id")
                    continue
                clinic_id = (appt.get("clinic") or {}).get("id")
                await append_to_google_sheet(scheduled_at_str, phone_number, status, (appt.get("clinic") or {}).get('name', ''))
                patient = appt.get("patient", {}) or {}
                patch_patient = {
                    "firstname": patient.get("firstname", ""),
                    "lastname":  patient.get("lastname",  ""),
                    "middlename":patient.get("middlename",""),
                    "birthdate": patient.get("birthdate",""),
                    "sex":       patient.get("sex",""),
                    "phone":     patient.get("phone",""),
                    "email":     patient.get("email",""),
                    "snils":     patient.get("snils",""),
                    "email_confirm": patient.get("email_confirm", False)
                }
                patch_items = []
                items = appt.get("items", []) or []
                if not items:
                    print(f"⚠️ Пропуск PATCH {appointment_id}: нет items")
                    continue

                for it in items:
                    provider_id = it.get("provider_id") or (it.get("provider") or {}).get("id") or ""
                    if provider_id == "00000000-0000-0000-0000-000000000000":
                        provider_id = ""

                    patch_items.append({
                        "service_id": (it.get("service") or {}).get("id") or it.get("service_id") or "",
                        "scheduled_at": it.get("scheduled_at"),
                        "status": status,
                        "provider_id": provider_id,
                        "refdoctor_id": (it.get("refdoctor") or {}).get("id") or it.get("refdoctor_id") or "",
                        "doctor_id": (it.get("doctor") or {}).get("id") or it.get("doctor_id") or "",
                        "profession_id": (it.get("profession") or {}).get("id") or "",
                        "partners_finances": it.get("partners_finances", False)
                    })
                patch_body = {
                    "clinic_id": clinic_id,
                    "patient_id": patient.get("id", ""),
                    "patient": patch_patient,
                    "items": patch_items
                }

                patch_url = f"{APPOINTMENTS_API_URL_V3}/{appointment_id}"
                resp = await client.patch(
                    patch_url,
                    json=patch_body,
                    headers={
                        "Authorization": f"Bearer {APPOINTMENTS_API_KEY}",
                        "Content-Type": "application/json"
                    }
                )
                print(f"📨 PATCH {appointment_id}: {resp.status_code} {resp.text} {patch_body}")
                resp.raise_for_status()
                total_patched += 1
            print(f"✅ Подтверждены items во всех апойтментах: PATCHов отправлено {total_patched}")

    except Exception as e:
        if db:
            db.rollback()
        print(f"❌ Ошибка в confirm_appointment_by_message: {e}")
    finally:
        if db:
            db.close()

def get_greenapi_chat_history(chat_id, count=18):
    """
    Получить историю сообщений из GreenAPI по chat_id
    """
    url = f"https://api.green-api.com/waInstance{GREENAPI_ID}/GetChatHistory/{GREENAPI_TOKEN}"
    payload = {"chatId": chat_id, "count": count}
    try:
        resp = httpx.post(url, json=payload, timeout=10, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Ошибка при получении истории из GreenAPI: {e}")
        return []

async def process_greenapi_webhook(request):
    def _parse_ai_control(ai_reply: str):
        """
        Пытается распарсить управляющий JSON:
        {"type": "confirm"|"cancel"|"operator_connect", "message": "..."}
        Возвращает dict или None, если это обычный текст.
        """
        if not ai_reply:
            return None
        s = ai_reply.strip()
        if not (s.startswith("{") and s.endswith("}")):
            return None
        try:
            data = json.loads(s)
            if isinstance(data, dict) and "type" in data:
                return data
        except Exception:
            return None
        return None

    body = await request.json()

    if body.get("typeWebhook") != "incomingMessageReceived":
        return {"status": "ignored"}

    message = ""
    msg_data = body.get("messageData", {})
    msg_type = msg_data.get("typeMessage")

    if msg_type == "textMessage":
        message = msg_data.get("textMessageData", {}).get("textMessage", "")
    elif msg_type == "extendedTextMessage":
        message = msg_data.get("extendedTextMessageData", {}).get("text", "")
    elif msg_type == "quotedMessage":
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
            # --- Контакт в Chatwoot ---
            contacts = await get_all_chatwoot_contacts(client, CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID, CHATWOOT_API_KEY)
            contact = next((c for c in contacts if c.get("phone_number") == formatted_phone), None)

            if not contact:
                contact_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                    json={"name": "afruz" or sender_name, "phone_number": formatted_phone},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                if contact_resp.status_code == 422 and "Phone number has already been taken" in contact_resp.text:
                    contacts = await get_all_chatwoot_contacts(client, CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID, CHATWOOT_API_KEY)
                    contact = next((c for c in contacts if c.get("phone_number") == formatted_phone), None)
                    if contact:
                        contact_id = contact["id"]
                    else:
                        raise Exception("Контакт с этим номером уже есть, но не найден в списке.")
                else:
                    contact_resp.raise_for_status()
                    contact_json = contact_resp.json()
                    contact_id = (
                        contact_json.get("id")
                        or contact_json.get("payload", {}).get("contact", {}).get("id")
                        or contact_json.get("contact", {}).get("id")
                    )
                    if not contact_id:
                        raise Exception(f"Не удалось определить contact_id: {contact_json}")
            else:
                contact_id = contact["id"]

            # --- Поиск/создание разговора ---
            convs_resp = await client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
            )
            convs_resp.raise_for_status()
            conversations = convs_resp.json().get("payload", [])

            if conversations:
                conversation_id = conversations[0]["id"]
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
                conversation_id = new_conv.get("id")
                if not conversation_id:
                    logger.warning("Не удалось получить ID созданного разговора.")
                    return {"status": "error", "detail": "no conversation id"}

            # Назначить оператора 3
            await client.patch(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}",
                json={"assignee_id": 3},
                headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
            )

            # Добавить входящее сообщение пользователя
            msg_resp = await client.post(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                json={"content": message, "message_type": "incoming"},
                headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
            )
            msg_resp.raise_for_status()

            # --- AI обработка ---
            # История из GreenAPI для контекста
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
            logger.debug(f"AI reply raw: {ai_reply!r}")

            ctrl = _parse_ai_control(ai_reply)

            # --- Управляющие команды от ИИ ---
            if ctrl and ctrl.get("type") == "operator_connect":
                operator_message = ctrl.get("message") or "Клиенту требуется оператор."
                await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": operator_message, "message_type": "outgoing"},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                logger.info("Оповещение операторов отправлено")
                await unassign_conversation(phone)
                return {"status": "ok"}

            if ctrl and ctrl.get("type") == "confirm":
                thank_you_msg = "Спасибо за подтверждение записи. Ждём вас за 15 минут до приёма."
                ai_msg_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": thank_you_msg, "message_type": "outgoing"},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                ai_msg_resp.raise_for_status()
                await change_appointment_by_message(ctrl.get("message", ""), phone, "confirm")
                return {"status": "ok"}

            if ctrl and ctrl.get("type") == "cancel":
                cancel_msg = "Вашу запись отменяем. Если потребуется новая запись, напишите нам или позвоните."
                ai_msg_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": cancel_msg, "message_type": "outgoing"},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                ai_msg_resp.raise_for_status()
                await change_appointment_by_message(ctrl.get("message", ""), phone, "canceled")
                return {"status": "ok"}

            # --- Обычный текстовый ответ ИИ ---
            if ai_reply and not ctrl:
                ai_msg_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": ai_reply, "message_type": "outgoing"},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                ai_msg_resp.raise_for_status()
                logger.info("AI ответ отправлен в разговор %s", conversation_id)

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
            model="gpt-4.1-nano",
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
    doc_id = "1dQ2k6i_c8JpByTtPy75Vr0ErohIz-e73K7hvj86R2Go"
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