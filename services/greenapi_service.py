import logging
import httpx
import os,asyncio
import json, time, uuid, logging, os
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
from utils.send_message_to_tg_bot import send_message_to_tg_bot
from dateutil import parser
from constant.matchers import inbox_by_id_instance_match
load_dotenv()

logger = logging.getLogger("uvicorn.webhook")
logging.basicConfig(level=logging.INFO)
def _j(obj):  # безопасный json для логов
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return str(obj)
OPEN_API_KEY = os.getenv("OPENAI_API_KEY")
CHATWOOT_API_KEY = os.getenv("CHATWOOT_API_KEY")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_BASE_URL = "https://expert.tag24.ru"
APPOINTMENTS_API_KEY = os.getenv("APPOINTMENTS_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or "1aO1sI0cGAZAvr96unecOoVEkJ9upNbO8NfFDe3psFOg"
SHEET_NAME = os.getenv("SHEET_NAME", "Лист1")
GOOGLE_SA_FILE = os.getenv("GOOGLE_SA_FILE") 
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
def extract_scheduled_at(message ):
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

    def build_and_return(y: int, m: int, d: int, hh: int, mm: int) :
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
        await append_to_google_sheet(scheduled_at_str, phone_number, status, "Липецк 1 МРТ-Эксперт")
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

def get_greenapi_chat_history(chat_id, count=18, green_id = '' , green_token = ''): 
    """
    Получить историю сообщений из GreenAPI по chat_id
    """
    url = f"https://api.green-api.com/waInstance{green_id}/GetChatHistory/{green_token}"
    payload = {"chatId": chat_id, "count": count}
    try:
        resp = httpx.post(url, json=payload, timeout=10, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Ошибка при получении истории из GreenAPI: {e}")
        return []

async def process_greenapi_webhook(request):
    logger = logging.getLogger("uvicorn.webhook")

    # ======================== Labels & detection ========================
    ACTION_TO_LABEL = {
        "confirm":       "подтвердил_запись",
        "cancel":        "отмена",
        "desc_cons":     "консультация_по_описанию",
        "price_cons":    "консультация_по_стоимости_и_записи",
        "broken_time":   "нарушен_срок_описания",
        "tax_cert":      "справка_в_налоговую",
    }

    def detect_action_from_ai_reply(ai_reply: str):
        """Определяет action по точным формулировкам из таблицы (только для текстовых ответов)."""
        if not ai_reply:
            return None
        import re as _re

        def _norm(s: str) -> str:
            s = s.replace(" ", " ")
            s = _re.sub(r"\s+", " ", s.strip())
            return s.lower()

        t = _norm(ai_reply)

        # Главное меню
        if t.startswith(_norm("Пока в чате мы информируем по уже оформленным записям.")):
            return "price_cons"
        if t.startswith(_norm("Налоговый вычет возвращается не ранее года, следующего за годом оплаты.")):
            return "tax_cert"
        # Вложенные ответы
        if t.startswith(_norm("В поле «Фамилия» введите фамилию пациента, указанную в договоре (без инициалов, пробелов и опечаток).")):
            return "desc_cons"
        if t.startswith(_norm("Важно:")) or "telemedex" in t:
            return "desc_cons"
        if t.startswith(_norm("Спасибо за подтверждение записи.")):
            return "confirm"
        if t.startswith(_norm("Благодарим за обратную связь!")):
            return "cancel"
        # Просрочка описания (на случай текстовой формы)
        if "приносим извинение за увеличение сроков описания" in t:
            return "broken_time"
        return None

    # ======================== Chatwoot helpers ========================
    async def _cw_list_labels(client) -> list[dict]:
        r = await client.get(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/labels",
            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
        )
        r.raise_for_status()
        data = r.json()
        return (data.get("payload") or data.get("data") or data) if isinstance(data, (list, dict)) else []

    def _pick_label(existing_labels: list[dict], wanted_name: str):
        if not wanted_name:
            return None
        wn = wanted_name.strip().lower().replace(" ", "_")
        for it in existing_labels:
            name = (it.get("title") or it.get("name") or "").strip()
            if name.lower().replace(" ", "_") == wn:
                return name
        return None

    async def _cw_add_labels(client, conversation_id: int, labels: list[str]):
        """
        Низкоуровневый вызов: просто отправляет список labels.
        ВАЖНО: этот эндпоинт ПЕРЕЗАПИСЫВАЕТ ярлыки разговора целиком.
        Чтобы не потерять существующие ярлыки, используйте _cw_merge_and_add_label.
        """
        if not labels:
            return
        r = await client.post(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/labels",
            json={"labels": labels},
            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
        )
        r.raise_for_status()

    async def _cw_get_conversation_labels(client, conversation_id: int | str) -> list[str]:
        """Возвращает текущие ярлыки беседы (список строк), чтобы их не потерять."""
        r = await client.get(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}",
            headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
        )
        r.raise_for_status()
        data = r.json()
        conv = data.get("payload") or data.get("data") or data
        labels = conv.get("labels") or []
        return [str(x) for x in labels if isinstance(x, (str, int))]

    async def _cw_merge_and_add_label(client, conversation_id: int | str, wanted_label_machine_name: str):
        """
        - Проверяет существование 'машинного' ярлыка (по словарю ACTION_TO_LABEL) в аккаунте,
        - Берёт текущие ярлыки беседы,
        - Мерджит их с нужным ярлыком без дубликатов,
        - Отправляет объединённый список (не затирая предыдущие).
        """
        if not wanted_label_machine_name:
            return
        existing = await _cw_list_labels(client)
        label_to_use = _pick_label(existing, wanted_label_machine_name)
        if not label_to_use:
            # нет такого ярлыка в аккаунте — ничего не делаем
            return
        try:
            current = await _cw_get_conversation_labels(client, conversation_id)
        except Exception:
            current = []
        merged = list(dict.fromkeys([*current, label_to_use]))
        await _cw_add_labels(client, conversation_id, merged)

    # ===== Новые утилиты для поиска контакта и нормализации номера =====
    def _digits(s: str) -> str:
        return re.sub(r"\D+", "", s or "")
    async def _cw_search_contact_by_phone(client, phone_e164: str):
        """
        Поиск контакта через обычный GET /contacts/search?p={phone}
        (есть фолбэк на q= для совместимости).
        Возвращает первый точный матч по номеру.
        """
        base = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/search"
        headers = {"api_access_token": CHATWOOT_API_KEY}

        for params in ({"p": phone_e164}, {"q": phone_e164}):
            try:
                sr = await client.get(base, params=params, headers=headers, timeout=15)
                if sr.status_code != 200:
                    continue
                data = sr.json()
                arr = data.get("payload") or data.get("data") or (data if isinstance(data, list) else [])
                for c in (arr or []):
                    pn = c.get("phone_number") or ""
                    if _digits(pn) == _digits(phone_e164):
                        return c
            except Exception:
                continue
        return None

    # ======================== Existing utils (unchanged behavior) ========================
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

    def _extract_phone(text):
        """
        Достаёт номер телефона из текста напоминания, например:
        'Для переноса записи обратитесь к нам по телефону: 84742505105'
        Возвращает нормализованный номер (только цифры и ведущий '+').
        """
        if not text:
            return None

        m = re.search(r'(?:по\s+телефону|телефон)[^0-9+]*[:\-]?\s*([+()\-\s\d]{7,})', text, flags=re.IGNORECASE)
        cand = m.group(1).strip() if m else None

        if not cand:
            m2 = re.findall(r'(\+?\d[\d\-\s()]{6,}\d)', text)
            if m2:
                cand = m2[-1].strip()

        if not cand:
            return None

        num = re.sub(r'[^\d+]', '', cand)
        if num.count('+') > 1:
            num = '+' + num.replace('+', '')
        return num or None

    def _find_last_phone_in_history(greenapi_history):
        """
        Ищем телефон в последнем исходящем (outgoing) сообщении — там лежит шаблон уведомления.
        """
        for h in reversed(greenapi_history or []):
            if h.get("type") == "outgoing":
                txt = h.get("textMessage") or h.get("text") or ""
                ph = _extract_phone(txt)
                if ph:
                    return ph
        return None

    def _is_valid_ai_reply(reply: str) -> bool:
        """
        Проверяет, что ответ GPT не пустой/мусорный.
        """
        if not reply:
            return False
        text = reply.strip()
        if not text:
            return False
        if text in ("{}", "[]"):
            return False
        return True

    # ======================== Main flow ========================
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
    if "{{SWE003}}" in (message or ""):
        message = "1"
        await send_message_to_tg_bot(f"Replaced SWE003 in message: {message}")
    sender_chat_id = body.get("senderData", {}).get("chatId", "")
    sender_name = body.get("senderData", {}).get("senderName", "")
    instance_id = str(body.get("instanceData", {}).get("idInstance"))
    chatwoot_inbox_id = inbox_by_id_instance_match.get(instance_id, {}).get("inbox_id")
    logger.info(f"Webhook from instance {instance_id}, chat {sender_chat_id}: {message!r} {body}")
    green_token = inbox_by_id_instance_match.get(instance_id, {}).get("green_token")
    green_id = inbox_by_id_instance_match.get(instance_id, {}).get("green_id")
    if not message or not sender_chat_id:
        logger.warning("Нет текста или sender_chat_id")
        return {"status": "no content"}

    phone = sender_chat_id.replace("@c.us", "")
    formatted_phone = f"+{phone}"

    try:
        async with httpx.AsyncClient() as client:
            # --- Контакт в Chatwoot (поиск через /contacts/search?p=...) ---
            contact = await _cw_search_contact_by_phone(client, formatted_phone)

            if not contact:
                contact_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                    json={"name": "afruz" or sender_name, "phone_number": formatted_phone},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                if contact_resp.status_code == 422 and "Phone number has already been taken" in contact_resp.text:
                    # если гонка — повторный поиск через search
                    contact = await _cw_search_contact_by_phone(client, formatted_phone)
                    if contact:
                        contact_id = contact["id"]
                    else:
                        raise Exception("Контакт с этим номером уже есть, но не найден через search.")
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

            # --- Поиск/создание разговора именно для этого inbox ---
            convs_resp = await client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
            )
            convs_resp.raise_for_status()
            conversations = convs_resp.json().get("payload", [])

            conversation_id = None
            for c in conversations:
                if str(c.get("inbox_id")) == str(chatwoot_inbox_id):
                    conversation_id = c["id"]
                    break

            if not conversation_id:
                conv_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations",
                    json={
                        "inbox_id": int(chatwoot_inbox_id),
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

            # Назначить оператора 3 (для существующего диалога тоже)
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
            greenapi_history = get_greenapi_chat_history(sender_chat_id, green_token=green_token, green_id=green_id)
            system_prompt = fetch_google_doc_text() or "You are a helpful assistant."
            gpt_messages = [{"role": "system", "content": system_prompt}]
            for msg in reversed(greenapi_history):
                if msg.get("type") == "incoming":
                    gpt_messages.append({"role": "user", "content": msg.get("textMessage", "")})
                elif msg.get("type") == "outgoing":
                    gpt_messages.append({"role": "assistant", "content": msg.get("textMessage", "")})
            if not gpt_messages or gpt_messages[-1].get("role") != "user" or gpt_messages[-1].get("content") != message:
                gpt_messages.append({"role": "user", "content": message})

            ai_reply = await call_ai_service(gpt_messages)
            logger.debug(f"AI reply raw: {ai_reply!r}")

            if not _is_valid_ai_reply(ai_reply):
                logger.info("Игнорируем пустой/некорректный ответ от GPT")
                return {"status": "ok"}

            ctrl = _parse_ai_control(ai_reply)

            # телефон центра: из последнего исходящего напоминания; fallback — из текста
            phone_center = (
                _find_last_phone_in_history(greenapi_history)
                or _extract_phone(ai_reply)
                or _extract_phone(message)
            )

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

            if ctrl and ctrl.get("type") in ("confirm", "cancel"):
                # 1) финальный текст
                if ctrl["type"] == "confirm":
                    out = (
                        "Спасибо за подтверждение записи.\n"
                        "Ждем вас за 15 минут до начала приема со всеми необходимыми документами.\n"
                    )
                    if phone_center:
                        out += f"\nЕсли у вас изменятся планы, пожалуйста, свяжитесь с нами по телефону {phone_center}"
                else:
                    out = "Благодарим за обратную связь!\n"
                    if phone_center:
                        out += f"Вы в любой момент можете восстановить Вашу запись по телефону {phone_center}"

                r = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": out, "message_type": "outgoing"},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                r.raise_for_status()

                # 2) ярлык (мердж с текущими)
                try:
                    wanted_label = ACTION_TO_LABEL.get(ctrl["type"])
                    await _cw_merge_and_add_label(client, conversation_id, wanted_label)
                except Exception as lab_e:
                    logger.warning(f"Не удалось навесить ярлык/закрыть разговор: {lab_e}")

                # 3) доменная логика
                await change_appointment_by_message(
                    ctrl.get("message", ""), phone,
                    "confirm" if ctrl["type"] == "confirm" else "canceled"
                )
                return {"status": "ok"}

            # --- Обычный текстовый ответ ИИ (меню) ---
            if ai_reply and not ctrl:
                ai_msg_resp = await client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                    json={"content": ai_reply.strip(), "message_type": "outgoing"},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}
                )
                ai_msg_resp.raise_for_status()
                logger.info("AI ответ отправлен в разговор %s", conversation_id)

                # навешиваем ярлык в зависимости от текста (мердж, без перезаписи)
                try:
                    action = detect_action_from_ai_reply(ai_reply)
                    if action:
                        await _cw_merge_and_add_label(client, conversation_id, ACTION_TO_LABEL.get(action))
                        logger.info(f"Навешен ярлык для action '{action}' (мердж с существующими)")
                except Exception as lab_e:
                    logger.warning(f"Не удалось навесить ярлык по тексту: {lab_e}")

    except Exception as e:
        logger.exception("Ошибка: %s", e)
        return {"status": "error", "detail": str(e)}

    return {"status": "ok"}

async def call_ai_service(messages, why_tag: str = None) -> str:
    """
    Логирует входящие сообщения и метаданные ответа, чтобы понять
    почему модель дала такой ответ.
    Возвращает только строку результата (без обертки {"result": ...}),
    кроме управляющих JSON confirm/cancel/operator_connect.
    """
    if not OPEN_API_KEY:
        return "[Ошибка: не задан OPEN_API_KEY]"

    trace = why_tag or uuid.uuid4().hex[:8]
    client = openai.AsyncOpenAI(api_key=OPEN_API_KEY)

    # 1) Логируем ВХОД (весь стек сообщений + параметры)
    params = dict(model="gpt-4.1-nano", temperature=0.7, max_tokens=512)
    logger.info("[GPT %s] INPUT params=%s", trace, _j(params))
    logger.info("[GPT %s] INPUT messages=%s", trace, _j(messages))

    t0 = time.perf_counter()
    try:
        resp = await client.chat.completions.create(messages=messages, **params)
        dt_ms = int((time.perf_counter() - t0) * 1000)

        choice = resp.choices[0]
        content = (choice.message.content or "").strip()

        # --- Разворачиваем "обертку" {"result": "..."} ---
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                # Управляющий JSON (confirm/cancel/operator_connect) оставляем как есть
                if "type" in parsed and "message" in parsed:
                    content = json.dumps(parsed, ensure_ascii=False)
                # Обертка {"result": "..."} → вернуть только внутренний текст
                elif set(parsed.keys()) == {"result"}:
                    inner = parsed.get("result")
                    content = "" if inner is None else str(inner)
        except Exception:
            pass

        # 2) Логируем ВЫХОД
        logger.info(
            "[GPT %s] OUTPUT id=%s model=%s finish_reason=%s latency_ms=%d",
            trace, getattr(resp, "id", None), getattr(resp, "model", None),
            getattr(choice, "finish_reason", None), dt_ms
        )

        # usage (сколько токенов модель реально «видела» и сгенерила)
        usage = None
        try:
            usage = resp.usage.model_dump() if hasattr(resp.usage, "model_dump") else dict(resp.usage)
        except Exception:
            usage = str(getattr(resp, "usage", None))
        logger.info("[GPT %s] USAGE=%s", trace, _j(usage))

        # tool_calls/функции
        tool_calls = getattr(choice.message, "tool_calls", None)
        if tool_calls:
            logger.info("[GPT %s] TOOL_CALLS=%s", trace, _j(tool_calls))

        # Сам текст ответа
        logger.info("[GPT %s] OUTPUT content=%s", trace, _j(content))
        return content

    except Exception as e:
        dt_ms = int((time.perf_counter() - t0) * 1000)
        logger.exception("[GPT %s] ERROR after %dms: %s", trace, dt_ms, e)
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