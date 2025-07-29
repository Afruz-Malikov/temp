import logging
from datetime import datetime, timedelta, timezone
import httpx
import os
import json
from db import SessionLocal
from models.sended_message import SendedMessage
# import pyperclip
from pathlib import Path
from dotenv import load_dotenv
from pytz import timezone as pytz_timezone

# moscow_tz = pytz_timezone("Europe/Moscow")


load_dotenv()
logger = logging.getLogger("uvicorn.webhook")

GREENAPI_ID = os.getenv("GREENAPI_ID")
GREENAPI_TOKEN = os.getenv("GREENAPI_TOKEN")
CHATWOOT_API_KEY = os.getenv("CHATWOOT_API_KEY")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL")
GOOGLE_API_DOCS_SECRET = os.getenv("GOOGLE_API_DOCS_SECRET")
APPOINTMENTS_API_KEY = os.getenv("APPOINTMENTS_API_KEY") or 'CNvy6w6CRR1QLY2V6eq6gDQT'

LAST_PROCESSED_FILE = Path("last_processed.json")

def get_last_processed_time():
    tz_msk = timezone(timedelta(hours=3))
    now = datetime.now(tz=tz_msk)
    print('hop', now - timedelta(hours=1), now)
    # Всегда возвращаем текущее время минус 1 час в UTC+3
    return now - timedelta(hours=1)

def save_last_processed_time():
    try:
        with open(LAST_PROCESSED_FILE, 'w') as f:
            json.dump({
                'last_processed': datetime.now(timezone.utc).isoformat()
            }, f)
    except Exception as e:
        logger.error(f"Ошибка при сохранении времени последней обработки: {e}")

def get_all_chatwoot_contacts(client, base_url, account_id, api_key):
    contacts = []
    page = 1
    while True:
        resp = client.get(
            f"{base_url}/api/v1/accounts/{account_id}/contacts",
            params={"page": page},
            headers={"api_access_token": api_key, "Content-Type": "application/json"},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        payload = data.get("payload", [])
        if not payload:
            break
        contacts.extend(payload)
        if len(payload) < 15:
            break
        page += 1
    return contacts

def send_chatwoot_message(phone, message):
    try:
        with httpx.Client() as client:
            contacts = get_all_chatwoot_contacts(client, CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID, CHATWOOT_API_KEY)
            contact = next((c for c in contacts if c["phone_number"] == f'+{phone}'), None)
            if not contact:
                contact_resp = client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                    json={"name": f"+{phone}", "phone_number": f"+{phone}"},
                    headers={"api_access_token": CHATWOOT_API_KEY,"Content-Type": "application/json"}, timeout=10
                )
                contact_resp.raise_for_status()
                contact_json = contact_resp.json()
                contact_id = (
                    contact_json.get("id")
                    or contact_json.get("payload", {}).get("contact", {}).get("id")
                    or contact_json.get("contact", {}).get("id")
                )
                if not contact_id:
                    logger.error(f"Не удалось получить contact_id из ответа: {contact_resp.text}")
                    return
            else:
                contact_id = contact["id"]
            convs_resp = client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                headers={"api_access_token": CHATWOOT_API_KEY,   "Content-Type": "application/json"}, timeout=10
            )
            convs_resp.raise_for_status()
            conversations = convs_resp.json().get("payload", [])
            if conversations:
                conversation_id = conversations[0]["id"]
                # Назначить оператора 3 на conversation
                answ = client.patch(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}",
                    json={"assignee_id": 3},
                    headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}, timeout=10  
                )
                print("answ",answ)
            else:
                conv_resp = client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations",
                    json={
                        "inbox_id": int(CHATWOOT_INBOX_ID),
                        "contact_id": contact_id,
                        "source_id": f"{phone.replace('+', '')}@c.us",
                        "additional_attributes": {},
                        "status": "open"
                    },
                    headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                )
                conv_resp.raise_for_status()
                conversation_id = conv_resp.json().get("id")
            msg_resp = client.post(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                json={"content": message, "message_type": "outgoing"},
                headers={"api_access_token": CHATWOOT_API_KEY,  "Content-Type": "application/json"}, timeout=10
            )
            msg_resp.raise_for_status()
            logger.info(f"Chatwoot ответ: {msg_resp.text}")
    except Exception as e:
        logger.error(f"Ошибка отправки в Chatwoot: {e}")

city_data = {
    "0f2f2d09-8e7a-4356-bd4d-0b055d802e7b": {
        "address": "г Орехово-Зуево, ул Дзержинского, стр. 41/1",
        "site": "https://orz.mrtexpert.ru/",
        "phone": "84961111111"
    },
    "5f290be7-14ff-4ccd-8bc8-2871a9ca9d5f": {
        "address": "г Мытищи, ул Колпакова, д. 2А, помещ. 54",
        "site": "https://myt.mrtexpert.ru/",
        "phone": "84972222222"
    }
}
def process_items_cron():
    db = SessionLocal()
    try:
        last_processed = get_last_processed_time()
        now = datetime.now(timezone.utc)
        logger.info(f"🕐 Обработка данных с {last_processed.strftime('%Y-%m-%d %H:%M:%S')} до {now.strftime('%Y-%m-%d %H:%M:%S')}")
        # moscow_time = now.astimezone(moscow_tz)
        clinics_url = "https://apitest.mrtexpert.ru/api/v3/clinics"
        auth_header = {"Authorization": f"Bearer {APPOINTMENTS_API_KEY}"}
        city_ids = ["0f2f2d09-8e7a-4356-bd4d-0b055d802e7b", "5f290be7-14ff-4ccd-8bc8-2871a9ca9d5f"]

        clinics = []
        for city_id in city_ids:
            try:
                resp = httpx.get(f"{clinics_url}?city_id={city_id}", timeout=20, headers=auth_header)
                resp.raise_for_status()
                clinics.extend(resp.json().get("result", []))
            except Exception as e:
                logger.error(f"Ошибка при получении клиник {city_id}: {e}")

        clinic_map = {c['id']: c for c in clinics}
        all_appointments = []

        for clinic in clinics:
            cid = clinic.get("id")
            if not cid:
                continue
            try:
                app_resp = httpx.get(
                    f"https://apitest.mrtexpert.ru/api/v3/appointments?clinic_id={cid}",
                    timeout=20,
                    headers=auth_header
                )
                app_resp.raise_for_status()
                appointments = app_resp.json().get("result", [])
                all_appointments.extend(appointments)
            except Exception as e:
                logger.error(f"Ошибка при получении заявок клиники {cid}: {e}")

        now = datetime.now(timezone.utc)
        today = now.date()
        processed_count = 0
        notified_phones = set()

        for obj in all_appointments:
            patient = obj.get("patient", {})
            phone = patient.get("phone") or "998180817" 
            if not phone or phone in notified_phones:
                continue

            items = obj.get("items", [])    
            for item in items:
                item_id = item.get("id")
                scheduled_at_str = item.get("scheduled_at")
                if not scheduled_at_str:
                    continue
                try:
                    scheduled_at = datetime.fromisoformat(scheduled_at_str)
                    if scheduled_at.tzinfo is None:
                        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
                except Exception as e:
                    logger.warning(f"Неверный формат времени: {scheduled_at_str}, ошибка: {e}")
                    continue

                if scheduled_at < now:
                    continue
                delta = scheduled_at - now
                dt_str = scheduled_at.strftime('%d.%m.%Y в %H:%M')
                time_str = scheduled_at.strftime('%H:%M')
                clinic = obj.get('clinic', {})
                full_clinic = clinic_map.get(clinic.get("id"), clinic)
                address = full_clinic.get("address", "—")
                directions = full_clinic.get("directions", "")
                phone_center = city_data.get(full_clinic.get("city_id", ""), {}).get("phone", full_clinic.get("phone", "—"))
                # Проверка в БД
                sent_new = db.query(SendedMessage).filter_by(appointment_id=item_id, type="new_remind").first()
                if not sent_new:
                    if 0 <= scheduled_at.hour < 7:
                        logger.info(f"🌙 Ночь: пропускаем сообщение типа new_remind для {item_id}")
                        continue
                    # 1. Новое сообщение
                    new_msg = (
                        f"Здравствуйте!\n"
                        f"Вы записаны в МРТ Эксперт на {dt_str}.\n"
                        f"Адрес: {address}, {directions}\n"
                        f"В центре нужно быть за 15 минут до приема.\n"
                        f"При себе необходимо иметь паспорт, направление, если оно есть, и результаты предыдущих исследований\n"
                        f"Телефон для связи: {phone_center}"
                    )
                    send_chatwoot_message(phone, new_msg)
                    try:
                        service_resp = httpx.get(
                            f"https://apitest.mrtexpert.ru/api/v3/services/{item.get('service', {}).get('id', '')}?clinic_id={clinic.get('id')}",
                            timeout=20,
                            headers=auth_header
                        )
                        service_resp.raise_for_status()
                        prepare_message = service_resp.json().get("result", {}).get("prepare", "")
                        if prepare_message:
                            send_chatwoot_message(phone, prepare_message)
                            logger.info(f"📄 Отправлено сообщение с подготовкой: {item_id}")
                    except Exception as e:
                        logger.warning(f"Ошибка получения подготовки: {e}")
                    db.add(SendedMessage(appointment_id=item_id, type="new_remind"))
                    db.commit()
                    logger.info(f"🟢 Новая запись отправлена: {item_id}")
                    notified_phones.add(phone)
                    processed_count += 1
                    continue

                # 2. Напоминание за день
                minutes_to_appointment = int(delta.total_seconds() / 60)
                if 1400 <= minutes_to_appointment <= 1440:
                    if 0 <= scheduled_at.hour < 7:
                        logger.info(f"🌙 Ночь: пропускаем сообщение типа new_remind для {item_id}")
                        continue
                    confirm_msg = (
                        f"Здравствуйте!\n"
                        f"Напоминаем, что вы записаны в МРТ Эксперт на {dt_str}.\n"
                        f"Подтвердите свой визит ответным сообщением (только цифра):\n"
                        f"1 – подтверждаю\n2 – прошу перенести\n3 – прошу отменить\n"
                        f"Телефон для связи {phone_center}"
                    )
                    sent_day = db.query(SendedMessage).filter_by(appointment_id=item_id, type="day_remind").first()
                    if not sent_day:
                        send_chatwoot_message(phone, confirm_msg)
                        db.add(SendedMessage(appointment_id=item_id, type="day_remind"))
                        db.commit()
                        logger.info(f"🔔 Напоминание за день отправлено: {item_id}")
                        notified_phones.add(phone)
                        processed_count += 1
                        continue

                # 3. Напоминание за 2 часа
                if timedelta(hours=1, minutes=50) <= delta <= timedelta(hours=2):
                    remind_msg = (
                        
                        f"Здравствуйте!\n"
                        f"Напоминаем, что ваш прием в МРТ Эксперт сегодня в {time_str}.\n"
                        f"центре нужно быть за 15 минут до начала приема для оформления документов.\n"
                        f"Телефон для связи {phone_center}."
                    )
                    sent_remind = db.query(SendedMessage).filter_by(appointment_id=item_id, type="hour_remind").first()
                    if not sent_remind:
                        send_chatwoot_message(phone, remind_msg)
                        db.add(SendedMessage(appointment_id=item_id, type="hour_remind"))
                        db.commit()
                        logger.info(f"⏰ Напоминание за 2 часа отправлено: {item_id}")
                        notified_phones.add(phone)
                        processed_count += 1

        save_last_processed_time()
        logger.info(f"✅ Завершено. Уведомлений отправлено: {processed_count}")
    except Exception as e:
        logger.error(f"❌ Ошибка в process_items_cron: {e}")
        save_last_processed_time()
    finally:
        db.close()