import logging
from datetime import datetime, timedelta, timezone
import httpx
import os
import time
import copy
import json
from pathlib import Path

logger = logging.getLogger("uvicorn.webhook")

GREENAPI_ID = os.getenv("GREENAPI_ID")
GREENAPI_TOKEN = os.getenv("GREENAPI_TOKEN")
CHATWOOT_API_KEY = os.getenv("CHATWOOT_API_KEY")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL")
GOOGLE_API_DOCS_SECRET = os.getenv("GOOGLE_API_DOCS_SECRET")
APPOINTMENTS_API_KEY = os.getenv("APPOINTMENTS_API_KEY")

# Файл для хранения последнего обработанного времени
LAST_PROCESSED_FILE = Path("last_processed.json")

def get_last_processed_time():
    """Получить время последней обработки"""
    try:
        if LAST_PROCESSED_FILE.exists():
            with open(LAST_PROCESSED_FILE, 'r') as f:
                data = json.load(f)
                return datetime.fromisoformat(data['last_processed'])
    except Exception as e:
        logger.warning(f"Ошибка при чтении времени последней обработки: {e}")
    return datetime.now(timezone.utc) - timedelta(hours=1)  # По умолчанию час назад

def save_last_processed_time():
    """Сохранить текущее время как время последней обработки"""
    try:
        with open(LAST_PROCESSED_FILE, 'w') as f:
            json.dump({
                'last_processed': datetime.now(timezone.utc).isoformat()
            }, f)
    except Exception as e:
        logger.error(f"Ошибка при сохранении времени последней обработки: {e}")

def send_greenapi_message(chat_id, message):
    url = f"https://api.green-api.com/waInstance{GREENAPI_ID}/SendMessage/{GREENAPI_TOKEN}"
    payload = {"chatId": chat_id, "message": message}
    try:
        with httpx.Client() as client:
            resp = client.post(url, json=payload, timeout=10)
            logger.info(f"GreenAPI ответ: {resp.text}")
    except Exception as e:
        logger.error(f"Ошибка отправки в GreenAPI: {e}")

def send_chatwoot_message(phone, message, type="outgoing"):
    try:
        with httpx.Client() as client:
            contacts_resp = client.get(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                headers={"api_access_token": CHATWOOT_API_KEY, "Content-Type": "application/json"}, timeout=10
            )
            contacts_resp.raise_for_status()
            contacts = contacts_resp.json().get("payload", [])
            print(contacts)
            contact = next((c for c in contacts if c["phone_number"] == f'+{phone}'), None)
            if not contact:
                # Создать контакт, если не найден
                contact_resp = client.post(
                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                    json={"name": f"+{phone}", "phone_number": f"+{phone}"},
                    headers={"api_access_token": CHATWOOT_API_KEY,"Content-Type": "application/json"}, timeout=10
                )
                contact_resp.raise_for_status()
                contact_id = contact_resp.json().get("id")
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
            # Отправить сообщение
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
        "site": "https://orz.mrtexpert.ru/",
        "phone": "84961111111"
    },
    "5f290be7-14ff-4ccd-8bc8-2871a9ca9d5f": {
        "site": "https://myt.mrtexpert.ru/",
        "phone": "84972222222"
    }
}

def process_items_cron():
    try:
        last_processed = get_last_processed_time()
        now = datetime.now(timezone.utc)
        logger.info(f"🕐 Обработка данных с {last_processed.strftime('%Y-%m-%d %H:%M:%S')} до {now.strftime('%Y-%m-%d %H:%M:%S')}")

        # 1. Получаем список клиник
        
        clinics_url = "https://apitest.mrtexpert.ru/api/v3/clinics"
        auth_header = {
    "Authorization": f"Bearer {APPOINTMENTS_API_KEY}",
        }
        city_ids = [
            "0f2f2d09-8e7a-4356-bd4d-0b055d802e7b",
            "5f290be7-14ff-4ccd-8bc8-2871a9ca9d5f"
        ]
        clinics = []
        for city_id in city_ids:
            clinics_url = f"https://apitest.mrtexpert.ru/api/v3/clinics?city_id={city_id}"
            try:
                clinics_resp = httpx.get(clinics_url, timeout=20, headers=auth_header)
                clinics_resp.raise_for_status()
                clinics.extend(clinics_resp.json().get("result", []))
            except Exception as e:
                logger.error(f"Ошибка при получении клиник для города {city_id}: {e}")

        # Создаем словарь для быстрого доступа к полной информации о клинике по ее ID
        clinic_map = {c['id']: c for c in clinics}

        # 2. Для каждой клиники получаем заявки (appointments)
        print('copi', clinics)
        all_appointments = []
        for clinic in clinics:
            cid = clinic.get("id")
            if not cid:
                continue
            try:
                app_url = f"https://apitest.mrtexpert.ru/api/v3/appointments?clinic_id={cid}"
                app_resp = httpx.get(app_url, timeout=20, headers=auth_header)
                app_resp.raise_for_status()
                appointments = app_resp.json().get("result", [])
                all_appointments.extend(appointments)
            except Exception as e:
                logger.error(f"Ошибка при получении заявок для клиники {cid}: {e}")

        objects = all_appointments
        processed_count = 0
        for obj in objects:
            patient = obj.get('patient', {})
            phone =  "79255890919" or patient.get('phone')
            items = obj.get('items', [])
            created_at_str = obj.get('created_at')
            created_at = None
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str)
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                except Exception as e:
                    logger.warning(f"Некорректный формат времени created_at: {created_at_str}, ошибка: {e}")
            
            for item in items:
                scheduled_at_str = item.get('scheduled_at')
                if not scheduled_at_str:
                    continue
                try:
                    scheduled_at = datetime.fromisoformat(scheduled_at_str)
                    if scheduled_at.tzinfo is None:
                        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
                except Exception as e:
                    logger.warning(f"Некорректный формат времени: {scheduled_at_str}, ошибка: {e}")
                    continue
                
                delta = scheduled_at - now
                
                # Данные для шаблонов
                clinic = obj.get('clinic', {})
                full_clinic = clinic_map.get(clinic.get('id'), clinic)
                city_id = full_clinic.get('city_id')
                address = full_clinic.get('address', '—')
                city_url = city_data.get(city_id, {}).get('site', full_clinic.get('city_url', 'https://mrtexpert.ru'))
                directions = full_clinic.get('directions', '')
                prep_url = full_clinic.get('prep_url', city_url)
                phone_center = city_data.get(city_id, {}).get('phone', full_clinic.get('phone', '—'))
                dt_str = scheduled_at.strftime('%d.%m.%Y в %H:%M')
                time_str = scheduled_at.strftime('%H:%M')
                # 1. Новая запись
                if created_at and created_at > last_processed:
                    new_record_message = (
                        f"Здравствуйте!\n"
                        f"Вы записаны в МРТ Эксперт на {dt_str}.\n"
                        f"Адрес: {address}, {directions}\n"
                        f"Схема проезда {city_url}\n"
                        f"В центре нужно быть за 15 минут до приема.\n"
                        f"*Для вашего исследования необходима подготовка. Ознакомиться с ней можно по ссылке {prep_url}\n"
                        f"При себе необходимо иметь паспорт, направление, если оно есть, и результаты предыдущих исследований\n"
                        f"Телефон для связи: {phone_center}"
                    )
                    notified_new = False
                    # Формируем персонализированные сообщения
                    patient_name = f"{patient.get('lastname', '')} {patient.get('firstname', '')}".strip()
                    doctor_name = f"{item.get('doctor', {}).get('lastname', '')} {item.get('doctor', {}).get('firstname', '')}".strip()
                    service_name = item.get('service', {}).get('names', {}).get('name_display', 'МРТ')
                    scheduled_time = scheduled_at.strftime("%d.%m.%Y в %H:%M")
                    
                    try:
                        with httpx.Client() as client:
                            contacts_resp = client.get(
                                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                                headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                            )
                            contacts_resp.raise_for_status()
                            contacts = contacts_resp.json().get("payload", [])
                            contact = next((c for c in contacts if c["phone_number"] == f'+{phone}'), None)
                            if contact:
                                contact_id = contact["id"]
                                convs_resp = client.get(
                                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                                    headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                                )
                                convs_resp.raise_for_status()
                                conversations = convs_resp.json().get("payload", [])
                                for conv in conversations:
                                    conversation_id = conv["id"]
                                    msgs_resp = client.get(
                                        f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                                        headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                                    )
                                    msgs_resp.raise_for_status()
                                    messages = msgs_resp.json().get("payload", [])
                                    for msg in messages:
                                        if msg.get("content") == new_record_message:
                                            notified_new = True
                                            break
                                    if notified_new:
                                        break
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке сообщений о новой записи: {e}")
                    if not notified_new:
                        send_chatwoot_message(phone, new_record_message)
                        logger.info(f"Item {item.get('id', 'нет id')} новая запись: {scheduled_at_str}")
                        processed_count += 1
                    else:
                        logger.info(f"Item {item.get('id', 'нет id')} уже уведомлён о новой записи")

                # 2. Подтверждение записи (24 часа ± 1 час)
                if timedelta(hours=25) >= delta >= timedelta(hours=23):
                    confirm_message = (
                        f"Здравствуйте!\n"
                        f"Напоминаем, что вы записаны в МРТ Эксперт на {dt_str}.\n"
                        f"Подтвердите свой визит ответным сообщением (только цифра):\n"
                        f"1 – подтверждаю\n2- прошу перенести  \n3 – прошу отменить\n"
                        f"Телефон для связи {phone_center}"
                    )
                    notified_day = False
                    # Формируем персонализированное напоминание за день
                    patient_name = f"{patient.get('lastname', '')} {patient.get('firstname', '')}".strip()
                    doctor_name = f"{item.get('doctor', {}).get('lastname', '')} {item.get('doctor', {}).get('firstname', '')}".strip()
                    service_name = item.get('service', {}).get('names', {}).get('name_display', 'МРТ')
                    scheduled_time = scheduled_at.strftime("%d.%m.%Y в %H:%M")
                
                    try:
                        with httpx.Client() as client:
                            contacts_resp = client.get(
                                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                                headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                            )
                            contacts_resp.raise_for_status()
                            contacts = contacts_resp.json().get("payload", [])
                            contact = next((c for c in contacts if c["phone_number"] == f'+{phone}'), None)
                            if contact:
                                contact_id = contact["id"]
                                convs_resp = client.get(
                                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                                    headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                                )
                                convs_resp.raise_for_status()
                                conversations = convs_resp.json().get("payload", [])
                                for conv in conversations:
                                    conversation_id = conv["id"]
                                    msgs_resp = client.get(
                                        f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                                        headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                                    )
                                    msgs_resp.raise_for_status()
                                    messages = msgs_resp.json().get("payload", [])
                                    for msg in messages:
                                        if msg.get("content") == confirm_message:
                                            notified_day = True
                                            break
                                    if notified_day:
                                        break
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке сообщений о напоминании за день: {e}")
                    if not notified_day:
                        send_chatwoot_message(phone, confirm_message)
                        logger.info(f"Item {item.get('id', 'нет id')} напоминание за день: {scheduled_at_str}")
                        processed_count += 1
                    else:
                        logger.info(f"Item {item.get('id', 'нет id')} уже получил напоминание за день: {scheduled_at_str}")

                # 3. Напоминание за 2 часа до приема
                if timedelta(hours=2) >= delta > timedelta(0):
                    reminder_message = (
                        f"Здравствуйте!\n"
                        f"Напоминаем, что ваш прием в МРТ Эксперт сегодня в {time_str}.\n"
                        f"Телефон для связи {phone_center}."
                    )
                    notified = False
                    # Формируем персонализированное напоминание
                    patient_name = f"{patient.get('lastname', '')} {patient.get('firstname', '')}".strip()
                    doctor_name = f"{item.get('doctor', {}).get('lastname', '')} {item.get('doctor', {}).get('firstname', '')}".strip()
                    service_name = item.get('service', {}).get('names', {}).get('name_display', 'МРТ')
                    scheduled_time = scheduled_at.strftime("%d.%m.%Y в %H:%M")
                
                    try:
                        with httpx.Client() as client:
                            contacts_resp = client.get(
                                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts",
                                headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                            )
                            contacts_resp.raise_for_status()
                            contacts = contacts_resp.json().get("payload", [])
                            contact = next((c for c in contacts if c["phone_number"] == f'+{phone}'), None)
                            if contact:
                                contact_id = contact["id"]
                                convs_resp = client.get(
                                    f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations",
                                    headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                                )
                                convs_resp.raise_for_status()
                                conversations = convs_resp.json().get("payload", [])
                                for conv in conversations:
                                    conversation_id = conv["id"]
                                    msgs_resp = client.get(
                                        f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages",
                                        headers={"api_access_token": CHATWOOT_API_KEY}, timeout=10
                                    )
                                    msgs_resp.raise_for_status()
                                    messages = msgs_resp.json().get("payload", [])
                                    for msg in messages:
                                        if msg.get("content") == reminder_message:
                                            notified = True
                                            break
                                    if notified:
                                        break
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке сообщений: {e}")
                    if not notified:
                        send_chatwoot_message(phone, reminder_message)
                        logger.info(f"Item {item.get('id', 'нет id')} запланирован через 2 часа: {scheduled_at_str}")
                        processed_count += 1
                    else:
                        logger.info(f"Item {item.get('id', 'нет id')} уже был уведомлен, повтор не требуется: {scheduled_at_str}")
        # Сохраняем время последней обработки
        save_last_processed_time()
        logger.info(f"✅ Обработка завершена. Обработано элементов: {processed_count}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке элементов: {e}")
        # Даже при ошибке сохраняем время обработки, чтобы не повторять обработку
        save_last_processed_time()
