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

PENDING_NOTIFICATIONS_FILE = Path("pending_notifications.json")

def load_pending_notifications():
    if PENDING_NOTIFICATIONS_FILE.exists():
        try:
            with open(PENDING_NOTIFICATIONS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Ошибка при чтении pending_notifications: {e}")
    return {}
            
def save_pending_notifications(data):
    try:
        with open(PENDING_NOTIFICATIONS_FILE, 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка при сохранении pending_notifications: {e}")

def get_last_user_message(phone):
    # Возвращает последнее входящее сообщение пользователя (из Chatwoot)
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
                    # Найти последнее входящее сообщение пользователя
                    for msg in reversed(messages):
                        if msg.get("message_type") == "incoming":
                            return msg.get("content"), msg.get("created_at")
    except Exception as e:
        logger.warning(f"Ошибка при получении последнего сообщения пользователя: {e}")
    return None, None

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
        processed_count = 0
        
        # Группируем записи по пользователю (телефону) и дате
        user_appointments = {}
        for obj in all_appointments:
            patient = obj.get('patient', {})
            phone = patient.get('phone') or "998998180817"
            items = obj.get('items', [])
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
                # Логируем нужную запись
                if scheduled_at_str.startswith("2025-07-16T13:10:00"):
                    logger.info(f"[DEBUG] Найдена запись с scheduled_at=2025-07-16T13:10:00+03:00: obj={json.dumps(obj, ensure_ascii=False)}, item={json.dumps(item, ensure_ascii=False)}")
                # Ключ: (phone, дата)
                date_key = scheduled_at.date().isoformat()
                created_at_str = obj.get('created_at')
                created_at = None
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str)
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                    except Exception as e:
                        logger.warning(f"Некорректный формат времени created_at: {created_at_str}, ошибка: {e}")

                user_appointments.setdefault(phone, {}).setdefault(date_key, []).append({
                    'obj': obj,
                    'item': item,
                    'scheduled_at': scheduled_at,
                    'created_at': created_at,
                })
        # Загружаем состояние уведомлений
        pending_notifications = load_pending_notifications()
        for phone, dates in user_appointments.items():
            for date_key, appts in dates.items():
                appts.sort(key=lambda x: x['scheduled_at'])
                queue = appts
                state = pending_notifications.get(phone, {}).get(date_key, {})
                idx = state.get('last_sent_idx', 0)
                last_sent_time = state.get('last_sent_time')
                last_sent_type = state.get('last_sent_type')
                last_sent_content = state.get('last_sent_content')
                last_answer_time = state.get('last_answer_time')
                answered = False
                if last_sent_time:
                    last_user_msg, last_user_msg_time = get_last_user_message(phone)
                    if last_user_msg_time and last_user_msg_time > last_sent_time:
                        answered = True
                        state['last_answer_time'] = last_user_msg_time
                if answered or not last_sent_time:
                    if idx < len(queue):
                        # Только одно уведомление за запуск крона
                        entry = queue[idx]
                        obj = entry['obj']
                        item = entry['item']
                        scheduled_at = entry['scheduled_at']
                        created_at = entry['created_at']
                        delta = scheduled_at - now
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
                            send_chatwoot_message(phone, new_record_message)
                            logger.info(f"[QUEUE] {phone} уведомление: {dt_str} (новая запись)")
                            pending_notifications.setdefault(phone, {})[date_key] = {
                                'last_sent_idx': idx + 1,
                                'last_sent_time': datetime.now(timezone.utc).isoformat(),
                                'last_sent_type': 'new_record',
                                'last_sent_content': new_record_message,
                                'last_answer_time': state.get('last_answer_time'),
                            }
                            processed_count += 1
                            break  # Только одно уведомление за запуск
                        # 2. Напоминание за день
                        elif timedelta(hours=25) >= delta >= timedelta(hours=23):
                            confirm_message = (
                                f"Здравствуйте!\n"
                                f"Напоминаем, что вы записаны в МРТ Эксперт на {dt_str}.\n"
                                f"Подтвердите свой визит ответным сообщением (только цифра):\n"
                                f"1 – подтверждаю\n2- прошу перенести  \n3 – прошу отменить\n"
                                f"Телефон для связи {phone_center}"
                            )
                            send_chatwoot_message(phone, confirm_message)
                            logger.info(f"[QUEUE] {phone} уведомление: {dt_str} (напоминание за день)")
                            pending_notifications.setdefault(phone, {})[date_key] = {
                                'last_sent_idx': idx + 1,
                                'last_sent_time': datetime.now(timezone.utc).isoformat(),
                                'last_sent_type': 'confirm',
                                'last_sent_content': confirm_message,
                                'last_answer_time': state.get('last_answer_time'),
                            }
                            processed_count += 1
                            break  # Только одно уведомление за запуск
                        # 3. Напоминание за 2 часа
                        elif timedelta(hours=2) >= delta > timedelta(0):
                            reminder_message = (
                                f"Здравствуйте!\n"
                                f"Напоминаем, что ваш прием в МРТ Эксперт сегодня в {time_str}.\n"
                                f"Телефон для связи {phone_center}."
                            )
                            send_chatwoot_message(phone, reminder_message)
                            logger.info(f"[QUEUE] {phone} уведомление: {dt_str} (напоминание за 2 часа)")
                            pending_notifications.setdefault(phone, {})[date_key] = {
                                'last_sent_idx': idx + 1,
                                'last_sent_time': datetime.now(timezone.utc).isoformat(),
                                'last_sent_type': 'reminder',
                                'last_sent_content': reminder_message,
                                'last_answer_time': state.get('last_answer_time'),
                            }
                            processed_count += 1
                            break  # Только одно уведомление за запуск
                    else:
                        logger.info(f"[QUEUE] {phone} нет подходящего уведомления для {scheduled_at}")
        # Сохраняем состояние уведомлений
        save_pending_notifications(pending_notifications)
        # Сохраняем время последней обработки
        save_last_processed_time()
        logger.info(f"✅ Обработка завершена. Обработано элементов: {processed_count}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке элементов: {e}")
        # Даже при ошибке сохраняем время обработки, чтобы не повторять обработку
        save_last_processed_time()
