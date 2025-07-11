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

def process_items_cron():
    try:
        # Получаем время последней обработки
        last_processed = get_last_processed_time()
        now = datetime.now(timezone.utc)
        
        logger.info(f"🕐 Обработка данных с {last_processed.strftime('%Y-%m-%d %H:%M:%S')} до {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Здесь должен быть реальный API запрос для получения данных
        # Пока используем тестовые данные
        data = {
            "success": True,
            "result": [
                {
                    "id": "d2b2cf91-8eb3-11ef-b7d4-0050560c1b69",
                    "clinic": {
                        "id": "376bcf13-05d7-11e5-bc43-002590e38b62",
                        "name": "Москва1 МРТ-Эксперт"
                    },
                    "patient": {
                        "id": "365dd5c8-8ebe-11ef-b35f-0050560c262e",
                        "firstname": "Илья",
                        "lastname": "Анисимов",
                        "middlename": "Николаевна",
                        "birthdate": "1989-12-20",
                        "sex": "F",
                        "phone": "79255890919",
                        "email": "template@bk.ru",
                        "snils": "",
                        "email_confirm": True
                    },
                    "items": [
                        {
                            "id": "808657a2-8ef0-4f8e-b3a3-25270192e116",
                            "ris_id": [
                                "4cc0661b-147d-4842-b4a3-96eb518a2b21"
                            ],
                            "service": {
                                "id": "31d7dfc5-ac1f-11e9-b820-00505693b6f1",
                                "names": {
                                    "name_mz": "Магнитно-резонансная томография позвоночника (один отдел)/шейный отдел",
                                    "name_display": "Магнитно-резонансная томография позвоночника (один отдел)/шейный отдел"
                                },
                                "duration": 30,
                                "price": {
                                    "amount": 5750,
                                    "currency": "rub"
                                }
                            },
                            "scheduled_at": (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat(),
                            "status": "confirmed",
                            "doctor": {
                                "id": "9b3bfc64-cb60-11ee-b7ce-0050560c10ce",
                                "firstname": "Никита",
                                "lastname": "Стуколов",
                            },
                            "profession": {
                                "id": "6cae07d0-67ba-11eb-b822-005056b387b3",
                                "bank": False,
                                "position": "Врач-рентгенолог",
                                "position_id": "c1c325e4-86ca-11e9-b81f-00505693b6f1",
                                "specialization": "МРТ.",
                                "specialization_id": "a14f4089-b932-11ed-bc3b-00155d000204"
                            },
                            "provider": {
                                "id": "83ad5a3e-bc83-11ec-b822-005056b3ebff",
                                "name": "СберЗдоровье"
                            },
                            "refdoctor": None,
                            "partners_finances": False
                        }
                    ],
                   
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": "2025-07-05T10:17:58+03:00"
                }
            ],
            "errors": [],
            "info": {
                "count": 3776,
                "page": 1,
                "more": True,
                "limit": 1
            }
        }
        
        # Добавим дополнительные тестовые записи для демонстрации всех типов уведомлений
        # Вторая запись для напоминания за день
        second_obj = copy.deepcopy(data['result'][0])
        second_item = copy.deepcopy(second_obj['items'][0])
        second_item['scheduled_at'] = (now + timedelta(hours=23, minutes=30)).isoformat()
        second_obj['items'] = [second_item]
        second_obj['created_at'] = (now - timedelta(hours=25)).isoformat()
        data['result'].append(second_obj)
        
        # Третья запись - новая запись
        third_obj = copy.deepcopy(data['result'][0])
        third_item = copy.deepcopy(third_obj['items'][0])
        third_item['scheduled_at'] = (now + timedelta(hours=48)).isoformat()
        third_obj['items'] = [third_item]
        third_obj['created_at'] = now.isoformat()
        data['result'].append(third_obj)
        
        # Настраиваем тестовые данные для разных сценариев
        # Первая запись — для напоминания за 2 часа (до приема < 2 часа)
        if data['result']:
            first_obj = data['result'][0]
            if first_obj.get('items'):
                first_obj['items'][0]['scheduled_at'] = (now + timedelta(hours=1, minutes=59)).isoformat()
                first_obj['created_at'] = (now - timedelta(hours=25)).isoformat()  # чтобы не было нового уведомления
                logger.info(f"Первая запись настроена для напоминания за 2 часа: {first_obj['items'][0]['scheduled_at']}")
        
        # Вторая запись — для напоминания за день (до приема ~24 часа)
        if len(data['result']) > 1:
            second_obj = data['result'][1]
            if second_obj.get('items'):
                second_obj['items'][0]['scheduled_at'] = (now + timedelta(hours=23, minutes=30)).isoformat()
                second_obj['created_at'] = (now - timedelta(hours=25)).isoformat()  # старая запись
                logger.info(f"Вторая запись настроена для напоминания за день: {second_obj['items'][0]['scheduled_at']}")
        
        # Третья запись — новая запись (для тестирования создания записей)
        if len(data['result']) > 2:
            third_obj = data['result'][2]
            if third_obj.get('items'):
                third_obj['items'][0]['scheduled_at'] = (now + timedelta(hours=48)).isoformat()
                third_obj['created_at'] = now.isoformat()  # НОВАЯ запись
                logger.info(f"Третья запись настроена как НОВАЯ: {third_obj['items'][0]['scheduled_at']}")
        
        # Остальные записи — только к следующему запуску (до приема > 2 часа)
        for i, obj in enumerate(data['result'][3:], start=3):
            for item in obj.get('items', []):
                item['scheduled_at'] = (now + timedelta(hours=72 + i)).isoformat()
            obj['created_at'] = (now - timedelta(hours=1)).isoformat()
            logger.info(f"Запись {i+1} настроена на будущее: {obj['items'][0]['scheduled_at']}")
        
        objects = data.get('result', [])
        processed_count = 0
        for obj in objects:
            patient = obj.get('patient', {})
            phone = patient.get('phone')
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
                
                # Уведомление о новой записи (создана после последней обработки)
                if created_at and created_at > last_processed:
                    notified_new = False
                    # Формируем персонализированные сообщения
                    patient_name = f"{patient.get('lastname', '')} {patient.get('firstname', '')}".strip()
                    doctor_name = f"{item.get('doctor', {}).get('lastname', '')} {item.get('doctor', {}).get('firstname', '')}".strip()
                    service_name = item.get('service', {}).get('names', {}).get('name_display', 'МРТ')
                    scheduled_time = scheduled_at.strftime("%d.%m.%Y в %H:%M")
                    
                    new_record_message = f"Здравствуйте {patient_name}, вы успешно создали запись к {doctor_name} на {service_name} в {scheduled_time}"
                    
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

                # Напоминание за день до приема (24 часа ± 1 час)
                if timedelta(hours=25) >= delta >= timedelta(hours=23):
                    notified_day = False
                    # Формируем персонализированное напоминание за день
                    patient_name = f"{patient.get('lastname', '')} {patient.get('firstname', '')}".strip()
                    doctor_name = f"{item.get('doctor', {}).get('lastname', '')} {item.get('doctor', {}).get('firstname', '')}".strip()
                    service_name = item.get('service', {}).get('names', {}).get('name_display', 'МРТ')
                    scheduled_time = scheduled_at.strftime("%d.%m.%Y в %H:%M")
                    
                    day_reminder_message = f"Здравствуйте {patient_name}, напоминаем вам о записи завтра у {doctor_name} на {service_name} в {scheduled_time}"
                
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
                                        if msg.get("content") == day_reminder_message:
                                            notified_day = True
                                            break
                                    if notified_day:
                                        break
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке сообщений о напоминании за день: {e}")
                    if not notified_day:
                        send_chatwoot_message(phone, day_reminder_message)
                        logger.info(f"Item {item.get('id', 'нет id')} напоминание за день: {scheduled_at_str}")
                        processed_count += 1
                    else:
                        logger.info(f"Item {item.get('id', 'нет id')} уже получил напоминание за день: {scheduled_at_str}")

                # Напоминание за 2 часа до приема (проверяем, что не отправляли в последний час)
                if timedelta(hours=2) >= delta > timedelta(0):
                    # отправить напоминание, если еще не отправляли
                    notified = False
                    # Формируем персонализированное напоминание
                    patient_name = f"{patient.get('lastname', '')} {patient.get('firstname', '')}".strip()
                    doctor_name = f"{item.get('doctor', {}).get('lastname', '')} {item.get('doctor', {}).get('firstname', '')}".strip()
                    service_name = item.get('service', {}).get('names', {}).get('name_display', 'МРТ')
                    scheduled_time = scheduled_at.strftime("%d.%m.%Y в %H:%M")
                    
                    reminder_message = f"Здравствуйте {patient_name}, напоминаем вам о записи у {doctor_name} на {service_name} через 2 часа в {scheduled_time}"
                
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
