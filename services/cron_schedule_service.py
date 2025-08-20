import logging
from datetime import datetime, timedelta, timezone
import httpx
import os
import json
from db import SessionLocal
from models.sended_message import SendedMessage
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
load_dotenv()
logger = logging.getLogger("uvicorn.webhook")
GREENAPI_ID = os.getenv("GREENAPI_ID")
GREENAPI_TOKEN = os.getenv("GREENAPI_TOKEN")
CHATWOOT_API_KEY = os.getenv("CHATWOOT_API_KEY")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")
CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL")
GOOGLE_API_DOCS_SECRET = os.getenv("GOOGLE_API_DOCS_SECRET")
APPOINTMENTS_API_KEY = os.getenv("APPOINTMENTS_API_KEY") or 'ENByZZnh5rvXfxHd8LeqrhVA'

LAST_PROCESSED_FILE = Path("last_processed.json")

def get_last_processed_time():
    tz_msk = timezone(timedelta(hours=3))
    now = datetime.now(tz=tz_msk)
    print('hop', now - timedelta(hours=1), now)
    # Всегда возвращаем текущее время минус 1 час в UTC+3
    return now - timedelta(hours=1)
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
    except Exception as e:
        logger.error(f"Ошибка отправки в Chatwoot: {e}")

city_data = {
    "19901c01-523d-11e5-bd0c-c8600054f881": {
        "address": "г. Липецк, пл. Петра Великого, дом 2",
        "site": "https://lip.mrtexpert.ru/clinics/1/map.svg",
        "phone": "84742505105"
    }
}

def save_last_processed_time(): 
    try:
        db = SessionLocal()
        moscow_tz = timezone(timedelta(hours=3))
        now = datetime.now(moscow_tz) 
        local_hour = now.astimezone(timezone(timedelta(hours=3))).hour 
        with open(LAST_PROCESSED_FILE, 'w') as f:
            json.dump({'last_processed': now.isoformat()}, f)
            pending_messages = db.query(SendedMessage).filter(
            SendedMessage.type.in_(["pending", "pending_day"])
            ).all()
        processed_count = 0
        notified_phones = set()
        for msg in pending_messages:
            try:
                phone = msg.phone_number
                if phone in notified_phones:
                    continue  
                scheduled_at = msg.scheduled_at
                if scheduled_at.tzinfo is None:
                    scheduled_at = scheduled_at.replace(tzinfo=moscow_tz)
                else:
                    scheduled_at = scheduled_at.astimezone(moscow_tz)

                delta = scheduled_at - now
                minutes_to_appointment = int(delta.total_seconds() / 60)
                if minutes_to_appointment <= 0:
                    continue  

                dt_str = scheduled_at.strftime('%d.%m.%Y в %H:%M')
                time_str = scheduled_at.strftime('%H:%M')
                phone_center = msg.phone_center

                sent_types = [
                    m.type for m in db.query(SendedMessage).filter(
                        SendedMessage.appointment_id == msg.appointment_id,
                        SendedMessage.type.in_(["new_remind", "day_remind", "hour_remind"])
                    ).all()
                ]
                # === Обработка pending (обычные) → hour_remind ===
                if msg.type == "pending" and 90 <= minutes_to_appointment <= 120 and "hour_remind" not in sent_types and local_hour >= 8  and local_hour < 21:
                    hour_msg = (    
                        f"Здравствуйте!\n"
                        f"Напоминаем, что ваш прием в МРТ Эксперт сегодня в {time_str}.\n"
                        f"В центре нужно быть за 15 минут до начала приема для оформления документов.\n"
                        f"Телефон для связи {phone_center}."
                    )
                    send_chatwoot_message(phone, hour_msg)
                    db.add(SendedMessage(
                        appointment_id=msg.appointment_id,
                        type="hour_remind",
                        scheduled_at=scheduled_at,
                        phone_number=phone,
                        phone_center=phone_center,
                        appointment_json=msg.appointment_json
                    ))
                    db.commit()
                    logger.info(f"⏰ Утром отправлено hour_remind из pending: {msg.appointment_id}")
                    processed_count += 1
                # === Обработка pending → day_remind ===
                if msg.type == "pending" and 1400 <= minutes_to_appointment <= 1440 and "day_remind" not in sent_types and local_hour >= 8  and local_hour < 21:
                    day_msg = (
                        f"Здравствуйте!\n"
                        f"Напоминаем, что вы записаны в Клинику Эксперт на {dt_str}.\n"
                        f"Подтвердите свой визит ответным сообщением (только цифра):\n"
                        f"1 – подтверждаю\n3 – прошу отменить\n"
                        f"Для переноса записи обратитесь к нам по телефону: {phone_center}"
                    )
                    send_chatwoot_message(phone, day_msg)
                    db.add(SendedMessage(
                        appointment_id=msg.appointment_id,
                        type="day_remind",
                        scheduled_at=scheduled_at,
                        phone_number=phone,
                        phone_center=phone_center
                    ))
                    db.commit()
                    logger.info(f"📆 Утром отправлено day_remind из pending: {msg.appointment_id}")
                    processed_count += 1
                    continue
                if msg.type == "pending" and now.hour == 20 and msg.send_after:
                    if 24 * 60 <= minutes_to_appointment <= 24 * 60 + 13 * 60 and "day_remind" not in sent_types:
                        day_msg = (
                            f"Здравствуйте!\n"
                            f"Напоминаем, что вы записаны в МРТ Эксперт на {dt_str}.\n"
                            f"Подтвердите свой визит ответным сообщением (только цифра):\n"
                            f"1 – подтверждаю\n3 – прошу отменить\n"
                            f"Телефон для связи: {phone_center}"
                        )
                        send_chatwoot_message(phone, day_msg)
                        db.add(SendedMessage(
                            appointment_id=msg.appointment_id,
                            type="day_remind",
                            scheduled_at=scheduled_at,
                            phone_number=phone,
                            phone_center=phone_center,
                            appointment_json=msg.appointment_json
                        ))
                        db.commit()
                        logger.info(f"🌙 Догнали day_remind (+13ч): {msg.appointment_id}")

                    # 2ч-догон: [120 .. 120+13h]
                    if  2 * 60  <= minutes_to_appointment <=  90 + 13 * 60 and "hour_remind" not in sent_types:
                        hour_msg = (
                            f"Здравствуйте!\n"
                            f"Напоминаем, что ваш прием в МРТ Эксперт сегодня в {time_str}.\n"
                            f"В центре нужно быть за 15 минут до начала приема для оформления документов.\n"
                            f"Телефон для связи {phone_center}."
                        )
                        send_chatwoot_message(phone, hour_msg)
                        db.add(SendedMessage(
                            appointment_id=msg.appointment_id,
                            type="hour_remind",
                            scheduled_at=scheduled_at,
                            phone_number=phone,
                            phone_center=phone_center,
                            appointment_json=msg.appointment_json
                        ))
                        db.commit()
                        logger.info(f"🌙 Догнали hour_remind (+13ч): {msg.appointment_id}")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при обработке pending сообщения {msg.appointment_id}: {e}")

        logger.info(f"✅ Обработка отложенных уведомлений завершена. Отправлено: {processed_count}")

    except Exception as e:
        logger.error(f"❌ Ошибка в save_last_processed_time: {e}")
    finally: 
        db.close()

def process_items_cron():
    db = SessionLocal()
    try:
        # Часовой пояс Москвы (+03:00)
        moscow_tz = timezone(timedelta(hours=3))
        # Последний обработанный момент — тоже в МСК
        last_processed = get_last_processed_time().astimezone(moscow_tz)
        # Текущее время в МСК
        now = datetime.now(moscow_tz)

        logger.info(f"🕐 Обработка данных с {last_processed.strftime('%Y-%m-%d %H:%M:%S')} до {now.strftime('%Y-%m-%d %H:%M:%S')}")

        auth_header = {"Authorization": f"Bearer {APPOINTMENTS_API_KEY}"}
        skip_statuses = ['paid', 'done', 'canceled', 'started']

        clinics = [ {
            "id": "19901c01-523d-11e5-bd0c-c8600054f881",
            "name": "Липецк 1 МРТ-Эксперт",
            "region": "Липецкая обл",
            "region_code": 48,
            "city_id": "eacb5f15-1a2e-432e-904a-ca56bd635f1b",
            "address": "398001, Липецкая обл, Липецк г, Петра Великого пл, владение № 2",
            "work_time": {
                "mo": "07:00-23:00",
                "tu": "07:00-23:00",
                "we": "07:00-23:00",
                "th": "07:00-23:00",
                "fr": "07:00-23:00",
                "sa": "07:00-23:00",
                "su": "07:00-23:00"
            },
            "longitude": "0",
            "latitude": "0"
        },]

        clinic_map = {c['id']: c for c in clinics}
        all_appointments = []

        for clinic in clinics:
            cid = clinic.get('id', '')
            if not cid:
                continue
            try:
                today_str = now.strftime('%Y-%m-%d')
                app_resp = httpx.get(
                    f"https://api.mrtexpert.ru/api/v3/appointments?clinic_id={cid}&created_from={today_str}&created_to={today_str}",
                    timeout=60,
                    headers=auth_header
                )
                upd_resp = httpx.get(
                    f"https://api.mrtexpert.ru/api/v3/appointments?clinic_id={cid}&updated_from={today_str}&updated_to={today_str}",
                    timeout=60,
                    headers=auth_header
                )
                app_resp.raise_for_status()
                upd_resp.raise_for_status()
                created = app_resp.json().get("result", [])
                updated = upd_resp.json().get("result", [])
                updated_ids = {appt['id'] for appt in updated}
                merged_appointments = [appt for appt in created if appt["id"] not in updated_ids]
                all_appointments.extend(updated + merged_appointments)
            except Exception as e:
                logger.error(f"Ошибка при получении заявок клиники {cid}: {e}")
        grouped = defaultdict(lambda: defaultdict(list))
        grouped_full = defaultdict(lambda: defaultdict(list))
        for appt in all_appointments:
            phone = appt.get("patient", {}).get("phone")
            clinic = appt.get("clinic", {})
            patient = appt.get("patient", {})
            for item in appt.get("items", []):
                scheduled_at = item.get("scheduled_at")
                if not phone or not scheduled_at:
                    logger.info(f"⛔ Пропущена запись: нет телефона или времени (phone={phone}, scheduled_at={scheduled_at})")
                    continue
                try:
                    dt = datetime.fromisoformat(scheduled_at).astimezone(moscow_tz)
                except ValueError:
                    logger.info(f"⛔ Пропущена запись: неверный формат времени '{scheduled_at}'")
                    continue
                date_key = dt.date().isoformat()
                grouped[phone][date_key].append({
                    "appointment_id": appt.get("id"),
                    "item": item,
                    "scheduled_at": scheduled_at,
                    "dt": dt,
                    "clinic": clinic,
                    "patient": patient
                })
                grouped_full[phone][date_key].append(appt)
        processed_count = 0
        notified_phones = set()
        services_prepare_messages = {}
        for phone, dates in grouped.items():
            if not phone:
                logger.info("⛔ Пропуск: пустой номер телефона")
                continue
            for date_str, items in dates.items():
                if phone in notified_phones:
                    logger.info(f"⛔ Пропуск: номер уже получил сообщение: {phone}")
                    continue
                if not items:
                    logger.info(f"⛔ Пропуск: нет записей у {phone} на {date_str}")
                    continue
                
                earliest_item_obj = None
                earliest_time = None

                for appt in items:
                    item = appt.get("item")
                    scheduled_at_str = appt.get("scheduled_at")
                    if not item or not scheduled_at_str:
                        logger.info(f"⛔ Пропущен элемент: нет item или времени (item={item}, scheduled_at={scheduled_at_str})")
                        continue
                    try:
                        dt = datetime.fromisoformat(scheduled_at_str)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=moscow_tz)
                        else:
                            dt = dt.astimezone(moscow_tz)
                    except Exception as e:
                        logger.warning(f"Неверный формат времени: {scheduled_at_str}, ошибка: {e}")
                        continue

                    if earliest_time is None or dt < earliest_time:
                        earliest_time = dt
                        earliest_item_obj = appt
                if not earliest_item_obj or earliest_time < now:
                    logger.info(f"⛔ Пропуск: запись в прошлом ({earliest_time.isoformat() if earliest_time else 'None'})")
                    continue
                item = earliest_item_obj["item"]
                print("sdcsds", item)
                item_id = item.get("id")
                list_of_apt_in_one_day = grouped_full[phone][datetime.fromisoformat(scheduled_at).date().isoformat()]
                item_status = item.get("status")
                clinic = earliest_item_obj.get("clinic", {})
                patient = earliest_item_obj.get("patient", {})

                appointment_in_db = db.query(SendedMessage).filter(
                    SendedMessage.appointment_id == item_id,
                    SendedMessage.type.in_(['pending', 'pending_day'])
                ).first()

                if item_status in skip_statuses:
                    logger.info(f"⛔ Пропуск: статус {item_status} из списка исключений")
                    if appointment_in_db:
                        db.delete(appointment_in_db)
                        db.commit()
                        logger.info(f"🗑 Удалено pending сообщение для {item_id} (статус: {item_status})")
                    continue
                if appointment_in_db and appointment_in_db.scheduled_at != earliest_time:
                    appointment_in_db.scheduled_at = earliest_time
                    outdated_reminders = db.query(SendedMessage).filter(
                        SendedMessage.appointment_id == item_id,
                        SendedMessage.type.in_(['new_remind', 'day_remind', 'hour_remind'])
                    ).all()
                    print("ggh",appointment_in_db)
                    for reminder in outdated_reminders:
                        db.delete(reminder)
                    db.commit()
                    logger.info(f"✏️ Обновлено pending сообщение для {item_id}: новое время {earliest_time.isoformat()}")
                delta = earliest_time - now
                dt_str = earliest_time.strftime('%d.%m.%Y %H:%M')
                full_clinic = clinic_map.get(clinic.get("id"), clinic)
                address = full_clinic.get("address", "—")
                directions = full_clinic.get("directions", "")
                phone_center = city_data.get(full_clinic.get("city_id", ""), {}).get("phone", full_clinic.get("phone", "84742505105"))
                minutes_to_appointment = int(delta.total_seconds() / 60)
                if minutes_to_appointment <= 30:
                    logger.info(f"⏩ Пропущено: осталось {int(delta.total_seconds() // 60)} мин до приёма в {earliest_time.strftime('%d.%m.%Y %H:%M')}")
                    continue
                sent_new = db.query(SendedMessage).filter_by(appointment_id=item_id, type="new_remind").first()
                if not sent_new:
                    logger.info(f"📨 Отправка напоминания {item_id} ({dt_str}) для {phone}")
                    new_msg = (
                            f"Здравствуйте!\n"
                            f"\n"
                            f"Вы записаны в МРТ Эксперт на { 'несколько услуг, первый прием в' if len(list_of_apt_in_one_day) > 1 else ''} {dt_str}.\n"
                            f"\n"
                            f"Адрес: {address}, {directions}\n"
                            f"\n"
                            f"В центре нужно быть за 15 минут до приема.\n"
                            f"\n"
                            f"При себе необходимо иметь паспорт, снилс , направление, если оно есть, и результаты предыдущих исследований\n"
                            f"\n"
                            f"Телефон для связи: {phone_center}\n"
                            f"\n"
                            f"Если вы проходите процедуру МРТ впервые, рекомендуем посмотреть видео описание о том как проходит процедура по ссылке: https://vk.com/video-48669646_456239221?list=ec01502c735e906314"
                    )
                    send_chatwoot_message(phone, new_msg)
                    try:
                        service_id = item.get('service', {}).get('id', '')
                        if not service_id:
                            continue  # или лог, если ID отсутствует

                        if service_id not in services_prepare_messages:
                            try:
                                service_resp = httpx.get(
                                    f"https://api.mrtexpert.ru/api/v3/services/{service_id}?clinic_id={clinic.get('id')}",
                                    timeout=20,
                                    headers=auth_header
                                )
                                service_resp.raise_for_status()
                                prepare_message = service_resp.json().get("result", {}).get("prepare", "")
                                services_prepare_messages[service_id] = prepare_message
                                if prepare_message:
                                    send_chatwoot_message(phone, prepare_message)
                                    logger.info(f"📄 Отправлено сообщение с подготовкой: {item_id}")
                            except Exception as e:
                                logger.warning(f"Ошибка получения подготовки для service_id {service_id}: {e}")
                        else:
                            # Повторное использование сохранённого сообщения
                            saved_prepare_message = services_prepare_messages[service_id]
                            if saved_prepare_message:
                                send_chatwoot_message(phone, saved_prepare_message)
                                logger.info(f"📄 Отправлено сохраненное сообщение с подготовкой: {item_id}")
                    except Exception as e:
                        logger.warning(f"Ошибка получения подготовки: {e}")
                    db.add(SendedMessage(
                       appointment_id=item_id,
                        type="new_remind",
                        scheduled_at=earliest_time,
                        phone_number=phone,
                        phone_center=phone_center,
                        appointment_json=list_of_apt_in_one_day
                    ))
                    db.add(SendedMessage(
                        appointment_id=item_id,
                        type="pending",
                        scheduled_at=earliest_time,
                        phone_number=phone,
                        phone_center=phone_center,
                        appointment_json=list_of_apt_in_one_day,
                        send_after=True if earliest_time.hour >= 21 or earliest_time.hour < 8 else False
                    ))
                    db.commit()

                    logger.info(f"🟢 Новая запись отправлена: {item_id}")
                    notified_phones.add(phone)
                    processed_count += 1
                    continue
                if 1400 <= minutes_to_appointment <= 1440 and 0 <= earliest_time.hour < 7:
                    logger.info(f"🌙 Ночь: откладываем сообщение (pending_day) для {item_id}")
                    is_created_type = db.query(SendedMessage).filter_by(
                        appointment_id=item_id, type="pending_day"
                    ).first()
                    if not is_created_type:
                        db.add(SendedMessage(
                            appointment_id=item_id,
                            type="pending_day",
                            scheduled_at=earliest_time,
                            phone_number=phone,
                            phone_center=phone_center
                        ))
                        db.commit()
            logger.info(f"✅ Завершено. Уведомлений отправлено: {processed_count}")

    except Exception as e:
        logger.error(f"❌ Ошибка в process_items_cron: {e}")
    finally:
        save_last_processed_time()
        db.close()
def cleanup_old_messages():
    try:
        logger.info("🧹 Запуск ежедневной очистки старых сообщений")
        db = SessionLocal()
        tz_msk = timezone(timedelta(hours=3))
        now = datetime.now(tz=tz_msk)
        messages = db.query(SendedMessage).all()
        deleted_count = 0

        for msg in messages:
            try:
                scheduled_at_str = msg.scheduled_at
                scheduled_at = datetime.fromisoformat(scheduled_at_str)
                if scheduled_at.tzinfo is None:
                    scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
                if scheduled_at < now:
                    db.delete(msg)
                    deleted_count += 1
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при парсинге даты у сообщения {msg.id}: {e}")
        db.commit()
        logger.info(f"🗑 Удалено {deleted_count} устаревших сообщений")
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке старых сообщений: {e}")
    finally:
        db.close()