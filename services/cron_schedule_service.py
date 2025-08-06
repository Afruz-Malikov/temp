import logging
from datetime import datetime, timedelta, timezone
import httpx
import os
import json
from db import SessionLocal
from models.sended_message import SendedMessage
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import or_, and_

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
        "address": "г Мытищи, ул Колпакова, д. 2А,  помещ. 54",
        "site": "https://myt.mrtexpert.ru/",
        "phone": "84953080411"
    }
}

def save_last_processed_time():
    try:
        db = SessionLocal()
        now = datetime.now(timezone.utc)
        local_hour = now.astimezone(timezone(timedelta(hours=3))).hour

        # сохраняем время последней обработки
        with open(LAST_PROCESSED_FILE, 'w') as f:
            json.dump({'last_processed': now.isoformat()}, f)
        processed_count = 0
        notified_phones = set()
        # --- pending_day ---
        pending_day_messages = db.query(SendedMessage).filter(
            or_(
                SendedMessage.type == "pending",
                and_(
                    SendedMessage.type == "pending_day",
                    SendedMessage.scheduled_at >= (now + timedelta(minutes=1400)),
                    SendedMessage.scheduled_at <= (now + timedelta(minutes=1440))
                )
            )
        ).all()

        for msg in pending_day_messages:
            try:
                phone = msg.phone_number
                if phone in notified_phones:
                    continue

                scheduled_at = msg.scheduled_at  # уже datetime
                if scheduled_at.tzinfo is None:
                    scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)

                minutes_to_appointment = int((scheduled_at - now).total_seconds() / 60)
                if minutes_to_appointment <= 0:
                    continue

                dt_str = scheduled_at.strftime('%d.%m.%Y в %H:%M')
                phone_center = msg.phone_center
                sent_types = [
                    m.type for m in db.query(SendedMessage).filter(
                        SendedMessage.appointment_id == msg.appointment_id,
                        SendedMessage.type.in_(["day_remind", "hour_remind"])
                    ).all()
                ]

                if "day_remind" not in sent_types and local_hour > 7:
                    day_msg = (
                        f"Здравствуйте!\n"
                        f"Напоминаем, что вы записаны в МРТ Эксперт на {dt_str}.\n"
                        f"Подтвердите свой визит ответным сообщением (только цифра):\n"
                        f"1 – подтверждаю\n2 – прошу перенести\n3 – прошу отменить\n"
                        f"Телефон для связи {phone_center}"
                    )
                    send_chatwoot_message(phone, day_msg)

                    db.add(SendedMessage(
                        appointment_id=msg.appointment_id,
                        type="day_remind",
                        scheduled_at=scheduled_at,
                        phone_number=phone,
                        phone_center=phone_center
                    ))
                    db.add(SendedMessage(
                        appointment_id=msg.appointment_id,
                        type="pending",
                        scheduled_at=scheduled_at,
                        phone_number=msg.phone_number,
                        phone_center=msg.phone_center
                    ))
                    db.commit()

                    logger.info(f"📆 Отправлено day_remind из pending_day: {msg.appointment_id}")
                    processed_count += 1
                    notified_phones.add(phone)
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при обработке pending_day {msg.appointment_id}: {e}")
        # --- pending_hour ---
        pending_hour_messages = db.query(SendedMessage).filter(
            or_(
                SendedMessage.type == "pending",
                and_(
                    SendedMessage.scheduled_at >= (now + timedelta(minutes=110)),
                    SendedMessage.scheduled_at <= (now + timedelta(minutes=120))
                )
            )
        ).all()

        for msg in pending_hour_messages:
            try:
                phone = msg.phone_number
                if phone in notified_phones:
                    continue

                scheduled_at = msg.scheduled_at  # уже datetime
                if scheduled_at.tzinfo is None:
                    scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)

                minutes_to_appointment = int((scheduled_at - now).total_seconds() / 60)
                if minutes_to_appointment <= 0:
                    continue

                time_str = scheduled_at.strftime('%H:%M')
                phone_center = msg.phone_center

                sent_types = [
                    m.type for m in db.query(SendedMessage).filter(
                        SendedMessage.appointment_id == msg.appointment_id,
                        SendedMessage.type.in_(["hour_remind"])
                    ).all()
                ]

                if "hour_remind" not in sent_types:
                    hour_msg = (
                        f"Здравствуйте!\n"
                        f"Напоминаем, что ваш приём в МРТ Эксперт сегодня в {time_str}.\n"
                        f"В центре нужно быть за 15 минут до начала приёма для оформления документов.\n"
                        f"Телефон для связи {phone_center}."
                    )
                    send_chatwoot_message(phone, hour_msg)

                    db.add(SendedMessage(
                        appointment_id=msg.appointment_id,
                        type="hour_remind",
                        scheduled_at=scheduled_at,
                        phone_number=phone,
                        phone_center=phone_center
                    ))
                    db.commit()

                    logger.info(f"⏰ Отправлено hour_remind из pending: {msg.appointment_id}")
                    processed_count += 1
                    notified_phones.add(phone)
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при обработке pending {msg.appointment_id}: {e}")

        logger.info(f"✅ Обработка отложенных уведомлений завершена. Отправлено: {processed_count}")

    except Exception as e:
        logger.error(f"❌ Ошибка в save_last_processed_time: {e}")
    finally:
        db.close()
def process_items_cron():
    db = SessionLocal()
    try:
        last_processed = get_last_processed_time()
        now = datetime.now(timezone.utc)
        logger.info(f"🕐 Обработка данных с {last_processed.strftime('%Y-%m-%d %H:%M:%S')} до {now.strftime('%Y-%m-%d %H:%M:%S')}")
        auth_header = {"Authorization": f"Bearer {APPOINTMENTS_API_KEY}"}
        skip_statuses = ['paid','done','canceled','started']
        clinics = [{
            "id": "c389c091-be9c-11e5-9fce-a45d36c3a76c",
            "name": "Мытищи МРТ-Эксперт",
            "region": "Московская обл",
            "region_code": 50,
            "city_id": "5f290be7-14ff-4ccd-8bc8-2871a9ca9d5f",
            "address": "141002, Московская обл, г Мытищи, ул Колпакова, д. 2А, помещ. 54",
            "work_time": {
                "mo": "12:00-23:45",
                "tu": "12:00-23:45",
                "we": "12:00-23:45",
                "th": "12:00-23:45",
                "fr": "12:00-23:45",
                "sa": "12:00-23:45",
                "su": "12:00-23:45"
            },
            "longitude": "0",
            "latitude": "0"
        }]
        clinic_map = {c['id']: c for c in clinics}
        all_appointments = []
        for clinic in clinics:
            cid = clinic.get('id', '')
            if not cid:
                continue
            try:
                today_str = datetime.today().strftime('%Y-%m-%d')
                app_resp = httpx.get(
                    f"https://apitest.mrtexpert.ru/api/v3/appointments?clinic_id={cid}&created_from={today_str}&created_to={today_str}",
                    timeout=60,
                    headers=auth_header 
                )
                upd_resp = httpx.get(
                    f"https://apitest.mrtexpert.ru/api/v3/appointments?clinic_id={cid}&updated_from={today_str}&updated_to={today_str}",
                    timeout=60,
                    headers=auth_header
                )
                app_resp.raise_for_status()
                upd_resp.raise_for_status()
                created = app_resp.json().get("result", [])
                updated = upd_resp.json().get("result", [])
                # Строим map обновлённых по appointment_id  
                updated_ids = {appt['id'] for appt in updated}
                # Добавляем только те, которых нет в updated
                merged_appointments = []
                for appt in created:
                    if appt["id"] not in updated_ids:
                        merged_appointments.append(appt)
                    
                all_appointments.extend(updated + merged_appointments)
            except Exception as e:
                logger.error(f"Ошибка при получении заявок клиники {cid}: {e}")
        
        now = datetime.now(timezone.utc)
        processed_count = 0
        notified_phones = set()
        for obj in all_appointments:
            patient = obj.get("patient", {})
            phone =   patient.get("phone") or '998998180817'
            if not phone or phone in notified_phones:
                continue
            items = obj.get("items", [])
            if not items:
                continue
            # 1. Находим самую раннюю дату приёма среди всех услуг
            earliest_item = None
            earliest_time = None
            for item in items:
                scheduled_at_str = item.get("scheduled_at")
                if not scheduled_at_str:
                    continue
                try:
                    dt = datetime.fromisoformat(scheduled_at_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                except Exception as e:
                    logger.warning(f"Неверный формат времени: {scheduled_at_str}, ошибка: {e}")
                    continue
                if earliest_time is None or dt < earliest_time:
                    earliest_time = dt
                    earliest_item = item
            if not earliest_item or earliest_time < now:
                continue
            item_id = earliest_item.get("id")
            item_status = earliest_item.get("status")
            # 2. Проверка статусов
            appointment_in_db = db.query(SendedMessage).filter(
                SendedMessage.appointment_id == item_id,
                SendedMessage.type.in_(['pending', 'pending_new', 'pending_day'])
            ).first()
            
            if item_status in skip_statuses:
                if appointment_in_db:
                    db.delete(appointment_in_db)
                    db.commit()
                    logger.info(f"🗑 Удалено pending сообщение для {item_id} (статус: {item_status})")
                continue
          # 3. Если дата изменилась%
            if appointment_in_db and appointment_in_db.scheduled_at != earliest_time:
                appointment_in_db.scheduled_at = earliest_time
                db.commit()

                outdated_reminders = db.query(SendedMessage).filter(
                    SendedMessage.appointment_id == item_id,
                    SendedMessage.type.in_(['new_remind', 'day_remind', 'hour_remind'])
                ).all()
                for reminder in outdated_reminders:
                    db.delete(reminder)
                logger.info(f"✏️ Обновлено pending сообщение для {item_id}: новое время {earliest_time.isoformat()}")

            delta = earliest_time - now
            dt_str = earliest_time.strftime('%d.%m.%Y в %H:%M')
            clinic = obj.get('clinic', {})
            full_clinic = clinic_map.get(clinic.get("id"), clinic)
            address = full_clinic.get("address", "—")
            directions = full_clinic.get("directions", "")
            phone_center = city_data.get(full_clinic.get("city_id", ""), {}).get("phone", full_clinic.get("phone", "—"))
            if delta <= timedelta(minutes=29):
                logger.info(f"⏳ Пропуск {item_id} — до приёма осталось меньше 30 минут")
                continue

            sent_new = db.query(SendedMessage).filter_by(appointment_id=item_id, type="new_remind").first()
            if not sent_new:
                # Отправляем новое сообщение
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
                        f"https://apitest.mrtexpert.ru/api/v3/services/{earliest_item.get('service', {}).get('id', '')}?clinic_id={clinic.get('id')}",
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

                db.add(SendedMessage(
                    appointment_id=item_id,
                    type="new_remind",
                    scheduled_at=earliest_time,
                    phone_number=phone,
                    phone_center=phone_center
                ))
                db.add(SendedMessage(
                    appointment_id=item_id,
                    type="pending",
                    scheduled_at=earliest_time,
                    phone_number=phone,
                    phone_center=phone_center
                ))
                db.commit()

                logger.info(f"🟢 Новая запись отправлена: {item_id}")
                notified_phones.add(phone)
                processed_count += 1
                continue

            # Напоминание за день
            minutes_to_appointment = int(delta.total_seconds() / 60)
            if 1400 <= minutes_to_appointment <= 1440:
                if 0 <= earliest_time.hour < 7:
                    logger.info(f"🌙 Ночь: пропускаем сообщение типа new_remind для {item_id}")
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

        save_last_processed_time()
        logger.info(f"✅ Завершено. Уведомлений отправлено: {processed_count}")
    except Exception as e:
        logger.error(f"❌ Ошибка в process_items_cron: {e}")
        save_last_processed_time()
    finally:
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