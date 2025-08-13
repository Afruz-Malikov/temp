from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.dialects.postgresql import JSONB 
from db import Base

class SendedMessage(Base):
    __tablename__ = "sended_messages"

    id = Column(Integer, primary_key=True, index=True)
    send_after = Column(Boolean)
    # Идентификаторы
    appointment_id = Column(String, index=True)
    type = Column(String)   
    appointment_json = Column(JSONB)
    phone_number = Column(String)
    scheduled_at = Column(DateTime(timezone=True))
    phone_center = Column(String)
