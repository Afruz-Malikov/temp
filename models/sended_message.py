from sqlalchemy import Column, Integer, String
from db import Base

class SendedMessage(Base):
    __tablename__ = "sended_messages"

    id = Column(Integer, primary_key=True, index=True)
    appointment_id = Column(String, index=True)
    type = Column(String)
