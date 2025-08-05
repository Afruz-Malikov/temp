from sqlalchemy import Column, Integer, String,DateTime
from db import Base

class SendedMessage(Base):
    __tablename__ = "sended_messages"

    id = Column(Integer, primary_key=True, index=True)
    appointment_id = Column(String, index=True)
    type = Column(String)
    phone_number =  Column(String) 
    phone_center =  Column(String) 
    scheduled_at = Column(DateTime(timezone=True))      
    
    