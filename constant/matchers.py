import os
from dotenv import load_dotenv
load_dotenv()

GREENAPI_ID = os.getenv("GREENAPI_ID")
GREENAPI_ID_2 = os.getenv("GREENAPI_ID_2")
GREENAPI_ID_3 = os.getenv("GREENAPI_ID_3")
GREENAPI_TOKEN = os.getenv("GREENAPI_TOKEN")
GREENAPI_TOKEN_2 = os.getenv("GREENAPI_TOKEN_2")
GREENAPI_TOKEN_3 = os.getenv("GREENAPI_TOKEN_3")

inbox_by_id_instance_match = {
  "1103299906": {"inbox_id": "6", "green_token": GREENAPI_TOKEN, "green_id": GREENAPI_ID},
  "1103308629": {"inbox_id": "7", "green_token": GREENAPI_TOKEN_2, "green_id": GREENAPI_ID_2 },
  "1103277144": {"inbox_id": "4", "green_token": GREENAPI_TOKEN_3, "green_id": GREENAPI_ID_3 },
}

instance_by_inbox_id = {
   "6": {"token": GREENAPI_TOKEN, "id": GREENAPI_ID},
   "7": {"token": GREENAPI_TOKEN_2, "id": GREENAPI_ID_2},
   "4": {"token": GREENAPI_TOKEN_3, "id": GREENAPI_ID_3},  
}