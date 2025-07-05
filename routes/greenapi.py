from fastapi import APIRouter, Request
from services.greenapi_service import process_greenapi_webhook

greenapi_router = APIRouter()

@greenapi_router.post("/greenapi/webhook")
async def greenapi_webhook(request: Request):
    return await process_greenapi_webhook(request) 