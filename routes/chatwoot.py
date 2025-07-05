from fastapi import APIRouter, Request
from services.chatwoot_service import process_chatwoot_webhook

chatwoot_router = APIRouter()

@chatwoot_router.post("/chatwoot/webhook")
async def chatwoot_webhook(request: Request):
    return await process_chatwoot_webhook(request) 