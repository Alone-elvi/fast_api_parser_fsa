from fastapi import APIRouter
from ..base_routes import router as api_router

router = APIRouter()
router.include_router(api_router) 