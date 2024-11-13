from fastapi import APIRouter
from .api_v1 import router as router_v1

router = APIRouter()

# Подключаем роутеры разных версий
router.include_router(router_v1, prefix="/api/v1") 