from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Добавляем CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/analytics/certificates/monthly")
async def get_monthly_stats():
    try:
        # TODO: Добавить запрос к ClickHouse
        return {"message": "Monthly statistics endpoint"}
    except Exception as e:
        logger.error(f"Error getting monthly stats: {str(e)}")
        return {"error": str(e)}

@app.get("/analytics/certificates/violations")
async def get_violations_stats():
    try:
        # TODO: Добавить запрос к ClickHouse
        return {"message": "Violations statistics endpoint"}
    except Exception as e:
        logger.error(f"Error getting violations stats: {str(e)}")
        return {"error": str(e)}
