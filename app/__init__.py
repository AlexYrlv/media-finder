from fastapi import FastAPI
from .api import router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Video Search API",
    description="API для поиска и обработки видео",
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешить все источники
    allow_credentials=True,
    allow_methods=["*"],  # Разрешить все методы
    allow_headers=["*"],  # Разрешить все заголовки
)

# Подключение маршрутов
app.include_router(router)
