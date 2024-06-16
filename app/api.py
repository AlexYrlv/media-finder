from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import JSONResponse
from typing import List
from .controller import search_videos, upload_video_by_link
from .structures import VideoLink

router = APIRouter()

# Маршрут для отображения страницы поиска
@router.get("/", response_class=JSONResponse)
async def read_root():
    return {"message": "Welcome to the video search API"}

# Маршрут для обработки поискового запроса и отображения результатов
@router.post("/search/", response_class=JSONResponse)
async def search(query: str = Form(...)):
    results = await search_videos(query)
    return {"query": query, "results": results}

# Маршрут для загрузки и анализа видео по ссылке
@router.post("/upload_video_by_link/", response_class=JSONResponse)
async def upload_video(video: VideoLink):
    return await upload_video_by_link(video)