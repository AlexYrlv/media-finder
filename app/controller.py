from typing import List
from opensearchpy import OpenSearch
import requests
import os

# Инициализация клиента OpenSearch
es = OpenSearch([{'host': os.environ.get('OPENSEARCH_HOST', 'localhost'), 'port': os.environ.get('OPENSEARCH_PORT', 9200), 'scheme': 'http'}])

RECOGNIZE_SPEECH_HOST = os.environ.get('RECOGNIZE_SPEECH_HOST', 'recognize-speech')
RECOGNIZE_SPEECH_PORT = os.environ.get('RECOGNIZE_SPEECH_PORT', 6000)

async def search_videos(query: str) -> List[str]:
    search_body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["tags"]
            }
        }
    }
    response = es.search(index="videos", body=search_body)
    hits = response['hits']['hits']
    return [hit['_source']['url'] for hit in hits]

async def upload_video_by_link(video):
    # Отправка запроса к сервису recognize-speech для анализа видео
    recognize_speech_url = f"http://{RECOGNIZE_SPEECH_HOST}:{RECOGNIZE_SPEECH_PORT}/get_video_text/{video.link}"
    response = requests.get(recognize_speech_url)
    speech_recognition_result = response.json()

    # Обработка результата анализа
    video_id = generate_id(video.link)
    es.index(index="videos", id=video_id, body={"url": video.link, "tags": speech_recognition_result["ru-RU"].split()})

    return {"message": "Video uploaded and analyzed successfully", "link": video.link, "tags": speech_recognition_result["ru-RU"].split()}

def generate_id(url):
    import hashlib
    return hashlib.md5(url.encode()).hexdigest()
