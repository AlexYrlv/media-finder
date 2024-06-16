import pandas as pd
from opensearchpy import OpenSearch
import os
import requests
import concurrent.futures
import hashlib
import time

# Инициализация клиента OpenSearch
es = OpenSearch([{'host': os.environ.get('OPENSEARCH_HOST', 'opensearch'),
                  'port': os.environ.get('OPENSEARCH_PORT', 9200), 'scheme': 'http'}])

RECOGNIZE_SPEECH_HOST = os.environ.get('RECOGNIZE_SPEECH_HOST', 'recognize-speech')
RECOGNIZE_SPEECH_PORT = os.environ.get('RECOGNIZE_SPEECH_PORT', 6000)


def create_index():
    index_body = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "rebuilt_russian": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "russian_stop",
                            "russian_keywords",
                            "russian_stemmer"
                        ]
                    }
                },
                "filter": {
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_keywords": {
                        "type": "keyword_marker",
                        "keywords": []
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "url": {
                    "type": "text"
                },
                "tags": {
                    "type": "text",
                    "analyzer": "rebuilt_russian"
                }
            }
        }
    }

    es.indices.delete(index="videos", ignore=[400, 404])  # Удаление индекса, если он уже существует
    es.indices.create(index="videos", body=index_body)


def generate_id(url):
    return hashlib.md5(url.encode()).hexdigest()


def process_video(row):
    start_time = time.time()
    video_url = row['link']
    video_id = generate_id(video_url)

    # Отправка запроса к сервису recognize-speech для анализа видео
    recognize_speech_url = f"http://{RECOGNIZE_SPEECH_HOST}:{RECOGNIZE_SPEECH_PORT}/get_video_text/{video_url}"
    response = requests.get(recognize_speech_url)
    speech_recognition_result = response.json()
    tags = speech_recognition_result["ru-RU"].split()

    # Индексация видео в OpenSearch
    es.index(index="videos", id=video_id, body={"url": video_url, "tags": tags})

    end_time = time.time()
    processing_time = end_time - start_time
    return video_id, processing_time


def index_dataset(csv_file_path):
    try:
        # Проверка состояния сервера OpenSearch
        if not es.ping():
            print("Ping to OpenSearch failed")
            raise ValueError("Connection to OpenSearch failed")

        create_index()

        df = pd.read_csv(csv_file_path)

        # Замена пропущенных значений на пустые строки
        df['description'] = df['description'].fillna('')

        total_rows = len(df)
        print(f"Starting indexing of {total_rows} videos...")

        # Использование ThreadPoolExecutor для параллельной обработки видео
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_video, row) for index, row in df.iterrows()]
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    video_id, processing_time = future.result()
                    # Вывод прогресса каждые 1000 записей
                    if (i + 1) % 1000 == 0 or (i + 1) == total_rows:
                        print(f"Indexed {i + 1}/{total_rows} videos")
                    print(f"Processed video {video_id} in {processing_time:.2f} seconds")
                except Exception as e:
                    print(f"Error indexing video: {str(e)}")

        print("Indexing completed successfully")

    except Exception as e:
        print(f"Error during indexing: {str(e)}")


# Путь к CSV файлу с датасетом
csv_file_path = "./data/yappy_hackaton_2024_400k.csv"

# Индексация видео из датасета
index_dataset(csv_file_path)
import pandas as pd
from opensearchpy import OpenSearch
import os
import requests
import concurrent.futures
import hashlib
import time

# # Инициализация клиента OpenSearch
# es = OpenSearch([{'host': os.environ.get('OPENSEARCH_HOST', 'opensearch'),
#                   'port': os.environ.get('OPENSEARCH_PORT', 9200), 'scheme': 'http'}])
#
# RECOGNIZE_SPEECH_HOST = os.environ.get('RECOGNIZE_SPEECH_HOST', 'recognize-speech')
# RECOGNIZE_SPEECH_PORT = os.environ.get('RECOGNIZE_SPEECH_PORT', 6000)
#
#
# def create_index():
#     index_body = {
#         "settings": {
#             "analysis": {
#                 "analyzer": {
#                     "rebuilt_russian": {
#                         "tokenizer": "standard",
#                         "filter": [
#                             "lowercase",
#                             "russian_stop",
#                             "russian_keywords",
#                             "russian_stemmer"
#                         ]
#                     }
#                 },
#                 "filter": {
#                     "russian_stop": {
#                         "type": "stop",
#                         "stopwords": "_russian_"
#                     },
#                     "russian_keywords": {
#                         "type": "keyword_marker",
#                         "keywords": []
#                     },
#                     "russian_stemmer": {
#                         "type": "stemmer",
#                         "language": "russian"
#                     }
#                 }
#             }
#         },
#         "mappings": {
#             "properties": {
#                 "url": {
#                     "type": "text"
#                 },
#                 "tags": {
#                     "type": "text",
#                     "analyzer": "rebuilt_russian"
#                 }
#             }
#         }
#     }
#
#     es.indices.delete(index="videos", ignore=[400, 404])  # Удаление индекса, если он уже существует
#     es.indices.create(index="videos", body=index_body)
#
#
# def generate_id(url):
#     return hashlib.md5(url.encode()).hexdigest()
#
#
# def process_video(row):
#     start_time = time.time()
#     video_url = row['link']
#     video_id = generate_id(video_url)
#
#     # Отправка запроса к сервису recognize-speech для анализа видео
#     recognize_speech_url = f"http://{RECOGNIZE_SPEECH_HOST}:{RECOGNIZE_SPEECH_PORT}/get_video_text/{video_url}"
#     response = requests.get(recognize_speech_url)
#     if response.status_code == 200:
#         speech_recognition_result = response.json()
#         tags = speech_recognition_result.get("ru-RU", "").split()
#
#         # Индексация видео в OpenSearch
#         es.index(index="videos", id=video_id, body={"url": video_url, "tags": tags})
#
#     end_time = time.time()
#     processing_time = end_time - start_time
#     return video_id, processing_time
#
#
# def index_dataset(csv_file_path):
#     try:
#         # Проверка состояния сервера OpenSearch
#         if not es.ping():
#             print("Ping to OpenSearch failed")
#             raise ValueError("Connection to OpenSearch failed")
#
#         create_index()
#
#         df = pd.read_csv(csv_file_path)
#
#         # Замена пропущенных значений на пустые строки
#         df['description'] = df['description'].fillna('')
#
#         total_rows = len(df)
#         print(f"Starting indexing of {total_rows} videos...")
#
#         # Использование ThreadPoolExecutor для параллельной обработки видео
#         with concurrent.futures.ThreadPoolExecutor() as executor:
#             futures = [executor.submit(process_video, row) for index, row in df.iterrows()]
#             for i, future in enumerate(concurrent.futures.as_completed(futures)):
#                 try:
#                     video_id, processing_time = future.result()
#                     # Вывод прогресса каждые 1000 записей
#                     if (i + 1) % 1000 == 0 or (i + 1) == total_rows:
#                         print(f"Indexed {i + 1}/{total_rows} videos")
#                     print(f"Processed video {video_id} in {processing_time:.2f} seconds")
#                 except Exception as e:
#                     print(f"Error indexing video: {str(e)}")
#
#         print("Indexing completed successfully")
#
#     except Exception as e:
#         print(f"Error during indexing: {str(e)}")
#
#
# # Путь к CSV файлу с датасетом
# csv_file_path = "./data/yappy_hackaton_2024_400k.csv"
#
# # Индексация видео из датасета
# index_dataset(csv_file_path)
