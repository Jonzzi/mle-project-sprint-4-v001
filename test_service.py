import requests
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(
    filename="test_service.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)

load_dotenv()

url = "http://127.0.0.1:8000"
headers = {"Content-type": "application/json", "Accept": "text/plain"}

# тестирование персональных рекомендаций:
params = {"user_id": 0}
resp = requests.post(url + "/recommendations", headers=headers, params=params)
if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    logging.info(f"status code: {resp.status_code}")
logging.info(f"Recommendations ID: {recs}")


# тестирование рекомендаций по популярности:
params = {"user_id": 3000000000000}
resp = requests.post(url + "/recommendations", headers=headers, params=params)
if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    logging.info(f"status code: {resp.status_code}")
logging.info(f"Recommendations ID: {recs}")


# запрос на загрузку обновленных файлов рекомендаций
params = {"rec_type": "default", "file_path": "top_popular.parquet"}
resp = requests.get(url + "/load_recommendations", headers=headers, params=params)
logging.info(f"status_code: {resp.status_code}")


# запрос на вывод статистики
resp = requests.get(url + "/get_statistics")
if resp.status_code == 200:
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")


test_user_id = 302 # тестовый пользователь
logging.info(f"Testing user: {test_user_id}")

# запрос на получение событий для тестовго пользователя
params = {"user_id": test_user_id, "k": 10}
resp = requests.post(url + "/get_statistics", headers=headers, params=params)
if resp.status_code == 200:
    logging.info(f"Get events for testing user")
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")


# запрос на добавление событий для тестовго пользователя
for i in [1, 2, 3, 4, 5]:
    params = {"user_id": test_user_id, "item_id": i}
    resp = requests.post(url + "/put_user_event", headers=headers, params=params)
    if resp.status_code == 200:
        logging.info(f"Put events for testing user")
        logging.info(resp.json())
    else:
        logging.info(f"status code: {resp.status_code}")


# запрос на получение событий для тестового пользователя
params = {"user_id": test_user_id, "k": 10}
resp = requests.post(url + "/get_user_events", headers=headers, params=params)
if resp.status_code == 200:
    logging.info(f"Get events for testing user")
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")


# проверка генерации онлайн-рекомендаций для тестового пользователя
params = {"user_id": test_user_id, "k": 100, "N": 10}
resp = requests.post(url + "/get_online_u2i", headers=headers, params=params)
if resp.status_code == 200:
    logging.info(f"On-line recomendations for testing user")
    logging.info(resp.json())
else:
    logging.info(f"status code: {resp.status_code}")


# итоговые смешанные рекомендации для тестового пользователя
params = {"user_id": test_user_id}
resp = requests.post(url + "/recommendations", headers=headers, params=params)
if resp.status_code == 200:
    logging.info(f"Final recomendations for testing user")
    recs = resp.json()
else:
    recs = []
    logging.info(f"status code: {resp.status_code}")
logging.info(f"Recommendations ID: {recs}")

print('Тестовый скрипт завершил работу')