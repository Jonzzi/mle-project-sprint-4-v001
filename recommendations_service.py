from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from dotenv import load_dotenv

import pandas as pd
import pickle

from implicit.als import AlternatingLeastSquares

global logger
logger = logging.getLogger("uvicorn.error")


# определение классов
class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,  # счетчик персональных рекомендаций
            "request_default_count": 0,  # счетчик рекомендаций по популярности
        }

    def load(self, type, path):
        logger.info(f"Loading recommendations, type: {type}")
        self._recs[type] = pd.read_parquet(path)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")
        logger.info("Loaded")
        logger.info(self._recs[type].head(1))

    def get(self, user_id, k=10):
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:int(k)]
            self._stats["request_personal_count"] += 1
            logger.info(f"Found {len(recs)} personal recommendations!")
        except:
            recs = self._recs["default"]
            recs = recs["item_id"].to_list()[:int(k)]
            self._stats["request_default_count"] += 1
            logger.info(f"Found {len(recs)} TOP-recommendations!")

        if not recs:
            logger.error("No recommendations found")
            recs = []

        return recs

    def stats(self):
        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ")
        print(self._stats)
        return self._stats


class EventStore:
    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        user_events = self.events.get(user_id, [])
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]

    def get(self, user_id, k):
        user_events = self.events.get(user_id, [])

        return user_events


# определение функций
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")

    # для оффайн-рекомендаций
    rec_store.load(
        type="personal",
        path="personal_als.parquet"
    )
    # для оффайн-рекомендаций
    rec_store.load(
        type="default",
        path="top_popular.parquet"
    )

    yield
    logger.info("Stopping")


async def get_als_i2i(item_id, items, N=1):
    try:
        item_id = items.query("item_id == @item_id")["item_id"].to_list()[0]
    except:
        item_id = 1 # т.к. делал децимацию и не все данные попали в обработку

    similar_items = als_model.similar_items(item_id, N=N)
    similar_tracks_enc = similar_items[0].tolist()[1 : N + 1]
    similar_tracks_scores = similar_items[1].tolist()[1 : N + 1]

    similar_tracks = []
    for i in similar_tracks_enc:
        similar_tracks.append(
            items.query("item_id == @i")["item_id"].to_list()[0]
        )

    return similar_tracks, similar_tracks_scores


def dedup_ids(ids):
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids


# определяем основные переменные
app = FastAPI(title="FastAPI-микросервис для выдачи рекомендаций", lifespan=lifespan)
rec_store = Recommendations()
events_store = EventStore()

# подгружаем ранее сохраненные файлы данных и модели
with open('als_model.pkl', 'rb') as f:
    als_model = pickle.load(f)
logger.info('als_model loaded')

items = pd.read_parquet("items.parquet")
logger.info('Items loaded')
logger.info(items.head(1))

@app.post("/recommendations", name="Получение рекомендаций для пользователя")
async def recommendations(user_id, k=10):
    recs_offline = rec_store.get(user_id, k)

    recs_online = await get_online_u2i(user_id, items, k, N=10)
    recs_online = recs_online["recs"]

    min_length = min(len(recs_offline), len(recs_online))

    logger.info(f"recs_offline {recs_offline}")
    logger.info(f"recs_online {recs_online}")

    recs_blended = []
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        recs_blended.append(recs_online[i])
        recs_blended.append(recs_offline[i])

    recs_blended = [recs_blended]

    logger.info(f"recs_blended {recs_blended}")

    # добавляем оставшиеся элементы в конец
    recs_blended.append(recs_offline[min_length:])
    recs_blended.append(recs_online[min_length:])

    # удаляем дубликаты
    recs_blended = dedup_ids(sum(recs_blended, []))

    # оставляем только первые k рекомендаций
    recs_blended[:int(k)]

    # посмотрим, какие треки выдал итоговый-рекоммендатор
    if recs_blended:
        for i in recs_blended:
            print(
                "online rec track name: ",
                items.query("item_id == @i")["track_name"].to_list()[0],
            )
            print(
                "online rec artist name: ",
                items.query("item_id == @i")["artists_names"].to_list()[0],
            )

    return {"recs": recs_blended}


@app.post("/get_online_u2i")
async def get_online_u2i(user_id, items, k=100, N=10):
    # получаем список k-последних событий пользователя
    events = await get_user_events(user_id=user_id, k=k)
    events = events["events"]

    # получаем список из N треков, похожих на последние k, с которыми взаимодействовал пользователь
    sim_item_ids = []
    sim_track_scores = []
    if len(events) > 0:
        for item_id in events:
            sim_item_id, sim_track_score = await get_als_i2i(item_id, items, N=N)
            sim_item_ids.append(sim_item_id)
            sim_track_scores.append(sim_track_score)
        sim_item_ids = sum(sim_item_ids, [])
        sim_track_scores = sum(sim_track_scores, [])
    else:
        recs = []

    # сортируем похожие объекты по scores в убывающем порядке
    combined = list(zip(sim_item_ids, sim_track_scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)

    # посмотрим, какие треки выдал онлайн-рекоммендатор
    if recs:
        for i in recs:
            print(
                "online rec track name: ",
                items.query("item_id == @i")["track_name"].to_list()[0],
            )
            print(
                "online rec artist name: ",
                items.query("item_id == @i")["artists_names"].to_list()[0],
            )

    return {"recs": recs}


@app.post("/put_user_event")
async def put_user_event(user_id, item_id):
    events_store.put(user_id, item_id)

    return {"result": "ok"}


@app.post("/get_user_events")
async def get_user_events(user_id, k=10):
    events = events_store.get(user_id, k)

    return {"events": events}


@app.get("/load_recommendations", name="Загрузка рекомендаций из файла")
async def load_recommendations(rec_type, file_path):
    rec_store.load(
        type=rec_type,
        path=file_path
        )


@app.get("/get_statistics", name="Получение статистики по рекомендациям")
async def get_statistics():
    return rec_store.stats()

