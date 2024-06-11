import asyncio
import aiogram
from aiogram import Bot, Dispatcher, types
import motor.motor_asyncio
from datetime import datetime, timedelta
import pymongo
import json


TOKEN = ""
bot = Bot(token=TOKEN)
dp = Dispatcher()

uri = "mongodb+srv://***:***@cluster0.n0v7iwi.mongodb.net/"
cluster = motor.motor_asyncio.AsyncIOMotorClient(uri)
collection = cluster.MyDB.sample_collection


@dp.message()
async def get_json(message: types.Message):
    error = ''
    try:
        request = eval(message.text)
    except Exception:
        error = 'Невалидный запос. Пример запроса:\n'\
                '{"dt_from": "2022-09-01T00:00:00", "dt_upto":'\
                ' "2022-12-31T23:59:00", "group_type": "month"}'
    if not error and not isinstance(request, dict):
        error = 'Невалидный запос. Пример запроса:\n'\
                    '{"dt_from": "2022-09-01T00:00:00", "dt_upto":'\
                    ' "2022-12-31T23:59:00", "group_type": "month"}'
    if not error and 'dt_from' in request and 'dt_upto' in request:
        dt1 = datetime.fromisoformat(request["dt_from"])
        dt2 = datetime.fromisoformat(request["dt_upto"])
        data = {"dataset": [], "labels": []}
        cursor = collection.find({'dt': {'$gte': dt1, '$lte': dt2}}).sort(
            'dt', pymongo.ASCENDING
        )
    else:
        error = 'Невалидный запос. Пример запроса:\n'\
                '{"dt_from": "2022-09-01T00:00:00", "dt_upto":'\
                    ' "2022-12-31T23:59:00", "group_type": "month"}'
    if not error and 'group_type' in request and not error:
        if request['group_type'] == "month"\
            and dt1.strftime('%H:%M:%S') == '00:00:00'\
                and dt2.strftime('%H:%M:%S') == '23:59:00':
            DT = dt1
            M = dt1.month
            S = 0
            for document in await cursor.to_list(length=None):
                dt = document["dt"]
                current_m = dt.month
                if current_m != M or DT.year != dt.year:
                    if (dt-DT).days > 31:
                        while M < current_m-1:
                            data["dataset"].append(0)
                            data["labels"].append(
                                datetime(
                                    year=DT.year, month=DT.month, day=1
                                ).isoformat()
                            )
                            DT = datetime(year=DT.year, month=DT.month, day=1)\
                                + timedelta(days=31)
                    data["dataset"].append(S)
                    S = 0
                    data["labels"].append(
                        datetime(year=DT.year, month=M, day=1).isoformat()
                    )
                    DT = dt
                    M = current_m
                S += document["value"]
            if S > 0:
                data["dataset"].append(S)
                data["labels"].append(
                    datetime(
                        year=dt.year, month=dt.month, day=1
                    ).isoformat()
                )
        elif request['group_type'] == "day" \
            and dt1.strftime('%H:%M:%S') == '00:00:00' \
                and dt2.strftime('%H:%M:%S') == '23:59:00':
            DT = dt1
            D = DT.day
            S = 0
            for document in await cursor.to_list(length=None):
                dt = document["dt"]
                current_d = dt.day
                if current_d != D or dt.month != DT.month:
                    if (dt - DT).days > 1:
                        while D < current_d - 1:
                            data["dataset"].append(0)
                            data["labels"].append(
                                datetime(
                                    year=DT.year, month=DT.month, day=D
                                ).isoformat()
                            )
                            DT = datetime(year=DT.year, month=DT.month, day=D)\
                                + timedelta(days=1)
                            D = DT.day
                    data["dataset"].append(S)
                    S = 0
                    data["labels"].append(
                        datetime(
                            year=DT.year, month=DT.month, day=D
                        ).isoformat()
                    )
                    DT = dt
                    D = current_d
                S += document["value"]
            if S > 0:
                data["dataset"].append(S)
                data["labels"].append(
                    datetime(
                        year=dt.year, month=dt.month, day=dt.day
                    ).isoformat()
                )

        elif request['group_type'] == "hour" \
            and dt1.strftime('%M:%S') == '00:00' \
                and dt2.strftime('%M:%S') == '00:00':
            DT = dt1
            H = DT.hour
            S = 0
            for document in await cursor.to_list(length=None):
                dt = document["dt"]
                current_h = dt.hour
                if current_h != H or dt.month != DT.month or dt.day != DT.day:
                    if (dt-DT).seconds > 3600:
                        while H < current_h-1:
                            data["dataset"].append(0)
                            data["labels"].append(
                                datetime(
                                    year=DT.year,
                                    month=DT.month,
                                    day=DT.day,
                                    hour=H
                                ).isoformat()
                            )
                            DT = datetime(
                                year=DT.year,
                                month=DT.month,
                                day=DT.day,
                                hour=H
                                ) + timedelta(hours=1)
                            H = DT.hour
                    data["dataset"].append(S)
                    S = 0
                    data["labels"].append(
                        datetime(
                            year=DT.year, month=DT.month, day=DT.day, hour=H
                        ).isoformat()
                    )
                    DT = dt
                    H = current_h
                S += document["value"]
            if S > 0:
                data["dataset"].append(S)
                data["labels"].append(
                    datetime(
                        year=dt.year, month=dt.month, day=dt.day, hour=dt.hour
                    ).isoformat()
                )
            while H < dt2.hour or DT.day != dt2.day:
                DT = datetime(
                    year=DT.year, month=DT.month, day=DT.day, hour=H
                    ) + timedelta(hours=1)
                H = DT.hour
                data["dataset"].append(0)
                data["labels"].append(
                    datetime(year=DT.year, month=DT.month, day=DT.day, hour=H
                             ).isoformat()
                )
        else:
            error = 'Допустимо отправлять только следующие запросы:\n'\
                '{"dt_from": "2022-09-01T00:00:00", "dt_upto":'\
                ' "2022-12-31T23:59:00", "group_type": "month"}\n'\
                '{"dt_from": "2022-10-01T00:00:00", "dt_upto":'\
                ' "2022-11-30T23:59:00", "group_type": "day"}\n'\
                '{"dt_from": "2022-02-01T00:00:00", "dt_upto":'\
                ' "2022-02-02T00:00:00", "group_type": "hour"}'
    else:
        error = 'Невалидный запос. Пример запроса:\n'\
                '{"dt_from": "2022-09-01T00:00:00", "dt_upto":'\
                ' "2022-12-31T23:59:00", "group_type": "month"}'
    if not error:
        info = json.dumps(data)
    else:
        info = error

    if len(info) > 4096:
        for x in range(0, len(info), 4096):
            await bot.send_message(message.chat.id, info[x:x+4096])
    else:
        await bot.send_message(message.chat.id, info)


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
