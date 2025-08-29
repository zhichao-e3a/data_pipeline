from config.configs import MONGO_CONFIG

from datetime import datetime, timezone
from contextlib import asynccontextmanager
import json
import hashlib

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from pymongo import UpdateOne
from pymongo.errors import AutoReconnect, BulkWriteError

class MongoDBConnector:

    def __init__(self):
        pass

    def build_client(self):

        return AsyncIOMotorClient(
            MONGO_CONFIG["DB_HOST"],
            minPoolSize = 5,
            maxPoolSize = 50
        )

    @asynccontextmanager
    async def resource(self, coll_name):

        client = self.build_client()

        try:
            await client.admin.command('ping')
            db      = client[MONGO_CONFIG["DB_NAME"]]
            coll    = db[coll_name]
            yield coll

        finally:
            client.close()

    async def flush(self, coll, ops):

        try:
            await coll.bulk_write(ops, ordered=False)

        except BulkWriteError as bwe:
            codes = {e.get("code") for e in (bwe.details or {}).get("writeErrors", [])}
            if codes & {6, 7, 89, 91, 189, 9001}:
                await asyncio.sleep(0.5)
                await coll.bulk_write(ops, ordered=False)

        except AutoReconnect:
            await asyncio.sleep(0.5)
            await coll.bulk_write(ops, ordered=False)

    def _fingerprint(self, obj):

        clean = {
            k:v for k,v in obj.items() if k in {
                "uc", "fhr", "gest_age"
            }
        }

        blob = json.dumps(
            clean, sort_keys=True, indent=4, separators=(',', ': '), default=str
        ).encode()

        return hashlib.sha1(blob).hexdigest()


    async def upsert_records(self, records, coll_name, batch_size=500):

        async with self.resource(coll_name) as coll:

            if coll_name == "logs":

                _id = records.get("job_id")
                to_insert = dict(records)
                to_insert.pop("job_id")

                to_insert = UpdateOne({"_id": _id}, {"$set": to_insert}, upsert=True)
                await self.flush(coll, [to_insert])

            else:

                ops = []

                for item in records:

                    to_insert = dict(item)

                    if "row_id" in item:
                        _id = item.get("row_id")
                        to_insert.pop("row_id")
                    else:
                        print("!!!")
                        _id = item.get("id")
                        to_insert.pop("id")

                    h = self._fingerprint(to_insert)

                    op = UpdateOne(
                        {
                            "_id": _id,
                            "$or" : [
                                {
                                    "doc_hash" : {
                                        "$ne" : h
                                    }
                                },
                                {
                                    "doc_hash" : {
                                        "$exists" : False
                                    }
                                }
                            ]

                        },
                        {
                            "$set" : {
                                **to_insert,
                                "doc_hash"      : h,
                                "fetched_at"    : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                            },
                            "$setOnInsert": {
                                "created_at"    : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                            }
                        },
                        upsert=True
                    )

                    ops.append(op)

                    if len(ops) >= batch_size:
                        await self.flush(coll, ops)
                        ops = []

                if ops:
                    await self.flush(coll, ops)

    async def count_documents(self, coll_name, count_filter=None):

        async with self.resource(coll_name) as coll:

            count = await coll.count_documents(count_filter or {})

            return count

    async def get_all_documents(self, coll_name, projection=None, batch_size=1000):

        async with self.resource(coll_name) as coll:

            try:
                cursor = coll.find({})
                if batch_size:
                    cursor = cursor.batch_size(batch_size)
                return [doc async for doc in cursor]

            except AutoReconnect:
                await asyncio.sleep(0.5)
                cursor = coll.find({}, projection)
                if batch_size:
                    cursor = cursor.batch_size(batch_size)
                return [doc async for doc in cursor]
