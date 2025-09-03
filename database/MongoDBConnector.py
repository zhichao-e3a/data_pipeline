from config.configs import MONGO_CONFIG, DEFAULT_MONGO_CONFIG

import json
import hashlib
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from pymongo import UpdateOne
from pymongo.errors import AutoReconnect, BulkWriteError

class MongoDBConnector:

    def __init__(self, remote):

        self.remote = remote

    def build_client(self):

        if self.remote:
            config = DEFAULT_MONGO_CONFIG
        else:
            config = MONGO_CONFIG

        return AsyncIOMotorClient(
            config["DB_HOST"],
            minPoolSize = 5,
            maxPoolSize = 50
        )

    @asynccontextmanager
    async def resource(self, coll_name):

        client = self.build_client()

        if self.remote:
            config = DEFAULT_MONGO_CONFIG
        else:
            config = MONGO_CONFIG

        try:
            await client.admin.command('ping')
            db      = client[config["DB_NAME"]]
            coll    = db[coll_name]
            yield coll

        finally:
            client.close()

    @staticmethod
    async def flush(coll, ops):

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

    async def upsert_records(self, records, coll_name, batch_size=500):

        async with self.resource(coll_name) as coll:

            if coll_name == "logs":

                _id = records.get("job_id")
                to_insert = dict(records)
                to_insert.pop("job_id")

                to_insert = UpdateOne(
                    {"_id": _id},
                    {"$set": to_insert},
                    upsert=True
                )

                await self.flush(coll, [to_insert])

            elif coll_name == "consolidated_patients":

                ops = []

                for item in records:

                    _id = item.get("contact")
                    to_insert = dict(item)
                    to_insert.pop("contact")

                    to_insert.setdefault(
                        "fetched_at",
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    )

                    op = UpdateOne(
                        {"_id": _id},
                        {"$set": to_insert},
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

    async def get_all_documents(self, coll_name, query={}, projection=None, batch_size=1000):

        async with self.resource(coll_name) as coll:

            try:

                cursor = coll.find(query)

                if batch_size:
                    cursor = cursor.batch_size(batch_size)
                return [doc async for doc in cursor]

            except AutoReconnect:
                await asyncio.sleep(0.5)
                cursor = coll.find({}, projection)
                if batch_size:
                    cursor = cursor.batch_size(batch_size)
                return [doc async for doc in cursor]

    @staticmethod
    def _fingerprint(obj, hash_type):

        if hash_type == "measurement":

            clean = {
                k:v for k,v in obj.items() if k in {
                    "uc", "fhr", "gest_age", "expected_delivery", "actual_delivery"
                }
            }

        elif hash_type == "watermark":

            clean = {
                k: v for k, v in obj.items() if k in {
                    "last_utime", "last_job_id", "time"
                }
            }

        blob = json.dumps(
            clean, sort_keys=True, indent=4, separators=(',', ': '), default=str
        ).encode()

        return hashlib.sha1(blob).hexdigest()


    async def upsert_records_hashed(self, records, coll_name, batch_size=500):

        async with self.resource(coll_name) as coll:

            ops = []

            for item in records:

                if coll_name == "watermarks":
                    to_insert = dict(item)
                    _id = item["pipeline_name"]
                    to_insert.pop("pipeline_name")
                    h = await asyncio.to_thread(self._fingerprint, to_insert, "watermark")
                else:
                    to_insert = dict(item)
                    _id = item.get("row_id")
                    to_insert.pop("row_id")
                    h = await asyncio.to_thread(self._fingerprint, to_insert, "measurement")

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
