from config.configs import MONGO_CONFIG, DEFAULT_MONGO_CONFIG

import json
import hashlib
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Dict, Any, List, AsyncIterator, Optional, Tuple

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from pymongo import UpdateOne
from pymongo.errors import AutoReconnect, BulkWriteError

class MongoDBConnector:

    def __init__(self, mode):

        self.mode = mode

    def _config(self) -> Dict[str, Any]:

        if self.mode == "remote":
            return DEFAULT_MONGO_CONFIG
        elif self.mode == "local":
            return MONGO_CONFIG

    def build_client(self) -> AsyncIOMotorClient:

        config = self._config()

        return AsyncIOMotorClient(
            config["DB_HOST"],
            minPoolSize = 5,
            maxPoolSize = 50
        )

    @asynccontextmanager
    async def resource(self, coll_name):

        client = self.build_client()

        config = self._config()

        try:
            await client.admin.command('ping')
            db      = client[config["DB_NAME"]]
            coll    = db[coll_name]
            yield coll

        finally:
            client.close()

    async def stream_all_documents(

            self,
            coll_name   : str,
            query       : Optional[Dict[str, Any]] = {},
            projection  : Optional[Dict[str, int]] = None,
            sort        : Optional[List[Tuple[str, int]]] = None,
            batch_size  : Optional[int] = 10_000

    ) -> AsyncIterator[List[Dict[str, Any]]]:

        async with self.resource(coll_name) as coll:

            sort = sort or [("utime", 1), ("_id", 1)]

            async with self.resource(coll_name) as coll:

                def make_cursor(base_q: Dict[str, Any], after_id=None):
                    q = dict(base_q)  # shallow copy
                    if after_id is not None:
                        if "_id" in q and isinstance(q["_id"], dict):
                            q["_id"] = {**q["_id"], "$gt": after_id}
                        else:
                            q["_id"] = {"$gt": after_id}
                    return coll.find(
                        filter=q,
                        projection=projection,
                        sort=sort,
                        batch_size=batch_size,
                        no_cursor_timeout=True,
                    )

                cursor = make_cursor(query)
                buf: List[Dict[str, Any]] = []
                last_id = None
                retried = False

                try:
                    while True:
                        try:
                            doc = await cursor.next()
                        except StopAsyncIteration:
                            break
                        except AutoReconnect:
                            if retried:
                                raise
                            await cursor.close()
                            await asyncio.sleep(0.5)
                            cursor = make_cursor(query, after_id=last_id)
                            retried = True
                            continue

                        buf.append(doc)
                        if "_id" in doc:
                            last_id = doc["_id"]

                        if len(buf) >= batch_size:
                            yield buf
                            buf = []

                    if buf:
                        yield buf

                finally:
                    await cursor.close()

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

    async def count_documents(self, coll_name, count_filter=None):

        async with self.resource(coll_name) as coll:

            count = await coll.count_documents(count_filter or {})

            return count

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

    @staticmethod
    def _fingerprint(obj, hash_type):

        if hash_type == "measurement":

            clean = {
                k:v for k,v in obj.items() if k in {
                    "edd",
                    "add",
                    "uc",
                    "fhr",
                    "fmov"
                    "onset",
                    "annotations",
                    "notes"
                }
            }

        elif hash_type == "watermark":

            clean = {
                k: v for k, v in obj.items() if k in {
                    "last_utime",
                    "last_job_id",
                }
            }

        blob = json.dumps(
            clean, sort_keys=True, indent=4, separators=(',', ': '), default=str
        ).encode()

        return hashlib.sha1(blob).hexdigest()

    async def upsert_documents_hashed(self, records, coll_name, batch_size=500):

        async with self.resource(coll_name) as coll:

            ops = []

            for item in records:

                to_insert = dict(item)

                if coll_name == "watermarks":

                    _id = item["pipeline_name"]

                    to_insert.pop("pipeline_name")

                    h = await asyncio.to_thread(self._fingerprint, to_insert, "watermark")

                else:

                    _id = item.get("_id")

                    to_insert.pop("_id")

                    try:
                        to_insert.pop("doc_hash"); to_insert.pop('utime') ; to_insert.pop('ctime')
                    except KeyError:
                        pass

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
                            "doc_hash" : h,
                            "utime"    : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        },
                        "$setOnInsert": {
                            "ctime" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

    async def delete_document(self, coll_name, query):

        async with self.resource(coll_name) as coll:

            try:
                res = await coll.delete_one(query)
                return res.deleted_count

            except AutoReconnect:
                await asyncio.sleep(0.5)
                res = await coll.delete_one(query)
                return res.deleted_count
