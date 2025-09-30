from database.MongoDBConnector import MongoDBConnector
from schemas.pipeline import schema_add, schema_onset

import os
import io
import asyncio

import pyarrow as pa
import pyarrow.ipc as ipc

mongo = MongoDBConnector(mode=os.getenv("MODE"))

async def run_streaming(coll_name: str):

    bio      = io.BytesIO()
    sink     = pa.output_stream(bio)
    schema   = schema_add if coll_name == "model_data_add" else schema_onset
    writer   = ipc.new_stream(sink, schema)

    sink.flush()

    last_len = 0

    bio.seek(last_len)
    hdr = bio.read()
    if hdr:
        yield hdr
        last_len = bio.tell()

    try:
        async for batch in mongo.stream_all_documents(
                coll_name = coll_name
        ):

            record_batch = pa.RecordBatch.from_pylist(batch, schema=schema)

            writer.write_batch(record_batch)

            sink.flush()

            bio.seek(last_len)
            chunk = bio.read()

            if chunk:
                yield chunk
                last_len = bio.tell()

            await asyncio.sleep(0)

    except asyncio.CancelledError:
        pass

    finally:

        try:
            writer.close()

        except Exception as e:
            print(e)
            pass

        sink.flush()
        bio.seek(last_len)
        tail = bio.read()

        if tail:
            try:
                yield tail
            except RuntimeError:
                pass