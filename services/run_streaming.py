from database.MongoDBConnector import MongoDBConnector
from schemas.pipeline import schema_add, schema_onset

import os
import io
import asyncio

import pyarrow as pa
import pyarrow.ipc as ipc

mongo = MongoDBConnector(mode=os.getenv("MODE"))

def build_array(col_vals, typ):

    return pa.array(col_vals, type=typ, from_pandas=True)

def pivot_to_table(rows, schema):

    arrays = [] ; names = []

    for field in schema:

        f_name, f_type = field.name, field.type

        col_vals = [r[f_name] for r in rows]

        arr = build_array(col_vals, f_type)

        arrays.append(arr)
        names.append(f_name)

    return pa.Table.from_arrays(arrays, names=names)

async def run_streaming(coll_name: str):

    bio      = io.BytesIO()
    sink     = pa.output_stream(bio)
    schema   = schema_add if coll_name == "model_data_add" else schema_onset
    writer   = ipc.new_stream(sink, schema)
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

            table = pivot_to_table(batch, schema)
            writer.write_table(table)

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

        bio.seek(last_len)
        tail = bio.read()

        if tail:
            try:
                yield tail
            except RuntimeError:
                pass