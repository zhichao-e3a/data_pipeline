from database.MongoDBConnector import MongoDBConnector

import os
import asyncio

import pyarrow as pa
import pyarrow.ipc as ipc

mongo   = MongoDBConnector(mode=os.getenv("MODE"))

async def run_streaming(coll_name: str):

    sink     = pa.BufferOutputStream()
    writer   = None
    last_len = 0

    try:

        async for batch in mongo.stream_all_documents(
                coll_name = coll_name
        ):

            table = pa.Table.from_pylist(batch)

            if writer is None:
                writer = ipc.new_stream(sink, table.schema)

            writer.write_table(table)

            buf  = sink.getvalue()
            new  = buf.to_pybytes()[last_len:]

            if new:
                yield new
                last_len = len(buf)

            # Let event loop breathe (avoid idle/proxy timeouts)
            await asyncio.sleep(0)

    except asyncio.CancelledError:
        pass

    finally:
        if writer is not None:
            try:
                writer.close()
            except OSError:
                pass
            else:
                buf = sink.getvalue()
                tail = buf.to_pybytes()[last_len:]
                if tail:
                    try:
                        yield tail
                    except RuntimeError:
                        pass