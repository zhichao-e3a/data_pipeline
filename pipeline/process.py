from database.MongoDBConnector import MongoDBConnector

from utils.process_signals import process_signals

from services.shared import log_watermark

import anyio
from typing import Dict

async def process(

        job_id  : str,
        mongo   : MongoDBConnector,
        origin  : str

) -> Dict[str, int]:

    curr_watermark = await mongo.get_all_documents(
        coll_name="watermarks",
        query={
            "_id": {
                "$eq": f"filt_{origin}"
            },
        }
    )

    last_utime = curr_watermark[0]['last_utime']

    if origin == 'hist':
        filt_records = mongo.stream_all_documents(
            coll_name = "filt_hist",
            query = {
                'utime': {
                    '$gt': last_utime,
                }
            },
            sort=[
                ("utime", 1),
                ("_id", 1)
            ]
        )

    elif origin == 'rec':
        filt_records = mongo.stream_all_documents(
            coll_name="filt_rec",
            query={
                'utime': {
                    '$gt': last_utime,
                }
            },
            sort=[
                ("utime", 1),
                ("_id", 1)
            ]
        )

    total_records = 0
    total_skipped = 0
    async for batch in filt_records:

        batch_max_utime = batch[-1]["utime"]

        proc_records, skipped = await anyio.to_thread.run_sync(
            lambda : process_signals(batch)
        )

        total_skipped += skipped

        if len(proc_records) > 0:

            total_records += len(proc_records)

            if origin == 'hist':
                await mongo.upsert_documents_hashed(proc_records, coll_name = 'proc_hist')

            elif origin == 'rec':
                await mongo.upsert_documents_hashed(proc_records, coll_name = 'proc_rec')

        watermark_log = log_watermark(
            pipeline_name=f'filt_{origin}',
            utime=batch_max_utime,
            job_id=job_id
        )

        # Upsert watermark to MongoDB
        await mongo.upsert_documents_hashed([watermark_log], "watermarks")

    return {
        'n_rows'        : total_records,
        'rows_skipped'  : total_skipped
    }