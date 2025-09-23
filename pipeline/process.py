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
        filt_records = await mongo.get_all_documents(
            coll_name = 'filt_hist',
            query = {
                'utime' : {
                    '$gt': last_utime
                }
            }
        )
    elif origin == 'rec':
        filt_records = await mongo.get_all_documents(
            coll_name = 'filt_rec',
            query = {
                'utime' : {
                    '$gt': last_utime
                }
            }
        )

    proc_records, skipped = await anyio.to_thread.run_sync(
        lambda : process_signals(filt_records)
    )

    if len(proc_records) > 0:

        if origin == 'hist':
            await mongo.upsert_records_hashed(proc_records, coll_name = 'proc_hist')

        elif origin == 'rec':
            await mongo.upsert_records_hashed(proc_records, coll_name = 'proc_rec')

        # Update watermark only if there were records fetched
        filt_utime = [i['utime'] for i in filt_records]
        latest_utime = max(filt_utime)

        watermark_log = log_watermark(
            pipeline_name=f'filt_{origin}',
            utime=latest_utime,
            job_id=job_id
        )

        # Upsert watermark to MongoDB
        await mongo.upsert_records_hashed([watermark_log], "watermarks")

    return {
        'n_rows'        : len(proc_records),
        'n_cols'        : len(proc_records[0]) if len(proc_records) > 0 else 0,
        'rows_skipped'  : skipped
    }