from database.MongoDBConnector import MongoDBConnector

from services.shared import log_watermark

from typing import Dict

async def filter(

        job_id  : str,
        mongo   : MongoDBConnector,
        origin  : str

) -> Dict[str, int]:

    curr_watermark = await mongo.get_all_documents(
        coll_name="watermarks",
        query={
            "_id": {
                "$eq": f"raw_{origin}"
            },
        }
    )

    last_utime = curr_watermark[0]['last_utime']

    if origin == 'hist':
        raw_records = await mongo.get_all_documents(
            coll_name = 'raw_hist',
            query = {
                'utime' : {
                    '$gt': last_utime
                }
            }
        )

    elif origin == 'rec':
        raw_records = await mongo.get_all_documents(
            coll_name='raw_rec',
            query = {
                'utime' : {
                    '$gt': last_utime
                }
            }
        )

    filt_records = []
    bad_uc_fhr   = 0
    no_gest_age  = 0
    for record in raw_records:

        # Check if UC/FHR are both >= 20 minutes
        uc_data     = record['uc']
        fhr_data    = record['fhr']
        if len(uc_data) < 60*20 and len(fhr_data) < 60*20:
            bad_uc_fhr += 1
            continue

        # Check if gestational age is present
        gest_age = record['gest_age']
        if gest_age is None:
            no_gest_age += 1
            continue

        filt_records.append(record)

    if len(filt_records) > 0:

        if origin == 'hist':
            await mongo.upsert_records_hashed(filt_records, coll_name = 'filt_hist')

        elif origin == 'rec':
            await mongo.upsert_records_hashed(filt_records, coll_name = 'filt_rec')

        # Update watermark only if there were records fetched
        raw_utime       = [i['utime'] for i in raw_records]
        latest_utime    = max(raw_utime)

        watermark_log = log_watermark(
            pipeline_name=f'raw_{origin}',
            utime=latest_utime,
            job_id=job_id
        )

        # Upsert watermark to MongoDB
        await mongo.upsert_records_hashed([watermark_log], "watermarks")

    return {
        'n_rows'        : len(filt_records),
        'n_cols'        : len(filt_records[0]) if len(filt_records) > 0 else 0,
        'bad_uc_fhr'    : bad_uc_fhr,
        'no_gest_age'   : no_gest_age
    }

