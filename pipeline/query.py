from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector
from database.queries import *

from utils.download_data import async_process_df
from utils.extract_gest_age import extract_gest_age

from services.shared import log_watermark

import anyio
import pandas as pd
from datetime import datetime
from typing import Dict

async def query(

        job_id  : str,
        sql     : SQLDBConnector,
        mongo   : MongoDBConnector,
        origin  : str

) -> Dict[str, int]:

    curr_watermark = await mongo.get_all_documents(
        coll_name = "watermarks",
        query = {
            "_id" : {
                "$eq" : f"sql_{origin}"
            }
        },
        projection = {
            "_id"        : 0,
            "last_utime" : 1
        }
    )

    last_utime = curr_watermark[0]['last_utime']

    # Historical patients
    if origin == "hist":
        df = await anyio.to_thread.run_sync(
            lambda: sql.query_to_dataframe(
                query = HISTORICAL.format(
                   last_utime = last_utime
               )
            )
        )
    # Recruited patients
    elif origin == "rec":
        # Query existing Recruited patients from MongoDB
        recruited_mobile = await mongo.get_all_documents(
            coll_name = "patients_unified",
            query = {
                'recruitment_type' : 'recruited'
            },
            projection = {
                '_id'        : 0,
                'patient_id' : 1,
            }
        )

        # Get mobile numbers of recruited patients
        query_string = ",".join(
            [
                f"'{i["patient_id"]}'" for i in recruited_mobile
            ]
        )

        df = await anyio.to_thread.run_sync(
            lambda: sql.query_to_dataframe(
                query = RECRUITED.format(
                    start = "'2025-03-01 00:00:00'",
                    end = f"'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'",
                    numbers = query_string,
                    last_utime = last_utime
                )
            )
        )

        # # Get data for EDD, ADD, LMP
        # expected_delivery = {
        #     i["patient_id"]: i["estimated_delivery_date"] for i in recruited_patients
        # }
        #
        # actual_delivery = {
        #     i["patient_id"]: i["delivery_datetime"] for i in recruited_patients
        # }

        # [18 Sep] Not using LMP to obtain gest_age
        # last_menstrual = {
        #     i["patient_id"]: i["last_menstrual_period"] for i in recruited_patients
        # }

    # UC, FHR, FMov measurements not ordered yet
    uc_results, fhr_results, fmov_results = await async_process_df(df)

    # Order UC and FHR measurements
    sorted_uc_list      = sorted(uc_results, key=lambda x: x[0])
    sorted_fhr_list     = sorted(fhr_results, key=lambda x: x[0])
    sorted_fmov_list    = sorted(fmov_results, key=lambda x: x[0])

    record_list = []
    for idx, row in df.iterrows():

        row_id          = row['id']
        mobile          = row['mobile']

        m_date          = datetime.fromtimestamp(int(row['start_ts']))\
            .strftime("%Y-%m-%d %H:%M:%S")

        start_test_ts   = datetime.fromtimestamp(int(row['start_test_ts']))\
            .strftime("%Y-%m-%d %H:%M:%S") if row['start_test_ts'] else None

        # Extract UC, FHR data ; Do not filter by < 20 minutes yet
        uc_data     = sorted_uc_list[idx][1].split("\n")
        fhr_data    = sorted_fhr_list[idx][1].split("\n")

        # Extract raw FMov data
        raw_fmov_data = sorted_fmov_list[idx][1].split("\n") if sorted_fmov_list[idx][1] else None

        # Extract gestational age
        conclusion = row['conclusion'] ; basic_info = row['basic_info']
        gest_age = extract_gest_age(conclusion, basic_info)

        # Build record (gest_age can be NULL, UC/FHR can be < 20 minutes)
        record = {
            '_id': row_id,
            'mobile': mobile,
            'measurement_date': m_date,
            'start_test_ts': start_test_ts,
            'uc': uc_data,
            'fhr': fhr_data,
            'fmov': raw_fmov_data,
            'gest_age': gest_age
        }

        # Handle EDD, ADD for historical patients
        if origin == 'hist':

            edd = row['expected_born_date'].strftime("%Y-%m-%d %H:%M:%S")

            add = datetime.fromtimestamp(int(row['end_born_ts'])) \
                .strftime("%Y-%m-%d %H:%M:%S")

            record['edd'] = edd
            record['add'] = add

        # Handle EDD, ADD, gest_age for recruited patients
        # [18 Sep] Not using LMP to obtain gest_age
        # elif origin == 'rec':
        #
        #     edd = datetime.strptime(expected_delivery[mobile], "%Y-%m-%d") \
        #         .strftime("%Y-%m-%d %H:%M:%S") if expected_delivery[mobile] else None
        #
        #     add = actual_delivery[mobile]

            # gest_age
            # last_menstrual_str = last_menstrual[mobile]
            #
            # if last_menstrual_str and not gest_age:
            #     last_menstrual_date = datetime.strptime(last_menstrual_str, "%Y-%m-%d")
            #
            #     diff = datetime.strptime(m_date, "%Y-%m-%d %H:%M:%S")\
            #            - last_menstrual_date
            #
            #     gest_age = diff.days

        record_list.append(record)

    if len(record_list) > 0:

        # Upsert records to MongoDB
        if origin == 'hist':
            await mongo.upsert_documents_hashed(record_list, coll_name = 'raw_hist')
        elif origin == 'rec':
            await mongo.upsert_documents_hashed(record_list, coll_name = 'raw_rec')

        # Update watermark only if there were records fetched
        latest_utime = pd.to_datetime(df["utime"]) \
            .max().strftime("%Y-%m-%d %H:%M:%S")

        watermark_log = log_watermark(
            pipeline_name = f'sql_{origin}',
            utime = latest_utime,
            job_id = job_id,
        )

        # Upsert watermark to MongoDB
        await mongo.upsert_documents_hashed([watermark_log], "watermarks")

    return {
        'n_rows' : len(record_list),
        'n_cols' : len(record_list[0]) if len(record_list) > 0 else 0
    }