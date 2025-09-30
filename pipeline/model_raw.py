from core import states

from database.MongoDBConnector import MongoDBConnector

from utils.combine_data import combine_data
from utils.extract_features import extract_features

import anyio
import asyncio

from itertools import islice
from typing import List, Dict, Any, Iterable

def _chunks(

        seq: List[Dict[str, Any]],
        size: int

) -> Iterable[List[Dict[str, Any]]]:

    it = iter(seq)

    while True:
        block = list(islice(it, size))
        if not block:
            break
        yield block

async def model_raw(
        mongo : MongoDBConnector
):

    all_patients = await mongo.get_all_documents(
        coll_name="patients_unified",
        projection = {
            "_id"                       : 0,
            "patient_id"                : 1,
            "delivery_datetime"         : 1,
            "delivery_type"             : 1,
            "estimated_delivery_date"   : 1,
            "onset_datetime"            : 1,
            "recruitment_type"          : 1
        }
    )

    rec_patients, hist_patients = [], []
    for patient in all_patients:

        origin = patient["recruitment_type"]

        if origin == "recruited":
            rec_patients.append(patient)

        elif origin == "historical":
            hist_patients.append(patient)

    # Handle Recruited data

    rec_measurements = await mongo.get_all_documents(
        coll_name="filt_rec",
        projection={
            "ctime"     : 0,
            "utime"     : 0,
            "doc_hash"  : 0
        }
    )

    rec_add, rec_onset = await anyio.to_thread.run_sync(
        lambda: combine_data(
            rec_measurements,
            rec_patients,
            'rec'
        )
    )

    # Handle Historical data

    hist_measurements = await mongo.get_all_documents(
        coll_name="filt_hist",
        projection={
            "ctime"     : 0,
            "utime"     : 0,
            "doc_hash"  : 0
        }
    )

    hist_add, hist_onset = await anyio.to_thread.run_sync(
        lambda : combine_data(
            hist_measurements,
            hist_patients,
            'hist'
        )
    )

    onset_records   = rec_onset + hist_onset
    add_records     = rec_add + hist_add

    loop    = asyncio.get_running_loop()
    chunk   = 3000
    async def _proc_map(

            records: List[Dict[str, Any]],
            kind: str

    ) -> List[Dict[str, Any]]:

        futures = [
            loop.run_in_executor(states.PROC_POOL, extract_features, c, kind)
            for c in _chunks(records, chunk)
        ]

        results: List[Dict[str, Any]] = []

        for fut in asyncio.as_completed(futures):
            part = await fut
            if part:
                results.extend(part)

        return results

    try:
        extracted_onset, extracted_add = await asyncio.gather(
            _proc_map(onset_records, 'onset'),
            _proc_map(add_records, 'add'),
        )
    except asyncio.CancelledError:
        raise

    await mongo.upsert_documents_hashed(
        extracted_onset,
        coll_name="model_data_onset"
    )

    await mongo.upsert_documents_hashed(
        extracted_add,
        coll_name="model_data_add",
    )

    return {
        "n_onset"   : len(extracted_onset),
        "n_add"     : len(extracted_add)
    }