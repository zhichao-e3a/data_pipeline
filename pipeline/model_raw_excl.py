from database.MongoDBConnector import MongoDBConnector

from utils.remove_data import remove_data

import anyio

async def model_raw_excl(
        mongo : MongoDBConnector
):

    all_measurements = await mongo.get_all_documents(
        coll_name = 'model_data_raw'
    )

    good_measurements, skipped = await anyio.to_thread.run_sync(
        lambda : remove_data(all_measurements)
    )

    await mongo.upsert_records_hashed(
        good_measurements,
        coll_name = 'model_data_raw_excl'
    )

    return {
        'n_rows'        : len(good_measurements),
        'rows_skipped'  : skipped
    }