from database.MongoDBConnector import MongoDBConnector

from utils.combine_data import combine_data
from utils.extract_features import extract_features

import anyio

async def model_raw(
        mongo : MongoDBConnector
):

    all_patients = await mongo.get_all_documents(coll_name="patients_unified")

    rec_patients, hist_patients = [], []
    valid_patients = 0
    for patient in all_patients:

        onset       = patient["onset_datetime"]
        delivery    = patient["delivery_type"]
        origin      = patient["recruitment_type"]

        if origin == "recruited":
            rec_patients.append(patient)

        elif origin == "historical":
            hist_patients.append(patient)

        # Include only patients with onset and natural/emergency c-section deliveries
        if onset is not None and delivery != "c-section":
            valid_patients += 1

    rec_measurements = await mongo.get_all_documents(
        coll_name="filt_rec",
        query={
            "mobile": {
                "$in": [i["patient_id"] for i in rec_patients]
            }
        }
    )

    hist_measurements = await mongo.get_all_documents(
        coll_name="filt_hist",
        query={
            "mobile": {
                "$in": [i["patient_id"] for i in hist_patients]
            }
        }
    )

    rec_data, rec_skipped   = await anyio.to_thread.run_sync(
        lambda : combine_data(rec_measurements, rec_patients)
    )

    hist_data, hist_skipped = await anyio.to_thread.run_sync(
        lambda : combine_data(hist_measurements, hist_patients)
    )

    extracted_data = await anyio.to_thread.run_sync(
        lambda : extract_features(rec_data+hist_data)
    )

    await mongo.upsert_documents_hashed(
        extracted_data,
        coll_name="model_data_raw"
    )

    return {
        "rec_patients"          : len(rec_patients),
        "hist_patients"         : len(hist_patients),
        "valid_patients"        : valid_patients,
        "valid_measurements"    : len(rec_data)+len(hist_data),
        "no_onset"              : rec_skipped["no_onset"] + hist_skipped["no_onset"],
        "c_sections"            : rec_skipped["c_section"] + hist_skipped["c_section"]
    }