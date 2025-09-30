import pandas as pd
from datetime import datetime

def combine_data(measurements, patients, origin):

    measurements_df = pd.DataFrame(measurements)
    patients_df = pd.DataFrame(patients)

    merged = pd.merge(
        measurements_df,
        patients_df,
        left_on="mobile",
        right_on="patient_id",
        how="left"
    )

    combined_data_add = [] ; combined_data_onset = []

    for idx, row in merged.iterrows():

        data = {
            "_id"               : row["_id"],               # filt
            "mobile"            : row["mobile"],            # filt
            "measurement_date"  : row["measurement_date"],  # filt
            "start_test_ts"     : row["start_test_ts"],     # filt
            "uc"                : row["uc"],                # filt
            "fhr"               : row["fhr"],               # filt
            "fmov"              : row["fmov"],              # filt
            "gest_age"          : row["gest_age"],          # filt
            "onset"             : row["onset_datetime"]     # unified (nullable)
        }

        if origin == 'rec':

            data["edd"] = datetime.strptime(row["estimated_delivery_date"], "%Y-%m-%d")\
                .strftime("%Y-%m-%d %H:%M:%S")\
                if row["estimated_delivery_date"] else None

            data["add"] = datetime.strptime(row["delivery_datetime"], "%Y-%m-%d %H:%M")\
                .strftime("%Y-%m-%d %H:%M:%S")\
                if row["delivery_datetime"] else None

        elif origin == 'hist':

            data["edd"] = row["edd"]
            data["add"] = row["add"]

        if data["add"]:
            combined_data_add.append(data)

        if not pd.isna(data["onset"]) and row["delivery_type"] != "c-section":
            combined_data_onset.append(data)

    return combined_data_add, combined_data_onset