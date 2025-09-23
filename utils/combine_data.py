import pandas as pd

def combine_data(measurements, patients):

    skipped = {
        "no_onset"  : 0,
        "c_section" : 0
    }

    measurements_df = pd.DataFrame(measurements)
    patients_df     = pd.DataFrame(patients)

    merged          = pd.merge(
        measurements_df,
        patients_df,
        left_on     = "mobile",
        right_on    = "patient_id",
        how         = "left"
    )

    combined_data = []

    for idx, row in merged.iterrows():

        if not row["onset_datetime"]:
            skipped["no_onset"] += 1
            continue

        if row["delivery_type"] == "c-section":
            skipped["c_section"] += 1
            continue

        data = {
            "_id"               : row["_id_x"],
            "mobile"            : row["mobile"],
            "measurement_date"  : row["measurement_date"],
            "start_test_ts"     : row["start_test_ts"],
            "uc"                : row["uc"],
            "fhr"               : row["fhr"],
            "gest_age"          : row["gest_age"],

            # Use expected and actual delivery from 'patients_unified'
            "edd"               : row["edd"] if row["recruitment_type"] == "historical" else row["estimated_delivery_date"],
            "add"               : row["add"] if row["recruitment_type"] == "historical" else row["delivery_datetime"],

            "onset"             : row["onset_datetime"]
        }

        combined_data.append(data)

    return combined_data, skipped