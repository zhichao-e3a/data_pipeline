import json
import pandas as pd
from pathlib import Path

def consolidate(measurements, patients):

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

    consolidated_data = []

    for idx, row in merged.iterrows():

        if not row["onset_datetime"]:
            skipped["no_onset"] += 1
            continue

        if row["delivery_type"] == "c-section":
            skipped["c_section"] += 1
            continue

        data = {
            "row_id"            : row["_id_x"],
            "mobile"            : row["mobile"],
            "measurement_date"  : row["measurement_date"],
            "start_test_ts"     : row["start_test_ts"],
            "uc"                : row["uc"],
            "fhr"               : row["fhr"],
            "gest_age"          : row["gest_age"],

            # Use expected and actual delivery from the measurements (not from Excel)
            "expected_delivery" : row["expected_delivery"],
            "actual_delivery"   : row["actual_delivery"],

            "onset"             : row["onset_datetime"]
        }

        consolidated_data.append(data)

    return consolidated_data, skipped

def remove_poor(consolidated):

    root = Path(__file__).resolve().parent.parent
    path = root / "resources" / "recordstoremove.json"
    with open(path, "r") as f:
        content = f.read()
        to_remove = json.loads(content)

    count = 0
    good_measurements = []
    bad_measurements = set()

    for measurement in consolidated:

        mobile  = measurement["mobile"]
        time    = measurement["start_test_ts"]

        for r_mobile in to_remove:

            for r_time in to_remove[r_mobile]:

                if r_mobile == mobile and r_time == time:
                    bad_measurements.add(
                        (r_mobile, r_time)
                    )

    for measurement in consolidated:

        mobile  = measurement["mobile"]
        time    = measurement["start_test_ts"]

        if (mobile, time) not in bad_measurements:
            good_measurements.append(measurement)
            continue

        count += 1

    return good_measurements, count