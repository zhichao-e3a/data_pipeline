import pandas as pd

def combine_data_onset(measurements, patients):

    # Accounts for ONSET and DELIVERY TYPE (both found in unified)

    measurements_df = pd.DataFrame(measurements)
    patients_df     = pd.DataFrame(patients)

    merged          = pd.merge(
        measurements_df,
        patients_df,
        left_on     = "mobile",
        right_on    = "patient_id",
        how         = "left"
    )

    combined_data_onset = []

    for idx, row in merged.iterrows():

        onset       = row["onset_datetime"]
        delivery    = row["delivery_type"]

        # If there is onset datetime and delivery is not a c-section
        if onset and delivery != "c-section":

            data = {
                "_id"               : row["_id_x"],  # filt
                "mobile"            : row["mobile"],  # filt
                "measurement_date"  : row["measurement_date"],  # filt
                "start_test_ts"     : row["start_test_ts"],  # filt
                "uc"                : row["uc"],  # filt
                "fhr"               : row["fhr"],  # filt
                "fmov"              : row["fmov"],  # filt
                "gest_age"          : row["gest_age"],  # filt
                "edd"               : row["edd"],  # filt
                "add"               : row["add"],  # filt
                "onset"             : onset
            }

            combined_data_onset.append(data)

    return combined_data_onset

def combine_data_add(measurements):

    # Accounts for ADD only ; does not account for delivery type

    combined_data_add = []

    for row in measurements:

        add = row["add"]

        if add:

            combined_data_add.append(row)

    return combined_data_add