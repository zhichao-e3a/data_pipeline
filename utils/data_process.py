import json
from datetime import datetime

def process_data(df, unsorted_uc_list, unsorted_fhr_list, data_origin):

    processed_list = []
    skipped = 0

    uc_list = sorted(unsorted_uc_list, key=lambda x: x[0])
    fhr_list = sorted(unsorted_fhr_list, key=lambda x: x[0])

    for idx, row in df.iterrows():

        uc = uc_list[idx][1].split("\n")
        fhr = fhr_list[idx][1].split("\n")

        if len(uc) < 60 * 20 and len(fhr) < 60 * 20:
            skipped += 1
            print(f"Process DF: Skipped row {idx}")
            continue

        data = {
            "row_id"            : row["id"],
            "mobile"            : row["mobile"],
            "measurement_date"  : datetime.fromtimestamp(
                int(row["start_ts"])
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "gest_age"          : None,
            "uc"                : uc,
            "fhr"               : fhr,
        }

        if data_origin == "hist":

            data["expected_delivery"] = row["expected_born_date"].strftime("%Y-%m-%d %H:%M:%S")
            data["actual_delivery"]   = datetime.fromtimestamp(
                int(row["end_born_ts"])
            ).strftime("%Y-%m-%d %H:%M:%S")

        conclusion = row["conclusion"]
        basic_info = row["basic_info"]
        basic_info_json = json.loads(basic_info)

        gest_age = None

        if conclusion:

            gest_string = conclusion.split("。")[0]

            digits = [int(c) for c in gest_string if c.isdigit()]

            if len(digits) == 3:
                gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
            elif len(digits) == 2:
                gest_age = digits[0] * 10 * 7 + digits[1] * 7

        elif basic_info_json["setPregTime"]:

            gest_string = basic_info_json["pregTime"]

            digits = [int(c) for c in gest_string if c.isdigit()]

            if len(digits) == 3:
                gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
            elif len(digits) == 2:
                gest_age = digits[0] * 10 * 7 + digits[1] * 7

        if gest_age:
            data["gest_age"] = gest_age
            processed_list.append(data)
        else:
            skipped += 1
            print(f"Process DF: Skipped row {idx}")

    return processed_list, skipped
