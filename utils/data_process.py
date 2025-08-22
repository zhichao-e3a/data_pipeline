from utils.signal_processing import *

import json
import numpy as np
from datetime import datetime

def process_data_1(df, unsorted_uc_list, unsorted_fhr_list, data_type):

    processed_list = []
    skipped = 0

    uc_list     = sorted(unsorted_uc_list, key=lambda x: x[0])
    fhr_list    = sorted(unsorted_fhr_list, key=lambda x: x[0])

    for idx, row in df.iterrows():

        uc  = uc_list[idx][1].split("\n")
        fhr = fhr_list[idx][1].split("\n")

        if len(uc) < 60*20 and len(fhr) < 60*20:
            skipped += 1
            print(f"Process DF: Skipped row {idx}")
            continue

        edd = row["expected_born_date"]
        edd_dt = datetime(edd.year, edd.month, edd.day) if not pd.isna(edd) else None

        add = row["end_born_ts"]
        add_dt = datetime(add.year, add.month, add.day) if not pd.isna(add) else None

        data = {
            "mobile"            : row["mobile"],
            "row_id"            : row["id"],
            "user_id"           : row["user_id"],
            "measurement_date"  : row["start_ts"],
            "gest_age"          : None,
            "uc"                : uc,
            "fhr"               : fhr,
            "expected_delivery" : edd_dt,
            "actual_delivery"   : add_dt,
            "onset"             : None
        }

        if data_type == "rec":
            data["onset"] = row["onset"] if not pd.isna(row["onset"]) else None

        conclusion      = row["conclusion"]
        basic_info      = row["basic_info"]
        basic_info_json = json.loads(basic_info)
        gest_age        = None

        if conclusion:

            gest_string = conclusion.split("。")[0]

            digits = [int(c) for c in gest_string if c.isdigit()]

            if digits:
                if len(digits) == 3:
                    gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
                elif len(digits) == 2:
                    gest_age = digits[0] * 10 * 7 + digits[1] * 7
                elif len(digits) == 1:
                    skipped += 1
                    print(f"Process DF: Skipped row {idx}")
                    continue

        elif basic_info_json["setPregTime"]:

            gest_string = basic_info_json["pregTime"]

            digits = [int(c) for c in gest_string if c.isdigit()]

            if len(digits) == 3:
                gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
            elif len(digits) == 2:
                gest_age = digits[0] * 10 * 7 + digits[1] * 7
            elif len(digits) == 1:
                skipped += 1
                print(f"Process DF: Skipped row {idx}")
                continue

        else:
            skipped += 1
            print(f"Process DF: Skipped row {idx}")
            continue

        if gest_age:
            data["gest_age"] = gest_age
            processed_list.append(data)

    return processed_list, skipped

def process_data_2(data_list):

    processed_list = []
    skipped = 0

    for idx, row in enumerate(data_list):

        uc, fhr = row['uc'], row['fhr']
        max_len = max(len(uc), len(fhr))

        while len(uc) < max_len:
            uc.append("")
        while len(fhr) < max_len:
            fhr.append("")

        uc_truncated, fhr_truncated = uc[45:], fhr[45:]
        uc_raw  = [float(i) if i not in ("", None) else np.nan for i in uc_truncated]
        fhr_raw = [float(i) if i not in ("", None) else np.nan for i in fhr_truncated]

        uc_cleaned  = clean_uc_signal(uc_raw)
        fhr_cleaned = clean_fhr_signal(fhr_raw)

        uc_final    = [float(u) for u in uc_cleaned if not np.isnan(u)]
        fhr_final   = [float(f) for f in fhr_cleaned if not np.isnan(f)]

        if not uc_final or not fhr_final:
            skipped += 1
            continue

        row['uc']   = uc_final
        row['fhr']  = fhr_final

        processed_list.append(row)

    return processed_list, skipped
