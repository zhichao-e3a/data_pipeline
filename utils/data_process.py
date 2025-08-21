import json
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.signal import butter, filtfilt

def process_data_1(df, unsorted_uc_list, unsorted_fhr_list):

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

        edd     = row["expected_born_date"]
        edd_dt  = datetime(edd.year, edd.month, edd.day)

        data = {
            "mobile"            : row["mobile"],
            "row_id"            : row["id"],
            "user_id"           : row["user_id"],
            "measurement_date"  : row["start_ts"],
            "gest_age"          : None,
            "uc"                : uc,
            "fhr"               : fhr,
            "expected_delivery" : edd_dt,
            "actual_delivery"   : row["end_born_ts"]
        }

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

def linear_interpolate_nan(arr, max_gap_length=15):

    arr_copy = arr.copy()
    start = None

    for i in range(len(arr_copy)):

        if np.isnan(arr_copy[i]):

            if start is None:
                start = i
        else:
            if start is not None:
                end = i
                gap_length = end - start
                if gap_length <= max_gap_length:
                    start_val = arr_copy[start - 1] if start > 0 else arr_copy[end]
                    end_val = arr_copy[end]
                    for j in range(start, end):
                        arr_copy[j] = start_val + ((end_val - start_val) * (j - start + 1)) / (gap_length + 1)
                else:
                    arr_copy[start:end] = np.nan
                start = None

    return arr_copy

def lowpass_filter(sig, cutoff=0.03, fs=1, order=4):

    b, a = butter(order, cutoff / (fs / 2), btype='low')

    return filtfilt(b, a, sig)

def median_filter_3(arr: np.ndarray) -> np.ndarray:

    # 3-point median filter to remove single sample spikes
    arr_copy = arr.copy()
    n = len(arr_copy)

    for i in range(1, n - 1):

        window = [arr[i - 1], arr[i], arr[i + 1]]
        vals = [x for x in window if not np.isnan(x)]
        arr_copy[i] = np.median(vals) if vals else np.nan

    return arr_copy

def clean_fhr_signal(raw_fhr):

    fhr = np.array(raw_fhr, dtype=float)

    # Clamp invalid values to NaN
    fhr[(fhr == 0) | (fhr < 50) | (fhr > 180)]   = np.nan

    # Interpolate short gaps
    fhr = linear_interpolate_nan(fhr, max_gap_length=15)

    # Fill tiny edges NaN
    s = pd.Series(fhr).interpolate(limit=10, limit_direction="both").ffill().bfill()

    return s.values

def clean_uc_signal(uc_list: list[float]) -> np.ndarray:

    arr = np.array(uc_list, dtype=float)

    # Clamp invalid values to NaN
    arr[(arr < 0) | (arr>100)]    = np.nan

    # Remove single-sample spikes via 3-point median filter
    arr = median_filter_3(arr)

    # Interpolate short gaps and fill edges
    arr = linear_interpolate_nan(arr, max_gap_length=15)
    arr = pd.Series(arr).interpolate(limit=60, limit_direction='both').ffill().bfill().values

    # Final zero-phase low-pass to kill residual jitter
    smoothed = lowpass_filter(arr, cutoff=0.02, fs=1, order=4)

    return smoothed

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
