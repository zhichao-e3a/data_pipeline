import numpy as np
import pandas as pd
from scipy.signal import butter, filtfilt

def process_signals(data_list):

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

        uc_clamped  = np.array([clean_uc_fhr_pair(u, f)[0] for u, f in zip(uc_raw, fhr_raw)])
        fhr_clamped = np.array([clean_uc_fhr_pair(u, f)[1] for u, f in zip(uc_raw, fhr_raw)])

        fhr_interp  = linear_interpolate_nan(fhr_clamped)
        fhr_cleaned = clean_fhr_signal(fhr_interp)

        uc_cleaned = clean_uc_signal(uc_clamped.tolist())

        cleaned_pairs = [
            [float(u), float(f)]
            for u, f in zip(uc_cleaned, fhr_cleaned)
            if not (np.isnan(f) or np.isnan(u))
        ]

        if not cleaned_pairs:
            skipped += 1

        row['uc']   = [i[0] for i in cleaned_pairs]
        row['fhr']  = [i[1] for i in cleaned_pairs]

        processed_list.append(row)

    return processed_list, skipped

def clean_uc_fhr_pair(uc_value, fhr_value):

    uc_value = float(uc_value) if uc_value not in ["", None] else np.nan
    fhr_value = float(fhr_value) if fhr_value not in ["", None] else np.nan

    if uc_value < 0 or uc_value > 100:
        uc_value = np.nan

    if fhr_value == 0 or fhr_value < 50 or fhr_value > 180:
        fhr_value = np.nan

    return uc_value, fhr_value

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

        window      = [arr[i - 1], arr[i], arr[i + 1]]
        vals        = [x for x in window if not np.isnan(x)]
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
    arr[(arr < 0) | (arr>100)] = np.nan

    # Remove single-sample spikes via 3-point median filter
    arr = median_filter_3(arr)

    # Interpolate short gaps and fill edges
    arr = linear_interpolate_nan(arr, max_gap_length=15)
    arr = pd.Series(arr).interpolate(limit=60, limit_direction='both').ffill().bfill().values

    # Final zero-phase low-pass to kill residual jitter
    smoothed = lowpass_filter(arr, cutoff=0.02, fs=1, order=4)

    return smoothed