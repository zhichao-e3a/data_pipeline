import numpy as np
from tqdm.auto import tqdm

import neurokit2 as nk
from numpy import trapezoid
from scipy.signal import butter, filtfilt

WINDOW_SIZE_SECONDS = 10 * 60

def _percentile_bt(series, pct=10):

    h, bins = np.histogram(series, bins=np.arange(series.min(), series.max() + 2))
    cdf = np.cumsum(h) / h.sum()
    idx = np.searchsorted(cdf, pct / 100)

    return bins[idx]

def _bt_series(uc_vals, fs=1):

    win = 10 * 60 * fs
    half = win // 2
    bt = np.zeros_like(uc_vals, float)
    for i in range(len(uc_vals)):
        lo, hi = max(0, i - half), min(len(uc_vals), i + half)
        bt[i] = _percentile_bt(uc_vals[lo:hi], 10)

    return bt

def compute_fhr_baseline(fhr, fs=1, cutoff=0.005):

    def _lp(sig):
        b, a = butter(4, cutoff / (fs / 2), btype='low')
        padlen = 3 * max(len(a), len(b))
        if len(sig) <= padlen:
            return sig
        return filtfilt(b, a, sig)

    baseline = _lp(fhr)

    for i in range(3):
        upper = baseline + (20 - 5 * i)
        lower = baseline - 20
        clipped = np.clip(fhr, lower, upper)
        baseline = _lp(clipped)

    return baseline

def extract_features(data, kind):

    extracted = []

    for row in tqdm(data):

        uc = np.array(row["uc"], dtype=np.float64)

        # Total AUC
        total_auc       = float(trapezoid(uc, dx=1))

        # Baseline Tone
        bt_series       = _bt_series(uc, fs=1)
        baseline_tone   = float(np.median(bt_series))

        # Sample Entropy
        sample_ent = float(nk.entropy_sample(uc, dimension=2, r=0.2 * np.std(uc))[0])

        record = {
            "_id"               : row["_id"],
            "mobile"            : row["mobile"],
            "measurement_date"  : row["measurement_date"],
            "start_test_ts"     : row["start_test_ts"],
            "uc"                : row["uc"],
            "fhr"               : row["fhr"],
            "fmov"              : row["fmov"],
            "gest_age"          : row["gest_age"],
            "add"               : row["add"],
            "edd"               : row["edd"],
            "total_auc"         : total_auc,
            "baseline_tone"     : baseline_tone,
            "sample_entropy"    : sample_ent
        }

        if kind == "onset":
            record["onset"] = row["onset"]

        extracted.append(record)

    return extracted