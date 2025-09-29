import pyarrow as pa

from pydantic import BaseModel

class ResponseModel(BaseModel):

    job_id : str

schema_add = pa.schema(
    [
        ("_id", pa.int64()),
        ("mobile", pa.string()),
        ("measurement_date", pa.string()),
        ("start_test_ts", pa.string()),
        ("gest_age", pa.int64()),
        ("uc", pa.list_(pa.string())),
        ("fhr", pa.list_(pa.string())),
        ("fmov", pa.list_(pa.string())),
        ("baseline_tone", pa.int64()),
        ("sample_entropy", pa.int64()),
        ("total_auc", pa.int64()),
        ("add", pa.string()),
        ("utime", pa.string()),
    ]
)

schema_onset = pa.schema(
    [
        ("_id", pa.int64()),
        ("mobile", pa.string()),
        ("measurement_date", pa.string()),
        ("start_test_ts", pa.string()),
        ("gest_age", pa.int64()),
        ("uc", pa.list_(pa.string())),
        ("fhr", pa.list_(pa.string())),
        ("fmov", pa.list_(pa.string())),
        ("baseline_tone", pa.int64()),
        ("sample_entropy", pa.int64()),
        ("total_auc", pa.int64()),
        ("onset", pa.string()),
        ("utime", pa.string()),
    ]
)