HISTORICAL = """
SELECT
uu.mobile,
r.id,
r.start_ts,
r.start_test_ts,
r.contraction_url,
r.hb_baby_url,
r.raw_fetal_url,
r.basic_info,
r.conclusion,
tt.expected_born_date,
tt.end_born_ts,
r.utime
FROM
extant_future_user.user AS uu
INNER JOIN
extant_future_data.origin_data_record AS r
ON uu.id = r.user_id
INNER JOIN
extant_future_user.user_detail AS tt
ON uu.id = tt.uid
AND r.contraction_url <> ''
AND r.hb_baby_url <> ''
AND tt.end_born_ts IS NOT NULL
AND tt.end_born_ts <> 0
AND r.utime > '{last_utime}'
;
"""

RECRUITED = """
SELECT
uu.mobile,
r.id,
r.start_ts,
r.start_test_ts,
r.contraction_url,
r.hb_baby_url,
r.raw_fetal_url,
r.basic_info,
r.conclusion,
r.utime
FROM
extant_future_user.user AS uu
INNER JOIN
origin_data_record AS r
ON uu.id = r.user_id
AND r.contraction_url <> ''
AND r.hb_baby_url <> ''
AND uu.mobile IN ({numbers})
AND r.start_ts BETWEEN UNIX_TIMESTAMP({start}) AND UNIX_TIMESTAMP({end})
AND r.utime > '{last_utime}'
;
"""