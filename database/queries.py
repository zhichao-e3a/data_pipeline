HISTORICAL = """
SELECT
uu.mobile,
r.id,
start_ts,
r.contraction_url,
r.hb_baby_url,
r.basic_info,
r.conclusion,
tt.expected_born_date,
tt.end_born_ts,
r.utime
FROM
extant_future_user.user AS uu                -- extant_future_user.user
INNER JOIN
extant_future_data.origin_data_record AS r   -- extant_future_data.origin_data_record
ON uu.id = r.user_id
AND r.hb_baby_url <> ''
AND r.contraction_url <> ''
INNER JOIN
extant_future_user.user_detail AS tt         -- extant_future_user.user_detail
ON uu.id = tt.uid
AND tt.end_born_ts IS NOT NULL
AND tt.end_born_ts <> 0
AND r.utime > '{last_utime}'
;
"""

RECRUITED = """
SELECT
uu.mobile,
r.id,
start_ts,
r.contraction_url,
r.hb_baby_url,
r.basic_info,
r.conclusion,
r.utime
FROM
extant_future_user.user AS uu        -- extant_future_user.user
INNER JOIN
origin_data_record AS r              -- extant_future_data.origin_data_record
ON uu.id = r.user_id
AND r.hb_baby_url <> ''
AND r.contraction_url <> ''
AND uu.mobile IN ({numbers})
AND r.start_ts BETWEEN UNIX_TIMESTAMP({start}) AND UNIX_TIMESTAMP({end})
AND r.utime > '{last_utime}'
;
"""