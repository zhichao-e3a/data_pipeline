HISTORICAL = """
SELECT
uu.mobile,
r.id,
r.user_id,
r.psn,
FROM_UNIXTIME(r.start_ts, '%%Y-%%m-%%d %%H:%%i:%%s') AS start_ts,
CASE 
WHEN r.start_test_ts = 0 THEN '0'
ELSE FROM_UNIXTIME(r.start_test_ts, '%%Y-%%m-%%d %%H:%%i:%%s')
END AS start_test_ts,
r.hb_baby_url,
r.hb_sound_url,
r.basic_info,
r.conclusion,
r.ctime,
r.contraction_url,
tt.expected_born_date,
FROM_UNIXTIME(tt.end_born_ts, '%%Y-%%m-%%d %%H:%%i:%%s') AS end_born_ts
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
;
"""

RECRUITED = """
SELECT
uu.mobile,
r.id,
r.user_id,
r.psn,
FROM_UNIXTIME(r.start_ts, '%%Y-%%m-%%d %%H:%%i:%%s') AS start_ts,
CASE 
WHEN r.start_test_ts = 0 THEN '0'
ELSE FROM_UNIXTIME(r.start_test_ts, '%%Y-%%m-%%d %%H:%%i:%%s')
END AS start_test_ts,
r.hb_baby_url,
r.hb_sound_url,
r.basic_info,
r.conclusion,
r.ctime,
r.contraction_url
FROM
extant_future_user.user AS uu        -- extant_future_user.user
INNER JOIN
origin_data_record AS r              -- extant_future_data.origin_data_record
ON uu.id = r.user_id
AND r.hb_baby_url <> ''
AND r.contraction_url <> ''
AND uu.mobile IN ({numbers})
AND r.start_ts BETWEEN UNIX_TIMESTAMP({start}) AND UNIX_TIMESTAMP({end})
;
"""