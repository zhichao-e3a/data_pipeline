from datetime import datetime

def extract_fetal_movement(raw_fmov, start_ts, length):

    fmov_idx = [] ; record = [] ; unique_time_set = set()
    start_dt = datetime.strptime(start_ts, '%Y-%m-%d %H:%M:%S')

    for _fmov in raw_fmov:

        fmov_unix   = int(_fmov.split('：')[1].split(' ')[0])
        fmov_deg    = _fmov.split('：')[2]
        fmov_dt     = datetime.fromtimestamp(fmov_unix)
        idx         = fmov_dt - start_dt
        idx_s       = idx.seconds
        fmov_tuple  = (idx_s, fmov_deg)

        if idx_s not in unique_time_set:
            fmov_idx.append(fmov_tuple)
            unique_time_set.add(idx_s)

    fmov_idx.sort(key=lambda x: x[0])

    counter = 0
    pointer = 0

    while pointer < len(fmov_idx):

        curr = fmov_idx[pointer]

        if counter != curr[0]:
            record.append("0")
        else:
            record.append(curr[1])
            pointer += 1

        counter += 1

    while len(record) < length:

        record.append("0")

    return record, len(record)