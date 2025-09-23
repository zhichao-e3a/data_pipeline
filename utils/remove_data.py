import json
from pathlib import Path

def remove_data(consolidated):

    root = Path(__file__).resolve().parent.parent
    path = root / "resources" / "recordstoremove.json"
    with open(path, "r") as f:
        content = f.read()
        to_remove = json.loads(content)

    skipped = 0
    good_measurements = []
    bad_measurements = set()

    for measurement in consolidated:

        mobile  = measurement["mobile"]
        time    = measurement["start_test_ts"]

        for r_mobile in to_remove:

            for r_time in to_remove[r_mobile]:

                if r_mobile == mobile and r_time == time:
                    bad_measurements.add(
                        (r_mobile, r_time)
                    )

    for measurement in consolidated:

        mobile  = measurement["mobile"]
        time    = measurement["start_test_ts"]

        if (mobile, time) not in bad_measurements:
            good_measurements.append(measurement)
            continue

        skipped += 1

    return good_measurements, skipped