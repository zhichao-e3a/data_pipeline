import json
from typing import Optional

def extract_gest_age(

        conclusion : str,
        basic_info : str

) -> Optional[int]:

    gest_age        = None
    basic_info_json = json.loads(basic_info)

    # Check if gest_age can be obtained from 'basic_info' field
    if basic_info_json["setPregTime"]:

        gest_string = basic_info_json["pregTime"]

        digits = [int(c) for c in gest_string if c.isdigit()]

        if len(digits) == 3:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
        elif len(digits) == 2:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7

    # If 'conclusion' field available and gest_age still not found
    if conclusion and not gest_age:

        gest_string = conclusion.split("。")[0]

        digits = [int(c) for c in gest_string if c.isdigit()]

        if len(digits) == 3:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
        elif len(digits) == 2:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7

    return gest_age