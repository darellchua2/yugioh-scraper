import re


def is_link_card_set_code(set_card_code) -> tuple[str | None, str | None]:
    pat1 = re.compile(
        r"^(.[^\-]{1,5})-(.[^\-]{1,5})$")
    pat_match = pat1.match(set_card_code)
    if pat_match:
        set_code = pat_match.group(1)
        return set_card_code, set_code
    else:
        return None, None
