from sim.config import M

# Maximum identifier value (modulus for the ring)
MAX_ID = 2 ** M


def in_interval(key: int, start: int, end: int,
                inc_start: bool = False, inc_end: bool = False) -> bool:
    """
    Return True if key lies in the circular interval from start to end on a ring [0, 2^M).
    """
    key   %= MAX_ID
    start %= MAX_ID
    end   %= MAX_ID

    if start < end:
        # Straight interval
        if key == start and not inc_start:
            return False
        if key == end and not inc_end:
            return False
        if inc_start and key == start:
            return True
        if inc_end and key == end:
            return True
        return start < key < end
    else:
        # Wrapping interval
        # Interval is (start, MAX_ID) U [0, end)
        if key == start and not inc_start:
            return False
        if key == end and not inc_end:
            return False
        in_high = (key > start) or (inc_start and key == start)
        in_low = (key < end)   or (inc_end   and key == end)
        return in_high or in_low


def mod_add(a: int, b: int) -> int:
    """
    Return (a + b) mod 2^M.
    """
    return (a + b) % MAX_ID


def mod_sub(a: int, b: int) -> int:
    """
    Return (a - b) mod 2^M.
    """
    return (a - b) % MAX_ID
