import datetime


def int14_to_datetime(dt: int) -> datetime.datetime:
    year = dt // 10000000000
    # %= is slower than this.
    # var%=int translate to bytes code INPLACE_MODULO, and var = var % int translate to BINARY_MODULO
    dt = dt % 10000000000
    month = dt // 100000000
    dt = dt % 100000000
    day = dt // 1000000
    dt = dt % 1000000
    hour = dt // 10000
    dt = dt % 10000
    minute = dt // 100
    second = dt % 100
    return datetime.datetime(year, month, day, hour, minute, second)


def int8_to_date(dt: int) -> datetime.date:
    year, dt = dt // 10000, dt % 10000
    month, day = dt // 100, dt % 100
    return datetime.date(year, month, day)


def date2int8(dt: datetime.date) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day


def datetime2int14(dt: datetime.datetime) -> int:
    return (
        dt.year * 10000000000
        + dt.month * 100000000
        + dt.day * 1000000
        + dt.hour * 10000
        + dt.minute * 100
        + dt.second
    )
