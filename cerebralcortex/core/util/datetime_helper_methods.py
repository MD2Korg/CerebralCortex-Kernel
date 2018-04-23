import datetime as dt
import pytz


def get_timezone(tz_offset:float, common_only:bool=False):
    """
    Returns a timezone for a given offset in milliseconds
    :param tz_offset: milliseconds
    :param common_only:
    :return:
    """
    if tz_offset is None or tz_offset=="":
        raise ValueError("Offset cannot be None or empty.")

    # pick one of the timezone collections
    timezones = pytz.common_timezones if common_only else pytz.all_timezones

    # convert the milliseconds offset to a timedelta
    offset_days, offset_seconds = 0, int(tz_offset/1000)
    if offset_seconds < 0:
        offset_days = -1
        offset_seconds += 24 * 3600
    desired_delta = dt.timedelta(offset_days, offset_seconds)

    # Loop through the timezones and find any with matching offsets
    null_delta = dt.timedelta(0, 0)
    results = ""
    for tz_name in timezones:
        tz = pytz.timezone(tz_name)
        non_dst_offset = getattr(tz, '_transition_info', [[null_delta]])[-1]
        if desired_delta == non_dst_offset[0]:
            return tz_name
    return results