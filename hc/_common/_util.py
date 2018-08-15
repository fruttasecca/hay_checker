"""
Common utilities file.
"""


def seconds_to_time_format(seconds):
    """
    Returns a number of seconds amouting to <= 24 hours
    as a string of the format HH:mm:ss
    :param seconds: Seconds to return as a HH:mm:ss string, must total <= 24 hours.
    :return: A HH:mm:ss string representing the seconds.
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)
