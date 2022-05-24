from datetime import datetime


def timestamp_to_datetime(timestamp):
    t = timestamp
    yyyy, MM, dd, HH, mm, ss = int(t[0:4]), int(t[4:6]), int(t[6:8]), int(t[8:10]), int(t[10:12]), int(t[12:14])
    return datetime(yyyy, MM, dd, HH, mm, ss)
