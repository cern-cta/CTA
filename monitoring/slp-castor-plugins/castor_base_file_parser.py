import calendar

class BaseFileParser:
    """
    BaseFileParser class containing common utility functions to be used by all
    FileParsers.
    """

    def __init__(self):
        self._epochcache = dict()

    def _get_epoch(self, timestamp, timezone):
        """
        Parse a given timestamp representing a time value of the format
        '%Y-%m-%dT%H:%M:%S' to seconds since January 1st 1970 (epoch). Note: In
        an ideal world we would using the time.strptime() call here but
        benchmark tests show that this is incredibly expensive.

        As a result we parse the timestamp manually giving us a factor 10
        increase in throughput.
        See: http://bugs.python.org/issue474274
        """
        # Prune the epochcache if it's too large.
        if len(self._epochcache) > 100000:
            self._epochcache.clear()

        # See if we already have a cached value for the given timestamp
        # otherwise generate a new entry.
        try:
            return self._epochcache[timestamp+timezone]
        except KeyError:
            # we convert the timezone into seconds, then substract if from
            # the "local epoch" of the timestamp, in order to get the REAL epoch UTC
            timezone_hour, timezone_minutes = timezone[1:].split(':')
            timezone_epoch = int(timezone[0]+'1') * ( (int(timezone_hour) * 3600) + (int(timezone_minutes) * 60) )

            epoch = calendar.timegm((int(timestamp[0:4]),   # tm_year
                                     int(timestamp[5:7]),   # tm_mon
                                     int(timestamp[8:10]),  # tm_mday
                                     int(timestamp[11:13]), # tm_hour
                                     int(timestamp[14:16]), # tm_min
                                     int(timestamp[17:19]), # tm_sec
                                     -1,                    # tm_wday
                                     -1,                    # tm_yday
                                     -1)) - timezone_epoch # tm_isdst

            self._epochcache[timestamp+timezone] = epoch
            return epoch

    def _get_hostname(self, host):
        return host.split('.')[0]

