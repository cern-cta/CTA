import re
import os
import datetime
import socket
from castor_base_file_parser import BaseFileParser

month_dict = { "Jan" : "01",
               "Feb" : "02",
               "Mar" : "03",
               "Apr" : "04",
               "May" : "05",
               "Jun" : "06",
               "Jul" : "07",
               "Aug" : "08",
               "Sep" : "09",
               "Oct" : "10",
               "Nov" : "11",
               "Dec" : "12"}

class RSyslogFileOldHeaderParser(BaseFileParser):
    """
    A FileParser which supports the parsing of RSYSLOG_FileFormat based log 
    files. E.g: 2011-01-09T04:08:33.759308+01:00 c2atlassrv102 stagerd[28041]:
    """

    def __init__(self, config):
        BaseFileParser.__init__(self)

        # Regular expression to parse the header components of an "old style"
        # formatted log message.
        self._regexhdrold = re.compile(r"""
            (\w+)                \s*           # Month
            (\d{2})              \s*           # Day
            (\d\d:\d\d:\d\d)     \s*           # Hour
            (\w+)                              # Daemon name
            \[
            (\d+)                              # PID
            ,?\d?                              # Thread ID (deprecated, discard)
            \]:                  \s*
            (.*)                               # Body
            """, re.VERBOSE | re.MULTILINE)
        
        # Find the hostname
        self._hostname = socket.gethostname().split('.')[0]
        # Get Castor instance from config
        self.castor_instance = config['castor_instance']

    def parseline(self, dic, data):
        """
        Parse the given message and return a dictionnary containing the key-value
        pairs.
        """

        if not data:
            return dic, data

        try:
            for match in self._regexhdrold.findall("".join(data)):
                # Assembly the timestamp:
                now = datetime.datetime.now()                 
                # Timezone (obtained from a system call for now)
                dateoutput = os.popen('date "+%:z"')
                timezone = dateoutput.readlines()[0].strip()
                # Convert from month abbreviation to number:
                MONTH = str(month_dict[match[0]])
                # The TIMESTAMP:            month            day              hour    timezone
                TIMESTAMP = str(now.year)+'-'+MONTH+'-'+str(match[1])+'T'+str(match[2])+timezone

                result = { 'TIMESTAMP'      : TIMESTAMP,
                           'USECS'          : '000000',
                           'EPOCH'          : str(self._get_epoch(TIMESTAMP, timezone)),
                           'HOSTNAME'       : self._hostname,
                           'DAEMON'         : match[3],
                           'PID'            : match[4],
                           'INSTANCE'       : self.castor_instance,
                           'LVL'            : 'Info',   
                           'PAYLOAD'            : match[5] }

            return dict(result.items() + dic.items()), ""

        except Exception, e:
            # If we are here we failed to parse.
            return dic, data


