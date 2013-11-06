import re
from castor_base_file_parser import BaseFileParser

class RSyslogFileHeaderParser(BaseFileParser):
    """
    A FileParser which supports the parsing of RSYSLOG_FileFormat based log 
    files. E.g: 2011-01-09T04:08:33.759308+01:00 c2atlassrv102 stagerd[28041]:
    """

    def __init__(self, config):
        BaseFileParser.__init__(self)

        # Regular expression to parse the header components of an rsyslog
        # formatted log message.
        self._regexhdr = re.compile(r"""
            (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)\.?  # Timestamp
            (\d{6})?                            # Usecs
            ([+\-]\d\d:\d\d)     \s*           # Timezone
            (\S+)                \s*           # Hostname
            (\w+)                              # Daemon name
            \[
            (\d+)                              # PID
            \]:                  \s*
            (.*)                               # Body
            """, re.VERBOSE | re.MULTILINE)

        self.castor_instance = config['castor_instance']

    def parseline(self, dic, data):
        """
        Parse the given message and return a dictionnary containing the key-value
        pairs.
        """

        try:
            # Try to parse the message header using the standard format:
            for match in self._regexhdr.findall("".join(data)):
                result = { 'TIMESTAMP'      : ''.join([match[0],'.',match[1],match[2]]),
                           'USECS'          : match[1] if match[1] else '000000',
                           'EPOCH'          : str(self._get_epoch(match[0], match[2])),
                           'HOSTNAME'       : self._get_hostname(match[3]),
                           'DAEMON'         : match[4],
                           'PID'            : match[5],
                           'INSTANCE'       : self.castor_instance }

            return dict(result.items() + dic.items()), match[6]

        except Exception, e:
            # If we are here we failed, probably because we have an "old" format log message.
            # We try to parse it with old format, otherwise just give up...
            return dic, data

