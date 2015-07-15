import re
from castor_base_file_parser import BaseFileParser

class RSyslogFileBodyParser(BaseFileParser):
    """
    A FileParser which supports the parsing of RSYSLOG_FileFormat based log 
    files. E.g: 2011-01-09T04:08:33.759308+01:00 c2atlassrv102 stagerd[28041]:
    """

    def __init__(self, config):
        BaseFileParser.__init__(self)

        # Regular expression to parse the body of an rsyslog formatted log
        # message. We are talking about key-value pairs separated by =
        self._regex_body = re.compile('([^ =]+) *= *("[^"]*"|[^ ]*)')

    def parseline(self, dic, data):
        """
        Parse the given message and return a dictionnary containing the key-value
        pairs.
        """
        result = dict()

        try:
            # Try to parse the message header using the standard format:
            for k, v in self._regex_body.findall(data):
                if v[:1] == '"':
                    result[k] = v[1:-1]
                else:
                    result[k] = v

            return dict(result.items() + dic.items()), ""

        except Exception, e:
            # If we are here we failed, probably because we have an "old" format log message.
            # We try to parse it with old format, otherwise just give up...
            return dic, data


