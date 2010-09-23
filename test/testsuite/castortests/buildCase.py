def tagForCastorFileName(s):
    try:
        # get the fileclass of the file
        fileClass = Popen('nsls --class ' + s).split()[0]
        # check the number of tapeCopies in this file class
        nslistOutput = Popen('nslistclass --id=' + fileClass).split()
        nbCopies = int(nslistOutput[nslistOutput.index('NBCOPIES')+1])
        # based on this, decide on the tag
        if nbCopies > 0:
            return 'tapeFileName'
        else:
            return 'noTapeFileName'
    except ValueError:
        # default to non tape
        return 'noTapeFileName'

def tagForFileName(s, context):
    # is it a castor or a local file ?
    if s.startswith('/castor/'):
        return tagForCastorFileName(s)
    elif s == options.get('Tags','localFileName'):
        return 'localFileName'
    else:
        return 'tmpLocalFileName'

isDiskOnlySvcClass = {}
def tagForSvcClass(s, context):
    global isDiskOnlySvcClass
    # Ask the user if we do not now yet
    if not isDiskOnlySvcClass.has_key(s):
        ans = raw_input('Is the "' + s + '" service class tape enabled ? [Y/n] ')
        isDiskOnlySvcClass[s] = (ans.lower() not in ('', 'y'))
    # select the tag acoordingly
    if isDiskOnlySvcClass[s]:
        return 'diskOnlyServiceClass'
    else:
        return 'tapeServiceClass'

def tagForXrootURL(s, context):
    if context.find('xrdcp') > -1:
        return 'xrdcpURL'
    else:
        return 'xrootURL'

# regexps identifying the different tags. Each regexp may be
# associated to a function that takes as single parameter the
# replaced string and returns the tag to use. If None, the regexp
# will be replaced with the rule name
# Note that regexps must have exactly one group matching the part to
# be replaced
castorRegexps = {
    'fileName' : ('(?:[\s=]|\A|file:///)(/[^ \t\n\r\f\v:\)]*)', tagForFileName),
    'rhhost'   : ('Looking up RH host - Using ([\w.]+)', None),
    'rhport'   : ('Looking up RH port - Using (\d+)', None),
    'userid'   : ('Setting euid: (\w+)', None),
    'groupid'  : ('Setting egid: (\w+)', None),
    'localhost': ('Localhost is: ([\w.]+)', None),
    'callback port' : ('Creating socket for castor callback - Using port (\d+)', None),
    'sending time' : ('stager: ([A-Z][a-z]{2}\s+\d+\s+[\d:]+\s+\(\d+\)) Sending request', None),
    'uuid'     : ('([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', None),
    'send duration' : ('SND ([\d.]+) s to send the request', None),
    'answer duration' : ('CBK ([\d.]+) s before callback', None),
    'server ip': ('callback from (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) was received', None),
    'fileid@ns': ('(\d+@[\w.]+)', None),
    'castorTag': ('(?:\s+-U\s+|Usertag=|stage_filequery type=2 param=)(?!NULL)(\w+)', None),
    'svcClass' : ('(?:\s+-S\s+|STAGE_SVCCLASS=)(\w+)', tagForSvcClass),
    'nb bytes' : ('(\d+)\s+bytes in', None),
    'time to transfer' : ('bytes in (\d+) seconds through', None),
    'interface in' : ('seconds through (\w+\s+\(\w+\)) and', None),    
    'interface out' : ('seconds through (?:\w+\s+\(\w+\)|<interface in>) and (\w+\s+\(\w+\)(?:\s\(\d+\s\w+/sec\))?)', None),
    'xrootURL' : ('.*(root://\S*/\S+)', tagForXrootURL),
    'rfioTURL' : ('(rfio://\S*/\S+)', None),
    'rootCastorURL' : ('(castor://\S*/\S+)', None),
    'xrdcp'    : ('((?:\S*/)?xrdcp)\s', None),
    'xrd'      : ('((?:\S*/)?xrd)\s', None),
    'rfcp'     : ('((?:\S*/)?rfcp)\s', None),
    'globus-url-copy' : ('((?:\S*/)?globus-url-copy)\s', None),
    'transfer speed' : ('\[(?:([0-9\.]+|inf)\s[A-Z]+/s)\]', None),
    'bytes transfered' : ('\[xrootd] Total ([0-9\.]+\s[A-Z]+)\s', None),
    'id'       : ('Id:\s(\d+)\s', None),
    'modtime'  : ('Modtime:\s(\d+)\s', None),
    'stageHost': ('\s('+options.get('Environment','STAGE_HOST')+')\s', None),
    'gsiftpURL': ('(?:\s|\A)(gsiftp://[^/]*/\S*)', None),
    'protocol' : ('\sProtocol=(\S*)\s', None)
    }
tagRegexps.update(castorRegexps)

# regexps of parts of the output that should be dropped
# Note that regexps must have exactly one group matching the part to
# be dropped
castorSuppressRegExps = [
    'seconds through (?:\w+\s+\(\w+\)) and (?:\w+\s+\(\w+\))(\s\(\d+\s\w+/sec\))',
    '\n(\s*stager: Looking up service class - Using \S*\s*\n)'
]
suppressRegExps = suppressRegExps + castorSuppressRegExps
