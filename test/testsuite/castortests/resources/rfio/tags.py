# drop rfcp speed info as it varies too much
self.suppressRegExp('seconds through (?:\w+\s+\(\w+\)) and (?:\w+\s+\(\w+\))(\s\(\d+\s\w+/sec\))')

def rfcp(self):
    return ['rfcp', 'rfcp -v2']
Setup.getTag_rfcp = rfcp

def rfioTURL(self, nb=0):
    global RfioTURLs
    RfioTURLs = ['',
                 'rfio:///castor\?path=',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/castor\?path=',
                 'rfio://',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT'],
                 'rfio:///',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/']
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : map(lambda x : x + self.getTag(test, 'noTapeFileName' + snb), RfioTURLs))
Setup.getTag_rfioTURL = rfioTURL
