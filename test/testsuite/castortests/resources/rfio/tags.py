def rfcp(self):
    return ['rfcp', 'rfcp -v2']
Setup.getTag_rfcp = rfcp

def rfcpupd(self):
    return ['rfcpupd', 'rfcpupd -v2']
Setup.getTag_rfcpupd = rfcpupd

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
