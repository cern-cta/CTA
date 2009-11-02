def rootbin(self):
    if not os.environ.has_key('ROOTSYS'):
        raise AssertionError("ROOTSYS environment variable is not defined nor given in test suite configuration")
    return os.environ['ROOTSYS'] + os.sep + 'bin' + os.sep + 'root -b -l'
Setup.getTag_rootbin = rootbin

def rootRFIOURL(self, nb=0):
    global RootTURLs
    RootTURLs = ['rfio:///castor\?path=',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/castor\?path=',
                 'rfio://',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT'],
                 'rfio:///',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/']
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : map(lambda x : x + self.getTag(test, 'noTapeFileName' + snb), RootTURLs))
Setup.getTag_rootRFIOURL = rootRFIOURL

def rootCastorURL(self, nb=0):
    global RootTURLs
    RootTURLs = ['castor:///castor\?path=',
                 'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/castor\?path=',
                 'castor://',
                 'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT'],
                 'castor:///',
                 'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/']
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : map(lambda x : x + self.getTag(test, 'noTapeFileName' + snb), RootTURLs))
Setup.getTag_rootCastorURL = rootCastorURL
