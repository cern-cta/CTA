def rootbin(self):
    if not os.environ.has_key('ROOTSYS'):
        raise AssertionError("ROOTSYS environment variable is not defined nor given in test suite configuration")
    return os.environ['ROOTSYS'] + os.sep + 'bin' + os.sep + 'root -b -l'
Setup.getTag_rootbin = rootbin

def rootRFIOURL(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : ['rfio:///castor\?path='+self.getTag(test, 'noTapeFileName'+snb),
                           'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/castor\?path='+self.getTag(test, 'noTapeFileName'+snb),
                           'rfio://'+self.getTag(test, 'noTapeFileName'+snb),
                           'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+self.getTag(test, 'noTapeFileName'+snb),
                           'rfio:///'+self.getTag(test, 'noTapeFileName'+snb),
                           'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/'+self.getTag(test, 'noTapeFileName'+snb)])
Setup.getTag_rootRFIOURL = rootRFIOURL

def rootCastorURL(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : ['castor:///castor\?path='+self.getTag(test, 'noTapeFileName'+snb),
                           'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/castor\?path='+self.getTag(test, 'noTapeFileName'+snb),
                           'castor://'+self.getTag(test, 'noTapeFileName'+snb),
                           'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+self.getTag(test, 'noTapeFileName'+snb),
                           'castor:///'+self.getTag(test, 'noTapeFileName'+snb),
                           'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/'+self.getTag(test, 'noTapeFileName'+snb)])
Setup.getTag_rootCastorURL = rootCastorURL

def rootCastorURLparam(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : ['castor:///castor\?path='+self.getTag(test, 'noTapeFileName'+snb)+'&',
                           'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/castor\?path='+self.getTag(test, 'noTapeFileName'+snb)+'&',
                           'castor://'+self.getTag(test, 'noTapeFileName'+snb)+'?',
                           'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+self.getTag(test, 'noTapeFileName'+snb)+'?',
                           'castor:///'+self.getTag(test, 'noTapeFileName'+snb)+'?',
                           'castor://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/'+self.getTag(test, 'noTapeFileName'+snb)+'?'])
Setup.getTag_rootCastorURLparam = rootCastorURLparam
