if os.name == 'posix':
    pathVar = 'LD_LIBRARY_PATH'
elif os.name == 'mac':
    pathVar = 'DYLD_LIBRARY_PATH'
else:
    raise OSError('Unsupported OS : ' + os.name)

if not os.environ.has_key('LD_LIBRARY_PATH'):
    os.environ[pathVar] = os.environ['XROOTSYS'] + os.sep + 'lib'
else:
    os.environ[pathVar] = os.environ['XROOTSYS'] + os.sep + 'lib' + os.pathsep + os.environ[pathVar]

def rootbin(self):
    if not os.environ.has_key('ROOTSYS'):
        raise AssertionError("ROOTSYS environment variable is not defined nor given in test suite configuration")
    return os.environ['ROOTSYS'] + os.sep + 'bin' + os.sep + 'root -b -l'
Setup.getTag_rootbin = rootbin

def xrootURL(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : 'root://'+os.environ['STAGE_HOST']+'/'+self.getTag(test, 'noTapeFileName' + snb)+'?stagerHost='+os.environ['STAGE_HOST'])
Setup.getTag_xrootURL = xrootURL

def xrootRootURL(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : ['root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb),
                           'root://'+os.environ['STAGE_HOST']+'//'+ self.getTag(test,'noTapeFileName'+snb),
                           'root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb)+'?stagerHost='+os.environ['STAGE_HOST']])
Setup.getTag_xrootRootURL = xrootRootURL

def xrootRootURLparam(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : ['root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb)+'?',
                           'root://'+os.environ['STAGE_HOST']+'//'+ self.getTag(test,'noTapeFileName'+snb)+'?',
                           'root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb)+'?stagerHost='+os.environ['STAGE_HOST']+'&'])
Setup.getTag_xrootRootURLparam = xrootRootURLparam

def simpleXrdcpTapeURL(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : 'root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'tapeFileName'+snb))
Setup.getTag_simpleXrdcpTapeURL = simpleXrdcpTapeURL

def simpleXrdcpURL(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : 'root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb))
Setup.getTag_simpleXrdcpURL = simpleXrdcpURL

def xrdcpURL(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : ['root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb),
                           'root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb)+'?stagerHost='+os.environ['STAGE_HOST'],
                           'root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb)+' -OSstagerHost='+os.environ['STAGE_HOST']])
Setup.getTag_xrdcpURL = xrdcpURL

def xrdcpURLparam(self, nb=0):
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : ['root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb)+'?',
                           'root://'+os.environ['STAGE_HOST']+'/'+ self.getTag(test,'noTapeFileName'+snb)+'?stagerHost='+os.environ['STAGE_HOST']+'\\&'])
Setup.getTag_xrdcpURLparam = xrdcpURLparam

def corexrdcp(self):
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrdcp -np'
Setup.getTag_corexrdcp = corexrdcp

def xrdcp(self):
    cmd = os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrdcp -np'
    return [cmd, 'KRB5CCNAME=nonexistinghost ' + cmd]
Setup.getTag_xrdcp = xrdcp

def xrdfs(self):
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrdfs'
Setup.getTag_xrdfs = xrdfs

