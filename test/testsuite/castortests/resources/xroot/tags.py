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
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrdcp'
Setup.getTag_corexrdcp = corexrdcp

def xrdcp(self):
    cmd = os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrdcp'
    return [cmd, 'KRB5CCNAME=nonexistinghost ' + cmd]
Setup.getTag_xrdcp = xrdcp

# This is the old xroot client, but we still use it for the prepare test
# as the new client (3.3.x) does not provide the equivalent command. It should
# be dropped once we adopt xroot 4.0.0, which provides a prepare command.
def xrd(self):
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrd'
Setup.getTag_xrd = xrd

def xrdfs(self):
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrdfs'
Setup.getTag_xrdfs = xrdfs

