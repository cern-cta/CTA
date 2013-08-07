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

def xrd(self):
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrd'
Setup.getTag_xrd = xrd
