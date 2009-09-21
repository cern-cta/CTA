if os.name == 'posix':
    pathVar = 'LD_LIBRARY_PATH'
elif os.name == 'mac':
    pathVar = 'DYLD_LIBRARY_PATH'
else:
    raise OSError('Unsupported OS : ' + os.name)
os.environ[pathVar] = os.environ['XROOTSYS'] + os.sep + 'lib' + os.pathsep + os.environ[pathVar]

def xrootURL(self, nb=0):
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

def xrdcp(self):
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrdcp'
Setup.getTag_xrdcp = xrdcp

def xrd(self):
    return os.environ['XROOTSYS'] + os.sep + 'bin' + os.sep + 'xrd'
Setup.getTag_xrd = xrd
