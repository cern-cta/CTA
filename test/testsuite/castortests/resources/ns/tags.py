############
### uuid ###
############
UUID = None
def sessionuuid(self):
    global UUID
    if UUID == None :
        sys.path.append(str(_topdir))
        import uuid
        UUID = str(uuid.uuid4())
    return UUID
Setup.getTag_sessionuuid = sessionuuid


##################
### basic tags ###
##################

def cnsHost(self):
    return self.options.get('Environment','CNS_HOST')
Setup.getTag_cnsHost = cnsHost

def _createDir(self, path):
    if path in self._alreadyCreatedDirs:
        return (path, False)
    # create the path in the namespace
    output = Popen('nsmkdir ' + path)
    # check it went fine
    assert len(output) == 0 or output.find('File exists') >= 0, \
        'Failed to create working directory ' + path + os.linesep + "Error :" + os.linesep + output
    # remember it
    self._alreadyCreatedDirs.add(path)
    # return the created dir
    if output.find('File exists') >= 0:
        return (path, False)
    else:
        return (path, True)
Setup._alreadyCreatedDirs = set()
Setup._createDir = _createDir

def _testSessionPath(self):
    # get the test path
    testpath = self.options.get('Generic','CastorTestDir')
    # create a unique directory
    return self._createDir(testpath + os.sep + self.getTag(None, 'sessionuuid'))[0]
Setup.getTag__testSessionPath = _testSessionPath

def nsDir(self):
    return (lambda test :  self.getTag(test, '_testSessionPath') + os.sep + test)
Setup.getTag_nsDir = nsDir

def nsFile(self, nb=0):
    return (lambda test : self._createDir(self.getTag(test, 'nsDir'))[0] + \
            os.sep + test + str(nb))
Setup.getTag_nsFile = nsFile


##################
### ns cleanup ###
##################

def nsCleanup(self):
    # TODO: must review this
    for entry in ['_testSessionPath']:
        if self.tags.has_key(entry):
            path = self.tags[entry]
            output = Popen('nschmod -R 777 ' + path + '; nsrm -r ' + path)
            assert len(output) == 0, \
                'Failed to cleanup working directory ' + path + \
                '. You may have to do manual cleanup' + os.linesep + \
                "Error :" + os.linesep + output
            del self.tags[entry]
Setup.cleanup_nameserver = nsCleanup

#####################################
###  local files related methods  ###
#####################################

def tmpLocalDir(self):
    d = tempfile.mkdtemp(prefix='CastorTestSuite')
    print os.linesep+'Temporary files will be stored in directory ' + d
    return d
Setup.getTag_tmpLocalDir = tmpLocalDir

def tmpLocalFileName(self, nb=0):
    return (lambda test : self.getTag(test, 'tmpLocalDir') + os.sep + test + '.' + str(nb))
Setup.getTag_tmpLocalFileName = tmpLocalFileName

def localCleanup(self):
    # also get rid of local files created
    if self.tags.has_key('tmpLocalDir'):
        # recursive deletion of all files and directories
        for root, dirs, files in os.walk(self.tags['tmpLocalDir'], topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        # remove the directory itself
        os.rmdir(self.tags['tmpLocalDir'])
        del self.tags['tmpLocalDir']
Setup.cleanup_local = localCleanup
