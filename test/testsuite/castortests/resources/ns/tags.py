############
### uuid ###
############
UUID = None
def sessionuuid(self):
    global UUID
    if UUID == None :
        sys.path.append(str(self.config.topdir))
        import uuid
        UUID = str(uuid.uuid4())
    return UUID
Setup.getTag_sessionuuid = sessionuuid


##################
### basic tags ###
##################

def _createCastorDir(self, path):
    # create the path in the namespace
    output = Popen('nsmkdir ' + path)
    # check it went fine
    assert len(output) == 0 or output.find('File exists'), \
        'Failed to create working directory ' + path + os.linesep + "Error :" + os.linesep + output
    # return the created dir
    return path
Setup._createCastorDir = _createCastorDir

def genericPath(self):
    # get the test path
    testpath = self.options.get('Generic','CastorTestDir')
    # create a unique directory
    return self._createCastorDir(testpath + os.sep + self.getTag(None, 'sessionuuid'))
Setup.getTag_genericPath = genericPath

def nsDir(self, nb=0):
    return (lambda test :  self.getTag(test, 'genericPath') + os.sep + test + str(nb))
Setup.getTag_nsDir = nsDir

def nsFile(self, nb=0):
    print 'inside nsFile',self,self.getTag
    try: 
	return (lambda test :  self.getTag(test, 'nsDir'))
    except Exception,e:
	import traceback
	traceback.print_stack()
	print e
	raise e
Setup.getTag_nsFile = nsFile


##################
### ns cleanup ###
##################

def nsCleanup(self):
    # get rid of all files used in castor by dropping the namespace temporary directories
    for entry in ['genericPath', 'noTapePath', 'tapePath']:
        if self.tags.has_key(entry):
            path = self.tags[entry]
            output = Popen('nsrm -r ' + path)
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
