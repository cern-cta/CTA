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
             
########################
### stageHost & tags ###
########################
def stageHost(self):
    return self.options.get('Environment','STAGE_HOST')
Setup.getTag_stageHost = stageHost
             
def castorTag(self, nb=0):
    return (lambda test : 'castorTag'+self.getTag(test, 'sessionuuid')+test+str(nb))
Setup.getTag_castorTag = castorTag

# declare new random order tags
self.declareRandomOrderTag('noTapeFileName')
self.declareRandomOrderTag('tapeFileName')
self.declareRandomOrderTag('fileid@ns')

##################################
### nameserver related methods ###
##################################
def _checkDirPath(self, name, isTape):
    try:
        print "Checking whether " + name + " has proper fileclass"
        # get the fileclass of the path
        fileClass = Popen('nsls --class -d ' + name).split()[0]
        # check the number of tapeCopies in this file class
        nslistOutput = Popen('nslistclass --id=' + fileClass).split()
        nbCopies = int(nslistOutput[nslistOutput.index('NBCOPIES')+1])
        # Check whether it is consistent with what is expected
        if isTape:
            assert nbCopies > 0, \
                name + ' should have a tape fileclass, but has fileclass ' + \
                fileClass + ' that has nbCopies = 0'
        else:
            assert nbCopies == 0, \
                name + ' should have a no tape fileclass, but has fileclass ' + \
                fileClass + ' that has nbCopies = ' + str(nbCopies)
    except ValueError:
        assert False, "Invalid nbCopies got for fileclass " + fileClass + \
            " when checking path " + name
Setup._checkDirPath = _checkDirPath

def _createCastorDir(self, path, filetype):
    # create the path in the namespace
    output = Popen('nsmkdir ' + path)
    # check it went fine
    assert len(output) == 0 or output.find('File exists'), \
        'Failed to create working directory ' + path + os.linesep + "Error :" + os.linesep + output
    # set ACL to enable the unprivileged user to write on this path
    output = Popen('nssetacl -m u:' + self.options.get('Tags', 'unprivUid') + ':rwx,m:rwx ' + path) + \
        Popen('nssetacl -m d:u:' + self.options.get('Tags', 'unprivUid') + ':rwx,d:u::rwx,d:g::rx,d:o:rx,d:m:rwx ' + path)
    assert len(output) == 0, \
        'Failed to set ACLs for user ' + self.options.get('Tags', 'unprivUid') + \
        ' on working directory ' + path + os.linesep + "Error :" + os.linesep + output
    # print directory name 
    print os.linesep+"Working in directory " + path + " for " + filetype + " files"
    # return the created dir
    return path
Setup._createCastorDir = _createCastorDir

def noTapePath(self):
    # get the noTape path and check it
    notapepath = self.options.get('Generic','CastorNoTapeDir')
    self._checkDirPath(notapepath, False)
    # create a unique directory
    return self._createCastorDir(notapepath + os.sep + self.getTag(None, 'sessionuuid'), 'non tape')
Setup.getTag_noTapePath = noTapePath

def noTapeFileName(self, nb=0):
    return (lambda test : self.getTag(test, 'noTapePath')+os.sep+test+str(nb))
Setup.getTag_noTapeFileName = noTapeFileName

def tapePath(self):
    # get the tape path and check it
    tapepath = self.options.get('Generic','CastorTapeDir')
    self._checkDirPath(tapepath, True)
    # create a unique directory
    return self._createCastorDir(tapepath + os.sep + self.getTag(None, 'sessionuuid'), 'tape')
Setup.getTag_tapePath = tapePath

def tapeFileName(self, nb=0):
    return (lambda test : self.getTag(test, 'tapePath')+os.sep+test+str(nb))
Setup.getTag_tapeFileName = tapeFileName

def nsCleanup(self):
    # get rid of all files used in castor by droping the namespace temporary directories
    for entry in ['noTapePath', 'tapePath']:
        if self.tags.has_key(entry):
            path = self.tags[entry]
            output = Popen('nsrm -r ' + path)
            assert len(output) == 0, \
                'Failed to cleanup working directory ' + path + \
                '. You may have to do manual cleanup' + os.linesep + \
                "Error :" + os.linesep + output
            del self.tags[entry]
Setup.cleanup_nameserver = nsCleanup

##################################
###  svcClass related methods  ###
##################################

def _checkServiceClass(self, name, expectedStatus):
    # write a small file
    os.environ['STAGE_SVCCLASS'] = name
    cmd = 'rfcp ' + self.getTag('IsTapeSvcClass-'+name, 'localFileName') + ' ' + self.getTag('IsTapeSvcClass-'+name, 'tapeFileName')
    print 'Executing ' + cmd
    output = Popen(cmd)
    del os.environ['STAGE_SVCCLASS']
    assert output.strip().endswith('bytes in remote file'), 'Could not check service class, transfer failed :\n' + output
    # and check it's status
    cmd = 'stager_qry -S ' + name + ' -M ' + self.getTag('IsTapeSvcClass-'+name, 'tapeFileName')
    print 'Executing ' + cmd
    output = Popen(cmd)
    assert output.strip().endswith(expectedStatus), 'Not properly configured'
Setup._checkServiceClass = _checkServiceClass

def tapeServiceClassList(self):
    # get list from config file
    l = map(lambda s: s.strip(), self.options.get('Generic','TapeServiceClasses').split(','))
    # check that they are tape enabled
    for sc in l:
        print os.linesep+'Checking Service Class ' + sc + ', should be tape enabled'
        self._checkServiceClass(sc, 'CANBEMIGR')
        print os.linesep+'Service Class ' + sc + ' is ok'
        return l
Setup.getTag_tapeServiceClassList = tapeServiceClassList

def diskOnlyServiceClassList(self):
    # get list from config file
    l = map(lambda s: s.strip(), self.options.get('Generic','DiskOnlyServiceClasses').split(','))
    # check that they are tape enabled
    for sc in l:
        print os.linesep+'Checking Service Class ' + sc + ', should be tape disk only'
        self._checkServiceClass(sc, 'STAGED')
        print os.linesep+'Service Class ' + sc + ' is ok'
        return l
Setup.getTag_diskOnlyServiceClassList = diskOnlyServiceClassList

def _getAndCheckSvcClassTag(self, test, tagName, nb, msg):
    l = self.getTag(test, tagName)
    assert nb < len(l), 'Not enough ' + msg + ' service classes declared in option files. Need ' + str(nb+1) + ', got ' + repr(l)
    return l[nb]
Setup._getAndCheckSvcClassTag = _getAndCheckSvcClassTag

def tapeServiceClass(self, nb=0):
    return lambda test : self._getAndCheckSvcClassTag(test, 'tapeServiceClassList', nb, 'tape')
Setup.getTag_tapeServiceClass = tapeServiceClass
  
def diskOnlyServiceClass(self, nb=0):
    return lambda test : self._getAndCheckSvcClassTag(test, 'diskOnlyServiceClassList', nb, 'disk only')
Setup.getTag_diskOnlyServiceClass = diskOnlyServiceClass

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
