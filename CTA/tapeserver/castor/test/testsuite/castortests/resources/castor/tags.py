########################
### stageHost & tags ###
########################
def stageHost(self):
    return self.options.get('Environment','STAGE_HOST')
Setup.getTag_stageHost = stageHost

def castorTag(self, nb=0):
    return (lambda test : 'castorTag'+self.getTag(test, 'sessionuuid')+test+str(nb))
Setup.getTag_castorTag = castorTag

Setup._tapeFileClass = self.options.get('Generic','TapeFileClass')
Setup._noTapeFileClass = self.options.get('Generic','DiskOnlyFileClass')

# declare new random order tags
self.declareRandomOrderTag('noTapeFileName')
self.declareRandomOrderTag('tapeFileName')
self.declareRandomOrderTag('fileid')

##################################
### nameserver related methods ###
##################################
def _setFileClass(self, path, fileclass):
    output = Popen('nschclass ' + fileclass + ' ' + path)
    assert len(output) == 0, \
           'Failed to set FileClass ' + fileclass + \
           ' on directory ' + path + os.linesep + "Error :" + os.linesep + output
Setup._setFileClass = _setFileClass

def _addACLforUnprivUser(self, path):
    # set ACL to enable the unprivileged user to write on this path
    output = Popen('nssetacl -m u:' + \
                   self.options.get('Tags', 'unprivUid') + \
                   ':rwx,m:rwx ' + \
                   path) + \
             Popen('nssetacl -m d:u:' + \
                   self.options.get('Tags', 'unprivUid') + \
                   ':rwx,d:u::rwx,d:g::rx,d:o:rx,d:m:rwx ' + \
                   path)
    assert len(output) == 0, \
           'Failed to set ACLs for user ' + self.options.get('Tags', 'unprivUid') + \
           ' on working directory ' + path + os.linesep + "Error :" + os.linesep + output
Setup._addACLforUnprivUser = _addACLforUnprivUser

def _configuredPath(self, test, fileclass):
    # create the directory and configure it
    path, created = self._createDir(self.getTag(test, '_testSessionPath') + os.sep + test)
    if created:
        self._setFileClass(path, fileclass)
        self._addACLforUnprivUser(path)
    return path
Setup._configuredPath = _configuredPath

def noTapeFileName(self, nb=0):
    fileclass = self._noTapeFileClass
    return (lambda test : self._configuredPath(test, fileclass)+os.sep+test+str(nb))
Setup.getTag_noTapeFileName = noTapeFileName

def tapeFileName(self, nb=0):
    fileclass = self._tapeFileClass
    return (lambda test : self._configuredPath(test, fileclass)+os.sep+test+str(nb))
Setup.getTag_tapeFileName = tapeFileName



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
        print os.linesep+'Checking Service Class ' + sc + ', should be disk only'
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
