def remoteDir(self):
    # create a unique directory name
    p = self.options.get('Generic','remoteSpace') + os.sep + getUUID()
        # create the directory
    output = Popen('rfmkdir ' + p)
    assert len(output) == 0, \
        'Failed to create remote directory ' + p + os.linesep + "Error :" + os.linesep + output
    print os.linesep+"Working in directory " + p + " for remote files"
    return p
Setup.getTag_remoteDir = remoteDir

def remoteFileName(self, nb=0):
    return (lambda test : self.getTag(test, 'remoteDir') + os.sep + test + '.' + str(nb))
Setup.getTag_remoteFileName = remoteFileName

def remoteCleanup(self):
    # get rid of remote files created
    if self.tags.has_key('remoteDir'):
        path = self.tags['remoteDir']
        output = Popen('yes | rfrm -r ' + path)
        # note that there is no way to know whether it worked...
        del self.tags['remoteDir']
Setup.cleanup_remote = remoteCleanup
