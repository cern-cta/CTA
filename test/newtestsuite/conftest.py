#
# Put a nice header here
#
# this is the core of the CASTOR test suite, based on py.test
#

import time, ConfigParser, os, py, tempfile, re, types, datetime, signal

################
# some configs #
################

# regexps of parts of the output that should be dropped
# Note that regexps must have exactly one group matching the part to
# be dropped
suppressRegExps = [
    'seconds through (?:\w+\s+\(\w+\)) and (?:\w+\s+\(\w+\))(\s\(\d+\s\w+/sec\))' # drop rfcp speed info as it varies too much
]

# list of tags that may appear several times in a single output
# the the different values in random order e.g. file name list
randomOrderTags = [ 'noTapeFileName', 'tapeFileName', 'fileid@ns' ]

#########################
# Some useful functions #
#########################

UUID = None
def getUUID():
    global UUID
    if UUID == None :
        newTime=time.gmtime()
        UUID = str(((newTime[7]*24+newTime[3])*60 + newTime[4])*60 +newTime[5])
    return UUID

def listTests(testDir, listResources=False):
    '''lists all tests in a given tree and spit out the fileNames'''
    # find out the complete list of test cases
    for dirpath, dirs, files in os.walk(testDir):
        # do not enter the 'resources' path
        if not listResources and dirpath.startswith('castortests'+os.sep+'resources'): continue
        # In each directory, only use the '.input' files
        for f in filter(lambda x: x.endswith('.input'),files):
            # extract file name, test set and test case name
            f = f[:-6]
            fn = os.path.join(dirpath, f)
            tn = fn.replace('castortests'+os.sep,'')
            ts = tn[0:tn.find(os.sep)]
            tn = tn.replace(os.sep,'_')
            yield ts, tn, fn

# handling of subprocesses, depending on the python version
try:
    import subprocess
    hasSubProcessModule = True
except Exception:
    hasSubProcessModule = False

class Timeout(Exception):
    None
    
def Popen(cmd, timeout=600):
    if hasSubProcessModule:
        start = datetime.datetime.now()
        process = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while process.poll() is None:
            time.sleep(0.1)
            now = datetime.datetime.now()
            if (now - start).seconds > timeout:
                os.kill(process.pid, signal.SIGKILL)
                os.waitpid(-1, os.WNOHANG)
                raise Timeout()
        return process.stdout.read()
    else:
        return os.popen4(cmd)[1].read()

#################################
# command line options handling #
#################################

mainTestDirs = [] 

def handleAllOption(option, opt, value, parser):
    for d in mainTestDirs:
        setattr(parser.values,d,True)
    
def pytest_addoption(parser):
    for d in filter(lambda f:os.path.isdir(os.path.join('castortests',f)), os.listdir('castortests')):
        # 'resources' is not really a test suite directory
        if d == 'resources' or d[0] == '.': continue
        # create 2 options to set and unset each test suite. By default, nothing is ran
        parser.addoption("--"+d,   action="store_true",  default=False, dest=d, help='Run the '+d+' tests')
        parser.addoption("--no"+d, action="store_false", default=False, dest=d, help='Do not run the '+d+' tests')
        mainTestDirs.append(d)
    # add a set of other options
    parser.addoption("-A", "--all",      action="callback",   callback=handleAllOption,       help='Forces all tests to run')
    parser.addoption("--nocleanup",action="store_true", default=False, dest='noCleanup',help='Prevents the cleanup of temporary files created by the test suite to take place (both in namespace and local disk)')
    parser.addoption("--failnores",action="store_true", default=False, dest='failNoRes',help='Forces to fail when a resource is not available, instead of skipping the tests')
    #parser.addoption("--fast",action="callback",   callback=handleFastOption, help='Selects test suitable to check an instance after an upgrade, aka fast testsuite')
    parser.addoption("-C", "--configfile", action="store", default='CastorTestSuite.options', dest='configFile', help='Name of the config file to be used. Defaults to CastorTestSuite.options')


########################
# option file handling #
########################

class CastorConfigParser(ConfigParser.ConfigParser):
    # predefine defaults for some options
    def __init__(self):
        ConfigParser.ConfigParser.__init__(self)
        self.add_section('Generic')
        self.set('Generic','NoCleanup','false')
        self.set('Generic','SkipTestsWhenResourceMissing','false')
        
    # overwrite the optionxform to make options case sensitive
    def optionxform(self,option):
        return option


###############################################
# Methods to parse input and outputs of tests #
###############################################

def buildErrorMsg(setup, text, output, gold, outputStart, outputEnd, goldStart, goldEnd):
    if (setup.config.option.tbstyle == "no") : return ''
    # extra line and line nb of problem in the output
    outputLineNb = output.count(os.linesep,0,outputStart) + 1
    outputLineStart = output.rfind(os.linesep,0,outputStart)+len(os.linesep)
    if -1 == outputLineStart : outputLineStart = 0
    outputLineEnd = output.find(os.linesep,outputStart)
    if -1 == outputLineEnd : outputLineEnd = None
    if outputEnd > outputLineEnd : outputEnd = outputLineEnd
    if outputEnd == None: outputEnd = len(output)
    # extra line and line nb of the matching section in the reference
    goldlLineNb = gold.count(os.linesep,0,goldStart) + 1
    goldLineStart = gold.rfind(os.linesep,0,goldStart)+len(os.linesep)
    if -1 == goldLineStart : goldLineStart = 0
    goldLineEnd = gold.find(os.linesep,goldStart)
    if -1 == goldLineEnd : goldLineEnd = None
    if goldEnd > goldLineEnd : goldEnd = goldLineEnd
    if goldEnd == None: goldEnd = len(output)
    r = text + ' on line ' + str(outputLineNb) + ' of output (line ' + \
        str(goldlLineNb) + ' of the reference)' + os.linesep + \
        'output :' + os.linesep + \
        output[outputLineStart:outputLineEnd] + os.linesep + \
        ' '*(outputStart-outputLineStart) + '^'*(outputEnd-outputStart) + os.linesep + \
        'reference :' + os.linesep + \
        gold[goldLineStart:goldLineEnd] + os.linesep + \
        ' '*(goldStart-goldLineStart) + '^'*(goldEnd-goldStart)
    if (setup.config.option.tbstyle == "long") :
        r = r + os.linesep + "Full output :" + os.linesep + output + \
            os.linesep + "Reference output :" + os.linesep + gold
    return r

def buildTagErrorMsg(setup, text, output, tagValue, outputStart, outputEnd, gold, tagPos = None):
    if (setup.config.option.tbstyle == "no") : return ''
    # extra line and line nb of problem in the output
    outputLineNb = output.count(os.linesep,0,outputStart) + 1
    outputLineStart = output.rfind(os.linesep,0,outputStart)+len(os.linesep)
    if -1 == outputLineStart : outputLineStart = 0
    outputLineEnd = output.find(os.linesep,outputStart)
    if -1 == outputLineEnd : outputLineEnd = None
    if outputEnd > outputLineEnd : outputEnd = outputLineEnd
    if outputEnd == None: outputEnd = len(output)
    r = text + ' on line ' + str(outputLineNb) + ' of output' + os.linesep + \
        'output :' + os.linesep + \
        output[outputLineStart:outputLineEnd] + os.linesep + \
        ' '*(outputStart-outputLineStart) + '^'*(outputEnd-outputStart) + os.linesep + \
        'expected value : ' + tagValue
    if tagPos != None :
        posLineNb = output.count(os.linesep,0,tagPos[0]) + 1
        posLineStart = output.rfind(os.linesep,0,tagPos[0])+len(os.linesep)
        if -1 == posLineStart : posLineStart = 0
        posLineEnd = output.find(os.linesep,posLineStart)
        if -1 == posLineEnd : posLineEnd = None
        r = r +  os.linesep + 'Found line ' + str(posLineNb) + ' :' + os.linesep + \
            output[posLineStart:posLineEnd] + os.linesep + \
            ' '*(tagPos[0]-posLineStart) + '^'*(tagPos[1]-tagPos[0])
    if (setup.config.option.tbstyle == "long") :
        r = r + os.linesep + "Full output :" + os.linesep + output + \
            os.linesep + "Reference output :" + os.linesep + gold
    return r

def parseAndReplace(setup, text, testName):
    ''' parse a text and replaces the tags in between '<' and '>' setup and testName'''
    parsedText = ''
    currIndex = 0
    while 1:
        # check if there is any tag to process
        tagIndex = text.find('<', currIndex)
        if tagIndex == -1: break
        # else add standard text before the tag to the parsed one
        parsedText = parsedText + text[currIndex:tagIndex]
        # find out the tag and add its value to the parsed text
        endTag = text.find('>', tagIndex+1)
        tag = text[tagIndex+1:endTag]
        tagValue = setup.getTag(testName, tag)
        # did we get a sequence of values ?
        if getattr(tagValue,'__iter__',False):
            # Yes, we have an iterable, let's spit out
            # all possible replacements by calling recursively
            # this method after we replaced that tag globally
            for v in tagValue:
                newtext = parsedText+v+text[endTag+1:].replace('<'+tag+'>',v)
                for r in parseAndReplace(setup, newtext, testName):
                    yield r
            return
        else:
            # regular value, we simply use the value
            parsedText = parsedText + tagValue
        # and loop
        currIndex = endTag + 1
    yield parsedText + text[currIndex:]

def _handleDrop(match):
    needle = match.group(0)
    return needle[:match.start(1)-match.start(0)]+needle[match.end(1)-match.start(0):]
    
def compareOutput(setup, output, gold, testName):
    # drop unwanted stuff from the output
    for t in suppressRegExps:
        output = re.sub(t, _handleDrop, output)
    # then compare
    outIndex = 0
    goldIndex = 0
    localTags = {}
    randomOrderNeeded = {}
    randomOrderFound = {}
    while 1:
        # check if there is any tag to process in gold
        tagIndex = gold.find('<', goldIndex)
        if tagIndex == -1:
            assert output[outIndex:] == gold[goldIndex:], \
                   buildErrorMsg(setup, "Mismatch in output", output, gold, outIndex, len(output), goldIndex, len(gold))
            break
        # else compare texts before the tag
        textLength = tagIndex - goldIndex
        assert output[outIndex:outIndex+textLength] == gold[goldIndex:tagIndex],\
               buildErrorMsg(setup, "Mismatch in output", output, gold, outIndex, outIndex+textLength, goldIndex, tagIndex)
        outIndex = outIndex+textLength
        goldIndex = tagIndex
        # parse the tag and move forward the gold index
        endTag = gold.find('>', tagIndex+1)
        tag = gold[tagIndex+1:endTag]
        goldIndex = endTag+1
        # Special case for 'IgnoreRestOfOutput' tag
        if tag == 'IgnoreRestOfOutput': break
        # find the tag value in the output
        nextTagIndex = gold.find('<', goldIndex)
        if nextTagIndex == -1: nextTagIndex = len(gold)
        nextGoldText = gold[goldIndex:nextTagIndex]
        endTagValue = output.find(nextGoldText,outIndex)
        assert endTagValue != -1, \
               buildErrorMsg(setup, "Mismatch in output after tag '" + tag + "'", \
                             output, gold, outIndex, len(output), goldIndex-len(tag)-1, len(gold))
        tagValue = output[outIndex:endTagValue]
        # check the tag value, especially when we know the expected value, or register it
        if setup.hasTag(tag):
            # predefined value for the tag
            tagRefValue = setup.getTag(testName, tag)
            # try to see whether the tag is one of the random order type
            matchObject = reduce(lambda x,y : x == None and y or x,
                                 map(lambda x : re.match('('+x+')\d*', tag),
                                     randomOrderTags))
            if matchObject:
                # special case for tags that may be reordered...
                baseTag = matchObject.group(1)
                if not randomOrderNeeded.has_key(baseTag): randomOrderNeeded[baseTag] = []
                if not randomOrderFound.has_key(baseTag): randomOrderFound[baseTag] = []
                randomOrderNeeded[baseTag].append(tagRefValue)
                randomOrderFound[baseTag].append(tagValue)
            else:
                # the tag is predefined, check its value in the output
                assert tagRefValue == tagValue, \
                       buildTagErrorMsg(setup, "Wrong value found for '" + tag + "'", \
                                        output, tagRefValue, outIndex, outIndex+len(tagValue), gold)
        elif localTags.has_key(tag):
            # this tag has already been seen, check that its value did not change
            tagRefValue = localTags[tag][0]
            assert tagRefValue == tagValue, \
                   buildTagErrorMsg(setup, "Tag '" + tag + "' has changed value", \
                                    output, tagRefValue, outIndex, outIndex+len(tagValue), gold, \
                                    (localTags[tag][1], localTags[tag][2]))
        else:
            # unknown tag, found out its value and register it
            # tags starting with 'variable' are not registered
            if not tag.startswith('variable'):
                localTags[tag] = (tagValue, outIndex, endTagValue)
        outIndex = endTagValue
    # in case reordering of filenames was involved, check that the list is ok
    for key in randomOrderNeeded.keys():
        neededValues = randomOrderNeeded[key]
        neededValues.sort()
        foundValues = randomOrderFound[key]
        foundValues.sort()
        assert(neededValues == foundValues, "Wrong set of values found in output for tag " + key)


########################################
# setup object to handle test settings #
########################################

class Setup:
    def __init__(self, request):
        # export last used setup as a global variable. Used to avoid nasty tracebacks in case of keyboard interupts
        global gsetup
        gsetup = self
        self.config = request.config
        # handling of tags. Note values can also be functions generating values depending on the test name
        self.tags = {}
        # whether a resource is ok or not. Contains True if ok, false if not, and no entry if not checked
        self.checkedResources = {}
        # instantiate an option parser with some defaults
        self.options = CastorConfigParser()
        # read the option file
        assert os.path.isfile(self.config.option.configFile), \
               'Could not find config file : ' + self.config.option.configFile
        self.options.read(self.config.option.configFile)
        # setup the environment
        for v in ['STAGE_SVCCLASS', 'STAGE_HOST', 'STAGE_PORT', 'STAGER_TRACE']:
            if os.environ.has_key(v): del os.environ[v]
        if self.options.has_section('Environment'):
            print os.linesep+"setting environment :"
            for o in self.options.items('Environment'):
                print "  " + o[0] + ' = ' + o[1]
                os.environ[o[0].upper()] = o[1]

    def noCleanup(self) :
        if self.config.option.noCleanup:
            return True
        else :
            return self.options.getboolean('Generic','NoCleanup')
    
    def cleanupAndReset(self):
        # get rid of all files used in castor by droping the namespace temporary directories
        for entry in ['noTapePath', 'tapePath']:
            if self.tags.has_key(entry) and not self.noCleanup():
                path = self.tags[entry]
                output = Popen('nsrm -r ' + path)
                assert len(output) == 0, \
                       'Failed to cleanup working directory ' + path + \
                       '. You may have to do manual cleanup' + os.linesep + \
                       "Error :" + os.linesep + output
                del self.tags[entry]
        # also get rid of local files created
        if self.tags.has_key('tmpLocalDir') and not self.noCleanup():
            # recursive deletion of all files and directories
            for root, dirs, files in os.walk(self.tags['tmpLocalDir'], topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            del self.tags['tmpLocalDir']
        # and get rid of remote files created
        if self.tags.has_key('remoteDir') and not self.noCleanup():
            path = self.tags['remoteDir']
            output = Popen('yes | rfrm -r ' + path)
            # note that there is no way to know whether it worked...
            del self.tags['remoteDir']

    def getTag(self, testName, tag):
        if self.tags.has_key(tag):
            v = self.tags[tag]
            if callable(v):
                return v(testName)
            else:
                return v
        elif self.options.has_option('Tags',tag):
            return self.options.get('Tags',tag)
        else:
            # try to build this tag by calling the appropriate function
            methodName = 'getTag_' + tag.replace('-','_')
            if hasattr(self, methodName):
                v = getattr(self, methodName)()
                self.tags[tag] = v
                # loop so that we check for callability
                return self.getTag(testName,tag)
            elif tag[-1:].isdigit():
                # in case of numbered tags, call common function, with number as parameter
                match = re.search('\d+$',tag)
                methodName = 'getTag_' + tag[:match.start()]
                if hasattr(self, methodName):
                    v = getattr(self, methodName)(int(match.group()))
                    self.tags[tag] = v
                    # loop so that we check for callability
                    return self.getTag(testName,tag)
                else:
                    raise NameError("Could not find value for tag '" + tag + "'")
            else:
                raise NameError("Could not find value for tag '" + tag + "'")

    def hasTag(self, tag):
        return self.tags.has_key(tag) or self.options.has_option('Tags',tag)

    def checkResource(self, name, skip=True):
        # if first time, really do the check
        if not self.checkedResources.has_key(name):
            print os.linesep+'Checking resource ' + name
            try:
                # find the resource
                path = os.sep.join(['castortests','resources',name])
                if not os.path.isdir(path):
                    raise NameError('Could not find resource ' + name)
                # run all tests
                for ts, tn, fn in listTests(path, listResources=True):
                    self.runTest(ts,tn,fn)
                # resource available
                self.checkedResources[name] = True
            except Exception, e:
                if skip:
                    # do not complain, just skip tests
                    print "Failed to get resource " + name + ", error was : '" + str(e) + "'"
                    self.checkedResources[name] = False
                else:
                    # complain :-)
                    raise e
        # skip the test case is the resource is not available
        if self.checkedResources[name] != True:
            py.test.skip("skipped as resource " + name + " is missing")

    def checkResources(self, path, skip=True, noRec=False):
        # distinguish files and directories
        if os.path.isfile(path):
            try:
                # we have a file, let's run all tests
                for r in filter(lambda x:len(x.strip())>0, open(path).read().split(os.linesep)):
                    self.checkResource(r, skip)
            except IOError:
                # non existing file, i.e. nothing needed, it's all ok
                pass
        elif os.path.isdir(path):
            # check for tags.py file and absorb new definitions
            if os.path.isfile(path+os.sep+'tags.py'):
                execfile(path+os.sep+'tags.py')
            # check all resources in default.resources file
            self.checkResources(path+os.sep+'default.resources',skip, noRec=True)
        # recursively check parent, if not top level, nor resources
        if not noRec and path != os.sep.join(['castortests','resources']):
            parent = os.path.dirname(path)
            if len(parent) != 0:
                self.checkResources(parent,skip)

    def runTest(self, testSet, testName, fileName):
        # check whether the test should be skipped or not
        if testSet != 'resources' and not getattr(self.config.option,testSet): 
            py.test.skip(testSet + " tests are skipped. Use --(no)" + testSet + " or --all to change this")
        # check resources, including all defaults
        self.checkResources(fileName+'.resources',
                            skip=(self.options.getboolean('Generic','SkipTestsWhenResourceMissing') and
                                  not self.config.option.failNoRes))
        # get the list of commands to run
        # Note that we can have several sets in case some tags can
        # get different values. We will run each set as a separate test
        for cmds in parseAndReplace(self, open(fileName+'.input').read(), testName):
            # go through them
            print
            i = 0
            for cmd in cmds.split(os.linesep):
                # skip empty lines in the input file
                if len(cmd) > 0:
                    print "executing ", cmd
                    try:
                        # run the command
                        output = Popen(cmd)
                        # and check its output
                        compareOutput(self, output, open(fileName+'.output'+str(i)).read(), testName)
                    except Timeout:
                        assert False, "Time out"
                    i = i + 1

    ###########################
    ### tag related methods ###
    ###########################

    def getTag_noTapeFileName(self, nb=0):
        return (lambda test : self.getTag(test, 'noTapePath')+os.sep+test+str(nb))
             
    def getTag_tapeFileName(self, nb=0):
        return (lambda test : self.getTag(test, 'tapePath')+os.sep+test+str(nb))
             
    def getTag_noTapePath(self):
        # create a unique directory name
        p = self.options.get('Generic','CastorNoTapeDir') + os.sep + getUUID()
        # create the directory
        output = Popen('nsmkdir ' + p)
        assert len(output) == 0 or output.find('File exists') > -1, \
               'Failed to create working directory ' + p + os.linesep + "Error :" + os.linesep + output
        print os.linesep+"Working in directory " + p + " for non tape files"
        return p

    def getTag_tapePath(self):
        # create a unique directory name
        p = self.options.get('Generic','CastorTapeDir') + os.sep + getUUID()
        # create the directory
        output = Popen('nsmkdir ' + p)
        assert len(output) == 0 or output.find('File exists'), \
               'Failed to create working directory ' + p + os.linesep + "Error :" + os.linesep + output
        print os.linesep+"Working in directory " + p + " for tape files"
        return p

    def _getAndCheckSvcClassTag(self, test, tagName, nb, msg):
        l = self.getTag(test, tagName)
        assert nb < len(l), 'Not enough ' + msg + ' service classes declared in option files. Need ' + str(nb+1) + ', got ' + repr(l)
        return l[nb]

    def getTag_tapeServiceClass(self, nb=0):
        return lambda test : self._getAndCheckSvcClassTag(test, 'tapeServiceClassList', nb, 'tape')
   
    def getTag_diskOnlyServiceClass(self, nb=0):
        return lambda test : self._getAndCheckSvcClassTag(test, 'diskOnlyServiceClassList', nb, 'disk only')

    def _checkServiceClass(self, name, expectedStatus):
        # write a small file
        os.environ['STAGE_SVCCLASS'] = name
        cmd = 'rfcp ' + self.getTag('IsTapeSvcClass-'+name, 'localFileName') + ' ' + self.getTag('IsTapeSvcClass-'+name, 'tapeFileName')
        print 'executing ' + cmd
        output = Popen(cmd)
        del os.environ['STAGE_SVCCLASS']
        # and check it's status
        cmd = 'stager_qry -S ' + name + ' -M ' + self.getTag('IsTapeSvcClass-'+name, 'tapeFileName')
        print 'executing ' + cmd
        output = Popen(cmd)
        assert(output.strip().endswith(expectedStatus))

    def getTag_tapeServiceClassList(self):
        # get list from config file
        l = map(lambda s: s.strip(), self.options.get('Generic','TapeServiceClasses').split(','))
        # check that they are tape enabled
        for sc in l:
            print os.linesep+'testing Service Class ' + sc
            try:
                self._checkServiceClass(sc, 'CANBEMIGR')
                print os.linesep+'Service Class ' + sc + ' is ok'
            except AssertionError:
                raise AssertionError('Service class "' + sc + '" is supposed to be tape enabled but does not seem to be')
        return l

    def getTag_diskOnlyServiceClassList(self):
        # get list from config file
        l = map(lambda s: s.strip(), self.options.get('Generic','DiskOnlyServiceClasses').split(','))
        # check that they are tape enabled
        for sc in l:
            try:
                self._checkServiceClass(sc, 'STAGED')
                print os.linesep+'Service Class ' + sc + ' is ok'
            except AssertionError:
                raise AssertionError('Service class "' + sc + '" is supposed to be disk only but does not seem to be')
        return l

    def getTag_tmpLocalDir(self):
        d = tempfile.mkdtemp(prefix='CastorTestSuite')
        print os.linesep+'Temporary files will be stored in directory ' + d
        return d

    def getTag_remoteDir(self):
        # create a unique directory name
        p = self.options.get('Generic','remoteSpace') + os.sep + getUUID()
        # create the directory
        output = Popen('rfmkdir ' + p)
        assert len(output) == 0, \
               'Failed to create remote directory ' + p + os.linesep + "Error :" + os.linesep + output
        print os.linesep+"Working in directory " + p + " for remote files"
        return p

    def getTag_tmpLocalFileName(self, nb=0):
        return (lambda test : self.getTag(test, 'tmpLocalDir') + os.sep + test + '.' + str(nb))

    def getTag_remoteFileName(self, nb=0):
        return (lambda test : self.getTag(test, 'remoteDir') + os.sep + test + '.' + str(nb))

    def getTag_castorTag(self, nb=0):
        return (lambda test : 'castorTag'+getUUID()+test+str(nb))

###############################################################
# funcarg function used by all tests for their setup argument #
###############################################################

def pytest_funcarg__setup(request):
    # the setup is cached per session
    try:
        return request.cached_setup(setup=lambda:Setup(request), \
                                    teardown=lambda val: val.cleanupAndReset(), \
                                    scope="session")
    except AssertionError,e:
        print 'Unable to create Setup\nError was : ' + str(e)
        # Let's not go through other tests, they will all fail the same way !
        request.config.option.exitfirst = True
        raise e


#################################################
# avoid being too verbose on keyboard interupts #
#################################################

def pytest_keyboard_interrupt(excinfo):
    gsetup.config.option.verbose = False
