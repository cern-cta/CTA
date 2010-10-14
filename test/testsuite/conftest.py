#
# this is the core of the test suite infrastructure, based on py.test
#

import time, ConfigParser, os, sys, py, tempfile, re, types, datetime, signal

#########################
# Some useful functions #
#########################

def listTests(testdir, topdir, listResources=False):
    '''lists all tests in a given tree and spit out the fileNames'''
    # find out the complete list of test cases
    for dirpath, dirs, files in os.walk(testdir):
        # do not enter the 'resources' path
        if not listResources and dirpath.startswith(os.sep.join([topdir,'resources'])): continue
        # In each directory, only use the '.input' files
        for f in filter(lambda x: x.endswith('.input'),files):
            # extract file name, test set and test case name
            f = f[:-6]
            fn = os.path.join(dirpath, f)
            tn = fn.replace(topdir+os.sep,'')
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
    # list the test directories
    candidates = filter(lambda x:x.endswith('tests'), os.listdir('.'))
    # build a list of options to be declared. options gives the group of each option
    options = {}
    for testdir in candidates:
        for d in filter(lambda f:os.path.isdir(os.path.join(testdir,f)), os.listdir(testdir)):
            # 'resources' is not really a test suite directory
            if d == 'resources' or d[0] == '.': continue
            # add option to the list, or modify it
            if d in options:
                options[d] = options[d] + ', ' + testdir
            else:
                options[d] = testdir
    # add test related options
    for d in options:
        group = parser.getgroup(options[d], "Specific options for " + options[d])
        group.addoption("--"+d,   action="store_true",  dest=d, help='Run the '+d+' tests')
        group.addoption("--no"+d, action="store_false", dest=d, help='Do not run the '+d+' tests')
        mainTestDirs.append(d)
    # add a set of other options
    parser.addoption("-A", "--all", action="callback", callback=handleAllOption, help='Forces all tests to run')
    parser.addoption("--nocleanup",action="store_true", default=False, dest='noCleanup',help='Do not cleanup temporary files created by the test suite')
    parser.addoption("--failnores",action="store_true", dest='failNoRes',help='Forces to fail when a resource is not available, instead of skipping the tests')
    parser.addoption("--skipnores",action="store_false", dest='failNoRes',help='Forces to skip when a resource is not available, instead of failing the tests')
    parser.addoption("-C", "--configfile", action="store", dest='configFile', help='Name of the config file to be used. Defaults to any .options files in the test directory')
    parser.addoption("-T", "--testdir", action="store", dest='testdir', help='Test directory to use. Defaults to any local directory whose name ends with "tests"')


########################
# option file handling #
########################

class CustomConfigParser(ConfigParser.ConfigParser):
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
    ''' parse a text and replaces the tags in between '<' and '>' with knows tags'''
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
        try:
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
        except NameError:
            # keep tag, supposing it will be a local time, defined at run time
            parsedText = parsedText + '<' + tag + '>'
        # and loop
        currIndex = endTag + 1
    yield parsedText + text[currIndex:]

def parseAndReplaceLocals(text, localTags):
    ''' parse a text and replaces the tags in between '<' and '>' with contextual local tags'''
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
        parsedText = parsedText + localTags[tag][0]
        # and loop
        currIndex = endTag + 1
    return parsedText + text[currIndex:]

def _handleDrop(match):
    needle = match.group(0)
    return needle[:match.start(1)-match.start(0)]+needle[match.end(1)-match.start(0):]
    
def compareOutput(setup, output, gold, testName, localTags):
    # drop unwanted stuff from the output
    for t in setup.suppressRegExps:
        output = re.sub(t, _handleDrop, output)
    # then compare
    outIndex = 0
    goldIndex = 0
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
                                     setup.randomOrderTags))
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
    def __init__(self, config):
        self.config = config
        # handling of tags. Note values can also be functions generating values depending on the test name
        self.tags = {}
        # list of tags that may appear several times in a single output
        # with the different values in random order e.g. file name list
        self.randomOrderTags = []
        # regexps of parts of the output that should be dropped
        # Note that regexps must have exactly one group matching the part to be dropped
        self.suppressRegExps = []
        # whether a resource is ok or not. Contains True if ok, false if not, and no entry if not checked
        self.checkedResources = {}
        # list of already processed tags.py files (actually of their directories)
        self.processedTagspy = []
        # instantiate an option parser with some defaults
        self.options = CustomConfigParser()
        # find out the test directory to use
        self.testdir = getattr(self.config.option,'testdir')
        if self.testdir == None:
            # try to guess
            testDirCandidates = filter(lambda x:x.endswith('tests'), os.listdir('.'))
            assert len(testDirCandidates) > 0, "No test directory found. Please use --testdir option"
            assert len(testDirCandidates) < 2, "Do not know which test directory to use. Please use --testdir option\nCandidates are " + ', '.join(testDirCandidates)
            self.testdir = testDirCandidates[0]
        # read the config file.
        configFile = getattr(self.config.option, 'configFile')
        if configFile == None:
            # try to guess
            configFileCandidates = filter(lambda x:x.endswith('.options'), os.listdir(self.testdir))
            assert len(configFileCandidates) > 0, "No .options file found in " + self.testdir
            assert len(configFileCandidates) < 2, "Do not know which .options file to use in " + self.testdir + " : " + ', '.join(configFileCandidates)
            configFile = os.sep.join([self.testdir, configFileCandidates[0]])
        else:
            # check it's a valid file
            assert os.path.isfile(configFile), 'Could not find config file : ' + configFile
        # read option file
        self.options.read(configFile)
        # setup the environment
        if self.options.has_section('Environment'):
            print os.linesep+"Setting environment :"
            for o in self.options.items('Environment'):
                print "  " + o[0] + ' = ' + o[1]
                os.environ[o[0].upper()] = o[1]
    
    def cleanupAndReset(self):
        if self.config.option.noCleanup or self.options.getboolean('Generic','NoCleanup'): return
        for f in filter(lambda x:x.startswith("cleanup_"),dir(self)):
            getattr(self,f)()

    def declareRandomOrderTag(self, tag):
        self.randomOrderTags.append(tag)

    def suppressRegExp(self, re):
        self.suppressRegExps.append(re)

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
                path = os.sep.join([self.testdir,'resources',name])
                if not os.path.isdir(path):
                    raise NameError('Could not find resource ' + name)
                # run all tests
                for ts, tn, fn in listTests(path, self.testdir, listResources=True):
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
        # recursively check parent, if not top level, nor resources
        if not noRec and path != os.sep.join([self.testdir,'resources']):
            parent = os.path.dirname(path)
            if len(parent) != 0:
                self.checkResources(parent,skip)
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
            if path not in self.processedTagspy:
                if os.path.isfile(path+os.sep+'tags.py'):
                    execfile(path+os.sep+'tags.py')
                    self.processedTagspy.append(path)
            # check all resources in default.resources file
            self.checkResources(path+os.sep+'default.resources',skip, noRec=True)

    def isTestEnabled(self, set, name):
        # check options
        opt = getattr(self.config.option,set)
        if opt != None: return opt
        # no command line option, go to default behavior defined in config file
        shouldrun = False
        matchlen = 0
        for o in self.options.items('TestsToRun'):
            # most precise one wins
            if name.startswith(o[0]) and len(o[0]) > matchlen:
                matchlen = len(o[0])
                shouldrun = self.options.getboolean('TestsToRun', o[0])
        return shouldrun

    def runTest(self, testSet, testName, fileName):
        # check whether the test should be skipped or not
        if testSet != 'resources' and not self.isTestEnabled(testSet, testName):
            py.test.skip(testSet + " tests are skipped. Use --(no)" + testSet + " or --all to change this or adapt your config file")
        # check resources, including all defaults
        self.checkResources(fileName+'.resources',
                            skip=(self.config.option.failNoRes == False or
                                  (self.options.getboolean('Generic','SkipTestsWhenResourceMissing') and
                                   self.config.option.failNoRes == None)))
        # get the list of commands to run
        # Note that we can have several sets in case some tags can
        # get different values. We will run each set as a separate test
        for cmds in parseAndReplace(self, open(fileName+'.input').read(), testName):
            # go through them
            print
            i = 0
            localTags = {}
            for cmd in cmds.split(os.linesep):
                # skip empty lines in the input file
                if len(cmd) > 0:
                    cmd = parseAndReplaceLocals(cmd, localTags)
                    print "Executing ", cmd
                    try:
                        # run the command
                        output = Popen(cmd)
                        # and check its output
                        compareOutput(self, output, open(fileName+'.output'+str(i)).read(), testName, localTags)
                    except Timeout:
                        assert False, "Time out"
                    i = i + 1

# create global setup at config stage
def pytest_configure(config):
    global globalsetup
    globalsetup = Setup(config)


#################################################
# avoid being too verbose on keyboard interupts #
#################################################

def pytest_keyboard_interrupt(excinfo):
    globalsetup.config.option.verbose = False


##################################
# build up our own list of tests #
##################################

def pytest_collect_file(path, parent):
    if path.basename.endswith('.input') and not path.relto(parent.config.topdir).split(os.sep)[1]=='resources':
        return CustomFile(path, parent)

class CustomItem(py.test.collect.Item):

    def __init__(self, path, parent):
        py.test.collect.Item.__init__(self, str(path), parent)
        
    def runtest(self):
        filename = self.fspath.relto(self.config.topdir).replace('.input','')
        testset = filename.split(os.sep)[1]
        testname = filename[filename.find(os.sep)+1:].replace('/','_')
        return globalsetup.runTest(testset,testname,filename)

    def reportinfo(self):
        filename = self.fspath.relto(os.sep.join([str(self.config.topdir),globalsetup.testdir])).replace(os.sep,'_')
        return self.fspath, None, filename.replace('.input','')


class CustomFile(py.test.collect.File):

    def collect(self):
        return [CustomItem(self.fspath, self)]
