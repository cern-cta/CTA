import os, sys, re, ConfigParser, os.path, readline

# handling of subprocesses, depending on the python version
try:
    import subprocess
    hasSubProcessModule = True
except Exception:
    hasSubProcessModule = False
    
def Popen(cmd):
    if hasSubProcessModule:
        return subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read()
    else:
        return os.popen4(cmd)[1].read()

# define and parse command line options
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-T", "--testdir", dest="testdir", action="store",  help="Test directory to use", metavar="FILE")
(options, args) = parser.parse_args()

# find out testdir
toptestdir = options.testdir
if toptestdir == None:
    # try to find default
    candidates = filter(lambda x:x.endswith('tests'), os.listdir('.'))
    if len(candidates) == 0:
        print "No test directory found. use --testdir option"
        sys.exit(0)
    elif len(candidates) > 1:
        print "Do not know which test directory to use. Please use --testdir option\nCandidates are " + ', '.join(candidates)
        sys.exit(0)
    else:
        toptestdir = candidates[0]
        print "Using test directory " + toptestdir 

# setup the environment identical to the test suite one by reading the test suite option file
class CustomConfigParser(ConfigParser.ConfigParser):
    def __init__(self):
        ConfigParser.ConfigParser.__init__(self)
    # overwrite the optionxform to make options case sensitive
    def optionxform(self,option):
        return option
# instantiate an option parser with some defaults
options = CustomConfigParser()
# read the option file
optionFileCandidates = filter(lambda x:x.endswith('.options'), os.listdir(toptestdir))
if len(optionFileCandidates) == 0:
    print "No .options file found in " + toptestdir
    sys.exit(0)
if len(optionFileCandidates) > 1:
    print "Do not know which .options file to use in " + toptestdir + " : " + ' '.join(optionFileCandidates)
    sys.exit(0)
options.read(os.sep.join([toptestdir, optionFileCandidates[0]]))

# setup the environment
if options.has_section('Environment'):
    print os.linesep+"setting environment :"
    for o in options.items('Environment'):
        print "  " + o[0] + ' = ' + o[1]
        os.environ[o[0].upper()] = o[1]

# regexps identifying the different tags. Each regexp may be
# associated to a function that takes as single parameter the
# replaced string and returns the tag to use. If None, the regexp
# will be replaced with the rule name
# Note that regexps must have exactly one group matching the part to
# be replaced
tagRegexps = {}

# regexps of parts of the output that should be dropped
# Note that regexps must have exactly one group matching the part to
# be dropped
suppressRegExps = []

# check for buildCase.py file and absorb new definitions
buildcasefile = os.sep.join([toptestdir,'buildCase.py'])
if os.path.isfile(buildcasefile):
    execfile(buildcasefile)

# welcome message
print
print "Welcome to the testCase building utility"
print "Just type commands as in a regular shell prompt"
print "I will take care of building the testCase out of it"
print "only say exit when you are over"
print

# main loop
goOn = True
cmds = []
outputs = []
historyPath = os.path.expanduser("~/.buildTestHist")
if os.path.exists(historyPath):
    readline.read_history_file(historyPath)
while goOn:
    # get command and log it
    cmd = raw_input('> ').strip()
    if cmd == 'exit': break
    cmds.append(cmd)
    if len(cmd) == 0 or cmd[0] == '#': continue
    # execute it
    output = Popen(cmd)
    outputs.append(output)
    # and print output
    print output

# we are over, get test name and place
testName = raw_input("Very good, what is the name of this test ? ").strip()
testDir = toptestdir+os.sep+raw_input("Ok, and where should I put it ? ").strip()
readline.write_history_file(historyPath)

def resetTags():
    global knownRepl, usedTags
    # list of already known replacements
    knownRepl = {}
    # list of used tags
    usedTags = {}
    # initialize predefined tags
    for t in options.options('Tags'):
        knownRepl[options.get('Tags',t)] = '<'+t+'>'
        usedTags[t] = options.get('Tags',t)

# initializes the known tags lists
resetTags()

# handling of tag replacements
def handleTagRepl(tag,repl,match):
    # get the metched string
    needle = match.group(0)
    tagValue = match.group(1)
    # if we do not know it yet, handle the new case and potentially create a new tag
    if needle not in knownRepl.keys():
        if repl != None:
            newTag = repl(tagValue, needle)
        else:
            newTag = tag
        # In case the tag is known
        reuse = False
        if newTag in usedTags.keys():
            # check whether it a reuse of it in a different context
            if tagValue == usedTags[newTag]:
                reuse = True
            else:
                # no a reuse of the bare tag, go through derived ones
                i = 1
                while newTag+str(i) in usedTags.keys():
                    if tagValue == usedTags[newTag+str(i)]:
                        reuse = True
                        break
                    i = i + 1
                # new name of the tag (whether reused or not)
                newTag = newTag + str(i)
        # update the knownRepl and usedTags tables
        knownRepl[needle] = needle[:match.start(1)-match.start(0)]+'<'+newTag+'>'+needle[match.end(1)-match.start(0):]
        if not reuse:
            usedTags[newTag] = tagValue
    # finally return the replacement string
    return knownRepl[needle]

def handleDrop(match):
    needle = match.group(0)
    return needle[:match.start(1)-match.start(0)]+needle[match.end(1)-match.start(0):]
    
def createTags(s):
    # first drop what should be dropped
    for t in suppressRegExps:
        s = re.sub(t, handleDrop, s)
    # then replace already known value
    for t in knownRepl.keys():
        s = s.replace(t, knownRepl[t])
    # and look for new tags
    for t in tagRegexps.keys():
        regexp, repl = tagRegexps[t]
        s = re.sub(regexp,lambda match:handleTagRepl(t, repl, match),s)
    return s

# create needed directories
try:
    os.makedirs(testDir)
except os.error:
    # already existing dir. Perfect !
    pass

# create input file
resetTags()
infile = open(os.sep.join([testDir,testName])+'.input','w')
for c in cmds:
    # modify commands to introduce tags
    infile.write(createTags(c)+os.linesep)
infile.close()

# create output files
for i in range(len(outputs)):
    outfile = open(os.sep.join([testDir,testName])+'.output'+str(i),'w')
    # modify output to introduce tags
    outfile.write(createTags(outputs[i]))
    outfile.close()

