from subprocess import Popen, PIPE, STDOUT
import os, re, ConfigParser

# some methods dealing with particular tags
def tagForCastorFileName(s):
    try:
        # get the fileclass of the file
        fileClass = Popen('nsls --class ' + s, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT).stdout.read().split()[0]
        # check the number of tapeCopies in this file class
        nslistOutput = Popen('nslistclass --id=' + fileClass, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT).stdout.read().split()
        nbCopies = int(nslistOutput[nslistOutput.index('NBCOPIES')+1])
        # based on this, decide on the tag
        if nbCopies > 0:
            return 'tapeFileName'
        else:
            return 'noTapeFileName'
    except ValueError:
        # default to non tape
        return 'noTapeFileName'

def tagForFileName(s):
    # is it a castor or a local file ?
    if s.startswith('/castor/'):
        return tagForCastorFileName(s)
    else:
        return 'localFileName'

isDiskOnlySvcClass = {}
def tagForSvcClass(s):
    global isDiskOnlySvcClass
    # Ask the user if we do not now yet
    if not isDiskOnlySvcClass.has_key(s):
        ans = raw_input('Is the "' + s + '" service class tape enabled ? [Y/n] ')
        isDiskOnlySvcClass[s] = (ans.lower() not in ('', 'y'))
    # select the tag acoordingly
    if isDiskOnlySvcClass[s]:
        return 'diskOnlyServiceClass'
    else:
        return 'tapeServiceClass'

# regexps identifying the different tags. Each regexp may be
# associated to a function that takes as single parameter the
# replaced string and returns the tag to use. If None, the regexp
# will be replaced with the rule name
# Note that regexps must have exactly one group matching the part to
# be replaced
tagRegexps = {
    'fileName' : ('[\s=](/\S*)', tagForFileName),
    'rhhost'   : ('Looking up RH host - Using ([\w.]+)', None),
    'rhport'   : ('Looking up RH port - Using (\d+)', None),
    'userid'   : ('Setting euid: (\w+)', None),
    'groupid'  : ('Setting egid: (\w+)', None),
    'localhost': ('Localhost is: ([\w.]+)', None),
    'callback port' : ('Creating socket for castor callback - Using port (\d+)', None),
    'sending time' : ('stager: ([A-Z][a-z]{2}\s+\d+\s+[\d:]+\s+\(\d+\)) Sending request', None),
    'uuid'     : ('([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', None),
    'send duration' : ('SND ([\d.]+) s to send the request', None),
    'answer duration' : ('CBK ([\d.]+) s before callback', None),
    'server ip': ('callback from (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) was received', None),
    'fileid@ns': ('(\d+@[\w.]+)', None),
    'castorTag': ('(?:\s+-U\s+|Usertag=|stage_filequery type=2 param=)(?!NULL)(\w+)', None),
    'svcClass' : ('(?:\s+-S\s+|STAGE_SVCCLASS=)(\w+)', tagForSvcClass),
    'nb bytes' : ('(\d+)\s+bytes in', None),
    'time to transfer' : ('bytes in (\d+) seconds through', None),
    'interface in' : ('seconds through (\w+\s+\(\w+\)) and', None),    
    'interface out' : ('seconds through (?:\w+\s+\(\w+\)|<interface in>) and (\w+\s+\(\w+\)(?:\s\(\d+\s\w+/sec\))?)', None)
    }

# regexps of parts of the output that should be dropped
# Note that regexps must have exactly one group matching the part to
# be dropped
suppressRegExps = [
    'seconds through (?:\w+\s+\(\w+\)) and (?:\w+\s+\(\w+\))(\s\(\d+\s\w+/sec\))'
]

# setup the environment identical to the test suite one
class CastorConfigParser(ConfigParser.ConfigParser):
    def __init__(self):
        ConfigParser.ConfigParser.__init__(self)
    # overwrite the optionxform to make options case sensitive
    def optionxform(self,option):
        return option
# instantiate an option parser with some defaults
options = CastorConfigParser()
# read the option file
options.read('CastorTestSuite.options')
# setup the environment
if options.has_section('Environment'):
    print os.linesep+"setting environment :"
    for o in options.items('Environment'):
        print "  " + o[0] + ' = ' + o[1]
        os.environ[o[0].upper()] = o[1]

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
while goOn:
    # get command and log it
    cmd = raw_input('> ').strip()
    if cmd == 'exit': break
    cmds.append(cmd)
    if len(cmd) == 0 or cmd[0] == '#': continue
    # execute it
    output = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT).stdout.read()
    outputs.append(output)
    # and print output
    print output

# we are over, get test name and place
testName = raw_input("Very good, what is the name of this test ? ").strip()
testDir = 'castortests'+os.sep+raw_input("Ok, and where should I put it ? ").strip()

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
            newTag = repl(tagValue)
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

