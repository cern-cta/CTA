#!/usr/bin/python
import sys, re, os, shutil, tempfile, socket, time

# list of platforms as a tuple (OS, arch, machine where to build)
platforms = (('SLC4', 'i386',   'lxc2slc4-i386.cern.ch'),
             ('SLC4', 'x86_64', 'lxc2slc4-x64.cern.ch'),
             ('SLC5', 'i386',   'lxc2slc5-i386.cern.ch'),
             ('SLC5', 'x86_64', 'lxc2slc5-x64.cern.ch'))

def usage():
    print sys.argv[0] + " <tag of the release>"

def runCommand(cmd, errorMessage):
    global intReleaseDir
    f = os.popen(cmd)
    f.read()
    if (f.close() != None):
        print errorMessage
        print "Was running :"
        print cmd
        print "Exiting"
        shutil.rmtree(workDir)
        if intReleaseDir != '':
            shutil.rmtree(intReleaseDir)
        sys.exit(1)

def findUpdates(d):
    res = []
    # take all relevant update scripts
    updRegExp = re.compile('(\w*_)?\d+.\d+.\d+(-\d+|)_to_' + fullVersion + '.sql?')
    for f in os.listdir(d):
        if updRegExp.match(f):
            res.append(f)
    return res

def findGrantAndDropScripts(workDir, dirs):
    res = []
    dgRegExp = re.compile('\w+_oracle_(grant|schema|user).sql?')
    for d in (dirs):
        d = workDir + os.sep + d
        for f in os.listdir(d):
            if dgRegExp.match(f):
                res.append(d + os.sep + f) 
    return res

intReleaseDir = ''

# First check the arguments
if len(sys.argv) != 2:
    print "wrong number of arguments"
    usage()
    sys.exit(1)
versionRegExp = re.compile('v(\d+)_(\d+)_(\d+)_(\d+)')
m = versionRegExp.match(sys.argv[1])
if not m:
    print "The argument given does not look like a release tag. Expecting something like v2_1_3_17"
    sys.exit(1)
version = sys.argv[1]
majversion = m.group(1)
minversion = m.group(2)
majrelease = m.group(3)
minrelease = m.group(4)
fullVersion = str(majversion) + '.' + str(minversion) + '.' + str(majrelease) + '-' + str(minrelease)
print "Building a release for CASTOR " + fullVersion

# Then checkout the source code
os.chdir('/tmp')
workDir = tempfile.mkdtemp('','CastorRelease', '/tmp')
print "Checking out code into " + workDir
os.chdir(workDir)
runCommand('svn --quiet co svn+ssh://svn/reps/CASTOR/CASTOR2/tags/' + version + ' CASTOR2',
           'Error while checking out source code')

# Create the tar ball
print "Building the tar ball"
os.chdir('CASTOR2')
runCommand('make -f Makefile.ini Makefiles', 'Could not create makefiles')
runCommand('make tar', 'Could not create tar ball')
os.chdir(workDir)
tarBall = 'castor-' + str(majversion) + '.' + str(minversion) + '.' + str(majrelease) + '.' + str(minrelease) + '.tar.gz'
os.chmod(tarBall, 0664)

# Prepare internal Release space
print "Preparing internal release space"
intReleaseDir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/intReleases' + os.sep + fullVersion
os.mkdir(intReleaseDir)
os.chdir(intReleaseDir)
for p in platforms:
    os.makedirs(p[0] + os.sep + p[1])
os.mkdir('dbupgrades')
os.mkdir('dbcreation')

# Copy some files to internal release space
shutil.copyfile(workDir + os.sep + tarBall, intReleaseDir + os.sep + tarBall)
shutil.copyfile(workDir + os.sep + 'CASTOR2' + os.sep + 'ReleaseNotes', intReleaseDir + os.sep + 'ReleaseNotes')
updDir = workDir + os.sep + 'CASTOR2' + os.sep + 'upgrades'
for f in findUpdates(updDir):
    shutil.copyfile(updDir + os.sep + f, intReleaseDir + os.sep + 'dbupgrades' + os.sep + f)
for f in findGrantAndDropScripts(workDir + os.sep + 'CASTOR2', 
                                 ['castor/db', 'castor/repack', 'dlf/scripts/oracle']):
    shutil.copy(f, intReleaseDir + os.sep + 'dbcreation' + os.sep)
os.chdir(workDir + os.sep + 'CASTOR2')
runCommand('./makesql.sh ' + intReleaseDir + os.sep + 'dbcreation', 'Could not publish SQL scripts')

# compile on the different architectures
outputs = []
print "Spawning remote builds..."
for p in platforms:
    # get a unique identifier to be safe against concurrent builds
    # since the uuid package in only available in python 2.5, we do it by hand : <ip>.<time in miliseconds>
    uniqueId = socket.getaddrinfo(socket.gethostname(),0)[-1][-1][0] + '.' + str(long(time.time() * 1000))
    # first copy over the tar ball and build script
    print "Sending tarBall to " + p[2]
    cmd = 'scp ' + workDir + os.sep + tarBall + ' ' + p[2] + ':/tmp/' + tarBall
    runCommand(cmd, 'Error while exporting tar ball to ' + p[2])
    cmd = 'scp ' + workDir + os.sep + 'CASTOR2/tools/buildRPMs.py ' + p[2] + ':/tmp/buildRPMs.py.' + uniqueId
    runCommand(cmd, 'Error while exporting buildRPMs.py to ' + p[2])
    # then launch the compilation in parallel
    print "Launching RPM build on " + p[2]
    cmd = 'ssh ' + p[2] + ' "python /tmp/buildRPMs.py.' + uniqueId + ' ' + p[0] + ' ' + p[1] + ' ' + fullVersion + ' /tmp/' + tarBall + '"'
    outputs.append((p[2], os.popen4(cmd)[1]))

for o in outputs:
    print "Output of build on " + o[0] + " :"
    print o[1].read()
    if o[1].close() != None:
        print "Error during remote build on", o[0]

# make a fresh checkout in the internal release space for easy debugging using AFS
print "Creating a fresh checkout in the internal release space"
os.chdir(intReleaseDir)
runCommand('svn --quiet co svn+ssh://svn/reps/CASTOR/CASTOR2/tags/' + version + ' CASTOR2',
           'Error while checking out release into internal release space')

# create a 'fast' testsuite directory
print "creating a fast testsuite..."
os.chdir(intReleaseDir)
os.mkdir('fastTestSuite')
os.chdir('fastTestSuite')
targets = [ ('CLIENT', 'PUT'),
            ('CLIENT', 'GET'),
            ('CLIENT', 'UPDV2'),
            ('CLIENT', 'UPDV3'),
            ('CLIENT', 'PUTDONE'),
            ('CLIENT', 'RM'),
            ('CLIENT', 'EXTRAQRY'),
            ('CLIENT', 'DISKONLY'),
            ('CLIENT', 'EXTRATEST'),
            ('RFIO', 'BASIC_RFCP'),
            ('ROOT', 'RFIO'),
            ('ROOT', 'CASTOR') ]
s = open('../CASTOR2/test/testsuite/CASTORTESTCONFIG').read()
for t in targets:
    r = re.compile(t[0]+'(\s+)'+t[1]+'(\s+)NO')
    m = r.search(s)
    s = s.replace(m.group(0), t[0]+m.group(1)+t[1]+m.group(2)+'YES')
f = open('CASTORTESTCONFIG', 'w')
f.write(s)
f.close()
files = ['CastorTestSuite.py',
         'ClientTest.py',
         'RfioTest.py',
         'RootTest.py',
         'TapeTest.py',
         'XRootTest.py',
         'UtilityForCastorTest.py']
for f in files:
    cmd = 'cp ../CASTOR2/test/testsuite/' + f + ' .'
    runCommand(cmd, 'Error while creating fast testsuite')
# create a tar ball of the test suite
os.chdir(intReleaseDir)
cmd = 'tar czf fastTestSuite.tar.gz fastTestSuite'
runCommand(cmd, 'Error while taring the testsuite')

# and build the doxygen documentation in it
print "Building doxygen documentation"
os.chdir(intReleaseDir + os.sep + 'CASTOR2')
runCommand('make -f Makefile.ini Makefiles',
           'Error while creating Makefiles')
runCommand('make doxygen',
           'Error while creating doxygen documentation')
runCommand('cat \'<META HTTP-EQUIV="refresh" CONTENT="CASTOR2/doc/html/index.html">\' > doxygenDoc.html',
           'Error while linking doxygen documentation from top level')

# cleanup
shutil.rmtree(workDir)
