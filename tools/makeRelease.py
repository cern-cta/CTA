#!/usr/bin/python
import sys, re, os, shutil

# list of platforms as a tuple (OS, arch, machine where to build)
platforms = (('SLC4', 'x86_64', 'lxbuild085.cern.ch'),
             ('SLC3', 'i386',   'lxs5010.cern.ch'),
             ('SLC4', 'i386',   'lxbuild020.cern.ch'))

def usage():
    print sys.argv[0] + " <CVS tag of the release>"

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
    # take all relevant update scripts (DLF included if needed)
    updRegExp = re.compile('(\w*_)?\d+.\d+.\d+-\d+_to_' + fullVersion + '.sql(plus)?')
    for f in os.listdir(d):
        if updRegExp.match(f):
            res.append(f)
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
workDir = os.tempnam('/tmp', 'CastorRelease')
print "Checking out code into " + workDir
os.mkdir(workDir)
os.chdir(workDir)
runCommand('cvs -d :kserver:isscvs.cern.ch:2000/local/reps/castor co -r ' + version + ' CASTOR2',
           'Error while checking out source code')

# Create the tar ball
print "Building the tar ball"
os.chdir('CASTOR2')
runCommand('make -f Makefile.ini Makefiles', 'could not create makefiles')
runCommand('make tar', 'could not create tar ball')
os.chdir(workDir)
tarBall = 'castor-' + str(majversion) + '.' + str(minversion) + '.' + str(majrelease) + '.tar.gz'
os.chmod(tarBall, 0664)

# Prepare internal Release space
print "Preparing internal release space"
intReleaseDir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/intReleases' + os.sep + fullVersion
os.mkdir(intReleaseDir)
os.chdir(intReleaseDir)
for p in platforms:
    os.makedirs(p[0] + os.sep + p[1])
os.mkdir('upgrades')

# Copy some files to internal release space
shutil.copyfile(workDir + os.sep + tarBall, intReleaseDir + os.sep + tarBall)
shutil.copyfile(workDir + os.sep + 'CASTOR2' + os.sep + 'ReleaseNotes', intReleaseDir + os.sep + 'ReleaseNotes')
updDir = workDir + os.sep + 'CASTOR2' + os.sep + 'upgrades'
for f in findUpdates(updDir):
    shutil.copyfile(updDir + os.sep + f, intReleaseDir + os.sep + 'upgrades' + os.sep + f)

# compile on the different architectures
outputs = []
print "Spawning remote builds..."
for p in platforms:
    # first copy over the tar ball
    print "Sending tarBall to " + p[2]
    cmd = 'scp ' + workDir + os.sep + tarBall + ' ' + p[2] + ':/tmp/' + tarBall
    runCommand(cmd, 'Error while exporting tar ball to ' + p[2])
    # then launch the compilation in parallel
    print "Launching RPM build on " + p[2]
    cmd = 'ssh ' + p[2] + ' "python /afs/cern.ch/project/castor/CASTOR2/tools/buildRPMs.py ' + p[0] + ' ' + p[1] + ' ' + fullVersion + ' /tmp/' + tarBall + '"'
    outputs.append((p[2], os.popen4(cmd)[1]))

for o in outputs:
    print "Output of build on " + o[0] + " :"
    print o[1].read()
    if o[1].close() != None:
        print "Error during remote build on", o[0]

# cleanup
shutil.rmtree(workDir)
