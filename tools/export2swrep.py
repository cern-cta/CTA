#!/usr/bin/python
import sys, re, os, threading

# list of platforms as a tuple (OS, arch)
platforms = (('SLC4', 'x86_64', 'x86_64_slc4'),
             ('SLC3', 'i386', 'i386_slc3'),
             ('SLC4', 'i386', 'i386_slc4'))
# internal release repository where to find the packages
intReleaseDir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/intReleases' + os.sep + fullVersion

# First check the arguments
if len(sys.argv) != 2:
    print "wrong number of arguments"
    usage()
    sys.exit(1)
versionRegExp = re.compile('v(\d+)_(\d+)_(\d+)_(\d+)')
m = versionRegExp.match(sys.argv[1])
if not m:
    # try the other way of giving versions
    versionRegExpal = re.compile('(\d+).(\d+).(\d+)-(\d+)')
    m = versionRegExp.match(sys.argv[1])
    if not m:
        print "Cannot parse the version. Expecting something like v2_1_3_17 or 2.1.3-17"
        sys.exit(1)
majversion = m.group(1)
minversion = m.group(2)
majrelease = m.group(3)
minrelease = m.group(4)
version = str(majversion) + '.' + str(minversion) + '.' + str(majrelease) + '-' + str(minrelease)
print "Exporting release " + version + " of CASTOR to swrep"

# exports a package
def export2swrep(dir, arch, rpm):
    for rpm in os.listdir(dir):
        cmd = "swrep-soap-client put " + arch + " /cern/cc " + rpm
        f = os.popen(cmd)
        out = f.read()
        if (f.close() != None):
            print cmd
            print out
    
# send all packages to swrep, with one thread per platform to be faster
class SWREP (threading.Thread):
    def __init__ (self, archDesc):
        self.OS = archDesc[0]
        self.arch = archDesc[1]
        self.swrepArch = archDesc[2]
    def run ( self ):
        export2swrep(intReleaseDir + os.sep + OS + os.sep + arch, self.swrepArch, rpm)
        if OS == 'SLC3' and arch == 'i386' and rpm.startswith('castor-lib-2'):
           castorlibslc3i386 = rpm 
castorlibslc3i386 = ''
for p in platforms:
    SWREP(p).start()
# export the 32bit castor-lib in 64 bits
export2swrep(intReleaseDir + os.sep + 'SLC3' + os.sep + 'i386', 'x86_64_slc4', castorlibslc3i386)
