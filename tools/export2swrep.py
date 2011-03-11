#!/usr/bin/python
import sys, re, os, threading

def usage():
    print sys.argv[0] + " <castor version>"

# list of platforms as a tuple (OS, arch)
platforms = (('SL4', 'x86_64', 'x86_64_slc4'),
             ('SL4', 'i386',   'i386_slc4'),
             ('SL5', 'x86_64', 'x86_64_slc5'),
             ('SL5', 'i386',   'i386_slc5'))

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

# internal release repository where to find the packages
intReleaseDir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/intReleases' + os.sep + version

# exports a package
def export2swrep(arch, rpm):
    cmd = "swrep-soap-client put " + arch + " /cern/cc " + rpm
    print cmd
    f = os.popen(cmd)
    out = f.read()
    if (f.close() != None):
        print cmd
        print out

# export packages
for p in platforms:
    dir = intReleaseDir + "/" + p[0]
    if p[1] == "x86_64":
	export2swrep(p[2], dir + "/i386/castor-lib-2* " + dir + "/i386/castor-devel-2*")
    export2swrep(p[2], dir + "/" + p[1] + "/")
