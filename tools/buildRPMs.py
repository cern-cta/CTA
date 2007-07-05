#!/usr/bin/python
import sys, os, shutil, re

def usage():
    print "Usage:", sys.argv[0], "os arch version tarball"
    print "This script is for internal use only. Please use makeRelease.py instead\n"
    sys.exit(1)

# First check arguments
if len(sys.argv) != 5:
    usage()
targetOs = sys.argv[1]
targetArch = sys.argv[2]
fullVersion = sys.argv[3]
tarball = sys.argv[4]

# Prepare directories
workDir = os.tempname('/build', 'CastorRelease')
os.mkdir(workDir)
os.chdir(workDir)
os.makedirs(['BUILD', 'RPMS', 'SOURCES', 'SPECS', 'SRPMS', 'RPMS/i386', 'RPMS/x86_64'])

# build RPMs
print 'Building RPMs for ' + targetOs + '/' + targetArch + ' ...'
cmd = 'RPM_BUILD_ROOT=`pwd` ORACLE_HOME=/afs/cern.ch/project/oracle/@sys/10203 LD_LIBRARY_PATH=$ORACLE_HOME/lib PATH=$ORACLE_HOME/bin:/usr/X11R6/bin:$PATH rpmbuild -ta ' + tarball
rpmOutput = os.popen4(cmd)[1].read()
m = false
if rpmOutput.find('Wrote'):
    resultRegExp = re.compile('Wrote: ([\w/]+)castor-(\w+)-' + fullVersion + '.' + targetArch + '.rpm')
    m = resultRegExp.match(rpmOutput)

rpmLog = open(workDir + '/castorBuildOutput', 'w')
rpmLog.write(rpmOutput)
rpmLog.close()
if not m or rpmOutput.close() != None:
    print 'The RPM build failed, output in ' + workDir + '/castorBuildOutput. Exiting'
    sys.exit(2)

rpmDir = m.group(1)
# copy RPMs to internal releases space
intReleaseDir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/intReleases/' + fullVersion
rpmList = os.listdir(rpmDir)
if len(rpmList) != 54:
    print 'Warning, not all RPMs were correctly generated'
for p in rpmList:
    shutil.copyfile(rpmDir + os.sep + p, intReleaseDir + os.sep + targetOs + os.sep + targetArch)
if targetOs == 'SLC3' and targetArch == 'i386':
    # here we also copy the source RPM
    shutil.copyfile(rpmDir + '/../SRPMS/castor-' + fullVersion + '.src.rpm', intReleaseDir)
    # this is temporarily needed until we get rid of GridFTPv1, which is only built for 32 bit  
    shutil.copyfile(rpmDir + os.sep + 'castor-lib-' + fullVersion + '.i386.rpm', intReleaseDir + os.sep + 'SLC4/x86_64')

# cleanup
shutil.rmtree(workDir)
shutil.rmtree(rpmDir)
#os.remove(rpmLog)
