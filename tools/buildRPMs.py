#!/usr/bin/python
import sys, os, shutil, re, tempfile

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
workDir = tempfile.mkdtemp('','CastorRelease', '/tmp')
os.chdir(workDir)
for m in ['BUILD', 'RPMS', 'SOURCES', 'SPECS', 'SRPMS', 'RPMS/i386', 'RPMS/x86_64']: os.mkdir(m)

# build RPMs
print 'Building RPMs for ' + targetOs + '/' + targetArch + ' ...'
oraPath = '/afs/cern.ch/project/oracle/@sys/10203'
basecmd = "ORACLE_HOME=" + oraPath + " LD_LIBRARY_PATH=" + oraPath + "/lib PATH=" + oraPath + "/bin:/usr/X11R6/bin:$PATH rpmbuild --define '_topdir " + workDir + "' --define '_specdir " + workDir + os.sep + "SPECS" + os.sep + "' --define '_sourcedir " + workDir + os.sep + "SOURCES" + os.sep + "' --define '_srcrpmdir " + workDir + os.sep + "SRPMS" + os.sep + "' --define '_rpmdir " + workDir + os.sep + "RPMS" + os.sep + "' --define '_buildroot " + workDir + os.sep + "BUILD" + os.sep + "' --define '_tmppath " + workDir + os.sep + "BUILD" + os.sep
cmd = basecmd + "' -ta " + tarball
rpmOutput = os.popen4(cmd)[1].read()
if rpmOutput.find('Wrote:') == -1:
    # keep the output for debugging
    rpmLog = open(workDir + '/castorBuildOutput', 'w')
    rpmLog.write(rpmOutput)
    rpmLog.close()
    print 'The RPM build failed, output in ' + workDir + '/castorBuildOutput Exiting'
    os.remove(sys.argv[0])
    sys.exit(2)
# Now build the nostk version of the castor-tape-server RPM
cmd2 = "CASTOR_NOSTK=YES " + basecmd + "' -tb " + tarball
rpmOutput2 = os.popen4(cmd2)[1].read()
if rpmOutput2.find('Wrote:') == -1:
    # keep the output for debugging
    rpmLog = open(workDir + '/castorBuildOutput.nostk', 'w')
    rpmLog.write(rpmOutput2)
    rpmLog.close()
    print 'The RPM build for nostk tapeserver failed, output in ' + workDir + '/castorBuildOutput.nostk Exiting'
    os.remove(sys.argv[0])
    sys.exit(2)

rpmDir = workDir + os.sep + 'RPMS' + os.sep + targetArch
# copy RPMs to internal releases space
print 'Copying RPMs to internal release area ...'
intReleaseDir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/intReleases/' + fullVersion
rpmList = os.listdir(rpmDir)
for p in rpmList:
    shutil.copyfile(rpmDir + os.sep + p, intReleaseDir + os.sep + targetOs + os.sep + targetArch + os.sep + p)
if targetOs == 'SL4' and targetArch == 'i386':
    print 'Copying source RPM to internal release area ...'
    # here we also copy the source RPM
    shutil.copyfile(rpmDir + '/../../SRPMS/castor-' + fullVersion + '.src.rpm', intReleaseDir + os.sep + 'castor-' + fullVersion + '.src.rpm')
# all was fine
print 'Release done successfully on ' + targetOs + '/' + targetArch

# cleanup
shutil.rmtree(workDir)
os.remove(tarball)
os.remove(sys.argv[0])
