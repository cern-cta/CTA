#!/bin/sh

# $Id: makedeb.sh,v 1.2 2005/01/20 10:50:36 jdurand Exp $

if [ "x${MAJOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MAJOR_CASTOR_VERSION environment variable"
  exit 1
fi
if [ "x${MINOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MINOR_CASTOR_VERSION environment variable"
  exit 1
fi

#
## Make sure makedepend (/usr/X11R6/bin) is in the path
#
PATH=${PATH}:/usr/X11R6/bin
export PATH

#
## First create the tarball
#
dir=`pwd`
cd /tmp
rm -rf castor-${MAJOR_CASTOR_VERSION} castor-${MAJOR_CASTOR_VERSION}.tar.gz
mkdir castor-${MAJOR_CASTOR_VERSION}
cd castor-${MAJOR_CASTOR_VERSION}
/bin/cp -Lfr ${dir}/* .
#
## Force build rules to YES for a lot of things
#
for this in BuildCommands BuildCommon BuildCupvClient BuildCupvDaemon BuildCupvLibrary BuildGcDaemon BuildHsmTools BuildMonitorClient BuildMonitorServer BuildMonitorLibrary BuildMsgClient BuildMsgServer BuildMsgLibrary BuildNameServerClient BuildNameServerDaemon BuildNameServerLibrary BuildRfioClient BuildRfioServer BuildRfioLibrary BuildRmcServer BuildRmcLibrary BuildJob BuildRtcopyClient BuildRtcopyServer BuildRtcopyLibrary BuildRtstat BuildStageClient BuildStageDaemon BuildStageLibrary BuildRmMaster BuildRmNode BuildRmClient BuildRmLibrary BuildTapeClient BuildTapeDaemon BuildTapeLibrary BuildTpusage BuildVdqmClient BuildVdqmServer BuildVdqmLibrary BuildVolumeMgrClient BuildVolumeMgrDaemon BuildVolumeMgrLibrary BuildDlfClient BuildDlfDaemon BuildDlfLibrary BuildExpertClient BuildExpertDaemon BuildExpertLibrary BuildRHCpp BuildOraCpp BuildCastorClientCPPLibrary Accounting HasNroff HasCDK UseCupv UseOracle UseVmgr UseScheduler UseExpert UseMaui; do
    perl -pi -e "s/$this.*(YES|NO)/$this\tYES/g" config/site.def
done
#
## Force build rules to NO for a lot of things
#
for this in BuildExamples BuildSchedPlugin BuildCppTest ClientLogging HasNis RFIODaemonRealTime UseAdns UseDMSCapi UseHpss UseMtx UseMySQL UseLsf FollowRtLinks BuildSecurity; do
    perl -pi -e "s/$this.*(YES|NO)/$this\tNO/g" config/site.def
done
cd ..
tar -cvhzf castor-${MAJOR_CASTOR_VERSION}.tar.gz castor-${MAJOR_CASTOR_VERSION}
cd castor-${MAJOR_CASTOR_VERSION}
fakeroot dpkg-buildpackage
status=$?

exit $status
