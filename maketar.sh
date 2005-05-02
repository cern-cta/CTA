#!/bin/sh

# $Id: maketar.sh,v 1.2 2005/05/02 10:13:40 jdurand Exp $

if [ "x${MAJOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MAJOR_CASTOR_VERSION environment variable"
  exit 1
fi
if [ "x${MINOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MINOR_CASTOR_VERSION environment variable"
  exit 1
fi

#
## We expect ${MAJOR_CASTOR_VERSION} in the form a.b
#
echo ${MAJOR_CASTOR_VERSION} | egrep -q '^[0-9]+\.[0-9]+$'
if [ $? -ne 0 ]; then
  echo "MAJOR_CASTOR_VERSION (${MAJOR_CASTOR_VERSION}) should be in the form a.b, example: 2.0"
  exit 1
fi

#
## We expect ${MINOR_CASTOR_VERSION} in the form c.d
#
echo ${MINOR_CASTOR_VERSION} | egrep -q '^[0-9]+\.[0-9]+$'
if [ $? -ne 0 ]; then
  echo "MINOR_CASTOR_VERSION (${MINOR_CASTOR_VERSION}) should be in the form c.d, example: 99.1"
  exit 1
fi

a=`echo ${MAJOR_CASTOR_VERSION} | sed 's/\..*//g'`
b=`echo ${MAJOR_CASTOR_VERSION} | sed 's/.*\.//g'`
c=`echo ${MINOR_CASTOR_VERSION} | sed 's/\..*//g'`
d=`echo ${MINOR_CASTOR_VERSION} | sed 's/.*\.//g'`

fakeroot=`which fakeroot 2>/dev/null`
if [ -n "${fakeroot}" ]; then
    #
    ## I assume you are on debian - Ahem I use
    ## the existence of fakeroot to determine
    ## that - AFAIK fakeroot only exist in debian
    #
    echo "### INFO ### Updating debian/changelog"
    #
    ## Ask for a changelog
    #
    cvs update -Ad debian/changelog
    dch --newversion ${a}.${b}.${c}-${d}
    cvs commit  -m "Version ${a}.${b}.${c}-${d}" debian/changelog
fi

echo "### INFO ### Making build directory"

#
## Make sure makedepend (/usr/X11R6/bin) is in the path
#
PATH=${PATH}:/usr/X11R6/bin
export PATH

# Go to castor-$a.$b.$c and do the changes in here
curdir=`pwd`
cd ..
[ -d "castor-${a}.${b}.${c}" ] && rm -rf castor-${a}.${b}.${c}
cp -Lr $curdir castor-${a}.${b}.${c}
cd castor-${a}.${b}.${c}

echo "### INFO ### Customizing build directory"

#
## Force build rules to YES for a lot of things
#
for this in Accounting BuildCastorClientCPPLibrary BuildCommands BuildCommon BuildCupvClient BuildCupvDaemon BuildCupvLibrary BuildDlfClient BuildDlfDaemon BuildDlfLibrary BuildExpertClient BuildExpertDaemon BuildExpertLibrary BuildGCCpp BuildHsmTools BuildJob BuildMonitorClient BuildMonitorLibrary BuildMonitorServer BuildMsgClient BuildMsgLibrary BuildMsgServer BuildNameServerClient BuildNameServerDaemon BuildNameServerLibrary BuildOraCpp BuildRHCpp BuildRfioClient BuildRfioLibrary BuildRfioServer BuildRmClient BuildRmLibrary BuildRmMaster BuildRmNode BuildRmcLibrary BuildRmcServer BuildRtcopyClient BuildRtcopyLibrary BuildRtcopyServer BuildRtcpclientd BuildRtstat BuildSchedPlugin BuildSecureCns BuildSecureCupv BuildSecureRfio BuildSecureRtcopy BuildSecureStage BuildSecureTape BuildSecureVdqm BuildSecureVmgr BuildStageClient BuildStageDaemon BuildStageLibrary BuildTapeClient BuildTapeDaemon BuildTapeLibrary BuildTpusage BuildVdqmClient BuildVdqmLibrary BuildVdqmServer BuildVolumeMgrClient BuildVolumeMgrDaemon BuildVolumeMgrLibrary HasCDK HasNroff UseCupv UseExpert UseGSI UseKRB4 UseKRB5 UseLsf UseMaui UseOracle UseScheduler UseVmgr; do
    perl -pi -e "s/$this.*(YES|NO)/$this\tYES/g" config/site.def
done

#
## Force build rules to NO for a lot of things
#
for this in BuildCppTest BuildExamples BuildGcDaemon BuildSecurity BuildTest ClientLogging FollowRtLinks HasNis RFIODaemonRealTime SecMakeStaticLibrary UseAdns UseDMSCapi UseHeimdalKrb5 UseHpss UseMtx UseMySQL; do
    perl -pi -e "s/$this.*(YES|NO)/$this\tNO/g" config/site.def
done
#
## Change __MAJOR_CASTOR_VERSION and __MINOR_CASTOR_VERSION everywhere it has to be changed
#
perl -pi -e s/__MAJOR_CASTOR_VERSION__/${MAJOR_CASTOR_VERSION}/g */Imakefile debian/*.install
perl -pi -e s/__MINOR_CASTOR_VERSION__/${MINOR_CASTOR_VERSION}/g */Imakefile debian/*.install
#
# Make spec file in sync
#
perl -pi -e s/__A__/${a}/g CASTOR.spec
perl -pi -e s/__B__/${b}/g CASTOR.spec
perl -pi -e s/__C__/${c}/g CASTOR.spec
perl -pi -e s/__D__/${d}/g CASTOR.spec
#
## Replace __BASEVERSION__, __PATCHLEVEL__ and __TIMESTAMP__ in patchlevel.h
#
timestamp=`date "+%s"`
perl -pi -e s/__BASEVERSION__/\"${a}.${b}.${c}\"/g h/patchlevel.h
perl -pi -e s/__PATCHLEVEL__/${d}/g h/patchlevel.h
perl -pi -e s/__TIMESTAMP__/\"${timestamp}\"/g h/patchlevel.h

echo "### INFO ### Customizing spec file"

#
## Append all sub-packages to CASTOR.spec
#
for this in `grep Package: debian/control | awk '{print $NF}'`; do
    package=`echo $this | sed 's/\.install//g'`
    echo "%package -n $package" >> CASTOR.spec
    echo "Summary: Cern Advanced mass STORage" >> CASTOR.spec
    echo "Group: Application/Castor" >> CASTOR.spec
    echo "%description -n $package" >> CASTOR.spec
    cat debian/control | perl -e '$package=shift; while (<>) {chomp($this .= " " . $_)}; $this =~ s/.*Package: $package//g; $this =~ s/Package:.*//g; $this =~ /Description: (.*)/; print "$1\n"' $package >> CASTOR.spec
    echo "%files -n $package" >> CASTOR.spec
    echo "%defattr(-,root,root)" >> CASTOR.spec
    if [ -s "debian/$package.install" ]; then
	for file in `cat debian/$package.install`; do
	    echo $file | egrep -q "etc\/castor|sysconfig"
	    if [ $? -eq 0 ]; then
		echo "%config(noreplace) /$file" >> CASTOR.spec
	    else
		echo "/$file" >> CASTOR.spec
	    fi
	done
    fi
    if [ -s "debian/$package.init" ]; then
	echo "%config /etc/init.d/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.logrotate" ]; then
	echo "%config(noreplace) /etc/logrotate.d/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.manpages" ]; then
	for man in `cat  debian/$package.manpages | sed 's/debian\/castor\///g'`; do
	    echo "%doc /$man" >> CASTOR.spec
	done
    fi
    echo >> CASTOR.spec
done

cd ..

echo "### INFO ### Creating tarball"

tar -zcf castor-${a}.${b}.${c}.tar.gz castor-${a}.${b}.${c}

cd castor-${a}.${b}.${c}
