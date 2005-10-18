#!/bin/sh

# $Id: maketar.sh,v 1.38 2005/10/18 14:02:19 jdurand Exp $

if [ "x${MAJOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MAJOR_CASTOR_VERSION environment variable - guessing from debian/changelog"
  MAJOR_CASTOR_VERSION=`egrep "^castor" debian/changelog | awk '{print $2}' | head -1 | perl -ne 'if (/\((\d+)\.(\d+)/) {print "$1.$2\n";}'`
  if [ "x${MAJOR_CASTOR_VERSION}" = "x" ]; then
    echo "No MAJOR_CASTOR_VERSION environment variable"
    exit 1
  fi
  export MAJOR_CASTOR_VERSION
  echo "... Got MAJOR_CASTOR_VERSION=${MAJOR_CASTOR_VERSION}"
fi
if [ "x${MINOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MINOR_CASTOR_VERSION environment variable - guessing from debian/changelog"
  MINOR_CASTOR_VERSION=`egrep "^castor" debian/changelog | awk '{print $2}' | head -1 | perl -ne 'if (/(\d+)\-(\d+)\)/) {print "$1.$2\n";}'`
  if [ "x${MINOR_CASTOR_VERSION}" = "x" ]; then
    echo "No MINOR_CASTOR_VERSION environment variable - guessing from debian/changelog"
    exit 1
  fi
  export MINOR_CASTOR_VERSION
  echo "... Got MINOR_CASTOR_VERSION=${MINOR_CASTOR_VERSION}"
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

echo "### INFO ### Making build directory"

#
## Make sure makedepend (/usr/X11R6/bin) is in the path
#
PATH=${PATH}:/usr/X11R6/bin
export PATH

# Go to castor-${a}.${b}.${c} and do the changes in here
curdir=`pwd`
cd ..
[ -d "castor-${a}.${b}.${c}" ] && rm -rf castor-${a}.${b}.${c}
cp -Lr $curdir castor-${a}.${b}.${c}
cd castor-${a}.${b}.${c}

echo "### INFO ### Customizing build directory"

#
## Force build rules to YES for a lot of things
#
for this in Accounting BuildCastorClientCPPLibrary BuildCommands BuildCommon BuildCupvClient BuildCupvDaemon BuildCupvLibrary BuildDlfClient BuildDlfDaemon BuildDlfLibrary BuildExpertClient BuildExpertDaemon BuildExpertLibrary BuildGCCpp BuildHsmTools BuildJob BuildMonitorClient BuildMonitorLibrary BuildMonitorServer BuildMsgClient BuildMsgLibrary BuildMsgServer BuildNameServerClient BuildNameServerDaemon BuildNameServerLibrary BuildOraCpp BuildRHCpp BuildRfioClient BuildRfioLibrary BuildRfioServer BuildRmClient BuildRmLibrary BuildRmMaster BuildRmNode BuildRmcLibrary BuildRmcServer BuildRtcopyClient BuildRtcopyLibrary BuildRtcopyServer BuildRtcpclientd BuildRtstat BuildSchedPlugin BuildSecureCns BuildSecureCupv BuildSecureRfio BuildSecureRtcopy BuildSecureStage BuildSecureTape BuildSecureVdqm BuildSecureVmgr BuildStageClient BuildStageClientOld BuildStageDaemon BuildStageLibrary BuildTapeClient BuildTapeDaemon BuildTapeLibrary BuildTpusage BuildVdqmClient BuildVdqmLibrary BuildVdqmServer BuildVDQMCpp BuildVolumeMgrClient BuildVolumeMgrDaemon BuildVolumeMgrLibrary HasCDK HasNroff UseCupv UseExpert UseGSI UseKRB4 UseKRB5 UseLsf UseMaui UseOracle UseScheduler UseVmgr; do
    perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tYES/g" config/site.def
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
perl -pi -e s/__MAJOR_CASTOR_VERSION__/${MAJOR_CASTOR_VERSION}/g */Imakefile debian/*.install.perm
perl -pi -e s/__MINOR_CASTOR_VERSION__/${MINOR_CASTOR_VERSION}/g */Imakefile debian/*.install.perm
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
## Attemps to get debian's Build-Requires
#
buildrequires=`grep Build-Depends: debian/control | grep -v debhelper | sed 's/Build-Depends: //g'`
if [ -n "${buildrequires}" ]; then
    #
    ## We know per construction that lsf-master is called LSF-master in CERN's rpms
    #
    buildrequires=`echo ${buildrequires} | sed 's/lsf/LSF/g'`
    # perl -pi -e "s/#BuildRequires:.*/BuildRequires: ${buildrequires}/g" CASTOR.spec
fi


#
## Functions to detect %if/%endif dependencies
#
function if_has_oracle {
    egrep -q "^$1\$" debian/if.has_oracle
    if [ $? -eq 0 ]; then
	return 1
    fi
    return 0
}

function if_has_lsf {
    egrep -q "^$1\$" debian/if.has_lsf
    if [ $? -eq 0 ]; then
	return 1
    fi
    return 0
}

function if_has_stk_ssi {
    egrep -q "^$1\$" debian/if.has_stk_ssi
    if [ $? -eq 0 ]; then
	return 1
    fi
    return 0
}
#
## Append all sub-packages to CASTOR.spec
#
for this in `grep Package: debian/control | awk '{print $NF}'`; do
    #
    ## Do we have an %if dependency ?
    #
    if_has_oracle $this
    if [ $? -eq 1 ]; then
	echo "%if %has_oracle" >> CASTOR.spec
    fi
    if_has_lsf $this
    if [ $? -eq 1 ]; then
	echo "%if %has_lsf" >> CASTOR.spec
    fi
    if_has_stk_ssi $this
    if [ $? -eq 1 ]; then
	echo "%if %has_stk_ssi" >> CASTOR.spec
    fi
    package=$this
    echo "%package -n $package" >> CASTOR.spec
    echo "Summary: Cern Advanced mass STORage" >> CASTOR.spec
    echo "Group: Application/Castor" >> CASTOR.spec
    #
    ## Except for Description that is truely a multi-line thing even within debian/control, the other
    ## fields are always on a single line:
    ## The following perl script isolates the Package: definition of a package, then split it using
    ## newlines, then look which line matches the keyword.
    ## Then we remove spaces
    #
    ## Get requires
    #
    ## Requires might give ${shlibs:Depends},${misc:Depends} in the output: we will drop that
    ## This is why there are extra sed's after the original that remove the spaces
    #
    #
    requires=`cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      map {if (/([^:]+):(.+)/) {$this{$1}=$2};} split("\n",$this);
      if (defined($this{$what})) {
        print "$this{$what}\n";
      }' $package Depends |
      sed 's/ //g' | sed 's/\${[^{},]*}//g' | sed 's/^,*//g' | sed 's/,,*/,/g'`
    if [ -n "${requires}" ]; then
	echo "Requires: ${requires}" >> CASTOR.spec
    fi
    #
    ## Get provides
    #
    provides=`cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      map {if (/([^:]+):(.+)/) {$this{$1}=$2};} split("\n",$this);
      if (defined($this{$what})) {
        print "$this{$what}\n";
      }' $package Provides |
      sed 's/ //g'`
    if [ -n "${provides}" ]; then
	echo "Provides: ${provides}" >> CASTOR.spec
    fi
    #
    ## Get conflicts
    #
    conflicts=`cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      map {if (/([^:]+):(.+)/) {$this{$1}=$2};} split("\n",$this);
      if (defined($this{$what})) {
        print "$this{$what}\n";
      }' $package Conflicts |
      sed 's/ //g'`
    if [ -n "${conflicts}" ]; then
	echo "Conflicts: ${conflicts}" >> CASTOR.spec
    fi
    #
    ## Get description
    #
    echo "%description -n $package" >> CASTOR.spec
    cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      $this =~ s/.*Description: +//sg;
      $this =~ s/\n*$//sg;
      $this =~ s/\n/ \- /sg;
      $this =~ s/  */ /sg;
      print "$this\n";' $package >> CASTOR.spec
    #
    ## Get file list
    #
    echo "%files -n $package" >> CASTOR.spec
    echo "%defattr(-,root,root)" >> CASTOR.spec
    if [ -s "debian/$package.init" ]; then
	echo "%attr(0755,root,bin) %config /etc/init.d/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.logrotate" ]; then
	echo "%attr(0644,root,root) %config(noreplace) /etc/logrotate.d/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.manpages" ]; then
	for man in `cat debian/$package.manpages | sed 's/debian\/castor\///g'`; do
	    echo "%attr(0644,root,bin) %doc /$man" >> CASTOR.spec
	done
    fi
    if [ -s "debian/$package.dirs" ]; then
	for dir in `cat debian/$package.dirs`; do
	    echo "%attr(-,root,bin) %dir /$dir" >> CASTOR.spec
	done
    fi
    if [ -s "debian/$package.cron.d" ]; then
	echo "%attr(0755,root,root) %config(noreplace) /etc/cron.d/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.cron.daily" ]; then
	echo "%attr(0755,root,root) %config(noreplace) /etc/cron.daily/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.cron.hourly" ]; then
	echo "%attr(0755,root,root) %config(noreplace) /etc/cron.hourly/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.cron.monthly" ]; then
	echo "%attr(0755,root,root) %config(noreplace) /etc/cron.monthly/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.cron.weekly" ]; then
	echo "%attr(0755,root,root) %config(noreplace) /etc/cron.weekly/$package" >> CASTOR.spec
    fi
    if [ -s "debian/$package.install.perm" ]; then
	cp -f debian/$package.install.perm debian/$package.install.perm.tmp
	#
	## Add a missing '/'
	#
	perl -pi -e 's/\) /\) \//g' debian/$package.install.perm.tmp
	#
	## Handle the case of config files unless for castor-lsf-tools where I know this
	## regexp will generate a bad output
	#
	if [ "$package" != "castor-lsf-tools" ]; then
	    perl -pi -e 's/\/etc\//\%config\(noreplace\) \/etc\//g' debian/$package.install.perm.tmp
	fi
	lib_or_lib64=0
	if [ "$package" != "castor-dbtools" ]; then
	    #
	    ## Check if we have to play with %ifarch
	    #
	    grep -q /lib debian/$package.install.perm
	    [ $? -eq 0 ] && lib_or_lib64=1
	fi
	if [ $lib_or_lib64 -eq 0 ]; then
	    cat debian/$package.install.perm.tmp >> CASTOR.spec
	else
	    echo "%ifarch x86_64" >> CASTOR.spec
	    cat debian/$package.install.perm.tmp | sed 's/\/lib\//\/lib64\//g' >> CASTOR.spec
	    echo "%else" >> CASTOR.spec
	    cat debian/$package.install.perm.tmp >> CASTOR.spec
	    echo "%endif" >> CASTOR.spec
	fi
	rm -f debian/$package.install.perm.tmp
    fi
    if [ "$package" != "castor-service" ]; then
        #
        ## Get %post section
        #
	if [ -s "debian/$package.postinst" ]; then
	    echo "%post -n $package" >> CASTOR.spec
	    echo "if [ \$1 -ge 1 ]; then" >> CASTOR.spec
	    cat debian/$package.postinst | sed 's/\${1+\"$@"}/configure/g' | grep -v /bin/sh | grep -v exit >> CASTOR.spec
	    echo "/bin/true" >> CASTOR.spec
	    echo "fi" >> CASTOR.spec
	    # Force ldconfig for packages with a shared library
	    [ "$package" = "castor-lib" ] && echo "/sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-oracle" ] && echo "/sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-mysql" ] && echo "/sbin/ldconfig" >> CASTOR.spec
	else
	    # Force ldconfig for packages with a shared library
	    [ "$package" = "castor-lib" ] && echo "%post -n $package -p /sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-oracle" ] && echo "%post -n $package -p /sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-mysql" ] && echo "%post -n $package -p /sbin/ldconfig" >> CASTOR.spec
	fi
        #
        ## Get %preun section
        #
	if [ -s "debian/$package.prerm" ]; then
	    echo "%preun -n $package" >> CASTOR.spec
	    echo "if [ \$1 -eq 0 ]; then" >> CASTOR.spec
	    cat debian/$package.prerm | sed 's/\${1+\"$@"}/remove/g' | grep -v /bin/sh | grep -v exit >> CASTOR.spec
	    echo "/bin/true" >> CASTOR.spec
	    echo "fi" >> CASTOR.spec
	fi
        #
        ## Get %postun section
        #
	if [ -s "debian/$package.postrm" ]; then
	    echo "%postun -n $package" >> CASTOR.spec
	    echo "if [ \$1 -eq 0 ]; then" >> CASTOR.spec
	    cat debian/$package.postrm | sed 's/\${1+\"$@"}/remove/g' | grep -v /bin/sh | grep -v exit >> CASTOR.spec
	    echo "/bin/true" >> CASTOR.spec
	    echo "fi" >> CASTOR.spec
	    # Force ldconfig for packages with a shared library
	    [ "$package" = "castor-lib" ] && echo "/sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-oracle" ] && echo "/sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-mysql" ] && echo "/sbin/ldconfig" >> CASTOR.spec
	else
	    # Force ldconfig for packages with a shared library
	    [ "$package" = "castor-lib" ] && echo "%postun -n $package -p /sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-oracle" ] && echo "%postun -n $package -p /sbin/ldconfig" >> CASTOR.spec
	    [ "$package" = "castor-lib-mysql" ] && echo "%postun -n $package -p /sbin/ldconfig" >> CASTOR.spec
	fi
    fi
    if_has_oracle $this
    if [ $? -eq 1 ]; then
	echo "%endif" >> CASTOR.spec
    fi
    if_has_lsf $this
    if [ $? -eq 1 ]; then
	echo "%endif" >> CASTOR.spec
    fi
    if_has_stk_ssi $this
    if [ $? -eq 1 ]; then
	echo "%endif" >> CASTOR.spec
    fi
    echo >> CASTOR.spec
done

cd ..

echo "### INFO ### Creating tarball"

tar -zcf castor-${a}.${b}.${c}.tar.gz castor-${a}.${b}.${c}

cd castor-${a}.${b}.${c}
