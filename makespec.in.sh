#!/bin/sh
# Generate the spec file from a Debian style package description collection.
output=$1

echo "Output file: ${output}"
tempdir=`mktemp -d`

function copyInstallPermInternal {
    package=$1
    file=$2
    if [ -s "debian/$package.conffiles" ]; then
      for f in `cat debian/$package.conffiles`; do
          g=`echo $f | sed 's/\\//\\\\\\//g'`
          sed -i "s/$g\$/%config(noreplace) $g/" $file
      done
    fi
    cat $file >> ${output}
}

function copyInstallPerm {
    package=$1
    tempdir=$2
    cp -f debian/$package.install.perm $tempdir/$package.install.perm.tmp
    # Add a missing '/'
    perl -pi -e 's/\) /\) \//g' $tempdir/$package.install.perm.tmp
    # compute 64 bits version :
    #   - replace usr/lib by usr/lib64
    #   - except for /usr/lib/perl
    #   - except for /usr/lib/log
    #   - except for /usr/lib/python
    #   - except for /usr/libexec
    #   - replace gcc32dbg by gcc64dbg (gridFTP specific)
    cp -f $tempdir/$package.install.perm.tmp $tempdir/$package.install.perm.64.tmp
    perl -pi -e 's/usr\/lib/usr\/lib64/g;s/usr\/lib64\/perl/usr\/lib\/perl/g;s/usr\/lib64\/python/usr\/lib\/python/g;s/usr\/lib64exec/usr\/libexec/g;s/usr\/lib64\/log/usr\/lib\/log/g;s/gcc32dbg/gcc64dbg/g' $tempdir/$package.install.perm.64.tmp
    # check whether to bother with 64 bits specific code
    diff -q $tempdir/$package.install.perm.tmp $tempdir/$package.install.perm.64.tmp > /dev/null
    if [ $? -eq 0 ]; then
      copyInstallPermInternal $package $tempdir/$package.install.perm.tmp
    else
      echo "%ifarch x86_64" >> ${output}
      copyInstallPermInternal $package $tempdir/$package.install.perm.64.tmp
      echo "%else" >> ${output}
      copyInstallPermInternal $package $tempdir/$package.install.perm.tmp
      echo "%endif" >> ${output}
    fi
}

#
## Append all sub-packages to CASTOR.spec
#
for this in `grep Package: debian/control | awk '{print $NF}'`; do
    package=$this
    actualPackage=$package

    #
    ## get package group if any
    #
    group=`cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      map {if (/([^:]+):(.+)/) {$this{$1}=$2};} split("\n",$this);
      if (defined($this{$what})) {
        print "$this{$what}\n";
      }' $package XBS-Group |
      sed 's/ //g' | sed 's/\${[^{},]*}//g' | sed 's/^,*//g' | sed 's/,,*/,/g'`
    if [ "${group}" != "Client" ]; then
        echo "%if ! %compile_client" >> $output # no a client package
    fi
    echo "%package -n $actualPackage" >> $output
    echo "Summary: Cern Advanced mass STORage" >> $output
    echo "Group: Application/Castor" >> $output
    #
    ## Except for Description that is truely a multi-line thing even within debian/control, the other
    ## fields are always on a single line:
    ## The following perl script isolates the Package: definition of a package, then split it using
    ## newlines, then look which line matches the keyword.
    ## Then we remove spaces
    #
    ## Get Requires
    #
    ## Requires might give ${shlibs:Depends},${misc:Depends} in the output: we will drop that
    ## This is why there are extra sed's after the original that remove the spaces
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
      sed 's/^[ \t]*//' | sed 's/\${[^{},]*}//g' | sed 's/^,*//g' | sed 's/,,*/,/g'`
    if [ -n "${requires}" ]; then
        echo "Requires: ${requires}" >> $output
    fi
    #
    ## Get BuildRequires
    #
    buildrequires=`cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      map {if (/([^:]+):(.+)/) {$this{$1}=$2};} split("\n",$this);
      if (defined($this{$what})) {
        print "$this{$what}\n";
      }' $package Build-Depends`
    if [ -n "${buildrequires}" ]; then
        echo "BuildRequires: ${buildrequires}" >> $output
    fi
    #
    ## Get Conditional BuildRequires
    #
    buildrequirescond=`cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      $this =~ /Build-Depends-Conditional:(.+%endif)/sg;
      $this = $1;
      print "$this\n";' $package`
    if [ -n "${buildrequirescond}" ]; then
        echo "${buildrequirescond}" >> $output
    fi
    #
    ## Get Provides
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
        echo "Provides: ${provides}" >> $output
    fi
    #
    ## Get Conflicts
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
        echo "Conflicts: ${conflicts}" >> $output
    fi
    #
    ## Get Obsoletes
    #
    obsoletes=`cat debian/control | perl -e '
      $package=shift;
      $what=shift;
      $this = do { local $/; <> };
      $this =~ s/.*Package: $package[^\w\-]//sg;
      $this =~ s/Package:.*//sg;
      map {if (/([^:]+):(.+)/) {$this{$1}=$2};} split("\n",$this);
      if (defined($this{$what})) {
        print "$this{$what}\n";
      }' $package XBS-Obsoletes |
      sed 's/ //g'`
    if [ -n "${obsoletes}" ]; then
        echo "Obsoletes: ${obsoletes}" >> $output
    fi
    #
    ## Get description
    #
    echo "%description -n $actualPackage" >> $output
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
      print "$this\n";' $package >> $output
    #
    ## Get file list
    #
    echo "%files -n $actualPackage" >> $output
    echo "%defattr(-,root,root)" >> $output
    ## deal with manpages
    if [ -s "debian/$package.manpages" ]; then
        for man in `cat debian/$package.manpages | sed 's/debian\/castor\///g'`; do
            echo "%attr(0644,root,bin) %doc /$man" >> $output
        done
    fi
    ## deal with directories
    if [ -s "debian/$package.dirs" ]; then
        cat debian/$package.dirs | while read dir; do
            if [ `echo $dir | grep -c %attr` -lt 1 ]; then
                echo "%attr(-,root,bin) %dir /$dir" >> $output
            else
                echo $dir | sed -e "s/) /) %dir \//" >> $output
            fi
        done
    fi
    ## deal with files
    if [ -s "debian/$package.install.perm" ]; then
        copyInstallPerm $package $tempdir
    fi
    ## deal with scripts
    if [ -s "debian/$package.postinst" ]; then
        echo "%post -n $actualPackage" >> $output
        cat debian/$package.postinst >> $output
    fi
    if [ -s "debian/$package.preun" ]; then
        echo "%preun -n $actualPackage" >> $output
        cat debian/$package.preun >> $output
    fi
    if [ -s "debian/$package.pre" ]; then
        echo "%pre -n $actualPackage" >> $output
        cat debian/$package.pre >> $output
    fi
    if [ -s "debian/$package.postun" ]; then
        echo "%postun -n $actualPackage" >> $output
        cat debian/$package.postun >> $output
    fi
    if [ "${group}" != "Client" ]; then
        echo "%endif" >> $output # end of client compilation if
    fi
    echo >> $output
done
rm -rf $tempdir

