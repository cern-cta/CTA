#!/bin/tcsh
# Usage: deploy-pkg [-nc]
# -nc won't create the debian source, otherwise you have to have already the deb packages
if ( $#argv > 0 ) then
	echo Building the package...
	dpkg-buildpackage -uc -us $1 -rfakeroot
endif
rm $CASTOR_WWW/DIST/debian/binary-i386/gencastor_*
cp ../gencastor_*.deb $CASTOR_WWW/DIST/debian/binary-i386
cp ../gencastor_*.dsc ../gencastor_*.tar.gz $CASTOR_WWW/DIST/debian/source
cd $CASTOR_WWW/DIST/debian
echo Scanning binary packages...
rm binary-i386/Packages.gz
dpkg-scanpackages binary-i386 /dev/null | gzip -9c > binary-i386/Packages.gz
rm source/Sources.gz
dpkg-scansources source /dev/null | gzip -9c > source/Sources.gz
cd -
