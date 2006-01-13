#!/usr/bin/tcsh

rm -f MagicNumbers

echo "    OBJECTS\n" > MagicNumbers
grep OBJ_ ../castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers

echo "\n    SERVICES\n" >> MagicNumbers
grep SVC_ ../castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers

echo "\n    REPRESENTATIONS\n" >> MagicNumbers
grep REP_ ../castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
echo >> MagicNumbers

foreach f ( `find ../castor/stager ../castor/vdqm -name '*Codes.hpp'` )
  set output=`grep '_' $f | grep '=' | grep -v 'm_' | grep -v "if" | sed 's/\/\/.*$//'`
  if !("$output" == "") then
    echo $output | sed 's/^\([^_]*\).*$/    \1\n/' >> MagicNumbers
    echo $output | sed 's/,/\n/g' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
    echo >> MagicNumbers
  endif
end

if ( `echo $SRMDEV_HOME` != '' ) then	
    echo "\n    SRM2 OBJECTS\n" >> MagicNumbers
    grep OBJ_ $SRMDEV_HOME/srm/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
    echo >> MagicNumbers

    foreach f ( `find $SRMDEV_HOME/srm -name '*Status.hpp'` )
      set output=`grep '_' $f | grep '=' | grep -v 'm_' | grep -v "if" | sed 's/\/\/.*$//'`
      if !("$output" == "") then
        echo $output | sed 's/^\([^_]*\).*$/    \1\n/' >> MagicNumbers
        echo $output | sed 's/,/\n/g' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
        echo >> MagicNumbers
      endif
    end
endif

a2ps --columns=4 -f 7.5 -c -o MagicNumbers.ps MagicNumbers
