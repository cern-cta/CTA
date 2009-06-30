#!/usr/bin/tcsh

if !($?CASTOR_ROOT) then
  setenv CASTOR_ROOT /usr/local/src/CASTOR2
endif

echo CASTOR_ROOT = $CASTOR_ROOT

rm -f MagicNumbers

echo Parsing Castor constants...
echo "     OBJECTS\n" > MagicNumbers
grep OBJ_ $CASTOR_ROOT/castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers

echo "\n     SERVICES\n" >> MagicNumbers
grep SVC_ $CASTOR_ROOT/castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers

echo "\n     REPRESENTATIONS\n" >> MagicNumbers
grep REP_ $CASTOR_ROOT/castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
echo >> MagicNumbers

foreach f ( `find $CASTOR_ROOT/castor/stager $CASTOR_ROOT/castor/monitoring $CASTOR_ROOT/castor/vdqm $CASTOR_ROOT/castor/repack -name '*.hpp' | grep -v DlfMessage` )
  set output=`grep -H '_' $f | grep '=' | grep -v 'm_' | grep -v "if" | grep -v 'static' | grep -v 'throw' | grep -v '(' | sed 's/\/\/.*$//' | sed 's/^.*\/\([a-zA-Z]*\).hpp:/\1/'`
  if !("$output" == "") then
    echo $output | sed 's/^ *\([^ ]*\).*$/    \1\n/' | sed 's/StatusCodes*//' | awk '{print "    ", toupper($1);}' >> MagicNumbers
    echo $output | sed 's/,/\n/g' | sed 's/ *[^ ]* *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
    echo >> MagicNumbers
  endif
end

if $?SRM_ROOT then
  echo Parsing SRM2 constants...
  echo "\n    SRM2 OBJECTS\n" >> MagicNumbers
  grep OBJ_ $SRM_ROOT/srm/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
  echo >> MagicNumbers

  foreach f ( `find $SRM_ROOT/srm -name '*Status.hpp'` )
    set output=`grep '_' $f | grep '=' | grep -v 'm_' | grep -v "if" | sed 's/\/\/.*$//'`
    if !("$output" == "") then
      echo $output | sed 's/^\([^_]*\).*$/    \1\n/' >> MagicNumbers
      echo $output | sed 's/,/\n/g' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
      echo >> MagicNumbers
    endif
  end
endif

a2ps --toc= --columns=4 -f 7.5 -c -o MagicNumbers.ps MagicNumbers
