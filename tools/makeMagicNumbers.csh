#!/usr/bin/tcsh

rm -f MagicNumbers

echo Parsing Castor constants...
echo "     OBJECTS\n" > MagicNumbers
grep OBJ_ ../castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers

echo "\n     SERVICES\n" >> MagicNumbers
grep SVC_ ../castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers

echo "\n     REPRESENTATIONS\n" >> MagicNumbers
grep REP_ ../castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
echo >> MagicNumbers

foreach f ( `find ../castor/stager ../castor/vdqm ../castor/repack -name '*.hpp'` )
  set output=`grep -H '_' $f | grep '=' | grep -v 'm_' | grep -v "if" | grep -v 'static' | sed 's/\/\/.*$//' | sed 's/^.*\/\([a-zA-Z]*\).hpp:/\1/'`
  if !("$output" == "") then
    echo $output | sed 's/^ *\([^ ]*\).*$/    \1\n/' | sed 's/StatusCodes*//' | awk '{print "    ", toupper($1);}' >> MagicNumbers
    echo $output | sed 's/,/\n/g' | sed 's/ *[^ ]* *\([^ ]*\) = \([0-9]*\).*$/\2  \1/' >> MagicNumbers
    echo >> MagicNumbers
  endif
end

#set srm=`env | grep SRMDEV_HOME`
if $?SRMDEV_HOME then
  echo Parsing SRM2 constants...
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

echo Creating SQL script for Type2Obj metatable...
rm -f fillType2Obj.sql
echo 'DROP TABLE Type2Obj;' > fillType2Obj.sql
echo 'CREATE TABLE Type2Obj (type INTEGER, object VARCHAR2(100));' >> fillType2Obj.sql
grep OBJ_ MagicNumbers | awk '{print "INSERT INTO Type2Obj VALUES(" $1 ", '\''" $2 "'\'');" }' >> fillType2Obj.sql

a2ps --columns=4 -f 7.5 -c -o MagicNumbers.ps MagicNumbers

