#!/bin/sh

# check arguments
if [ $# != 3 -a $# != 4 ]; then
  echo usage: $0 releaseTag templateScript targetScript [installDir]
  echo
  exit
fi

if [ $# == 3 ]; then
  rm -rf $3 $3plus
  sed 's/releaseTag/'$1'/g' $2 > $3
  if [ "`echo $2 | grep oracle`" != "" ]; then
    sed 's/^END;/END;\n\//' $3 | sed 's/^\(END castor[a-zA-Z]*;\)/\1\n\//' | sed 's/\(CREATE OR REPLACE TYPE .*\)$/\1\n\//' > $3plus
  fi
  echo Creation scripts for DLF generated with tag $1

else
  # install
  cp $3 $3plus $4
  echo Creation scripts for DLF installed in $4
fi

