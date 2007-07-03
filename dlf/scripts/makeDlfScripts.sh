#!/bin/sh

# check arguments
if [ $# != 3 ]; then
  echo usage: $0 releaseTag templateScript targetScript
  echo
  exit
fi

rm -rf $3 $3plus
sed 's/releaseTag/'$1'/g' $2 > $3
sed 's/^END;/END;\n\//' $3 | sed 's/^\(END castor[a-zA-Z]*;\)/\1\n\//' | sed 's/\(CREATE OR REPLACE TYPE .*\)$/\1\n\//' > $3plus

