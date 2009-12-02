#!/bin/sh

if test "x$CASTOR_SRC" = x; then
  echo "Error: The environment variable CASTOR_SRC is not set"
  echo
  echo "CASTOR_SRC should be the full path to the CASTOR source code"
  echo
  echo "    export CASTOR_SRC=/usr/local/src/CASTOR_SVN_CO/trunk"
  echo
  exit -1
fi

if test ! -d $CASTOR_SRC; then
  echo "Error: The directory specified by CASTOR_SRC does not exist"
  echo
  echo "CASTOR_SRC=\"$CASTOR_SRC\""
  echo
  exit -1
fi

FSTN_DIR="$CASTOR_SRC/castor/tape/doc/fstns"

if test ! -d $FSTN_DIR; then
  echo "Error: The following directory does not exist"
  echo
  echo "\"$FSTN_DIR\""
  echo
  exit -1
fi

which dot > /dev/null
if test $? -ne 0; then
  echo "Error: The dot command is not on the path"
  echo
  exit -1
fi

which ps2pdf > /dev/null
if test $? -ne 0; then
  echo "Error: The ps2pdf command is not on the path"
  echo
  exit -1
fi

DOTS=`ls $FSTN_DIR/*.dot`

for DOT in $DOTS; do
  PDF=`echo $DOT | sed 's/\.dot$/.pdf/'`

  echo "Generating $PDF"

  if test ! -f $DOT; then
    echo "Error: The following dot file does not exist:"
    echo
    echo "\"$DOT\""
    echo
    exit -1
  fi

  dot -Tps $DOT | ps2pdf - $PDF
done

exit
