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

COLLABORATION_DIR="$CASTOR_SRC/castor/tape/doc/collaboration_diagrams"

if test ! -d $COLLABORATION_DIR; then
  echo "Error: The following directory does not exist"
  echo
  echo "\"$COLLABORATION_DIR\""
  echo
  exit -1
fi

which dot > /dev/null
if test $? -ne 0; then
  echo "Error: The dot command is not on the path"
  echo
  exit -1
fi

DOTS=`ls $COLLABORATION_DIR/*.dot`

for DOT in $DOTS; do
  SVG=`echo $DOT | sed 's/\.dot$/.svg/'`

  echo "Generating $SVG"

  if test ! -f $DOT; then
    echo "Error: The following dot file does not exist:"
    echo
    echo "\"$DOT\""
    echo
    exit -1
  fi

  dot -T svg $DOT > $SVG
done

exit
