#!/bin/sh

if test "x$CASTOR_CVS" = x; then
  echo "Error: The environment variable CASTOR_CVS is not set"
  echo
  echo "CASTOR_CVS should be the full path to the CASTOR source code up to and"
  echo "including CASTOR2, e.g."
  echo
  echo "    export CASTOR_CVS=/usr/local/src/CASTOR2"
  echo
  exit -1
fi

if test ! -d $CASTOR_CVS; then
  echo "Error: The directory specified by CASTOR_CVS does not exist"
  echo
  echo "CASTOR_CVS=\"$CASTOR_CVS\""
  echo
  exit -1
fi

SEQUENCE_DIR="$CASTOR_CVS/castor/tape/doc/sequence_diagrams"

if test ! -d $SEQUENCE_DIR; then
  echo "Error: The following directory does not exist"
  echo
  echo "\"$SEQUENCE_DIR\""
  echo
  exit -1
fi

SEQUENCE_PIC="$SEQUENCE_DIR/sequence.pic"
if test ! -e $SEQUENCE_PIC; then
  echo "Error: The file $SEQUENCE_PIC does not exist"
  echo
  echo "One way to install it:"
  echo
  echo "UMLGRAPH_DIR=/usr/UMLGraph"
  echo "mkdir \$UMLGRAPH_DIR"
  echo "cd \$UMLGRAPH_DIR"
  echo "wget 'http://www.umlgraph.org/UMLGraph-5.2.tar.gz'"
  echo "tar -xvzf UMLGraph-5.2.tar.gz"
  echo
  echo "CASTOR_CVS=/usr/local/src/CASTOR2"
  echo "ln -s \$UMLGRAPH_DIR/UMLGraph-5.2/src/sequence.pic \$CASTOR_CVS/castor/tape/doc/sequence_diagrams/sequence.pic"
  echo
  exit -1
fi

which pic2plot > /dev/null
if test $? -ne 0; then
  echo "Error: The pic2plot command is not on the path"
  echo
  exit -1
fi

which ps2pdf > /dev/null
if test $? -ne 0; then
  echo "Error: The ps2pdf command is not on the path"
  echo
  exit -1
fi

PICS=`ls $SEQUENCE_DIR/*.pic | egrep -v '\/sequence.pic$'`

for PIC in $PICS; do
  PDF=`echo $PIC | sed 's/\.pic$/.pdf/'`

  echo "Generating $PDF"

  if test ! -f $PIC; then
    echo "Error: The following pic file does not exist:"
    echo
    echo "\"$PIC\""
    echo
    exit -1
  fi

  pic2plot -T ps $PIC | ps2pdf - $PDF
done

exit
