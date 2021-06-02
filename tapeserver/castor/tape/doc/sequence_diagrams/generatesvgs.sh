#!/bin/sh

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

SEQUENCE_DIR="$CASTOR_SRC/castor/tape/doc/sequence_diagrams"

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
  echo "CASTOR_SRC=/usr/local/src/CASTOR2"
  echo "ln -s \$UMLGRAPH_DIR/UMLGraph-5.2/src/sequence.pic \$CASTOR_SRC/castor/tape/doc/sequence_diagrams/sequence.pic"
  echo
  exit -1
fi

which pic2plot > /dev/null
if test $? -ne 0; then
  echo "Error: The pic2plot command is not on the path"
  echo
  exit -1
fi

PICS=`ls $SEQUENCE_DIR/*.pic | egrep -v '\/sequence.pic$'`

for PIC in $PICS; do
  SVG=`echo $PIC | sed 's/\.pic$/.svg/'`

  echo "Generating $SVG"

  if test ! -f $PIC; then
    echo "Error: The following pic file does not exist:"
    echo
    echo "\"$PIC\""
    echo
    exit -1
  fi

  pic2plot -T svg $PIC > $SVG
done

exit
