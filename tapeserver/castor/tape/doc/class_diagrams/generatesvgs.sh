#!/bin/bash

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

if test "x$UMLGRAPH_HOME" = x; then
  echo "Error: The environment variable UMLGRAPH_HOME is not set"
  echo
  echo "UMLGRAPH_HOME should be the full path to the UMLGraph installation"
  echo
  echo "    export UMLGRAPH_HOME=/usr/local/UMLGraph/UMLGraph-5.2"
  echo
  exit -1
fi

UMLGRAPH_JAR=$UMLGRAPH_HOME/lib/UmlGraph.jar

for CLASS_DIAGRAM in `echo *.java | sed 's/\.java//g'`; do
  java -classpath $UMLGRAPH_JAR org.umlgraph.doclet.UmlGraph -package $CLASS_DIAGRAM.java
  dot -Tsvg graph.dot > $CLASS_DIAGRAM.svg
done
