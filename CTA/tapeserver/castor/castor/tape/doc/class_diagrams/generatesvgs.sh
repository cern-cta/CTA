#!/bin/bash

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
