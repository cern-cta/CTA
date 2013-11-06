#!/bin/sh

# get directories
directory=/afs/cern.ch/project/castor
mandirectory=/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/docs/guides/man

# Then deal with man pages
# loop over files
while [ $# -ge 1 ]
do
  # Check we deal with a man page
  manfile=`echo $1 | grep '.man$'`
  if [ -n $manfile ]; then
    # create directory if needed
    mkdir -p $mandirectory/`dirname $1`
    # create html files from man pages
    rm -f ${mandirectory}/${1}.html
    /afs/cern.ch/project/castor/ADM/man2htm ${directory}/${1} > ${mandirectory}/${1}.html
  fi
  shift
done

