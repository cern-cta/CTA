#!/bin/bash

function exitWithUsage {
  echo "Usage: $0 stagehost svcclass castorns castordstdir filesize nbfiles"
  echo "Where: filesize is in MB (2^20 bytes)"
  exit -1
}

# Check the command-line arguments
if test "x$1" = "x"; then
  echo Error: Missing stagehost
  exitWithUsage
fi
if test "x$2" = "x"; then
  echo Error: Missing svcclass
  exitWithUsage
fi
if test "x$3" = "x"; then
  echo Error: Missing castorns
  exitWithUsage
fi
if test "x$4" = "x"; then
  echo Error: Missing castordstdir
  exitWithUsage
fi
if test "x$5" = "x"; then
  echo Error: Missing filesize
  exitWithUsage
fi
if test "x$6" = "x"; then
  echo Error: Missing nbfiles
  exitWithUsage
fi

# Store the command-line arguments
STAGE_HOST=$1
STAGE_SVCCLASS=$2
CNS_HOST=$3
CASTORDSTDIR=$4
FILESIZE=$5
NBFILES=$6

echo STAGE_HOST=${STAGE_HOST}
echo STAGE_SVCCLASS=${STAGE_SVCCLASS}
echo CNS_HOST=${CNS_HOST}
echo CASTORDSTDIR=${CASTORDSTDIR}
echo FILESIZE=${FILESIZE}
echo NBFILES=${NBFILES}

# Check the destination CASTOR directory exists
if test 0 -ne `2>&1 nsls $CASTORDSTDIR | egrep 'invalid path|No such file or directory' | wc -l`; then
  echo -n "Error: "
  nsls $CASTORDSTDIR
  exit -1
fi

# Set up the CASTOR client environment
export STAGE_HOST
export STAGE_SVCCLASS
export CNS_HOST
export RFIO_USE_CASTOR_V2=YES

# Create the seed file in the /tmp directory
SEEDFILE=/tmp/tapegateway100MBSeedFile_`uuidgen`_`hostname`
echo Creating seed file: ${SEEDFILE}
dd if=/dev/urandom of=${SEEDFILE} bs=1M count=${FILESIZE}
if test ! -f ${SEEDFILE}; then
  echo "Error: Failed to create seed file ${SEEDFILE}"
  exit -1
fi

# For each 100MB source file to be migrated to tape
for I in `seq $NBFILES`; do
  # Create the file from the seed file
  SRCFILE=/tmp/tapegateway100MBSrcFile_${I}_`uuidgen`_`hostname`
  echo ${SRCFILE} > ${SRCFILE}
  cat ${SEEDFILE} >> ${SRCFILE}

  # Clean-up the seed file and exit if the source file was not created
  if test ! -f ${SRCFILE}; then
    rm -f ${SEEDFILE}
    echo "Error: Failed to create source file ${SRCFILE}"
    exit -1
  fi

  # Copy the file into CASTOR
  COPYCMD="/usr/bin/rfcp ${SRCFILE} ${CASTORDSTDIR}"
  echo `date`: ${COPYCMD}
  ${COPYCMD}
  COPYCMDRC=$?

  # Clean up the source file
  rm -f ${SRCFILE}

  # Clean up the seed file and exit if the copy command failed
  if test ${COPYCMDRC} -ne 0; then
    rm -f ${SEEDFILE}
    echo "Error: Failed to execute copy command: ${COPYCMD}: rc=${COPYCMDRC}"
    exit -1
  fi
done

# Clean up the seed file
rm -f ${SEEDFILE}
