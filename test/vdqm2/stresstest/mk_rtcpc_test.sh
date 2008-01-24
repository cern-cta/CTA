#!/bin/sh

AWK_FILE=mk_rtcpc_test.awk

if test ! -f $AWK_FILE; then
  echo
  echo "Cannot find $AWK_FILE in current working directory"
  echo
  exit -1
fi

function usage
(
  echo    "Usage:"
  echo -e "\t$0 extension"
  echo    "Where:"
  echo -e "\textension is the file extension of the rtcopyd log file"
)

if test $# -ne 1; then
  echo
  echo "Wrong number of command-line arguments"
  echo
  usage
  echo
  exit -1
fi

fname="`uname -n`_$1_rtcpc_test.sh"
echo '#\!/bin/sh -f' > $fname
#echo 'setenv VDQM_PORT 8888' >> $fname
#echo 'setenv VDQM_HOST shd49' >> $fname
#echo 'setenv RTCOPYPORT 8889' >> $fname

egrep 'tpread|tpwrite' rtcopyd.$1 | cut -d' ' -f1-15 | awk -f $AWK_FILE >> $fname
chmod +x $fname 
echo $fname ready
