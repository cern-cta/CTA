#!/bin/csh -f
set fname="`uname -n`_$1_rtcpc_test.sh"
echo '#\!/bin/csh -f' >$fname
echo 'setenv VDQM_PORT 8888' >>$fname
echo 'setenv VDQM_HOST shd49' >>$fname
echo 'setenv RTCOPYPORT 8889' >>$fname
egrep 'tpread|tpwrite' rtcopyd.$1 | cut -d' ' -f1-15 | awk '{for (i=1;i<=NF;i++) {if (index($i,"-V") != 0 || index($i,"-v") != 0 ) {i++; vid=$i; reqtype=26; if ($5 == "tpread") {reqtype=25;}; printf("./rtcpc_test %s T10K60 \"\" \"\" %d\n",vid,reqtype)}};}' >>$fname
chmod +x $fname 
echo $fname ready
