#!CPSHELL
#
#  Copyright (C) 1994-1999 by CERN/CN/PDP/DH
#  All rights reserved
#
#       @(#)cpdskdsk.sh	1.6 06/15/99 CERN CN-PDP/DH Jean-Philippe Baud

# cpdskdsk   Copies disk file using rfio for VMS and Unix files
#	     and CLIO for VM files
#
# calling sequence:
#
# cpdskdsk [-b max_block_size] [-F record_format] [-L record_length] [-N nread]
#	[-s size] [-X xparm] [-Z reqid.key@stagehost] filename1 filename2
#
# possible exit codes are :
#
USERR=1		# User error
SYERR=2		# System error
UNERR=3		# Undefined error
CONFERR=4	# Configuration error
#
set -- `getopt b:F:L:N:s:X:Z: $*`
if [ $? -ne 0 ]
then
    exit $USERR
fi
#
size_present=0
while [ $# -gt 0 ]
do  case $1 in
    -b)     blksize=$2; shift;;
    -F)     recfm=$2; shift;;
    -L)     lrecl=$2; shift;;
    -N)     nread=$2; shift;;
    -s)     size=$2; size_present=1; shift;;
    -X)     xparm=$2; shift;;
    -Z)     stagerid=$2; shift;;
    --)     shift; break;;
    *)      echo "Bad parameter: " $1; exit $USERR;;
    esac
    shift
done
#
ifile=$1
ofile=$2
#
# do the copy
#
if [ $size_present -eq 1 ]
then
	rfcpstats=`BINDIR/rfcp -s $size $ifile $ofile`
	rfcprc=$?
else
	rfcpstats=`BINDIR/rfcp $ifile $ofile`
	rfcprc=$?
fi
#
# send statistics to stager
#
if [ $rfcprc -eq 0 ]
then
	echo $rfcpstats 1>&2
	bsent=`echo $rfcpstats | cut -f1 -d' '`
	if [ "$bsent" != "0" ]; then
		ttime=`echo $rfcpstats | cut -f4 -d' '`
	else
		#
		## Output is like: "0 bytes transferred " followed by two exclamation marks
		#
		ttime=0
	fi
	BINDIR/stageupdc -R $rfcprc -s $bsent -T $ttime -Z $stagerid
else
	#
	## Stat outfile
	#
	checkrc=`BINDIR/rfdir $ofile 2>&1`
	if [ $? -eq 0 ]; then
	    finalsize=`echo $checkrc | cut -f5 -d' '`
	else
	    finalsize=0
	fi
	BINDIR/stageupdc -R $rfcprc -s $finalsize -Z $stagerid
fi
stagrc=$?
if [ $stagrc -ne 0 ]
then
	exit $SYERR
fi
#
exit $rfcprc
