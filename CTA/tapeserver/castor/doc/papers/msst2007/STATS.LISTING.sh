#!/bin/sh

# ----------------------------------
# STATS.LISTING.sh
# Generate statistic on CASTOR usage
#
# Jean-Damien.Durand@cern.ch
# ----------------------------------
# 24/07/2001           First version
# ----------------------------------


SURVEY=$0.pid
rm -f $SURVEY
touch $SURVEY
echo "$0 ($$) starting on `hostname` at `date`" >> $SURVEY
echo "$0 ($$) starting on `hostname` at `date`"

#
## Go to working directory
#  -----------------------
whoami=`whoami`
if [ ${whoami} = "baud" ]; then
    #
    ## The 'official' sql script is there
    #
    SQL=/afs/cern.ch/user/b/baud/CASTOR/ns/usages.sql
    cd /afs/cern.ch/user/b/baud/CASTOR/ns
else
    SQL=/afs/cern.ch/user/m/mmarques/ADM/CASTOR/usages.sql
    if [ ${whoami} = "mmarques" ]; then
        cd /afs/cern.ch/user/m/mmarques/ADM/CASTOR
    fi
fi

echo "... Working directory is: `pwd`"

#
## Make sure CASTOR environment is loaded
#  --------------------------------------

if ! [ -x /afs/cern.ch/project/castor/ADM/castor_conf.sh ]; then
    echo "ERROR :: /afs/cern.ch/project/castor/ADM/castor_conf.sh is not executable"
    exit 1
fi

. /afs/cern.ch/project/castor/ADM/castor_conf.sh

#
## Make sure SQL script is readable
#  --------------------------------
if ! [ -r $SQL ]; then
    echo "ERROR :: $SQL is not readable"
    exit 1
fi

#
## Try to make sure sqlplus is available
#  -------------------------------------
sqlplus=`which sqlplus`
if [ $? -ne 0 ]; then
    if [ -r /afs/cern.ch/user/m/mmarques/oracle_env.sh ]; then
        echo "==========================================================="
        echo "No sqlplus in your path - sourcing HePiX Oracle environment"
        echo "==========================================================="
        . /afs/cern.ch/user/m/mmarques/oracle_env.sh
    else
        echo "============================================================"
        echo "No sqlplus in your path - No HePiX Oracle environment - Exit"
        echo "============================================================"
        exit 1
    fi
    TWO_TASK=`hostname`
    export TWO_TASK
    ORACLE_SID=CASTOR
    export ORACLE_SID
fi

echo "... ORACLE environment is:"
env | grep -i oracle

#
## Move old eventual statistic
#  ---------------------------
[ -r STATS.LISTING ] && mv -f STATS.LISTING STATS.LISTING.bak

#
## Execute sqlplus statements
#  --------------------------
echo "... Executing $SQL"
#sqlplus dscastor/castords@cerndb1 @$SQL
sqlplus dscastor/castords@castor @$SQL

#
## Check return code and output
#  ----------------------------
if [ $? -ne 0 ]; then
    echo "ERROR :: $SQL failed"
    exit 1
fi

#
## Put it on the WWW
#  -----------------
date=`date "+%Y.%m.%d"`
CASTOR_CGI=/afs/cern.ch/project/cndoc/wwwds/cgi-bin/castor
export CASTOR_CGI
echo "... Copy ./STATS.LISTING to $CASTOR_CGI/stat/stats/STATS.LISTING.$date"
cp ./STATS.LISTING $CASTOR_CGI/stat/stats/STATS.LISTING.$date
echo "... Copy ./STATS.LISTING to $CASTOR_CGI/stat/STATS.LISTING.$date"
cp ./STATS.LISTING $CASTOR_CGI/stat/STATS.LISTING.$date
echo "... Generate statistics"
$CASTOR_CGI/stat/STATS.LISTING.pl --generate
echo "... Generate latest global statistics table a-la-javascript"
grep -E "TOTAL" $CASTOR_CGI/stat/stats/STATS.LISTING.$date | $CASTOR_CGI/stat/STATS.LISTING.BITMAP.pl
echo "... Generate summarised graphic"
export REQUEST_METHOD=PUT
export STATS_LISTING_TERMINAL=" Png"
export STATS_LISTING_NOHEADER=1
CASTOR_WWW=/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR
export CASTOR_WWW
rm -f $CASTOR_WWW/global_statistics.png.new
$CASTOR_CGI/stat/STATS.LISTING.pl < /dev/null > $CASTOR_WWW/global_statistics.png.new
mv -f $CASTOR_WWW/global_statistics.png.new $CASTOR_WWW/global_statistics.png
export STATS_LISTING_TERMINAL=" Postscript "
rm -f $CASTOR_WWW/global_statistics.ps.new
$CASTOR_CGI/stat/STATS.LISTING.pl < /dev/null > $CASTOR_WWW/global_statistics.ps.new
mv -f $CASTOR_WWW/global_statistics.ps.new $CASTOR_WWW/global_statistics.ps
echo "$0 ($$) ending on `hostname` at `date`" >> $SURVEY
echo "$0 ($$) ending on `hostname` at `date`"

exit 0
