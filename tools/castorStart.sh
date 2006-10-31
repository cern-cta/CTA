#!/bin/bash
#
# File		$Id: castorStart.sh,v 1.3 2006/10/31 08:34:24 itglp Exp $
#
# Purpose	Start up CASTOR DAEMONS 
#
##################################################################
#                                                                #
# Startup script for castor daemons                              #
#                                                                #
# Felix Ehm, 1.03.2006                                           #
##################################################################
#
# description: Starting the LSF daemons
#

# global vars:
DAEMONS="dlfserver rhserver rmmaster stager rtcpclientd expertd"
CONFIGFILE="/etc/castor/castor.conf"
PID=''

echo ""



# Just in case we forgot the castor config:
if [ ! -f ${CONFIGFILE} ]
then
	echo "${CONFIGFILE} NOT AVAILABLE ! Abort.."
	exit 1
fi





#checks, if the user executing the script is root
function checkRoot() {
#
# Check root
#
	if [ `id -un` != "root" ]
	then
		echo "[ERROR] Not running as root" >&2
		exit 1
	fi
}




#gets the PID and sets the global variable
function getPIDs() {
	PID=`ps -ef |  grep ${DAEMON} | grep -v 'awk' | grep -v 'grep' | grep -v $0 | awk '{print $2}'`
}




#shuts down the castor daemons in global var DAEMONS
function shut_down_running_daemons() {
	for DAEMON in ${DAEMONS[*]}; do
		getPIDs
		#PID=`ps -ef |  grep ${DAEMON} | grep -v 'awk' | grep -v 'grep' | grep -v $0 | awk '{print $2}'`
		if [ ! -z "$PID" ] ; then
			echo $DAEMON " found -> killing: $PID"
			kill -2 $PID > /dev/null 2> /dev/null

			sleep 4 
			getPIDs
			#PID=`ps -ef |  grep ${DAEMON} | grep -v 'awk' | grep -v 'grep'  | awk '{print $2}'`
			if [ ! -z "$PID" ] ; then
						echo "WARNING:"
						echo "was not able to stop " $DAEMON "."
						echo "Please kill process manually!"
						echo "$PID"
				fi
		fi
	done
}





#guess what...
#the deamons are started with no options!
function start_daemons() {
	for DAEMON in ${DAEMONS[*]}; do
		echo "Starting ${DAEMON}.."
		case "$DAEMON" in
		rmmaster)  /usr/bin/rmmaster --lsf ;;
		MigHunter) /usr/bin/MigHunter -d ;;
		*) /usr/bin/${DAEMON} ;;
		esac
	done
}




#in case user wants to kill special daemons
[[ $2 != "" ]] && DAEMONS=$2


case "$1" in
   start)
	$0 restart 
   ;;

   stop)
	checkRoot
        shut_down_running_daemons	# and start the daemons
   ;;

   restart)
	$0 stop $2
	start_daemons
	$0 status $2
   ;;

   status)
	for DAEMON in ${DAEMONS[*]}; do
		getPIDs
		[[ ! -z "$PID" ]] && echo "${DAEMON} (${PID}) is running..."
	done
   ;;

   *)
	echo "Usage: $0 [stop {daemon1,daemon2,..} | start | restart {daemon1,daemon2,..} | status]"
	echo ""
	echo " Possible daemons :     ${DAEMONS} "
	echo ""
	exit 1
esac

echo ""

