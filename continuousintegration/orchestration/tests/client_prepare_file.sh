#!/bin/sh

EOSINSTANCE=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 [-e EOSINSTANCE] 
EOF
exit 1
}

FILES_LOCATION=( )

while getopts "e:f:" o; do
    case "${o}" in
        f)
            FILES_LOCATION+=( "$OPTARG" )
            ;;
        e)
            EOSINSTANCE=${OPTARG}
            ;;
    esac
done
shift $((OPTIND-1))

if [ "x${FILES_LOCATION}" = "x" ]; then
    die "Files location in a list should be provided"
fi

# get some common useful helpers for krb5
. /root/client_helper.sh

nbFilesToRetrieve=${#FILES_LOCATION[@]}

KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0

echo ${FILES_LOCATION[@]} | XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xargs --max-procs=10 -n 40 xrdfs ${EOSINSTANCE} prepare -s

nbFilesRetrieved=0
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=200

declare -A directoriesNbFiles
for filePath in ${FILES_LOCATION[@]}
do
  directoriesNbFiles[$(dirname $filePath)]=0
done

while [[ $nbFilesRetrieved != $nbFilesToRetrieve ]]
do
  nbFilesRetrieved=0
  for directory in ${!directoriesNbFiles[@]}
  do
    nbFilesRetrieved=$((nbFilesRetrieved + `eos root://${EOSINSTANCE} ls -y ${directory} | egrep '^d[1-9][0-9]*::t1' | wc -l`))
  done
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1
  echo "Waiting for file to be retrieved. Seconds passed = $SECONDS_PASSED"
  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    die "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for files to be retrieved from tape"
  fi
done

#fileRetrieved=0
#SECONDS_PASSED=0
#WAIT_FOR_RETRIEVED_FILE_TIMEOUT=50
#while [[ $fileRetrieved != 1 ]]
#do
#  fileRetrieved=`eos root://${EOSINSTANCE} ls -y ${FILE_LOCATION} | egrep '^d[1-9][0-9]*::t1' | wc -l`
#  sleep 1
#  let SECONDS_PASSED=SECONDS_PASSED+1
#  echo "Waiting for file to be retrieved. Seconds passed = $SECONDS_PASSED"
#  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
#    die "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved from tape"
#  fi
#done