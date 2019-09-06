#!/bin/bash

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

while getopts "n:" o; do
    case "${o}" in
        n)
            NAMESPACE=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ]; then
    usage
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

echo "Preparing namespace for the tests"
./prepare_tests.sh -n ${NAMESPACE}

NB_FILES=1
FILE_SIZE_KB=15

kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh

echo
echo "Launching client_ar.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} cp client_ar.sh client:/root/client_ar.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -v -A || exit 1

REPACK_BUFFER_URL=/eos/ctaeos/repack
echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
kubectl -n ${NAMESPACE} exec ctaeos -- eos mkdir ${REPACK_BUFFER_URL}
kubectl -n ${NAMESPACE} exec ctaeos -- eos chmod 1777 ${REPACK_BUFFER_URL}

source ./repack_helper.sh
kubectl -n ${NAMESPACE} cp repack_systemtest.sh client:/root/repack_systemtest.sh
kubectl -n ${NAMESPACE} cp repack_generate_report.sh client:/root/repack_generate_report.sh

echo
echo "Launching a round trip repack \"just move\" request"

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
echo
  echo "Launching the repack \"just move\" test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m || exit 1
else
  echo "No vid found to repack"
  exit 1
fi

echo "Reclaiming tape ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
echo
  echo "Launching the repack \"just move\" test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m || exit 1
else
  echo "No vid found to repack"
  exit 1
fi

echo "Reclaiming tape ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

echo "Launching a Repack Request on a disabled tape"
VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
  echo "Marking the tape ${VID_TO_REPACK} as disabled"
  kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape ch --disabled true --vid ${VID_TO_REPACK}
  echo "Wating 15 seconds so that the RetrieveQueueStatisticsCache is flushed"
  sleep 15
  echo "Launching the repack request test on VID ${VID_TO_REPACK}
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} || echo "OK" && exit 1
else
  echo "No vid found to repack"
  exit 1
fi;


NB_FILES=1152
kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -v -A || exit 1

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
echo
  echo "Launching the repack test \"just move\" on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m || exit 1
else
  echo "No vid found to repack"
  exit 1
fi

echo "Reclaiming tape ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ] 
then
  echo "Launching the repack \"just add copies\" test on VID ${VID_TO_REPACK} with all copies already on CTA"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -a -g || exit 1
else
  echo "No vid found to repack"
  exit 1
fi

repackJustAddCopiesResult=`kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json re ls | jq -r ". [] | select (.vid == \"${VID_TO_REPACK}\")"`

nbRetrievedFiles=`echo ${repackJustAddCopiesResult} | jq -r ".retrievedFiles"`
nbArchivedFiles=`echo ${repackJustAddCopiesResult} | jq -r ".archivedFiles"`

if [ $nbArchivedFiles == 0 ] && [ $nbRetrievedFiles == 0 ] 
then
  echo "Nb retrieved files = 0 and nb archived files = 0. Test OK"
else
  echo "Repack \"just add copies\" on VID ${VID_TO_REPACK} failed : nbRetrievedFiles = $nbRetrievedFiles, nbArchivedFiles = $nbArchivedFiles"
  exit 1
fi

tapepoolDestination1="ctasystest2"
tapepoolDestination2="ctasystest3"

echo "Creating two destination tapepool : $tapepoolDestination1 and $tapepoolDestination2"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tapepool add --name $tapepoolDestination1 --vo vo --partialtapesnumber 2 --encrypted false --comment "$tapepoolDestination1 tapepool"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tapepool add --name $tapepoolDestination2 --vo vo --partialtapesnumber 2 --encrypted false --comment "$tapepoolDestination2 tapepool"
echo "OK"

echo "Creating archive routes for adding two copies of the file"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin archiveroute add --instance ctaeos --storageclass ctaStorageClass --copynb 2 --tapepool $tapepoolDestination1 --comment "ArchiveRoute2"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin archiveroute add --instance ctaeos --storageclass ctaStorageClass --copynb 3 --tapepool $tapepoolDestination2 --comment "ArchiveRoute3"
echo "OK"

echo "Will change the tapepool of the tapes"

allVID=`kubectl -n ${NAMESPACE}  exec ctacli -- cta-admin --json tape ls --all | jq -r ". [] | .vid"`
read -a allVIDTable <<< $allVID

nbVid=${#allVIDTable[@]}

allTapepool=`kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json tapepool ls | jq -r ". [] .name"`

read -a allTapepoolTable <<< $allTapepool

nbTapepool=${#allTapepoolTable[@]}

nbTapePerTapepool=$(($nbVid / $nbTapepool))

allTapepool=`kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json tapepool ls | jq -r ". [] .name"`
read -a allTapepoolTable <<< $allTapepool


countChanging=0
tapepoolIndice=1 #We only change the vid of the remaining other tapes

for ((i=$(($nbTapePerTapepool+$(($nbVid%$nbTapepool)))); i<$nbVid; i++));
do
  echo "kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape ch --vid ${allVIDTable[$i]} --tapepool ${allTapepoolTable[$tapepoolIndice]}"
  kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape ch --vid ${allVIDTable[$i]} --tapepool ${allTapepoolTable[$tapepoolIndice]}
  countChanging=$((countChanging + 1))
  if [ $countChanging != 0 ] && [ $((countChanging % nbTapePerTapepool)) == 0 ]
  then
    tapepoolIndice=$((tapepoolIndice + 1))
  fi
done

echo "OK"

storageClassName=`kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json storageclass ls | jq -r ". [0] | .name"`
instanceName=`kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json storageclass ls | jq -r ". [0] | .diskInstance"`

echo "Changing the storage class $storageClassName nb copies"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin storageclass ch --instance $instanceName --name $storageClassName --copynb 3
echo "OK"

echo "Putting all drives up"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin dr up VD.*
echo "OK"

echo "Launching the repack \"Move and add copies\" test on VID ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -t 500 || exit 1
