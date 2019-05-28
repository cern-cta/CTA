#!/bin/bash

REPORT_DIRECTORY=/var/log

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 -v <vid> [-r <report_directory>]
Default report_directory = ${REPORT_DIRECTORY}
EOF
exit 1
}

if [ $# -lt 1 ]
then
  usage
fi;

while getopts "v:r:" o; do
    case "${o}" in
        v)
            VID=${OPTARG}
            ;;
        r)
            REPORT_DIRECTORY=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

# get some common useful helpers for krb5
. /root/client_helper.sh

# Get kerberos credentials for user1
admin_kinit
admin_klist > /dev/null 2>&1 || die "Cannot get kerberos credentials for user ${USER}"

echo "Generation of a repack report"

DATE=`date +%d-%m-%y-%H:%M:%S`

ARCHIVE_FILE_LS_RESULT_PATH=${REPORT_DIRECTORY}/${VID}_af_ls_${DATE}.json
NOT_REPACKED_JSON_PATH=${REPORT_DIRECTORY}/${VID}_report_not_repacked_${DATE}.json
SELF_REPACKED_JSON_PATH=${REPORT_DIRECTORY}/${VID}_report_self_repacked_${DATE}.json
REPACKED_JSON_PATH=${REPORT_DIRECTORY}/${VID}_report_repacked_${DATE}.json


echo "1. Generate archive file ls result into ${ARCHIVE_FILE_LS_RESULT_PATH} file..."
admin_cta --json archivefile ls --vid ${VID} > ${ARCHIVE_FILE_LS_RESULT_PATH}
echo "OK"

echo "2. Generate the non-repacked files report into ${NOT_REPACKED_JSON_PATH} file..."
jq -r '[.[] | select(.tf.supersededByVid == "")]' ${ARCHIVE_FILE_LS_RESULT_PATH} > ${NOT_REPACKED_JSON_PATH}
echo "OK"

echo "3. Generating the self-repacked files report into ${SELF_REPACKED_JSON_PATH} file..."
jq -r  '[.[] | select((.tf.supersededByVid == .tf.vid) and (.tf.fSeq < .tf.supersededByFSeq))]' ${ARCHIVE_FILE_LS_RESULT_PATH} > ${SELF_REPACKED_JSON_PATH}
echo "OK"

echo "4. Generate the repacked files report into ${REPACKED_JSON_PATH} file..."
jq -r  '[.[] | select((.tf.supersededByVid != "") and (.tf.supersededByVid != .tf.vid))]' ${ARCHIVE_FILE_LS_RESULT_PATH} > ${REPACKED_JSON_PATH}
echo "OK"

echo "5. Report of the repacked tape"
echo
NB_NON_REPACKED_FILES=$(jq '[.[]] | length' ${NOT_REPACKED_JSON_PATH})
echo "Number of non repacked files : ${NB_NON_REPACKED_FILES}" 
if [ ${NB_NON_REPACKED_FILES} -ne 0 ]
then
  header="ArchiveID\tFSeq\tSize"
  { echo -e $header; jq -r '.[] | [.af.archiveId,.tf.fSeq, .af.size] | @tsv' ${NOT_REPACKED_JSON_PATH}; } | column -t
fi; 
echo
NB_SELF_REPACKED_FILES=$(jq '[.[]] | length' ${SELF_REPACKED_JSON_PATH})
echo "Number of self-repacked files : ${NB_SELF_REPACKED_FILES}"
if [ ${NB_SELF_REPACKED_FILES} -ne 0 ]
then
  header="ArchiveID\tFSeq\tSize"
  { echo -e $header; jq -r '.[] | [.af.archiveId, .tf.fSeq, .af.size] | @tsv' ${SELF_REPACKED_JSON_PATH}; } | column -t
fi; 
echo
NB_REPACKED_FILES=$(jq '[.[]] | length' ${REPACKED_JSON_PATH})
echo "Number of repacked files : ${NB_REPACKED_FILES}"
if [ ${NB_REPACKED_FILES} -ne 0 ]
then
  header="DestinationVID\tNbFiles\ttotalSize\n"
  { echo -e $header; jq -r 'group_by(.tf.supersededByVid)[] | [(.[0].tf.supersededByVid),([.[] | .tf.supersededByFSeq] | length),(reduce [.[] | .af.size | tonumber][] as $currentSize (0; . + $currentSize))] | @tsv' ${REPACKED_JSON_PATH}; } | column -t
fi;

echo "End of the repack report"