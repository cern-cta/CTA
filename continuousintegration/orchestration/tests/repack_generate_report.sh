#!/bin/bash

REPORT_DIRECTORY=/var/log
ADD_COPIES_ONLY=0

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 -v <vid> [-r <report_directory>] [-a]
Default report_directory = ${REPORT_DIRECTORY}
-a: Specify if the repack is add copies only or not
EOF
exit 1
}

if [ $# -lt 1 ]
then
  usage
fi;

while getopts "v:r:a" o; do
    case "${o}" in
        v)
            VID=${OPTARG}
            ;;
        r)
            REPORT_DIRECTORY=${OPTARG}
            ;;
        a)
            ADD_COPIES_ONLY=1
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

repackRequest=`admin_cta --json repack ls --vid ${VID}`

echo $repackRequest

if [ "$repackRequest" == "" ];
then
  die "No repack request for this VID."
fi;

echo "Generation of a repack report of the vid ${VID}"

DATE=`date +%d-%m-%y-%H:%M:%S`

ARCHIVE_FILE_LS_RESULT_PATH=${REPORT_DIRECTORY}/af_ls_${DATE}.json
ARCHIVE_FILE_LS_VID_RESULT_PATH=${REPORT_DIRECTORY}/${VID}_af_ls_${DATE}.json
NOT_REPACKED_JSON_PATH=${REPORT_DIRECTORY}/${VID}_report_not_repacked_${DATE}.json
SELF_REPACKED_JSON_PATH=${REPORT_DIRECTORY}/${VID}_report_self_repacked_${DATE}.json
REPACKED_MOVE_JSON_PATH=${REPORT_DIRECTORY}/${VID}_report_repacked_move_${DATE}.json
REPACK_ADD_COPIES_JSON_PATH=${REPORT_DIRECTORY}/${VID}_report_repack_add_copies_${DATE}.json


echo "1. Generate archive file ls result into ${ARCHIVE_FILE_LS_RESULT_PATH} file..."
admin_cta --json archivefile ls --all > ${ARCHIVE_FILE_LS_RESULT_PATH}
echo "OK"

echo "2. Generate all the archive files that are on vid ${VID} into ${ARCHIVE_FILE_LS_VID_RESULT_PATH} file..."
jq -r "[.[] | select (.tf.vid == \"${VID}\")]" ${ARCHIVE_FILE_LS_RESULT_PATH} > ${ARCHIVE_FILE_LS_VID_RESULT_PATH}
echo "OK"

echo "3. Generate the non-repacked files report into ${NOT_REPACKED_JSON_PATH} file..."
jq -r "[.[] | select (.tf.supersededByVid == \"\" and .tf.vid == \"${VID}\")]" ${ARCHIVE_FILE_LS_VID_RESULT_PATH} > ${NOT_REPACKED_JSON_PATH}
echo "OK"

echo "4. Generating the self-repacked files report into ${SELF_REPACKED_JSON_PATH} file..."
jq -r  '[.[] | select((.tf.supersededByVid == .tf.vid) and (.tf.fSeq < .tf.supersededByFSeq))]' ${ARCHIVE_FILE_LS_VID_RESULT_PATH} > ${SELF_REPACKED_JSON_PATH}
echo "OK"

echo "5. Generate the repacked (moved) files report into ${REPACKED_MOVE_JSON_PATH} file..."
jq -r  '[.[] | select((.tf.supersededByVid != "") and (.tf.supersededByVid != .tf.vid))]' ${ARCHIVE_FILE_LS_VID_RESULT_PATH} > ${REPACKED_MOVE_JSON_PATH}
echo "OK"

echo "6. Generate the repack \"just add copies\" report into ${REPACK_ADD_COPIES_JSON_PATH} file..."
storageClass=`jq ".[0] | .af.storageClass" ${ARCHIVE_FILE_LS_VID_RESULT_PATH}`
copyNbToExclude=`jq -r ".[0] | .copyNb" ${ARCHIVE_FILE_LS_VID_RESULT_PATH}`
copyNbs=`admin_cta --json archiveroute ls | jq -r ".[] | select(.storageClass == $storageClass and .copyNumber != $copyNbToExclude) | .copyNumber"`

jq -r "[.[] | select(.copyNb == (\"$copyNbs\" | split(\"\n\")[]))]" ${ARCHIVE_FILE_LS_RESULT_PATH} > ${REPACK_ADD_COPIES_JSON_PATH}
echo "OK"

echo "7. Report of the repacked tape"

if [ ${ADD_COPIES_ONLY} -eq 0 ]; 
then
  echo
  NB_NON_REPACKED_FILES=$(jq '[.[]] | length' ${NOT_REPACKED_JSON_PATH} || 0)
  echo "Number of non repacked files : ${NB_NON_REPACKED_FILES}" 
  if [ ${NB_NON_REPACKED_FILES} -ne 0 ]
  then
    header="ArchiveID\tFSeq\tSize"
    { echo -e $header; jq -r '.[] | [.af.archiveId,.tf.fSeq, .af.size] | @tsv' ${NOT_REPACKED_JSON_PATH}; } | column -t
  fi;
fi;
 
echo
NB_SELF_REPACKED_FILES=$(jq '[.[]] | length' ${SELF_REPACKED_JSON_PATH} || 0)
echo "Number of self-repacked files : ${NB_SELF_REPACKED_FILES}"
if [ ${NB_SELF_REPACKED_FILES} -ne 0 ]
then
  header="ArchiveID\tFSeq\tSize"
  { echo -e $header; jq -r '.[] | [.af.archiveId, .tf.fSeq, .af.size] | @tsv' ${SELF_REPACKED_JSON_PATH}; } | column -t
fi; 
echo

NB_REPACKED_MOVE_FILES=$(jq '[.[]] | length' ${REPACKED_MOVE_JSON_PATH} || 0)
echo "Number of repacked (moved) files : ${NB_REPACKED_MOVE_FILES}"
if [ ${NB_REPACKED_MOVE_FILES} -ne 0 ]
then
  header="DestinationVID\tNbFiles\ttotalSize\n"
  { echo -e $header; jq -r 'group_by(.tf.supersededByVid)[] | [(.[0].tf.supersededByVid),([.[] | .tf.supersededByFSeq] | length),(reduce [.[] | .af.size | tonumber][] as $currentSize (0; . + $currentSize))] | @tsv' ${REPACKED_MOVE_JSON_PATH}; } | column -t
fi;
echo

NB_COPIED_FILES=$(jq '[.[]] | length' ${REPACK_ADD_COPIES_JSON_PATH} || 0)
echo "Number of copied files : $NB_COPIED_FILES"
if [ ${NB_COPIED_FILES} -ne 0 ]
then
  header="DestinationVID\tNbFiles\ttotalSize\n"
  { echo -e $header; jq -r 'group_by(.tf.vid)[] | [(.[0].tf.vid),([.[] | .tf.fSeq] | length),(reduce [.[] | .af.size | tonumber][] as $currentSize (0; . + $currentSize))] | @tsv' ${REPACK_ADD_COPIES_JSON_PATH}; } | column -t
fi;

echo "End of the repack report"