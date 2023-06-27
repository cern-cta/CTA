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

kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh

NB_FILES=100000
NB_DIRS=25
FILE_SIZE_KB=1
NB_PROCS=200
NB_DRIVES=10

# Need CTAADMIN_USER krb5
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

echo "Putting all tape drives up"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin dr up '.*'

#kubectl -n ${NAMESPACE} exec ctacli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --maxdrivesallowed ${NB_DRIVES} --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ctacli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ctacli -- cta-admin vo ch --vo vo --writemaxdrives ${NB_DRIVES} --readmaxdrives ${NB_DRIVES}

kubectl -n ${NAMESPACE} exec ctacli -- cta-admin mp ls

kubectl -n ${NAMESPACE} exec ctacli -- cta-admin dr ls

kubectl -n ${NAMESPACE} exec ctaeos -- eos fs config 1 scaninterval=0

# install eos-debuginfo (600MB -> only for stress tests)
# NEEDED because eos does not leave the coredump after a crash
kubectl -n ${NAMESPACE} exec ctaeos -- yum install -y eos-debuginfo

kubectl -n ${NAMESPACE} exec ctafrontend -- yum install -y xrootd-debuginfo

echo "Installing parallel"
kubectl -n ${NAMESPACE} exec client -- bash -c "yum -y install parallel" || exit 1
kubectl -n ${NAMESPACE} exec client -- bash -c "echo 'will cite' | parallel --bibtex" || exit 1

ls | xargs -I{} kubectl -n ${NAMESPACE} cp {} client:/root/{}
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ctaeos:/root/

echo
echo "Setting up environment for tests."
kubectl -n ${NAMESPACE} exec client -- bash -c "/root/client_setup.sh -n ${NB_FILES} -N ${NB_DIRS} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -v -r" || exit 1

TEST_PRERUN=". /root/client_env "
TEST_POSTRUN=""

VERBOSE=1
if [[ $VERBOSE == 1 ]]; then
  TEST_PRERUN="tail -v -f /mnt/logs/tpsrv0*/rmcd/cta/cta-rmcd.log & export TAILPID=\$! && ${TEST_PRERUN}"
  TEST_POSTRUN=" && kill \${TAILPID} &> /dev/null"
fi

echo
echo "Launching immutable file test on client pod"
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && echo yes | cta-immutable-file-test root://\${EOSINSTANCE}/\${EOS_DIR}/immutable_file ${TEST_POSTRUN} || die 'The cta-immutable-file-test failed.'" || exit 1

echo
echo "Launching client_ar.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
echo " Retrieving them as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && /root/client_ar.sh ${TEST_POSTRUN}" || exit 1
## Do not remove as listing af is not coming back???
#kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -A -N ${NB_DIRS} -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -e ctaeos -d /eos/ctaeos/preprod -v || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

exit 0
