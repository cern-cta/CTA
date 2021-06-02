# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

#default CI EOS instance
EOSINSTANCE=ctaeos
#default Repack timeout
WAIT_FOR_REPACK_TIMEOUT=300

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

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

kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh
kubectl -n ${NAMESPACE} cp client_ar.sh client:/root/client_ar.sh
kubectl -n ${NAMESPACE} cp multiple_repack.sh client:/root/multiple_repack.sh
kubectl -n ${NAMESPACE} cp repack_systemtest.sh client:/root/repack_systemtest.sh

NB_FILES_PER_TAPE=1000

SIZE_OF_TAPES=10

REPACK_BUFFER_URL=/eos/ctaeos/repack
echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
kubectl -n ${NAMESPACE} exec ctaeos -- eos mkdir ${REPACK_BUFFER_URL}
kubectl -n ${NAMESPACE} exec ctaeos -- eos chmod 1777 ${REPACK_BUFFER_URL}

echo "Enabling all drives"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin dr up ".*"

kubectl -n ${NAMESPACE} exec client -- bash /root/multiple_repack.sh -n ${NB_FILES_PER_TAPE} -s ${SIZE_OF_TAPES} -b ${REPACK_BUFFER_URL} || exit 1
