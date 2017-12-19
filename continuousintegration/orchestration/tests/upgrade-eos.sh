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

kubectl -n ${NAMESPACE} exec ctaeos -- yum-config-manager --disable cta-artifacts

echo -e "[eos-ci-eos]\nname=EOS CI repo for eos packages\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/citrine/commit/el-7/x86_64/\npriority=4\ngpgcheck=0\nenabled=1\n\n" | kubectl -n ${NAMESPACE} exec -i ctaeos -- bash -c "cat > /etc/yum.repos.d/eos-ci.repo"
echo -e "[eos-ci-eos-depend]\nname=EOS CI repo for eos depend packages\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/citrine-depend/el-7/x86_64/\npriority=4\ngpgcheck=0\nenabled=1\n\n"  | kubectl -n ${NAMESPACE} exec -i ctaeos -- bash -c "cat >> /etc/yum.repos.d/eos-ci.repo"

kubectl -n ${NAMESPACE} exec ctaeos -- eos version

kubectl -n ${NAMESPACE} exec ctaeos -- sed -i '/^.:eos.*/d' /etc/yum/pluginconf.d/versionlock.list

kubectl -n ${NAMESPACE} exec ctaeos -- sed -i '/.*protected=1.*/d' /etc/yum.repos.d/cta-ci.repo

kubectl -n ${NAMESPACE} exec ctaeos -- yum install -y eos-server eos-client

kubectl -n ${NAMESPACE} exec ctaeos -- systemctl restart eos@*

kubectl -n ${NAMESPACE} exec ctaeos -- systemctl status eos@*

kubectl -n ${NAMESPACE} exec ctaeos -- eos version

echo "Launching archive_retrieve.sh:"
./archive_retrieve.sh -n ${NAMESPACE}
