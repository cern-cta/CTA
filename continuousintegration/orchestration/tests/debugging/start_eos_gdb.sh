EOS_MGM_POD="eos-mgm-0"
EOS_MGM_CONTAINER="eos-mgm"
NAMESPACE="dev"

kubectl cp ./eos_gdb.sh ${EOS_MGM_POD}:/root/ -n ${NAMESPACE} -c ${EOS_MGM_CONTAINER}
kubectl exec -ti ${EOS_MGM_POD} -c ${EOS_MGM_CONTAINER} -n ${NAMESPACE} -- /root/eos_gdb.sh
