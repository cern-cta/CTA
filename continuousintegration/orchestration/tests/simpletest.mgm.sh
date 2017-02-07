# example test

kubectl --namespace ${NAMESPACE} exec mgm xrdcp /etc/group root://localhost//eos/mgm/cta/toto

kubectl --namespace ${NAMESPACE} exec mgm eos file workflow /eos/mgm/cta/toto default closew

kubectl --namespace ${NAMESPACE} exec mgm eos file workflow /eos/mgm/cta/toto default prepare
