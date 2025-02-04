### Run the following steps inside the ctaeos pod to test the xrdcp of a file - the CREATE workflow
EOS_MGM_HOST="ctaeos"
EOS_INSTANCE_NAME="ctaeos"

REPORT_DIRECTORY=/var/log

# would be eosdf but apparently this will not archive stuff to tape
EOSDF_BUFFER_BASEDIR=/eos/ctaeos/cta
EOSDF_BUFFER_URL=${EOSDF_BUFFER_BASEDIR}
TEST_FILE_NAME=testfile1_eosdf

echo "foo" > /root/${TEST_FILE_NAME}

FULL_EOSDF_BUFFER_URL=root://${EOS_INSTANCE_NAME}/${EOSDF_BUFFER_BASEDIR}
echo "Doing xrdcp of ${TEST_FILE_NAME} in the path root://${EOS_INSTANCE_NAME}/${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}"
xrdcp /root/${TEST_FILE_NAME} root://${EOS_INSTANCE_NAME}/${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}
