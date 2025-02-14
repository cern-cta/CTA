### Run the following steps inside the client (client-0) pod to test the xrdcp of a file - the CREATE workflow
EOS_MGM_HOST="ctaeos"
EOS_INSTANCE_NAME="ctaeos"

REPORT_DIRECTORY=/var/log

# would be eosdf but apparently this will not archive stuff to tape
EOSDF_BUFFER_BASEDIR=/eos/ctaeos/cta
EOSDF_BUFFER_URL=${EOSDF_BUFFER_BASEDIR}
TEST_FILE_NAME=testfile1_eosdf

echo "foo" > /root/${TEST_FILE_NAME}

xrdcp /root/${TEST_FILE_NAME} root://${EOS_INSTANCE_NAME}/${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}

## maybe these in order to get the test to work:
EOSDF_BUFFER_BASEDIR=/eos/ctaeos/eosdf
EOSDF_BUFFER_URL=${EOSDF_BUFFER_BASEDIR}

## in the ctaeos pod run the two following lines before starting the test
eos mkdir ${EOSDF_BUFFER_URL}
eos chmod 1777 ${EOSDF_BUFFER_URL}

# add test for the PREPARE workflow
xrdfs ${EOS_MGM_HOST} prepare -s ${EOSDF_BUFFER_URL}/${TEST_FILE_NAME}
