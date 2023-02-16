#!/usr/bin/env sh


# All the tests is inside a cpp file.
echo yes | cta-immutable-file-test root://${EOSINSTANCE}/${EOS_DIR}/immutable_file || die "The cta-immutable-file-test failed."
