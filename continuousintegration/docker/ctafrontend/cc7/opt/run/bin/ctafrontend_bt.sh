#!/bin/bash

COREFILE=$(ls /var/log/cta/core* | head -n 1)

test -z ${COREFILE} && (echo "NO COREFILE FOUND, EXITING"; exit 1)

echo "PROCESSING COREFILE: ${COREFILE}"

yum install -y xrootd-debuginfo cta-debuginfo

cat <<EOF > /tmp/ctabt.gdb
file /usr/bin/xrootd
core ${COREFILE}
bt
quit
EOF

gdb -x /tmp/ctabt.gdb > ${COREFILE}.bt

echo "BACKTRACE AVAILABLE IN ${COREFILE}.bt"
exit 0
