#!/bin/bash

for COREFILE in $(ls /var/log/tmp/*cta-tpd-*.core); do

test -z ${COREFILE} && (echo "NO COREFILE FOUND, EXITING"; exit 1)

echo "PROCESSING COREFILE: ${COREFILE}"

yum install -y xrootd-debuginfo cta-debuginfo

cat <<EOF > /tmp/ctabt.gdb
file /usr/bin/cta-taped
core ${COREFILE}
thread apply all bt
quit
EOF

gdb -x /tmp/ctabt.gdb > ${COREFILE}.bt

echo "BACKTRACE AVAILABLE IN ${COREFILE}.bt"

done

exit 0
