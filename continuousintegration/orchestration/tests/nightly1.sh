#!/bin/bash

cd /root
FAIL=0

. client_helper.sh
admin_kdestroy
admin_kinit

for DRIVE in $(admin_cta dr ls | grep Down | sed -e 's/ \+/ /g' | cut -d\  -f2); do
  admin_cta dr up ${DRIVE}
done

for ((i=0;i<4;i++)); do
  echo "Launching bash ./client_ar.sh -n 5000 -s 10 -p 6 -v"
  bash ./client_ar.sh -n 5000 -s 10 -p 6 -v &
done

for job in `jobs -p`
do
echo $job
    wait $job || let "FAIL+=1"
done

if [ "$FAIL" == "0" ];
then
echo "YAY!"
else
echo "FAIL! ($FAIL)"
fi

exit $FAIL
