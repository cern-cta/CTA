#!/bin/bash

source /etc/sysconfig/cta-taped
# for debugging
# echo "arguments passed: $@"
# eos:instance_name:eos_space
diskInstance=${1:-ctaeos}
spaceName=${2:-default}
# diskInstance=ctaeos
output=$(XrdSecSSSKT=$XrdSecSSSKT XrdSecPROTOCOL=$XrdSecPROTOCOL eos root://$diskInstance space ls -m)
if [[ $? -ne 0 ]]; then
  exit 1
fi
# we're interested in the result for the default namespace
freespace=$(echo "$output" | awk -v spaceName="$spaceName" -F ' ' '$0 ~ "name="spaceName {
  for (i=1; i<=NF; i++) if ($i ~ /sum.stat.statfs.freebytes\?configstatus@rw=/) {
      print substr($i, index($i, "=")+1)
    }
}')

echo "{\"freeSpace\":$freespace}"
