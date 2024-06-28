#!/bin/bash

diskInstance=${1:-localhost}
output=$(/usr/bin/eos root://$diskInstance space ls -m)
# we're interested in the result for the default namespace
freespace=$(echo "$output" | awk -F' ' '/name=default/ { for (i=1; i<=NF; i++) if ($i ~ /sum.stat.statfs.freebytes\?configstatus@rw=/) print $i }' | awk -F'=' '{print $2}')


echo "{\"freeSpace\": $freespace}"
