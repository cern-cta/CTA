#!/bin/bash

# An idea on how to analyse auth timings

NSD="localhost"
CMD="./authStress"

#Uncomment to turn on secure authentication
#export SECURE_CASTOR=yes
2>&1 ltrace -T -e Cns_ping $CMD --host=$NSD | grep -Po "\d+\.\d+" | awk '{printf "%.3f\n", $1}' | sort | uniq -c
