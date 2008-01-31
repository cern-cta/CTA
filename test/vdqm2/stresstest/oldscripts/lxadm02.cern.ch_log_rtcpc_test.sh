#!/bin/csh -f
setenv VDQM_PORT 8888
setenv VDQM_HOST shd49
setenv RTCOPYPORT 8889
./rtcpc_test T06925 T10K60 "" "" 26
./rtcpc_test T06305 T10K60 "" "" 25
./rtcpc_test T05795 T10K60 "" "" 25
./rtcpc_test T09327 T10K60 "" "" 25
./rtcpc_test T09145 T10K60 "" "" 25
./rtcpc_test T05675 T10K60 "" "" 25
./rtcpc_test T05201 T10K60 "" "" 25
./rtcpc_test T13973 T10K60 "" "" 26
