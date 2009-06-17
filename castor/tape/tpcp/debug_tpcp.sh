#!/bin/sh

export LD_LIBRARY_PATH=`find ../../.. -name '*.so*' | sed 's|/[^/]*$||' | sort | uniq`
export LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH | sed 's/ /:/g'`

`pwd`/tpcp $@
