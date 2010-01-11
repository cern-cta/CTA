#!/bin/sh

# guess version
ver=`grep "^castor" debian/changelog | head -1 | awk '{print $2}' | sed 's/)//' | sed 's/(//'`
tag=`echo $ver | sed 's/\./_/g' | sed 's/-/_/g'`

# generate creation scripts for the db-based CASTOR components
# if $1 != NULL, it is interpreted as destination directory where to install the generated script
tools/makeSqlScripts.sh cns $tag ns $1
tools/makeSqlScripts.sh dlf $tag dlf $1
tools/makeSqlScripts.sh mon $tag monitoring/procedures $1
tools/makeSqlScripts.sh repack $tag castor/repack $1
tools/makeSqlScripts.sh stager $tag castor/db $1
tools/makeSqlScripts.sh cupv $tag upv $1
tools/makeSqlScripts.sh vdqm $tag castor/vdqm $1
tools/makeSqlScripts.sh vmgr $tag vmgr $1
