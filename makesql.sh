#!/bin/sh

# guess version
ver=`grep "^castor" debian/changelog | head -1 | awk '{print $2}' | sed 's/)//' | sed 's/(//'`
tag=`echo $ver | sed 's/\./_/g' | sed 's/-/_/g'`

# generate creation scripts for the db-based CASTOR components
# if $1 != NULL, it is interpreted as destination directory where to install the generated script
tools/makeSqlScripts.sh castor $tag castor/db $1
tools/makeSqlScripts.sh repack $tag castor/repack $1
tools/makeSqlScripts.sh vdqm $tag castor/vdqm $1
tools/makeProCSqlScripts.sh dlf $tag dlf/scripts/oracle $1
tools/makeProCSqlScripts.sh cns $tag ns $1
tools/makeProCSqlScripts.sh upv $tag upv $1
tools/makeProCSqlScripts.sh vmgr $tag vmgr $1

