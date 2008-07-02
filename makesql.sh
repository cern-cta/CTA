#!/bin/sh

# guess version
ver=`grep "^castor" debian/changelog | head -1 | awk '{print $2}' | sed 's/)//' | sed 's/(//'`
tag=`echo $ver | sed 's/\./_/g' | sed 's/-/_/g'`

# generate creation scripts for the db-based CASTOR components
# if $1 != NULL, it is interpreted as destination directory where to install the generated script
tools/makeSqlScripts.sh castor castor/db $tag $1
tools/makeSqlScripts.sh repack castor/repack $tag $1
tools/makeSqlScripts.sh vdqm castor/vdqm $tag $1
cd dlf/scripts
./makeDlfScripts.sh $tag oracle/oracleCreate.sql oracle/dlf_oracle_create.sql $1
cd ../..

