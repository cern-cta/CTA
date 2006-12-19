#!/bin/tcsh
#
# This script checks the server version and the database revision of all castor2 instances at CERN.
#
# @(#)$RCSfile: checkc2version.csh,v $ $Revision: 1.1 $ $Date: 2006/12/19 15:40:12 $ $Author: itglp $
# 
# @author Castor Dev team, castor-dev@cern.ch
# ********************************************************************************

set castorinstances='alice atlas cms lhcb public itdc test'

echo Enter stager db password:
set passwd = $<
foreach c ( $castorinstances )
  echo
  echo \-\-\- castor${c}
  ssh root@c2${c}stager castor -v
  echo 'SELECT plsqlrevision FROM CastorVersion;' | sqlplus -S castor_stager/$passwd@c2${c}stgdb
end

