#!/bin/tcsh
#
# This script upgrades all castor2 stager databases by running the given SQL file.
#
# @(#)$RCSfile: upgradec2stgdb.csh,v $ $Revision: 1.1 $ $Date: 2006/03/22 15:49:15 $ $Author: itglp $
# 
# @author Castor Dev team, castor-dev@cern.ch
# ********************************************************************************

if ( $#argv == 0 ) then
  echo "Upgrade all castor2 stager databases."
  echo "Usage: $0 script.sql"
  exit
endif

set castordb='c2alicestgdb c2atlasstgdb c2cmsstgdb c2lhcbstgdb c2sc4stgdb castor2stgdb'
set castordevdb='main_dev/mase52yu@castor_dev main_dev2/mase52yu@castor_dev main_dev3/mase52yu@castor_dev castor_stager/stager_5698@tcastor2_stagerdb'

foreach db ( $castordb )
  echo
  echo \-\-\- upgrading $db ...
  echo sqlplus castor_stager/xxxxx@$db @$1
end

foreach db ( $castordevdb )
  echo
  echo \-\-\- upgrading $db ...
  sqlplus $db @$1
end
