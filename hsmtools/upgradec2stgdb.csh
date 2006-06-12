#!/bin/tcsh
#
# This script upgrades all castor2 stager databases by running the given SQL file.
#
# @(#)$RCSfile: upgradec2stgdb.csh,v $ $Revision: 1.2 $ $Date: 2006/06/12 08:24:22 $ $Author: itglp $
# 
# @author Castor Dev team, castor-dev@cern.ch
# ********************************************************************************

if ( $#argv == 0 ) then
  echo "Upgrade all castor2 stager databases."
  echo "Usage: $0 script.sqlplus"
  exit
endif

set castordb='c2alicestgdb c2atlasstgdb c2cmsstgdb c2lhcbstgdb c2sc4stgdb castor2stgdb c2teststgdb'
set castordevdb='main_dev main_dev2 main_dev3 main_dev4 main_dev5'

foreach dbu ( $castordevdb )
  echo
  echo \-\-\- upgrading devdb $dbu ...
  sqlplus $dbu/mase52yu@castor_dev @$1
end

foreach db ( $castordb )
  echo
  echo \-\-\- upgrading $db ...
  sqlplus castor_stager/xxxxx@$db @$1
end
