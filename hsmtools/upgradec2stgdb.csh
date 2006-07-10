#!/bin/tcsh
#
# This script upgrades all castor2 stager databases by running the given SQL file.
#
# @(#)$RCSfile: upgradec2stgdb.csh,v $ $Revision: 1.4 $ $Date: 2006/07/10 14:10:58 $ $Author: itglp $
# 
# @author Castor Dev team, castor-dev@cern.ch
# ********************************************************************************

if ( $#argv == 0 ) then
  echo "Upgrade all castor2 stager databases."
  echo "Usage: $0 script.sqlplus [-d]"
  echo "       -d  upgrade dev instances too"
  exit
endif

set castordb='c2alicestgdb c2atlasstgdb c2cmsstgdb c2lhcbstgdb c2sc4stgdb castor2stgdb c2teststgdb'
set castordevdb='main_dev main_dev2 main_dev3 main_dev4 main_dev5'

if ( $#argv == 2 && "$2" == "-d" ) then
foreach dbu ( $castordevdb )
  echo
  echo \-\-\- upgrading devdb $dbu ...
  sqlplus $dbu/mase52yu@castor_dev @$1
end
endif

echo Enter stager db password:
set passwd = $<
foreach db ( $castordb )
  echo
  echo \-\-\- upgrading $db ...
  sqlplus castor_stager/$passwd@$db @$1
end

