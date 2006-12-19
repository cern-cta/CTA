#!/bin/tcsh
#
# This script upgrades all castor2 stager databases by running the given SQL file.
#
# @(#)$RCSfile: upgradec2stgdb.csh,v $ $Revision: 1.6 $ $Date: 2006/12/19 15:40:12 $ $Author: itglp $
# 
# @author Castor Dev team, castor-dev@cern.ch
# ********************************************************************************

if ( $#argv == 0 ) then
  echo "Upgrade all castor2 stager databases."
  echo "Usage: $0 script.sqlplus [-d]"
  echo "       -d  upgrade dev instances too"
  exit
endif

set castordb='c2alicestgdb c2atlasstgdb c2cmsstgdb c2lhcbstgdb c2publicstgdb c2itdcstgdb c2teststgdb'
set castordevdb='main_dev main_dev2 main_dev3 main_dev4 main_dev5 main_dev6'

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

