#/******************************************************************************
# *                   castor_tools.pm
# *
# * This file is part of the Castor project.
# * See http://castor.web.cern.ch/castor
# *
# * Copyright (C) 2003  CERN
# * This program is free software; you can redistribute it and/or
# * modify it under the terms of the GNU General Public License
# * as published by the Free Software Foundation; either version 2
# * of the License, or (at your option) any later version.
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# *
# * @(#)$RCSfile: castor_tools.pm,v $ $Revision: 1.2 $ $Release$ $Date: 2005/07/28 09:40:26 $ $Author: sponcec3 $
# *
# *
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/


package castor_tools;
require Exporter;

our @ISA = qw(Exporter);
our @EXPORT =qw( castor_conf_getOraStagerSvc @SubreqStatus);

use strict;
use DBI;

our $ORASTAGERCONFIG = "/etc/castor/ORASTAGERCONFIG";

sub castor_conf_getOraStagerSvc {
  my $user = "";
  my $passwd = "";
  my $dbname = "";

  open (CONF, $ORASTAGERCONFIG) or return ("", "");
  while (<CONF>) {
    if (/OraCnvSvc\s+user\s+(\w+)/) {
      $user = $1;
    }
    if (/OraCnvSvc\s+passwd\s+(\w+)/) {
      $passwd = $1;
    }
    if (/OraCnvSvc\s+dbName\s+(\w+)/) {
      $dbname = $1;
    }
  }
  close(CONF);
  return ("$user\@$dbname", $passwd);

}

# option parsing configuration
use Getopt::Long;
Getopt::Long::Configure ('bundling');

1;

