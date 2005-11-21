package Cconstants;

#
# $Id: Cconstants.pm,v 1.1 2005/11/21 13:54:41 jdurand Exp $
#

use strict;                    # Strict coding
use warnings;                  # Verbose messages
use Cfilesystem;               # Filesystem class

BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);
    $VERSION = sprintf "%d.%03d", q$Revision: 1.1 $ =~ /(\d+)/g;

    @ISA         = qw(Exporter);

    # functions
    @EXPORT      = qw();

    # globals and optionnaly exported functions
    @EXPORT_OK   = qw($debug $mode $logfile);
}
our @EXPORT_OK;
our $debug;
our $mode;
our $logfile;

#
## Set to something non-zero to have debug
#
$debug = 0;

#
## Select the access mode for which you request to have a weight of all the filesystems
#
$mode = 0;                # 0=read-only, 1=write-only, 2=read-write

#
## Select log location
#
$logfile = \*STDOUT;
1;
