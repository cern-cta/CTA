#!/usr/bin/perl -w

use strict;
use diagnostics;
use POSIX;

my $replicaNb = 1;

END {print "$replicaNb\n";}

if ($#ARGV != 1) {
    print STDERR "Usage: $0 <svcClassName> <fileName>\n";
    exit(EXIT_FAILURE);
}

my ($svcClassName,$fileName) = (shift,shift);

if (index($fileName,"/castor/cern.ch/cms/") == 0) {
    #
    ## This is CMS file
    #
    $replicaNb = 3;
}

exit(EXIT_SUCCESS);
