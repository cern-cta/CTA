#!/usr/bin/perl -w

use strict;
use diagnostics;
use POSIX;

my $replicaNb = 1;

END {print "$replicaNb\n";}

my @input = split(" ",<>);

if ($#input != 1) {
    # print STDERR "Usage: echo policyName fileName | $0\n";
    exit(EXIT_FAILURE);
}

my $policyName = shift @input;
my $fileName = shift @input;

if (index($fileName,"/castor/cern.ch/cms/") == 0) {
    #
    ## This is CMS file
    #
    $replicaNb = 3;
}

exit(EXIT_SUCCESS);
