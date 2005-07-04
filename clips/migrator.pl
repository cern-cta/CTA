#!/usr/bin/perl -w
#
# Parent script for all migrator policies
#

use strict;
use diagnostics;
use POSIX;

my @input = split(" ",<>);

my $policyName = shift @input;

my $rc = system("/usr/bin/perl -w $policyName @input");

exit(EXIT_SUCCESS);
