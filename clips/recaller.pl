#!/usr/bin/perl -w

use strict;
use diagnostics;
use POSIX;

my @input = split(" ",<>);

my $policyName = shift @input;

my $rc = system("/usr/bin/perl -w $policyName @input");

exit(EXIT_SUCCESS);
