#!/usr/bin/perl -w

#
# $Id: CASTOR.cvslogs.pl,v 1.4 2006/04/20 18:23:46 itglp Exp $
#

use strict;
use diagnostics;
use File::Temp qw/ tempfile tempdir /;

#
## Usage: $0 [CVSLOGOPTION], please do cvs log --help for more. In particular:
#
## Example: rm -f /tmp/logs.txt; $0 "2005-04-27 16:32:45<2010-04-27 16:32:45" > /tmp/logs.txt
##          rm -f /tmp/logs.txt; $0 -rv2_0_3_0::                              > /tmp/logs.txt
##          rm -f /tmp/logs.txt; $0 -rv2_0_3_0::v2_0_3_1                      > /tmp/logs.txt
#

my $input;
my %commits = ();
my %types = ();

sub printit($$) {
    my($commits,$types) = @_;
    my $file;
    my %tmp = ();
    # print STDERR "[" . join("]\n[",(grep {$tmp{$_}++ < 2} sort keys %$commits)) . "]\n";
    foreach $file (sort keys %$commits) {
	printf "  * %s/%s:\n", $$types{$file}, $file;
	my $revisions = $commits{$file};
	my $revision;
	foreach $revision (sort {my $b2 = $b; $b2 =~ s/\.[^\.]+(\..*)//g; my $a2 = $a; $a2 =~ s/\.[^\.]+(\..*)//g; $b2 <=> $a2} keys %$revisions) {
	    my $what;
	    my $date = $$revisions{$revision}{date};
	    my $who = $$revisions{$revision}{who};
	    foreach $what (@{$$revisions{$revision}{what}}) {
		printf "    [r%4s,%s,%s] %s\n", $revision, $date, $who, $what;
	    }
	}
    }
}

# goto doitnow;
my $comm;

$comm = "cd CASTOR2; cvs log \"@ARGV\" . > ../CASTOR2.cvslog.txt";
print STDERR "$comm\n"; system($comm);

# doitnow:
my @input = qw/CASTOR2.cvslog.txt/;
foreach $input (@input) {

    my $type = 'CASTOR2';
    my $working_file = '';
    my $revision = 0;
    my $date = '';
    my $who = '';
    my @what = ();
    open(FILE, "<$input") || die "Cannot open $input, $!\n";
    while (defined($_ = <FILE>)) {
	chomp;
	if (/^===================/) {
 	    $working_file = '';
	    $revision = 0;
	    $date = '';
	    $who = '';
	    @what = ();
	    next;
	}
	if (/^\-\-\-\-\-\-\-\-\-\-\-/) {
	    next;
	}
	if (/^Working file: (.*)/) {
 	    $working_file = $1;
	    $revision = 0;
	    $date = '';
	    $who = '';
	    @what = ();
	    next;
	}
	if ($working_file) {
	    if (/^revision (.*)/) {
		$revision = $1;
		next;
	    }
	    if  ($revision) {
		if (/^date: ([\w\-]+).*author: (\w+)/) {
		    $date = $1;
		    $who = $2;
		    next;
		}
		# print "Pushing $_ for \$commits{$working_file}{$revision}{what,date=$date,who=$who}\n";
		#
		## Just in case it starts with '*' or \-' or ...
		#
		s/^[^\w]*//g;
		#
		## Eventual spaces
		#
		s/^ *//g;
		if (! defined($types{$working_file})) {
		    $types{$working_file}=$type;
		}
		push(@{$commits{$working_file}{$revision}{what}},$_);
		$commits{$working_file}{$revision}{date}=$date;
		$commits{$working_file}{$revision}{who}=$who;
		push(@what,$_);
		next;
	    }
	}
    }
    close(FILE);
}
printit(\%commits,\%types);

exit 0;
