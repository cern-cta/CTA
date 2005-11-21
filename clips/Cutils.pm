package Cutils;

#
# $Id: Cutils.pm,v 1.1 2005/11/21 13:54:41 jdurand Exp $
#

use strict;                         # Strict coding
use warnings;                       # Verbose messages
use Cconstants qw/$logfile $debug/; # For logging

BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);
    $VERSION = sprintf "%d.%03d", q$Revision: 1.1 $ =~ /(\d+)/g;

    @ISA         = qw(Exporter);

    # functions
    @EXPORT      = qw(&CASTOR_new
		      &clips2perl
		      &Cdebug
		      );

    # globals and optionnaly exported functions
    @EXPORT_OK   = qw($correctedFreeMemMax
		      $correctedFreeSwapMax
		      $correctedFreeRamMax
		      $correctedAvailProcMax
		      $correctedLoadMax
		      $correctedIoMax
		      $correctedSpaceMax
		      );
}
our @EXPORT_OK;
our $correctedFreeMemMax;          # Maximum value of correctedFreeMem over all diskservers
our $correctedFreeSwapMax;         # Maximum value of correctedFreeSwap over all diskservers
our $correctedFreeRamMax;          # Maximum value of correctedFreeRam over all diskservers
our $correctedAvailProcMax;        # Maximum value of correctedAvailProc over all diskservers
our $correctedLoadMax;             # Maximum value of correctedLoad over all diskservers
our $correctedIoMax;               # Maximum value of correctedIo over all diskservers
our $correctedSpaceMax;            # Maximum value of correctedSpace over all diskservers

#
## Generic constructor used as a normal routine - we do that instead of inheriting explicitely the constructor
## in classes Cfilesystem and Cinterface - because, then, the objects would be of class Cdiskserver instead of
## the package name
#

$correctedFreeMemMax = 0;
$correctedFreeSwapMax = 0;
$correctedFreeRamMax = 0;
$correctedAvailProcMax = 0;
$correctedLoadMax = 0;
$correctedIoMax = 0;
$correctedSpaceMax = 0;

sub CASTOR_new($$$$@) {
    my $self = shift;
    my $keys_must_be_true = shift;
    my $keys_can_be_empty = shift;
    my $keys_must_be_defined = shift;
    my %params = @_;
    my $key;
    foreach $key (@$keys_must_be_true) {
	die "Undefined '$key' key value" if (! defined($params{$key}));
	$self->{$key} = $params{$key};
    }
    foreach $key (@$keys_can_be_empty) {
	$self->{$key} = $params{$key} || 0;
    }
    foreach $key (sort %$keys_must_be_defined) {
	$self->{$key} = $params{$key} || ${$keys_must_be_defined}{$key};
    }
}

#
## Simple parser of CLIPS-like input, for backward compatibility with the stager
## We do not use regexp when possible to speedup things (the input format is very well known
## this is hardcoded in the stager)
## See file bestfs.input for an example of CLIPS-like input.
#

sub clips2perl($$) {
    my ($input,$refarray) = @_;
    my $line;
    my $in_make_instance = 0;
    my $nb = 0;
    foreach $line (split("\n",$input)) {
	if ($line eq "(make-instance of DISKSERVER") {
	    $in_make_instance = 1;
	    next;
	}
	if ($line eq ")") {
	    # if ($in_make_instance) {
	    $in_make_instance = 0;
	    # } # Test not needed
	    $nb++;
	    next;
	}
	if ($in_make_instance == 1) {
	    my $p;
	    my @tokens;
	    #
	    ## Remove surroundings () but not using regexps

	    #
	    # s/[\(\)]//g;
	    if (($p = index($line,"(")) >= 0) {
		substr($line,$p,1,' ');
	    }
	    if (($p = index($line,")")) >= 0) {
		substr($line,$p,1,' ');
	    }
	    #
	    ## Split using spaces
	    #
	    @tokens = split(" ",$line);
	    if (@tokens) {
		my $object;                  # filesystems or interfaces, i.e. embedded objects
		my $key;                     # key in an embedded object
		my %internalmap              # map between CLIPS-like input and embedded objects key names
		    = ('filesystems' => {'filesystemName' => 'name',
					 'filesystemStatus' => 'status',
					 'filesystemFactor' => 'factor',
					 'filesystemReadFactor' => 'readFactor',
					 'filesystemWriteFactor' => 'writeFactor',
					 'filesystemPartition' => 'partition',
					 'filesystemTotalSpace' => 'totalSpace',
					 'filesystemFreeSpace' => 'freeSpace',
					 'filesystemReadRate' => 'readRate',
					 'filesystemWriteRate' => 'writeRate',
					 'filesystemNbRdonly' => 'nbRdonly',
					 'filesystemNbWronly' => 'nbWronly',
					 'filesystemNbReadWrite' => 'nbReadWrite'},
		       'interfaces' => {'diskserverInterfaceName' => 'name',
					'diskserverInterfaceStatus' => 'status'}
		       );

		$$refarray[$nb]{name}      = $tokens[1] if ($tokens[0] eq "diskserverName");
		$$refarray[$nb]{status}    = $tokens[1] if ($tokens[0] eq "diskserverStatus");
		$$refarray[$nb]{totalMem}  = $tokens[1] if ($tokens[0] eq "diskserverTotalMem");
		$$refarray[$nb]{freeMem}   = $tokens[1] if ($tokens[0] eq "diskserverFreeMem");
		$$refarray[$nb]{totalRam}  = $tokens[1] if ($tokens[0] eq "diskserverTotalRam");
		$$refarray[$nb]{freeRam}   = $tokens[1] if ($tokens[0] eq "diskserverFreeRam");
		$$refarray[$nb]{totalSwap} = $tokens[1] if ($tokens[0] eq "diskserverTotalSwap");
		$$refarray[$nb]{freeSwap}  = $tokens[1] if ($tokens[0] eq "diskserverFreeSwap");
		$$refarray[$nb]{totalProc} = $tokens[1] if ($tokens[0] eq "diskserverTotalProc");
		$$refarray[$nb]{availProc} = $tokens[1] if ($tokens[0] eq "diskserverAvailProc");
		$$refarray[$nb]{load}      = $tokens[1] if ($tokens[0] eq "diskserverLoad");
		foreach $object (keys %internalmap) {
		    foreach $key (keys %{$internalmap{$object}}) {
			my $i = 0;
			map {$$refarray[$nb]{$object}[$i++]{$internalmap{$object}{$key}} = $_} @tokens[1..$#tokens] if ($tokens[0] eq $key);
		    }
		}
	    }
	}
    }
}

#
## Log method
#
sub Cdebug($$$) {
    my $objectname = shift;
    my $method = shift;
    my $string = shift;
    if ($debug) {
	print $logfile (localtime()) . " [$objectname] $method: $string\n";
    }
}

1;
