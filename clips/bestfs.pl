#!/usr/bin/perl -w

#
# $Id: bestfs.pl,v 1.2 2005/11/22 09:24:23 jdurand Exp $
#

use strict;                      # Strict coding
use diagnostics;                 # Verbose messages
use Cdiskserver;                 # Diskserver object
use IO::File;                    # For autoflush
use Data::Dumper;
use Cconstants
    qw/
    $mode
    $debug
    /;
use Cutils
    qw/
    clips2perl
    /;

autoflush STDOUT 1;            # Flush stdout

#
## Variables
#
my $input;                     # full stdin
my @hashes;                    # array of hashes after stdin parsing
my @objects;                   # The hashes objectified
my $refobj;                    # reference to one of these objects

#
## Get input
#
{local $/; $input = <>};

#
## Parse input
#
clips2perl($input,\@hashes);

#
## Select working mode (0=read-only, 1=write-mode, 2=read-write)
#
$mode = 2;

#
## Select debug level (0=off, not 0 means true)
#
$debug = 0;

#
## Create objects using output from input parsing
#
foreach (@hashes) {
    my $obj = new Cdiskserver(%$_);
    $obj->dump() if ($debug);
    push(@objects,$obj);
}

#
## Calculate all weights (diskservers and filesystems)
#
foreach (@objects) {
    $_->weight();
}

#
## Do a flat list of filesystems
#
my @filesystems = map {@{$_->{filesystems}}} @objects;

#
## Sort them according to diskserver->{weight} * filesystem{weight}
#
foreach (sort {$a->{parent}->{weight} * $a->{weight} <=> $b->{parent}->{weight} * $b->{weight}} @filesystems) {
    print "==> $_->{name} FreeSpace: $_->{freeSpace} TotalSpace: $_->{totalSpace} on $_->{parent}->{name} Weight $_->{parent}->{weight} fsDeviation $_->{weight} (freeRam,freeMem,freeSwap,availProc,Load[*100],io[MB/s])=($_->{parent}->{freeRam},$_->{parent}->{freeMem},$_->{parent}->{freeSwap},$_->{parent}->{availProc},$_->{parent}->{load},$_->{parent}->{correctedIo})\n";
}

#
## Exit
#
exit(0);
