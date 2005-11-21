package Cfilesystem;

#
# $Id: Cfilesystem.pm,v 1.1 2005/11/21 13:54:41 jdurand Exp $
#

use strict;                      # Strict coding
use warnings;                    # Verbose messages
use Cinterface;                  # Network interfaces class
use Cconstants qw/$mode $debug/; # For $debug and $mode
use Data::Dumper;                # For dump
use Cutils;

#
## List of all keys that NEED to be defined
#
my @keys_must_be_true =
    qw/
    name
    status
    /;

#
## List of all keys that do not need to be defined - they will get zero (0) value if not yet defined
#
my @keys_can_be_empty =
    qw/
    readFactor
    writeFactor
    totalSpace
    freeSpace
    readRate
    writeRate
    nbRdonly
    nbWronly
    nbReadWrite
    nbTot
    ioTot
    fFsIo
    weight
    /;

#
## List of all keys that can be undef by the user but needs an internal non-zero value
#
my %keys_must_be_defined = (
			    'factor'              => 1.0,              # How that filesystem count for the diskserver
			    'readRateImportance'  => 0.5,              # How read  rate affect filesystem io factor
			    'writeRateImportance' => 0.5               # How write rate affect filesystem io factor
			    );

#
## Constructor
#
sub new {
    my $method = "new";
    my $class = shift;
    my $self = {};
    CASTOR_new($self,\@keys_must_be_true,\@keys_can_be_empty,\%keys_must_be_defined,@_);
    bless($self, $class);
    #
    ## Compute nbTot and ioTot
    #
    $self->{nbTot} = $self->{nbRdonly} + $self->{nbWronly} + $self->{nbReadWrite};
    Cdebug($self->{name},$method,"nbRdonly + nbWronly + nbReadWrite gives nbTot = $self->{nbTot}");
    $self->{parent}->{nbTot} += $self->{nbTot};
    Cdebug($self->{name},$method,"diskserver nbTot is now $self->{parent}->{nbTot}");
    $self->{ioTot} = $self->{readRate} + $self->{writeRate};
    Cdebug($self->{name},$method,"readRate + writeRate gives ioTot = $self->{ioTot}");
    return($self);
}

#
## Apply factor to I/O
#
sub fFsIo {
    my $method = "fFsIo";
    my $self = shift;
    my $correctedReadRate = $self->{readRate} * $self->{readFactor};
    my $correctedWriteRate = $self->{writeRate} * $self->{writeFactor};
    Cdebug("$self->{parent}->{name}:$self->{name}",$method,"readRate * readFactor = $self->{readRate} * $self->{readFactor} = $correctedReadRate");
    Cdebug("$self->{parent}->{name}:$self->{name}",$method,"writeRate * writeFactor = $self->{writeRate} * $self->{writeFactor} = $correctedWriteRate");
    #
    ## We use our parent correction on read/write streams (this is a diskserver attribute)
    #
    Cdebug("$self->{parent}->{name}:$self->{name}",$method,"$self->{parent}->{name}'s readImportance is $self->{parent}->{readImportance}");
    Cdebug("$self->{parent}->{name}:$self->{name}",$method,"$self->{parent}->{name}'s writeImportance is $self->{parent}->{writeImportance}");
    $self->{fFsIo} =
	($correctedReadRate  * $self->{parent}->{readImportance}) +
	($correctedWriteRate * $self->{parent}->{writeImportance});
    Cdebug("$self->{parent}->{name}:$self->{name}",$method,"fFsIo is ($correctedReadRate * $self->{parent}->{readImportance}) + ($correctedWriteRate * $self->{parent}->{writeImportance}) = $self->{fFsIo}");
    $self->{fFsIo} *= $self->{factor};
    Cdebug("$self->{parent}->{name}:$self->{name}",$method,"factor of the filesystem is $self->{factor} -> $self->{fFsIo}");
    return($self->{fFsIo});
}

1;
