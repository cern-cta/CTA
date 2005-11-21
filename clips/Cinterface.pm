package Cinterface;

use strict;                      # Strict coding
use warnings;                    # Verbose messages
use Cconstants qw/$mode $debug/; # For $debug and $mode
use Data::Dumper;                # For dump
use Cutils;                      # CASTOR_new routine

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
    qw//;

#
## List of all keys that can be undef by the user but needs an internal non-zero value
#
my %keys_must_be_defined = ();

#
## Constructor
#
sub new {
    my $class = shift;
    my $self = {};
    Cutils::CASTOR_new($self,\@keys_must_be_true,\@keys_can_be_empty,\%keys_must_be_defined,@_);
    bless($self, $class);
    return($self);
}

1;
