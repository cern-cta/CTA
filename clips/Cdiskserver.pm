package Cdiskserver;

#
# $Id: Cdiskserver.pm,v 1.1 2005/11/21 13:54:41 jdurand Exp $
#

use strict;                      # Strict coding
use warnings;                    # Verbose messages
use Cfilesystem;                 # Filesystem class
use Cconstants
    qw/
    $mode
    $debug
    /;
use Data::Dumper;                # For dump
use Cutils
    qw/
    CASTOR_new
    Cdebug
    $correctedFreeMemMax
    $correctedFreeSwapMax
    $correctedFreeRamMax
    $correctedAvailProcMax
    $correctedLoadMax
    $correctedIoMax
    $correctedSpaceMax
    /;

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
    totalMem
    freeMem
    totalRam
    freeRam
    totalSwap
    freeSwap
    totalProc
    availProc
    load
    correctedIo
    correctedFreeRam
    correctedFreeMem
    correctedSwap
    correctedAvailProc
    correctedLoad
    correctedSpace
    correctedStream
    fRam
    minIo
    nbTot
    weight
    /;

#
## List of all keys that can be undef by the user but needs an internal non-zero value
#
my %keys_must_be_defined = (
	
			    'ramSign'             =>  1.,    # Does increase of ram           increase the diskserver weight ?
			    'memSign'             =>  1.,    # Does increase of mem           increase the diskserver weight ?
			    'swapSign'            => -1.,    # Does increase of swap          increase the diskserver weight ?
			    'procSign'            =>  1.,    # Does increase of proc          increase the diskserver weight ?
			    'loadSign'            => -1.,    # Does increase of load          increase the diskserver weight ?
			    'ioSign'              => -1.,    # Does increase of io            increase the diskserver weight ?
			    'spaceSign'           =>  1.,    # Does increase of space         increase the diskserver weight ?
			    'streamSign'          => -1.,    # Does increase of nb of streams increase the diskserver weight ?
			    'ramImportance'       =>  1.,    # How  increase of ram           increase the diskserver weight ?
			    'ramFactor'           =>  1.,    # How  ram of that diskserver is important relatively to others
			    'memImportance'       =>  1.,    # How  increase of mem           increase the diskserver weight ?
			    'memFactor'           =>  1.,    # How  mem of that diskserver is important relatively to others
			    'swapImportance'      =>  1.,    # How  increase of swap          increase the diskserver weight ?
			    'swapFactor'          =>  1.,    # How  swap of that diskserver is important relatively to others
			    'procImportance'      =>  1.,    # How  increase of proc          increase the diskserver weight ?
			    'procFactor'          =>  1.,    # How  proc of that diskserver is important relatively to others
			    'loadImportance'      =>  2.,    # How  increase of load          increase the diskserver weight ?
			    'loadFactor'          =>  1.,    # How  load of that diskserver is important relatively to others
			    'ioImportance'        =>  5.,    # How  increase of io            increase the diskserver weight ?
			    'ioFactor'            =>  1.,    # How  io of that diskserver is important relatively to others
			    'spaceImportance'     =>  3.,    # How  increase of space         increase the diskserver weight ?
			    'spaceFactor'         =>  1.,    # How  space of that diskserver is important relatively to others
			    'streamImportance'    =>  0.,    # How  increase of nb of streams increase the diskserver weight ?
			    'readImportance'      =>  1.,    # How  increase of read streams  increase the io weight ?
			    'writeImportance'     =>  1.,    # How  increase of read streams  increase the io weight ?
			    'readWriteImportance' =>  1.,    # How  increase of read streams  increase the io weight ?
			    );
#
## Constructor
#
sub new {
    my $class = shift;
    my $self = {};
    CASTOR_new($self,\@keys_must_be_true,\@keys_can_be_empty,\%keys_must_be_defined,@_);
    my %params = @_;
    if (defined($params{clips_input})) {
	#
	## Special case to handle backward compatibility with the stager, that was previously interfaced
	## to a CLIPS only method
	## Simply we parse the input and fill %params again, possibly overwriting values already setted
	#
	clips2perl($params{clips_input},\%params);
    }
    if (defined($params{filesystems})) {
	my $filesystem;
	foreach $filesystem (@{$params{filesystems}}) {
	    ${$filesystem}{diskserver} = $params{name};
	    push(@{$self->{filesystems}},new Cfilesystem(%$filesystem));
	}
    }
    foreach (@{$self->{filesystems}}) {
	$_->{parent} = $self;
    }
    if (defined($params{interfaces})) {
	my $interface;
	foreach $interface (@{$params{interfaces}}) {
	    push(@{$self->{interfaces}},new Cinterface(%$interface));
	}
    }
    foreach (@{$self->{interfaces}}) {
	$_->{parent} = $self;
    }
    #
    ## Compute the max value from now on : warning, destructing the object will not re-evalue the maxs
    #
    correctedFreeMem($self);
    correctedFreeSwap($self);
    correctedFreeRam($self);
    correctedAvailProc($self);
    correctedLoad($self);
    correctedIo($self);
    correctedSpace($self);
    correctedStream($self);
    bless($self, $class);
    return($self);
}


#
## Dump the object
#
sub dump {
    my $self = shift;
    print Dumper($self);
}

#
## Apply factor to memory
#
sub correctedFreeMem {
    my $method = "correctedFreeMem";
    my $self = shift;
    $self->{correctedFreeMem} = $self->{freeMem} * $self->{memFactor};
    Cdebug($self->{name},$method,"correctedFreeMem is $self->{correctedFreeMem}");
    if ($self->{correctedFreeMem} > $correctedFreeMemMax) {
	$correctedFreeMemMax = $self->{correctedFreeMem};
    }
    Cdebug($self->{name},$method,"correctedFreeMemMax is $correctedFreeMemMax");
}

#
## Apply factor to swap
#
sub correctedFreeSwap {
    my $method = "correctedFreeSwap";
    my $self = shift;
    $self->{correctedFreeSwap} = $self->{freeSwap} * $self->{swapFactor};
    Cdebug($self->{name},$method,"correctedFreeSwap is $self->{correctedFreeSwap}");
    if ($self->{correctedFreeSwap} > $correctedFreeSwapMax) {
	$correctedFreeSwapMax = $self->{correctedFreeSwap};
    }
    Cdebug($self->{name},$method,"correctedFreeSwapMax is $correctedFreeSwapMax");
}

#
## Apply factor to ram
#
sub correctedFreeRam {
    my $method = "correctedFreeRam";
    my $self = shift;
    $self->{correctedFreeRam} = $self->{freeRam} * $self->{ramFactor};
    Cdebug($self->{name},$method,"correctedFreeRam is $self->{correctedFreeRam}");
    if ($self->{correctedFreeRam} > $correctedFreeRamMax) {
	$correctedFreeRamMax = $self->{correctedFreeRam};
    }
    Cdebug($self->{name},$method,"correctedFreeRamMax is $correctedFreeRamMax");
}

#
## Apply factor to availProc
#
sub correctedAvailProc {
    my $method = "correctedAvailProc";
    my $self = shift;
    $self->{correctedAvailProc} = $self->{availProc} * $self->{procFactor};
    Cdebug($self->{name},$method,"correctedAvailProc is $self->{correctedAvailProc}");
    if ($self->{correctedAvailProc} > $correctedAvailProcMax) {
	$correctedAvailProcMax = $self->{correctedAvailProc};
    }
    Cdebug($self->{name},$method,"correctedAvailProcMax is $correctedAvailProcMax");
}

#
## Apply factor to swap
#
sub correctedLoad {
    my $method = "correctedLoad";
    my $self = shift;
    $self->{correctedLoad} = $self->{load} * $self->{loadFactor};
    Cdebug($self->{name},$method,"correctedLoad is $self->{correctedLoad}");
    if ($self->{correctedLoad} > $correctedLoadMax) {
	$correctedLoadMax = $self->{correctedLoad};
    }
    Cdebug($self->{name},$method,"correctedLoadMax is $correctedLoadMax");
}

#
## Apply factor to I/O
#
sub correctedIo {
    my $method = "correctedIo";
    my $self = shift;
    my $fs;
    foreach $fs (@{$self->{filesystems}}) {
	$self->{correctedIo} += $fs->fFsIo;
    }
    if ($self->{correctedIo} < $self->{minIo}) {
	Cdebug($self->{name},$method,"correctedIo is $self->{correctedIo} but minIo is $self->{minIo} -> $self->{minIo}");
    } else {
	Cdebug($self->{name},$method,"correctedIo is $self->{correctedIo}");
    }
    if ($self->{correctedIo} > $correctedIoMax) {
	$correctedIoMax = $self->{correctedIo};
    }
    Cdebug($self->{name},$method,"correctedIoMax is $correctedIoMax");
}

#
## Compute free space
#
sub correctedSpace {
    my $method = "correctedSpace";
    my $self = shift;
    my $fs;
    foreach $fs (@{$self->{filesystems}}) {
	Cdebug($self->{name},$method,"$fs->{name}'s free space is " . $fs->{freeSpace});
	$self->{correctedSpace} += $fs->{freeSpace};
    }
    Cdebug($self->{name},$method,"correctedSpace (i.e. freeSpace) is $self->{correctedSpace}");
    if ($self->{correctedSpace} > $correctedSpaceMax) {
	$correctedSpaceMax = $self->{correctedSpace};
    }
    Cdebug($self->{name},$method,"correctedSpaceMax is $correctedSpaceMax");

}

#
## Compute nb of streams
#
sub correctedStream {
    my $method = "correctedStream";
    my $self = shift;
    my $fs;
    foreach $fs (@{$self->{filesystems}}) {
	Cdebug($self->{name},$method,"$fs->{name}'s nbRdonly is " . $fs->{nbRdonly});
	Cdebug($self->{name},$method,"$fs->{name}'s nbWronly is " . $fs->{nbWronly});
	Cdebug($self->{name},$method,"$fs->{name}'s nbReadWrite is " . $fs->{nbReadWrite});
	$self->{correctedStream} += $fs->{nbRdonly} + $fs->{nbWronly} + $fs->{nbReadWrite};
    }
    Cdebug($self->{name},$method,"correctedStream (i.e. tot nb of streams) is $self->{correctedStream}");
}

#
## Compute fRam
#
sub fRam {
    my $method = "fRam";
    my $self = shift;
    my $fRam = 0.;
    if ($correctedFreeRamMax > 0) {
	$fRam = $self->{correctedFreeRam} / (1. * $correctedFreeRamMax);
    }
    Cdebug($self->{name},$method,"correctedFreeRam is $self->{correctedFreeRam}, max is $correctedFreeRamMax -> fRam is $fRam");
    return($fRam);
}

#
## Compute fMem
#
sub fMem {
    my $method = "fMem";
    my $self = shift;
    my $fMem = 0.;
    if ($correctedFreeMemMax > 0) {
	$fMem = $self->{correctedFreeMem} / (1. * $correctedFreeMemMax);
    }
    Cdebug($self->{name},$method,"correctedFreeMem is $self->{correctedFreeMem}, max is $correctedFreeMemMax -> fMem is $fMem");
    return($fMem);
}

#
## Compute fSwap
#
sub fSwap {
    my $method = "fSwap";
    my $self = shift;
    my $fSwap = 0.;
    if ($correctedFreeSwapMax > 0) {
	$fSwap = $self->{correctedFreeSwap} / (1. * $correctedFreeSwapMax);
    }
    Cdebug($self->{name},$method,"correctedFreeSwap is $self->{correctedFreeSwap}, max is $correctedFreeSwapMax -> fSwap is $fSwap");
    return($fSwap);
}

#
## Compute fAvailProc
#
sub fAvailProc {
    my $method = "fAvailProc";
    my $self = shift;
    my $fAvailProc = 0.;
    if ($correctedAvailProcMax > 0) {
	$fAvailProc = $self->{correctedAvailProc} / (1. * $correctedAvailProcMax);
    }
    Cdebug($self->{name},$method,"correctedAvailProc is $self->{correctedAvailProc}, max is $correctedAvailProcMax -> fAvailProc is $fAvailProc");
    return($fAvailProc);
}

#
## Compute fLoad
#
sub fLoad {
    my $method = "fLoad";
    my $self = shift;
    my $fLoad = 0.;
    if ($correctedLoadMax > 0) {
	$fLoad = $self->{correctedLoad} / (1. * $correctedLoadMax);
    }
    Cdebug($self->{name},$method,"correctedLoad is $self->{correctedLoad}, max is $correctedLoadMax -> fLoad is $fLoad");
    return($fLoad);
}

#
## Compute fIo
#
sub fIo {
    my $method = "fIo";
    my $self = shift;
    my $fIo = 0.;
    if ($correctedIoMax > 0) {
	$fIo = $self->{correctedIo} / (1. * $correctedIoMax);
    }
    Cdebug($self->{name},$method,"correctedIo is $self->{correctedIo}, max is $correctedIoMax -> fIo is $fIo");
    return($fIo);
}

#
## Compute fSpace
#
sub fSpace {
    my $method = "fSpace";
    my $self = shift;
    my $fSpace = 0.;
    if ($correctedSpaceMax > 0) {
	$fSpace = $self->{correctedSpace} / (1. * $correctedSpaceMax);
    }
    Cdebug($self->{name},$method,"correctedSpace is $self->{correctedSpace}, max is $correctedSpaceMax -> fSpace is $fSpace");
    return($fSpace);
}

#
## Compute fStream
#
sub fStream {
    my $method = "fStream";
    my $self = shift;
    my $fStream = $self->{correctedStream};
    Cdebug($self->{name},$method,"fStream (i.e. tot nb of streams) is $fStream");
    return($fStream);
}

#
## Compute weight
#
sub weight {
    my $method = "weight";
    my $self = shift;
    my $fRam = $self->fRam() * $self->{ramImportance};
    Cdebug($self->{name},$method,"ramImportance is $self->{ramImportance} -> $fRam");
    my $fMem = $self->fMem() * $self->{memImportance};
    Cdebug($self->{name},$method,"memImportance is $self->{memImportance} -> $fMem");
    my $fSwap = $self->fSwap() * $self->{swapImportance};
    Cdebug($self->{name},$method,"swapImportance is $self->{swapImportance} -> $fSwap");
    my $fAvailProc = $self->fAvailProc() * $self->{procImportance};
    Cdebug($self->{name},$method,"procImportance is $self->{procImportance} -> $fAvailProc");
    my $fLoad = $self->fLoad() * $self->{loadImportance};
    Cdebug($self->{name},$method,"loadImportance is $self->{loadImportance} -> $fLoad");
    my $fIo = $self->fIo() * $self->{ioImportance};
    Cdebug($self->{name},$method,"ioImportance is $self->{ioImportance} -> $fIo");
    my $fSpace = $self->fSpace() * $self->{spaceImportance};
    Cdebug($self->{name},$method,"spaceImportance is $self->{spaceImportance} -> $fSpace");
    my $fStream = $self->fStream() * $self->{streamImportance};
    Cdebug($self->{name},$method,"streamImportance is $self->{streamImportance} -> $fStream");
    Cdebug($self->{name},$method,"ramSign,memSign,swapSign,procSign,loadSign,ioSign,spaceSign,streamSign is $self->{ramSign},$self->{memSign},$self->{swapSign},$self->{procSign},$self->{loadSign},$self->{ioSign},$self->{spaceSign},$self->{streamSign}");
    $self->{weight} =
	$self->{ramSign}    * $fRam +
	$self->{memSign}    * $fMem +
	$self->{swapSign}   * $fSwap +
	$self->{procSign}   * $fAvailProc +
	$self->{loadSign}   * $fLoad +
	$self->{ioSign}     * $fIo +
	$self->{spaceSign}  * $fSpace +
	$self->{streamSign} * $fStream;
    Cdebug($self->{name},$method,"================> weight is $self->{weight}");
    #
    ## Now we compute the weight per filesystem
    #
    my $fs;
    foreach $fs (@{$self->{filesystems}}) {
	Cdebug($self->{name},$method,"$fs->{name}'s nbTot is " . $fs->{nbTot});
	Cdebug($self->{name},$method,"$fs->{name}'s ioTot is " . $fs->{ioTot});
	Cdebug($self->{name},$method,"$fs->{name}'s nbReadWrite is " . $fs->{nbReadWrite});
	my $factorNb = 0.;
	if ($self->{nbTot} > 0) {
	    $factorNb = $fs->{nbTot} / (1. * $self->{nbTot});
	    Cdebug("$self->{name}:$fs->{name}",$method,"$self->{name}'s nbTot is $self->{nbTot}, -> $fs->{name}'s nbTot / $self->{name}'s nbTot = $fs->{nbTot} / $self->{nbTot} = $factorNb");
	}
	my $factorIo = 0.;
	if ($self->{correctedIo} > 0) {
	    $factorIo = $fs->{fFsIo} / (1. * $self->{correctedIo});
	    Cdebug("$self->{name}:$fs->{name}",$method,"$self->{name}'s correctedIo is $self->{correctedIo}, -> $fs->{name}'s ioTot / $self->{name}'s correctedIo = $fs->{ioTot} / $self->{corrrectedIo} = $factorIo");
	}
	$self->{correctedStream} += $fs->{nbRdonly} + $fs->{nbWronly} + $fs->{nbReadWrite};
	my $factorSpace = 0.;
	if ($fs->{totalSpace} > 0) {
	    $factorSpace = 1. - ((1. * $fs->{freeSpace}) / (1. * $fs->{totalSpace}));
	    Cdebug("$self->{name}:$fs->{name}",$method,"$self->{name}'s correctedIo is $self->{correctedIo}, -> $fs->{name}'s ioTot / $self->{name}'s correctedIo = $fs->{ioTot} / $self->{correctedIo} = $factorIo");
	}
	my $factor = $factorNb + $factorIo + $factorSpace;
	Cdebug("$self->{name}:$fs->{name}",$method,"factorNB + factorIo + factorSpace = $factorNb + $factorIo + $factorSpace = $factor");
	my $fsWeight = abs($self->{weight} * $factor);
	Cdebug("$self->{name}:$fs->{name}",$method,"$self->{name}'s weight is $self->{weight}, $fs->{name}'s factor is $factor leaving to $fs->{name}'s weight $fsWeight (abs() is needed)");
	#
	## Depending on the mode, we consider readImportant, writeImportance or readWriteImportance
	#
	if ($mode == 0) {
	    Cdebug("$self->{name}:$fs->{name}",$method,"mode is $mode (read flavour), readImportance is $self->{readImportance}");
	    $fsWeight *= $self->{readImportance};
	} elsif ($mode == 1) {
	    Cdebug("$self->{name}:$fs->{name}",$method,"mode is $mode (write flavour), writeImportance is $self->{writeImportance}");
	    $fsWeight *= $self->{writeImportance};
	} elsif ($mode == 2) {
	    Cdebug("$self->{name}:$fs->{name}",$method,"mode is $mode (read/write flavour), readWriteImportance is $self->{readWriteImportance}");
	    $fsWeight *= $self->{readWriteImportance};
	} else {
	    die "Unknown mode $mode\n";
	}
	$fs->{weight} = $fsWeight;
	Cdebug("$self->{name}:$fs->{name}",$method,"================> weight is $fs->{weight}");
    }
}

1;
