#!/usr/bin/perl

##### uses two environment variables
##### env X2CASTORMONINTERVAL = default(2)
##### env X2CASTORMONDEBUG    = default(undef)
##### env X2CASTORMONDBSTRING = default(undef)

my $logdir = shift or exit -1;

my @procfiles = (
    "cmd1","cmd300","cmd60","delayread","delaywrite",
    "read1","read300","read60","readd1","readd300",
    "readd60","rm1","rm300","rm60",
    "serverread","serverwrite","start",
    "stat1","stat300","stat60","trace",
    "userread","userwrite","version",
    "write1","write300","write60",
    "zero"
    );

my $procstore;
my $msgid=-1;
my $host = `hostname -f`;

chomp $host;

# check for non-default interval settings
if ($ENV{X2CASTORMONINTERVAL} ne "") {
    my $val = int ($ENV{X2CASTORMONINTERVAL});
    if ($val <=0) {
	$ENV{X2CASTORMONINTERVAL} = 2;
    }
}

if ($ENV{X2CASTORMONDBSTRING} eq "") {
    $ENV{X2CASTORMONDBSTRING} = "castormon/changemenow64537\@test1_castor_mon";
}

while(1) {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
    my $nowstring = localtime(time);
    $nowstring =~ /\w*\s(\w*)\s.*/;
    $mon = $1;
    $year = sprintf "%02d",$year-100;
    $mday = sprintf "%02d",$mday;
    $msgid=-1;
    foreach (@procfiles) {
	$msgid++;
	my $newcontent="";
	if (open IN,"$logdir/$_") {
	    while(my $line = <IN>) {
		my $origline = $line;
		if ( ($_ eq "serverread") || ($_ eq "serverwrite") || ($_ eq "userread") || ($_ eq "userwrite")) {
		    # cut away the timestamp
		    $line =~ /[0-9]*\s(.*)/;
		    $line = $1;
		}
		if ( $_ eq "zero" ) {
		    next;
		}
		$line =~ /([\w:\.-]*)\s+([\w:\.-]*)/;
		my $tag = $1;
		my $val = $2;

		if ((! defined $val) || ($val eq "") ) {
		    $val = $tag;
		    $tag = $_;
		} else {
		    $tag = "$_::$tag";
		}

		if ($_ eq "start") {
		    $tag = "start";
		    $val = `date +%s -d \"$origline\"`;
		    chomp $val;
		}
		if ($_ eq "trace") {
		    $tag = "trace";
		    $val = hex($val);
		}

		if ($_ eq "version") {
		    $val =~ s/\.//g;
		}
#		printf "parsing: $_: $line into val=$val tag=$tag\n";

		chomp $line;
		$newcontent .= $line;
		$newcontent .= "EOF";
		
		if (defined $procstore->{$_}) {
#		    printf "Compare $procstore->{$_} to $line\n";
		    if ($procstore->{$_} =~ /${line}EOF/) {
			# line is there->not changed
#			printf "not changed\n";
			;
		    } else {
			# line changed
			if ($ENV{X2CASTORMONDEBUG} ne "")  {
			    printf "$mday-$mon-$year $hour:$min:$sec $msgid $tag $val\n";
			}
			monval($ENV{X2CASTORMONDBSTRING},"$mday-$mon-$year","$hour:$min:$sec", $msgid, "$tag","$val","$host");
		    }
		} else {
		    if ($ENV{X2CASTORMONDEBUG} ne "")  {
			printf "$mday-$mon-$year $hour:$min:$sec $msgid $tag $val\n";
		    }
		    monval($ENV{X2CASTORMONDBSTRING},"$mday-$mon-$year","$hour:$min:$sec", $msgid, "$tag","$val","$host");
		}
	    }
	    $procstore->{$_} = $newcontent;
	    close IN;
	} else {
	    printf STDERR "error: cannot open procfile $logdir/$_\n";
	}
    }
    sleep $ENV{X2CASTORMONINTERVAL};
}


#printf "monval gave %d ", monval("22-SEP-08","11:59:59","2","ddd","998");

use Inline Python => <<'END_OF_PYTHON_CODE';
import sys 
import cx_Oracle

def monval(indb,a,b,c,d,e,f) :
    connection = cx_Oracle.connect(indb)
    time = a + ' ' + b       # '22-SEP-08 11:59:59'
    msg_type = c                       # '1'
    msg_string = d                     # 'aaa'
    msg_int = e                        # '100'
    msg_servername = f                 # 'lxplus.cern.ch'
    cursor = connection.cursor()
    cursor.arraysize = 50
    cursor.execute("""insert into xrootd values (TO_DATE(:ts, 'DD-MON-YY HH:MI:SS'),:mt,:ms,:mi, :mh)""", ts=time, mt=msg_type, ms=msg_string, mi=msg_int, mh=msg_servername)
    return cursor.execute("""commit""")

END_OF_PYTHON_CODE


