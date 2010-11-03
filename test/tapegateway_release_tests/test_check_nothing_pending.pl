#!/usr/bin/perl -w

# This perl script that has to be run as root on the stager host will connect to
# the DB and report wether there are any pending mogrations for this stager
# On first version, it will report to both standard output and as an exit value.

# Imports
use strict;
use DBD::Oracle qw(:ora_types);

# Forward declarations
sub open_db();
sub check_leftoevers( $ );
sub print_leftovers ( $ );
sub nullize_arrays_undefs ( $ );
sub main();

# Hook for main.
main();

# Main: check for leftovers in DB, print out, return exit code accordingly
sub main ()
{
    my $dbh = open_db();
    my $ret = 0;
    if (check_leftovers ( $dbh )) {
        print_leftovers ( $dbh );
        $ret = 1;;
    }
    $dbh->disconnect();
    exit $ret;
}

# open_db : find connection parameters and open db connection
sub open_db()
{
    my ( $u, $p, $db );
    open ORACFG, "< /etc/castor/ORASTAGERCONFIG" 
      or die "Failed ot open /etc/castor/ORASTAGERCONFIG for reading: $!";
    while (<ORACFG>) {
        if (/^DbCnvSvc\W+user\W+(\w+)$/) { 
          $u = $1; 
        } elsif (/^DbCnvSvc\W+passwd\W+(\w+)$/) { 
          $p = $1; 
        } elsif (/^DbCnvSvc\W+dbName\W+(\w+)$/) { 
          $db = $1; 
        } 
    }
    my $dbh= DBI->connect('dbi:Oracle:'.$db ,$u, $p,
      { RaiseError => 1, AutoCommit => 0}) 
      or die "Failed to connect to DB as ".$u.'\@'.$db;
    return $dbh;
}

# check_leftovers : find wether there are any leftover unmigrated data in the stager
sub check_leftovers ( $ )
{
    my $dbh = shift;
    my $sth = $dbh -> prepare("SELECT count (*) from ( 
                              SELECT dc.id from diskcopy dc where
                                dc.status NOT IN ( 0 )
                              UNION ALL
                              SELECT tc.id from tapecopy tc)");
    $sth -> execute ();
    my @row = $sth->fetchrow_array();
    return $row[0];
}

# print_leftovers
sub print_leftovers ( $ )
{
    my $dbh = shift;
    # Print by castofile with corresponding tapecopies
    my $sth = $dbh -> prepare ("SELECT cf.lastknownfilename, dc.id, dc.status, tc.id, tc.status 
                                  FROM castorfile cf
                                  LEFT OUTER JOIN diskcopy dc ON dc.castorfile = cf.id
                                  LEFT OUTER JOIN tapecopy tc ON tc.castorfile = cf.id
                                 WHERE dc.status NOT IN ( 0 )");
    $sth -> execute();
    while ( my @row = $sth->fetchrow_array() ) {
	nullize_arrays_undefs ( \@row );
	print( "Remaining catorfile for $row[0]\n\twith diskcopy (id=$row[1], ".
	       "status=$row[2]) and tapecopy (id=$row[3], status=$row[4])\n" );
    }
    # print any other tapecopy not covered previously
    $sth = $dbh -> prepare ("SELECT cf.lastknownfilename, dc.id, dc.status, tc.id, tc.status 
                                  FROM castorfile cf
                                 RIGHT OUTER JOIN diskcopy dc ON dc.castorfile = cf.id
                                 RIGHT OUTER JOIN tapecopy tc ON tc.castorfile = cf.id
                                 WHERE dc.status IS NULL OR dc.status IN ( 0 )");
    $sth -> execute();
    while ( my @row = $sth->fetchrow_array() ) {
	nullize_arrays_undefs ( \@row );
	print( "Remaining tapecopy for $row[0]\n\twith diskcopy (id=$row[1], ".
	       "status=$row[2]) and tapecopy (id=$row[3], status=$row[4])\n" );
    }
}

# Replace undef members of an array by the string NULL
sub nullize_arrays_undefs ( $ )
{
    my $row=shift;
    for my $i ( 0 .. ( scalar(@{$row}) -1) ) {
        if ( ! defined ($row->[$i]) ) {
	    $row->[$i] = 'NULL';
	}
    }
}

