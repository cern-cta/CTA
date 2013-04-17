#!/usr/bin/perl -w
###############################################################################
#      test/tapegateway_release_tests/test_transition_2.1.11_to_2.1.12.pl
# 
#  This file is part of the Castor project.
#  See http://castor.web.cern.ch/castor
# 
#  Copyright (C) 2003  CERN
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; either version 2
#  of the License, or (at your option) any later version.
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# 
# 
# 
#  @author Castor Dev team, castor-dev@cern.ch
###############################################################################

# This test script will wipe and reinstall a fresh 2.1.13 tape gateway system.
# The daemons will be restarted, the test suite launched, and migrations 
# will be injected into the system.
# Results of all the steps will be checked.

# To run, the script:
# - needs a 2.1.13 checkout (of which is it part).
# - The RPMS for both releases.

# This perl script that has to be run as root on the stager host will connect to
# the DB and report wether there are any pending migrations for this stager
# On first version, it will report to both standard output and as an exit 

# Imports
use strict;
use POSIX;
use DBI;
use DBD::Oracle qw(:ora_types);
use BSD::Resource;
use Cwd 'abs_path';
use File::Basename;
use CastorTapeTests;


my @packages_headnode=  
(
 "castor-tape-tools",
 "castor-vmgr-client",
 "castor-lib-oracle",
 "castor-debuginfo",
 "castor-vdqm2-client",
 "castor-hsmtools",
 "castor-lib",
 "castor-dbtools",
 "castor-rmmaster-client",
 "castor-rmmaster-server",
 "castor-vmgr-server",
 "castor-rh-server",
 "castor-lib-monitor",
 "castor-tapegateway-server",
 "castor-config",
 "castor-ns-client",
 "castor-vdqm2-server",
 "castor-stager-client",
 "castor-logprocessor-server",
 "castor-lib-tape",
 "castor-transfer-manager",
 "castor-stager-server",
 "castor-transfer-manager-client",
 "castor-upv-server",
 "castor-vdqm2-lib-oracle",
 "castor-upv-client",
 "castor-rfio-client",
 "castor-ns-server"
 );


my @packages_disktape_server=
(
 "castor-tape-server-nostk",
 "castor-stager-client",
 "castor-lib-monitor",
 "castor-rmnode-server",
 "castor-diskserver-manager",
 "castor-tapebridge-server",
 "castor-rmmaster-client",
 "castor-tapebridge-client",
 "castor-vmgr-client",
 "castor-ns-client",
 "castor-config",
 "castor-lib",
 "castor-gridftp-dsi-common",
 "castor-job",
 "castor-gc-server",
 "castor-debuginfo",
 "castor-gridftp-dsi-ext",
 "castor-rmc-server",
 "castor-vdqm2-client",
 "castor-rfio-server",
 "castor-rtcopy-server",
 "castor-sacct",
 "castor-gridftp-dsi-int",
 "castor-lib-tape",
 "castor-tape-tools",
 "castor-rfio-client"
 );

my @services_headnode =
(
 "nsd",
 "vmgrd",
 "rhd",
 "tapegatewayd",
 "vdqmd",
 "logprocessord",
 "transfermanagerd",
 "stagerd",
 "cupvd"
 );

my @services_disktape_server=
(
 "taped",
 "rmnoded",
 "diskmanagerd",
 "tapebridged",
 "gcd",
 "rmcd",
 "rfiod",
 "rtcpd"
 );
 
sub main ( $ );
sub set_checkout_location ();
sub get_current_castor_packages ();
sub get_current_castor_packages_remote ( $ );
sub build_change_list ( $$$$ );
sub send_packages ( $$$ );
sub deploy_packages ( $ );
sub deploy_packages_remote ( $$ );
sub upgrade_cluster ( $$$ );
sub downgrade_cluster ( $$$$$ );
sub stop_daemons_remote( $ );
sub stop_daemons();
sub start_daemons_remote ( $ );
sub start_daemons();
sub check_daemons_remote ( $ );
sub check_daemons();
sub wipe_reinstall_stager_Db ();
sub wipe_reinstall_vdqm_Db ();
sub upgrade_stager_Db ();
sub upgrade_vdqm_Db ();
sub get_release_number ( $ );
sub preparePreTransitionBacklog ( $$$ );
sub managePostTransitionBacklog ( $ );
sub checkTapes ( );


sub get_current_castor_packages ()
{
    my $list = `rpm -qa | egrep "castor-.*2\.1\.1[123]" | sort`;
    return split ("\n", $list);
}

sub get_current_castor_packages_remote ( $ )
{
    my $host = shift;
    my $list = `ssh $host rpm -qa | egrep "castor-.*2\.1\.1[123]" | sort`;
    return split ("\n", $list);
}

sub build_change_list ( $$$$ )
{
    my $current_packages = shift;
    my $targeted_packages = shift;
    my $targeted_version = shift;
    my $RPMS_directory = shift;
    my @to_remove;
    my @to_update;
    my @to_install;
    my $rpmt_file;
    # Find the packages to install or update (among the targeted ones)
    my $p;
    for $p (@{$targeted_packages}) {
        $p =~ s/-2\.1\.1[123]\.\d+$//;
        if (!grep /$p/, @{$current_packages}) {
            push @to_install, $p;
        } else {
            push @to_update, $p;
        }
    }
    # Find the packages to remove (among the current ones)
    for $p (@{$current_packages}) {
        if (!grep /$p/,@{$targeted_packages}) {
            push @to_remove, $p;
        }
    }
    for $p (@to_remove) {
        $rpmt_file .= "-e ".$p."\n";
    }
    for $p (@to_update) {
        $rpmt_file .= "-u ".$RPMS_directory."/".$p."-".$targeted_version.".x86_64.rpm\n";
    }
    for $p (@to_install) {
        $rpmt_file .= "-i ".$RPMS_directory."/".$p."-".$targeted_version.".x86_64.rpm\n";
    }
    return $rpmt_file;
}

sub send_packages ( $$$ )
{
    my ( $rpmt_file, $host,  $RPMS_directory ) = ( shift, shift, shift );
    my @lines = split ( /\n/, $rpmt_file);
    my @selected_and_changed_lines = grep ( s:(-u|-i) .*/([^/]*)$:$RPMS_directory/$2:, @lines);
    my $file_list = join ( " ", @selected_and_changed_lines );
    print `ssh $host mkdir -p RPMS`;
    print `scp $file_list $host:RPMS`;
}

sub deploy_packages ( $ )
{
    my ( $file_list ) = ( shift );
    my $tempfile = `mktemp`;
    chomp $tempfile;
    open RPMT, "> $tempfile" or die "Could not open file $tempfile";
    print RPMT $file_list;
    close RPMT;
    print "t=".CastorTapeTests::elapsed_time."s. Deploying packages locally\n";
    print `rpmt-py --force -i$tempfile 2>&1`;
    unlink $tempfile;
    print `ldconfig`;
}

sub deploy_packages_remote ( $$ )
{
    my ( $file_list, $host ) = ( shift, shift );
    my $tempfile = `mktemp`;
    my $remote_file = `ssh $host mktemp`;
    chomp $tempfile;
    open RPMT, "> $tempfile" or die "Could not open file $tempfile";
    print RPMT $file_list;
    close RPMT;
    print "t=".CastorTapeTests::elapsed_time."s. Deploying packages to host $host\n";
    print `scp $tempfile $host:$remote_file`;
    print `ssh $host rpmt-py --force -i$remote_file 2>&1`;
    unlink $tempfile;
    print `ssh $host rm -f $remote_file`;
    print `ssh $host ldconfig`;
}

sub reinstall_cluster ( $$$$ )
{
    # Get parameters
    my ( $targeted_version, $RPMS_directory, $checkout_directory, $dbh ) 
           = ( shift, shift, shift, shift );
    defined $RPMS_directory or die "Missing parameter in renstall_cluster";

    # get test parameters
    my $castor_directory = CastorTapeTests::get_environment('castor_directory');
    my $single_subdir = CastorTapeTests::get_environment('castor_single_subdirectory');
    my $dual_subdir = CastorTapeTests::get_environment('castor_dual_subdirectory');
    my $file_number = CastorTapeTests::get_environment('file_number');
    my $username = CastorTapeTests::get_environment('username');
    my $run_testsuite = CastorTapeTests::get_environment('run_testsuite');

    # Get the list of the targeted diskservers
    my @disk_servers = CastorTapeTests::get_disk_servers();
    
    # Stop daemons
    for my $ds ( @disk_servers ) {
        stop_daemons_remote ( $ds );
    }
    stop_daemons ();
    
    # Reinstall software on disk servers then server
    my @current_packages;
    my $change_list;
    for my $ds ( @disk_servers ) {
        @current_packages  = get_current_castor_packages_remote ( $ds );
        $change_list = build_change_list ( \@current_packages, \@packages_disktape_server, 
            $targeted_version, "RPMS/" );
        send_packages ( $change_list, $ds, $RPMS_directory );
        deploy_packages_remote ( $change_list, $ds );
    }
    @current_packages  = get_current_castor_packages ();
    $change_list = build_change_list ( \@current_packages, \@packages_headnode, 
        $targeted_version,  $RPMS_directory );
    deploy_packages ( $change_list );

    # Reinstall the DBs: wipe and reinstall the stager Db, the vdqm Db,
    # the VMGR DB and the name server
    
    # First, name server get reinstalled
    CastorTapeTests::reinstall_ns_db_from_checkout  ( $checkout_directory );
    print `service nsd start`;
    
    CastorTapeTests::reinstall_stager_db_from_checkout  ( $checkout_directory );
    CastorTapeTests::reinstall_vdqm_db_from_checkout ( $checkout_directory );
    print `service vmgrd start`;
    sleep 2;
    # Now feed the VDQM with defaults (name server runs outside of the test environment)
    print `vdqmDBInit`;
    
    # Restart daemons on the headnode (some will fail)
    start_daemons ();
    # Restart the daemons on the disk/tape servers
    # Start daemons
    for my $ds ( @disk_servers ) {
        start_daemons_remote ( $ds );
    }   

    # Configure the fileclasses, service classes, fileclasses, etc... 
    # (diskservers are needed at that point)
    CastorTapeTests::configure_headnode_2113 ( );

    # Second pass to start daemons
    start_daemons ();

    # Re-create the directories:
    print `su $username -c "nsmkdir -p $castor_directory$single_subdir"`;
    print `su $username -c "nschclass largeuser $castor_directory$single_subdir"`;
    CastorTapeTests::register_remote ( $castor_directory.$single_subdir, "directory" );
    print `su $username -c "nsmkdir -p $castor_directory$dual_subdir"`;
    print `su $username -c "nschclass test2 $castor_directory$dual_subdir"`;
    CastorTapeTests::register_remote ( $castor_directory.$dual_subdir, "directory" );

    # This should make sure the tape drives are up
    for my $ds ( @disk_servers ) {
        start_daemons_remote ( $ds );
    }   
}

sub stop_daemons()
{
    for my $d ( @services_headnode ) {
       print `service $d stop`; 
    }
}

sub stop_daemons_remote( $ )
{
    my $host = shift;
    for my $d ( @services_disktape_server ) {
        print `ssh $host service $d stop`;
    }
}

sub start_daemons_remote ( $ )
{
    my $host = shift;
    for my $d ( @services_disktape_server ) {
        print `ssh $host service $d start`;
    }
    sleep 5;
    # put the remote drives up.
    my $drives_list = `ssh $host cat /etc/castor/TPCONFIG | cut -d " " -f 1`;
    my @drives= split ( /\n/, $drives_list );
    for my $d ( @drives ) {
        print "calling /usr/bin/tpconfig $d down\n";
        print `ssh $host /usr/bin/tpconfig $d down`;
        print "calling /usr/bin/tpconfig $d up\n";
        print `ssh $host /usr/bin/tpconfig $d up`;
     }
}

sub start_daemons()
{
    for my $d ( @services_headnode ) {
       print `service $d start`; 
    }
}

sub check_daemons()
{
    for my $d ( @services_headnode ) {
       print `service $d status`; 
    }
}

sub check_daemons_remote ( $ )
{
    my $host = shift;
    for my $d ( @services_disktape_server ) {
        print `ssh $host service $d status`;
    }
}

# Check that the tapes in the vmgr are not busy. Reset them if needed.
sub checkTapes ( )
{
    # Check that the tapes from the tape pool have been left in a proper state.
    my $tapepool = CastorTapeTests::get_environment('tapepool');
    my $tp_status = `vmgrlisttape -P $tapepool`;
    for ( split /^/, $tp_status) {
    if ( /BUSY/ ) {
        chomp;
        print "********** WARNING: ".$_."\n";
        if ( /^([[:alnum:]]+) / ) {
        `vmgrmodifytape -V $1 --st \"\"`;
        print "Removed the busy flag from tape $1\n";
        }
    }
    }
}

my $RPM_repository = shift;
main ($RPM_repository);

sub main ( $ )
{
    # Get the path to the RPMS.
    my $RPM_repository = ( shift );
    
    # Nuke and start clean, but make sure we're where we should be
    my $host = `hostname -s`; chomp $host;
    if ( ($host ne 'lxcastordev03') && ($host ne 'lxcastordev04') && ($host ne 'lxc2dev3') ) {
        die("ABORT: Unexpected host \"$host\"");
    }

    # Get the version number
    my $version = get_release_number ( "../.." );
    
    # Set ulimit to infinite
    setrlimit (RLIMIT_CORE, RLIM_INFINITY, RLIM_INFINITY);
    # Check we are running in ulimit -c unlimited
    my ($rlsoft, $rlhard) = getrlimit(RLIMIT_CORE);
    if ( $rlsoft != -1 ) {
        die "Failed to set to ulimit to unlimited";
    }
    # Initialize: setup the environment
    print "t=".CastorTapeTests::elapsed_time()."s. ";
    print `date`;
    my $conffile = './tapetests-lxcastordev.conf';
    CastorTapeTests::read_config($conffile);
    set_checkout_location();
    CastorTapeTests::check_environment ();    

    # Check the contents of the Db before proceeding
    my $dbh = CastorTapeTests::open_db();
    my $ret = 0;
    if (CastorTapeTests::check_leftovers ( $dbh )) {
        my $useless; # workaround to prevent emacs from fscking the indentation.
        #CastorTapeTests::print_leftovers ( $dbh );
        $dbh->disconnect();
        die "FATAL: Leftovers found in the database. Aborting test.";
    }
    
    # Past this point, we are good to go.
    reinstall_cluster ( $version, $RPM_repository, "../..", $dbh  );
    my $file_size =  CastorTapeTests::get_environment('file_size');
    my $seed_index = CastorTapeTests::make_seed ($file_size);

    # Get test parameters
    my $castor_directory = CastorTapeTests::get_environment('castor_directory');
    my $single_subdir = CastorTapeTests::get_environment('castor_single_subdirectory');
    my $dual_subdir = CastorTapeTests::get_environment('castor_dual_subdirectory');
    my $file_number = CastorTapeTests::get_environment('file_number');
    my $username = CastorTapeTests::get_environment('username');
    my $run_testsuite = CastorTapeTests::get_environment('run_testsuite');


    
    print `nsmkdir -p $castor_directory`;
    
    
    # First iteration of the test
    # Start testsuite in the background
    my $testsuite_pid;
    my $testsuite_rd;
    if ($run_testsuite) {
        ( $testsuite_pid, $testsuite_rd ) = CastorTapeTests::spawn_testsuite ();
        print "t=".CastorTapeTests::elapsed_time."s. ";
        print "Started testsuite -------------\n";
    }
    # Sleep a bit
    sleep 5;
    # We can inject dual tape copies here as they will be handled by the tapegateway.
    SingleAndDualCopyTest ( $dbh, $seed_index, $file_number, 1);
    # Wait for testsuite to complete and print out the result
    if ($run_testsuite) {
        print "t=".CastorTapeTests::elapsed_time."s. ";
        print "Waiting testsuite completion -------------\n";
        print CastorTapeTests::wait_testsuite ( $testsuite_pid, $testsuite_rd );
    }
    
    $dbh->disconnect();
    $dbh = CastorTapeTests::open_db();
    CastorTapeTests::print_leftovers ( $dbh );
    print "t=".CastorTapeTests::elapsed_time."s. ";
    print "Test done.\n";
    
    #print "Cleaning up test directories $castor_directory\{$single_subdir,$dual_subdir\}\n";
    #print `su $username -c "for p in $castor_directory\{$single_subdir,$dual_subdir\}; do nsrm -r -f \\\$p; done"`;
    exit 0;
}

# Find the checkout location based on the location of this script. Eliminates needs for keeping the location in config files
sub set_checkout_location ()
{
    # The checkout directory is 2 directories up from the location of the test script.
    my $checkout_dir = abs_path(Cwd::cwd()."/".dirname($0)."/../..");
    print "Setting checkout location to $checkout_dir\n";
    CastorTapeTests::set_environment("checkout_location", $checkout_dir); 
}


sub get_release_number ( $ )
{
    my $checkout_dir = shift;
    my $release;
    open (CHANGELOG, "<${checkout_dir}/debian/changelog") or die "Failed to open debian/changelog: $!";
    while (<CHANGELOG>)  {
        if (/^castor \(([[:digit:]\.-]*)\)/) {
            $release  = $1;
            print "release =  $release\n";
            print "svnversion = ".`(cd ${checkout_dir} ; svnversion)`;
            last;
        }
    }
    close (CHANGELOG);
    return $release;

  # [canoc3@lxcastordev03 castor-trunk]$ egrep "^castor" debian/changelog | awk '{print $2}' | head -1 | perl -ne 'if (/\((\d+)\.(\d+)/) {print "$1.$2\n";}'
  #  2.1
  # egrep -m 1 "^castor" debian/changelog | awk '{print $2}' | perl -ne 'if (/\((\d+)\.(\d+)/) {print "$1.$2\n";}'

}

# Inject single and dual tape copies files, wait for them to be migrated,
# stager_rm them and then recall them. Eventually, drop them.
sub SingleAndDualCopyTest ( $$$$ )
{
    my ( $dbh, $seed_index, $file_number, $do_dual_tapecopy ) = ( shift, shift, shift, shift );
    my $poll = CastorTapeTests::get_environment('poll_interval');
    my $timeout = CastorTapeTests::get_environment('migration_timeout');
    goodDayFileCreation ( $seed_index, $file_number, $do_dual_tapecopy );
    badDayFileCreation ( $dbh,  $seed_index, $file_number, $do_dual_tapecopy );
    CastorTapeTests::poll_moving_entries ( $dbh, $poll, $timeout, "cleanup_migrated stager_reget_from_tape" );
}

sub goodDayFileCreation ( $$$ )
{
    my ( $seed_index, $file_number, $do_dualcopy ) = ( shift, shift, shift);
    my $castor_directory = CastorTapeTests::get_environment('castor_directory');
    my $single_subdir = CastorTapeTests::get_environment('castor_single_subdirectory');
    my $dual_subdir = CastorTapeTests::get_environment('castor_dual_subdirectory');
    my $username = CastorTapeTests::get_environment('username');
    my $poll = CastorTapeTests::get_environment('poll_interval');
    my $timeout = CastorTapeTests::get_environment('migration_timeout');

    my @single_dual_copy_pattern = ( 0 );
    if ($do_dualcopy) {
        @single_dual_copy_pattern = (  0, 1 );
    }
    for my $is_dual_copy (@single_dual_copy_pattern) {
        for my $i (0 .. ($file_number - 1) ) {
            my $file_name="/tmp/".`uuidgen`;
            chomp $file_name;
            my $local_index = CastorTapeTests::make_localfile( $seed_index, $file_name );
            CastorTapeTests::rfcp_localfile ( $local_index, $is_dual_copy );
        }
    } 
}

# Inject error conditions to chekc on the reactions of the tape system
sub badDayFileCreation ( $$$$ )
{
    my ( $dbh, $seed_index, $file_number, $do_dualcopy ) = ( shift, shift, shift, shift );
    my $error_index = 0;
    # List of the breaking/stage combinations to use (they will simply be cycled)
    # breakings are:
    #           missing castorfile <= gets constraint violation
    #           missing filesystem
    #           missing fileclass <= gets constraint violation
    #           missing serviceclass <= gets constraint violation
    #           missing stream
    #           missing tapepool
    #           missing segment
    #           missing tape
    #           missing ns entry
    #           wrong checksum
    #           wrong size
    #           wrong segment
    #           broken diskserver <= gets a contraint violation
    #
    # stages are:
    #           rfcp
    #           invalidation
    #           reget
    #           partial migration
    #           migrated
    #           on tape
    #           recalled

#    my @error_list = ( #"missing ns entry on rfcp",
#                       #"missing filesystem on rfcp",
#                       #"missing stream on rfcp",
#                       "missing tapepool on rfcp",
#                       "missing segment on rfcp",
#                       "missing tape on rfcp",
#                       #"missing ns entry on rfcp",
#                       #"wrong checksum on rfcp",
#                       #"wrong size on rfcp",
#                       "wrong segment on rfcp",
#                       "missing second tape segment in ns on migrated"
#                       );
    my @error_list = ( #"missing ns entry on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "wrong size on invalidation",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp",
                        "nothing on rfcp");
    my $castor_directory = CastorTapeTests::get_environment('castor_directory');
    my $single_subdir = CastorTapeTests::get_environment('castor_single_subdirectory');
    my $dual_subdir = CastorTapeTests::get_environment('castor_dual_subdirectory');
    my $username = CastorTapeTests::get_environment('username');
    my $poll = CastorTapeTests::get_environment('poll_interval');
    my $timeout = CastorTapeTests::get_environment('migration_timeout');

    my @single_dual_copy_pattern = ( 0 );
    if ($do_dualcopy) {
        @single_dual_copy_pattern = (  0, 1 );
    }
    for my $is_dual_copy (@single_dual_copy_pattern) {
        for my $i (0 .. ($file_number - 1) ) {
            my $file_name="/tmp/".`uuidgen`;
            chomp $file_name;
            my $local_index = CastorTapeTests::make_localfile( $seed_index, $file_name );
            CastorTapeTests::rfcp_localfile_break ( $dbh, $local_index, $is_dual_copy, $error_list[$error_index] );
            $error_index = ($error_index + 1) % scalar(@error_list);
        }
    } 
}

# Inject file right before transition from one running configuration to the other.
sub preparePreTransitionBacklog ( $$$ )
{
    my ( $seed_index, $file_number, $do_dual_tapecopy ) = ( shift, shift, shift);
    # For the moment, reproduce the good day without the polling.
    goodDayFileCreation ( $seed_index, $file_number, $do_dual_tapecopy );
}

