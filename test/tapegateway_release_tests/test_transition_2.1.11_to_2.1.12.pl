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

# This test script will wipe and reinstall a fresh 2.1.11 tape gateway system.
# It will inject new migrations to it.
# Then the daemons will be shut down, and the stager upgraded to 2.1.12
# The daemons will be restarted, the test suite launched, and additionnal migrations 
# will be injected into the system.
# Results of all the steps will be checked.

# To run, the script:
# - needs a 2.1.12 checkout (of which is it part).
# - A reference 2.1.11 checkout (production release recommended).
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

my @packages_headnode_2111=  
(
 "castor-csec",
 "castor-mighunter-server",
 "castor-tape-tools",
 "castor-vmgr-client",
 "castor-lib-oracle",
 "castor-policies",
 "castor-debuginfo",
 "castor-vdqm2-client",
 "castor-hsmtools",
 "castor-lib",
 "castor-dbtools",
 "castor-rechandler-server",
 "castor-expert-server",
 "castor-repack-client",
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
 "castor-rtcopy-clientserver",
 "castor-logprocessor-server",
 "castor-lib-tape",
 "castor-transfer-manager",
 "castor-stager-server",
 "castor-transfer-manager-client",
 "castor-upv-server",
 "castor-vdqm2-lib-oracle",
 "castor-upv-client",
 "castor-rfio-client",
 "castor-repack-server"
 );

my @packages_headnode_2112=  
(
 "castor-csec",
 "castor-mighunter-server",
 "castor-tape-tools",
 "castor-vmgr-client",
 "castor-lib-oracle",
 "castor-debuginfo",
 "castor-vdqm2-client",
 "castor-hsmtools",
 "castor-lib",
 "castor-dbtools",
 "castor-rechandler-server",
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
 "castor-rfio-client"
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
 "castor-csec",
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

my @services_headnode_2111 =
(
 "mighunterd",
 "rechandlerd",
 "expertd",
 "rmmasterd",
 "vmgrd",
 "rhd",
 "tapegatewayd",
 "vdqmd",
 "logprocessord",
 "transfermanagerd",
 "stagerd",
 "cupvd"
 );

my @services_headnode_2112 =
(
 "rechandlerd",
 "expertd",
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

sub main ();
sub set_checkout_location ();
sub get_current_castor_packages ();
sub get_current_castor_packages_remote ( $ );
sub build_change_list ( @ );
sub send_packages ( $$ );
sub deploy_packages ( $ );
sub deploy_packages_remote ( $$ );
sub upgrade_cluster ();
sub downgrade_cluster ();
sub stop_daemons_2111();
sub stop_daemons_remote( $ );
sub stop_daemons_2112();
sub start_damons_2111();
sub start_daemons_remote ( $ );
sub start_damons_2112();
sub wipe_reinstall_stager_Db ();
sub wipe_reinstall_vdqm_Db ();
sub upgrade_stager_Db ();
sub upgrade_vdqm_Db ();



sub get_current_castor_packages ()
{
    my $list = `rpm -qa | egrep "castor-.*2\.1\.1[12]" | sort`;
    return split ("\n", $list);
}

sub get_current_castor_packages_remote ( $ )
{
    my $host = shift;
    my $list = `ssh $host rpm -qa | egrep "castor-.*2\.1\.1[12]" | sort`;
    return split ("\n", $list);
}

sub build_change_list ( @@$$ )
{
    my @current_packages = shift;
    my @targeted_packages = shift;
    my $targeted_version = shift;
    my $RPMS_directory = shift;
    my @to_remove;
    my @to_update;
    my @to_install;
    my $rpmt_file
    # Find the packages to remove
    foreach my $p (@current_packages) {
        if (!grep /$p/, @targeted_packages) {
            push @to_remove, $p;
        } else {
            push @to_update, $p;
        }
    }
    foreach $p (@targeted_packages) {
        if (!grep /$p/,@current_packages) {
            push @to_install, $p;
        }
    }
    foreach $p (@to_remove) {
        $rpmt_file .= "-e ".$p."\n";
    }
    foreach $p (@to_update) {
        $rpmt_file .= "-u ".$RPMS_directory."/".$p."-".$targeted_version.".x86_64.rpm\n";
    }
    foreach $p (@to_install) {
        $rpmt_file .= "-i ".$RPMS_directory."/".$p."-".$targeted_version.".x86_64.rpm\n";
    }
    return $rpmt_file;
}

sub send_packages ( $$$ )
{
    my ( $rpmt_file, $host,  $RPMS_directory ) = ( shift, shift, shift );
    my $file_list = join ( " ", \
                           grep ( s|(-u|-i) .*/([^/]*)$|$RPMS_directory/$2|, 
                                  split ( /\n/, $rpmt_file)
                                  ) 
                           );
    print `ssh $host mkdir -p RPMS`;
    print `scp $file_list $host:RPMS`;
}

sub deploy_packages ( $ )
{
    my ( $file_list ) = ( shift );
    my $tempfile = `mktemp`;
    chomp $tempfile;
    open RPMT, "> $tempfile" or die "Could not open file $temp_file";
    write RPMT, $file_list;
    close RPMT;
    print `rpmt-py -i$file_list`;
    unlink $tempfile;
}

sub deploy_packages_remote ( $$ )
{
    my ( $file_list, $host ) = ( shift, shift );
    my $tempfile = `mktemp`;
    my $remote_file = `ssh $host mktemp`;
    chomp $tempfile;
    open RPMT, "> $tempfile" or die "Could not open file $temp_file";
    write RPMT, $file_list;
    close RPMT;
    print `scp $temp_file $host:$remote_file`;
    print `ssh $host rpmt-py -i$remote_file`;
    unlink $tempfile;
    print `ssh $host rm -f $remote_file`;
}

sub downgrade_cluster ( $$$ )
{
    # Get parameters
    my ( $targeted_version, $old_version_directory, $RPMS_directory ) = ( shift, shift, shift );
    defined $RPMS_directory or die "Missing parameter in downgrade_cluster";

    # Get the list of the targeted diskservers
    my @disk_servers = CastorTapeTests::get_disk_servers();
    
    # Stop daemons
    for my $ds ( @disk_servers ) {
        stop_daemons_remote ( $ds );
    }
    stop_daemons_2111 ();
    stop_daemons_2112 ();
    
    # Downgrade software on disk servers then server
    for $ds ( @disk_servers ) {
        my @current_packages  = get_current_castor_packages_remote ( $ds );
        my $change_list = build_change_list ( @current_packages, @packages_disktape_server, 
            $targeted_version, "RPMS/" );
        send_packages ( $change_list, $ds, $RPMS_directory );
        deploy_packes_remote ( $change_list, $ds );
    }
    my @current_packages  = get_current_castor_packages ();
    my $change_list = build_change_list ( @current_packages, @packages_headnode_2111, 
        $targeted_version,  $RPMS_director );
    deploy_packages ( $change_list );

    # Downgrade the DBs: wipe and reinstall the stager Db and the vdqm Db.
    CastorTapeTests::resintall_stager_db_from_checkout ( $old_version_directory );
    CastorTapeTests::reinstall_vdqm_db_from_checkout ( $old_version_directory );
    # Now feed the VDQM with defaults (name server runs outside of the test environment)
    print `vdqmDBInit`;
    
    # Restart daemons on the headnode
    start_damons_2111 ();
    # Restart the daemons on the disk/tape servers
    # Start daemons
    for my $ds ( @disk_servers ) {
        start_daemons_remote ( $ds );
    }   
}

sub upgrade_cluster ()
{
    # Get parameters
    my $targeted_version = shift;

    # Get the list of the targeted diskservers
    my @disk_servers = CastorTapeTests::get_disk_servers();
    
    # Stop daemons
    for my $ds ( @disk_servers ) {
        stop_daemons_remote ( $ds );
    }
    stop_daemons_2111 ();
    stop_daemons_2112 ();
    
    # Downgrade software on disk servers then server
    for $ds ( @disk_servers ) {
        my @current_packages  = get_current_castor_packages_remote ( $ds );
        my $change_list = build_change_list ( @current_packages, @packages_disktape_server, $targeted_version );
        send_packages ( $change_list, $ds );
        deploy_packes_remote ( $change_list, $ds );
    }
    my @current_packages  = get_current_castor_packages ();
    my $change_list = build_change_list ( @current_packages, @packages_headnode_2111, $targeted_version );
    deploy_packages ( $change_list );

    # Apply the upgrade script on the server (stager and vdqm)
    CastorTapeTests::upgradeStagerDb ();
    CastorTapeTests::upgradeVDQMDb ();
    
    # Restart everything
    start_damons_2112 ();
    # Restart the daemons on the disk/tape servers
    # Start daemons
    for my $ds ( @disk_servers ) {
        start_daemons_remote ( $ds );
    }   
}

sub stop_daemons_2111()
{
    for my $d ( @services_headnode_2111 ) {
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

sub stop_daemons_2112()
{
    for my $d ( @services_headnode_2112 ) {
       print `service $d stop`; 
    }
}

sub start_damons_2111()
{
    for my $d ( @services_headnode_2111 ) {
       print `service $d start`; 
    }
}

sub start_daemons_remote ( $ )
{
    my $host = shift;
    for my $d ( @services_disktape_server ) {
        print `ssh $host service $d start`;
    }
}

sub start_damons_2112()
{
    for my $d ( @services_headnode_2112 ) {
       print `service $d stop`; 
    }
}


my ( $old_version, $old_version_checkout, $RPM_repository ) = ( shift, shift, shift );
main( $old_version, $old_version_checkout, $RPM_repository );

sub main ( $$ )
{
    # Get the command line parameters
    my ( $old_version, $old_version_checkout, $RPM_repository ) = ( shift, shift, shift );
    die "Expected directories of old checkout and RPMs repository as 2 command line argument" 
        unless defined $RPM_repository;
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

    my ( $old_version_checkout, $RPM_repository ) = ( shift, shift );

    # Check the contents of the Db before proceeding
    my $dbh = CastorTapeTests::open_db();
    my $ret = 0;
    if (CastorTapeTests::check_leftovers ( $dbh )) {
        my $useless; # workaround to prevent emacs from fscking the indentation.
        #CastorTapeTests::print_leftovers ( $dbh );
        $dbh->disconnect();
        die "FATAL: Leftovers found in the database. Aborting test.";
    }
    $dbh->disconnect();
    # Past this point, we are good to go.
    downgrade_cluster ();

    # Nuke and start clean
    my $host = `hostname -s`; chomp $host;
    if ( ($host ne 'lxcastordev03') && ($host ne 'lxcastordev04')) {
        die("ABORT: Unexpected host \"$host\"");
    }
    print "t=".CastorTapeTests::elapsed_time."s. ";
    print "Wiping the DB for a installation of castor 2.1.11  =============\n";
    CastorTapeTests::reinstall_stager_db();
    print "t=".CastorTapeTests::elapsed_time."s. ";
    CastorTapeTests::stopAndSwitchToTapeGatewayd ( $dbh );
    CastorTapeTests::startDaemons();
    print "Schema set to tapegatewayd mode. Current version is $version  ============================\n";

    my $seed_index = CastorTapeTests::make_seed ($file_size);  
    
    
}

# Find the checkout location based on the location of this script. Eliminates needs for keeping the location in config files
sub set_checkout_location ()
{
    # The checkout directory is 2 directories up from the location of the test script.
    my $checkout_dir = abs_path(Cwd::cwd()."/".dirname($0)."/../..");
    print "Setting checkout location to $checkout_dir\n";
    CastorTapeTests::set_environment("checkout_location", $checkout_dir); 
}

sub get_release_number ( $ );

# Get from the command line 3 directories: location of 2.1.11 checkout, location of 2.1.11 RPMS, 
# location of 2.1.12 RPMS
my ( $old_checkout_dir, $old_RPMS_dir, $new_RPMS_dir ) = ( shift, shift, shift );

# Guess the release number of the old version
my $olf_version = get_release_number( $old_checkout );

sub get_release_number ( $ )
{
    
    my $release;
    open CHANGELOG "<debian/changelog" or die "Failed to open debian/changelog: $!";
    while (<CHANGELOG>)  {
        if (/^castor/) {
            
        }
    }

  # [canoc3@lxcastordev03 castor-trunk]$ egrep "^castor" debian/changelog | awk '{print $2}' | head -1 | perl -ne 'if (/\((\d+)\.(\d+)/) {print "$1.$2\n";}'
  #  2.1
  # egrep -m 1 "^castor" debian/changelog | awk '{print $2}' | perl -ne 'if (/\((\d+)\.(\d+)/) {print "$1.$2\n";}'

}
