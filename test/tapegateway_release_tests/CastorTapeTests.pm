###############################################################################
#      test/tapegateway_release_tests/CastorTapeTests.pm
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
#  @author Steven.Murray@cern.ch, Eric.Cano@cern.ch and Castor tape-team.
###############################################################################

# CastorTapeTests package
#
# Not in a directory structure yet...
# Intended to contain all the utilities to run tape tests.

package CastorTapeTests;
my $package_name="CastorTapeTests";

# Imports
use strict;
use POSIX;
use DBI;
use DBD::Oracle qw(:ora_types);
use Data::Dumper;

our $VERSION = 1.00;
our @ISA = qw(Exporter);

our @export   = qw(
                   get_uid_for_username
                   get_gid_for_username
                   get_disk_servers
                   read_config
                   check_environment
                   get_environment
                   set_environment
                   clean_fileservers
                   register_remote_file
                   make_seed 
                   make_localfile 
                   open_db 
                   getConfParam
                   getCastorConfParam 
                   daemonIsRunning 
                   killDaemonWithTimeout
                   startDaemons
                   startSingleDaemon
                   executeSQLPlusScript
                   executeSQLPlusScriptNoFilter
                   stopAndSwitchToTapeGatewayd
                   check_leftovers
                   check_leftovers_poll_timeout
                   nullize_arrays_undefs
                   print_leftovers
                   print_file_info
                   reinstall_stager_db
                   reinstall_stager_db_from_checkout
                   reinstall_vdqm_db_from_checkout
                   upgradeStagerDb
                   postUpgradeStagerDb
                   upgradeVDQMDb
                   getOrastagerconfigParam
                   getOraVdqmConfigParam
                   getOraCNSLoginInfo
                   cleanup
                   elapsed_time
                   spawn_testsuite
                   wait_testsuite
                   configure_headnode_2111
                   configure_headnode_2112
                   configure_headnode_2113
                   ); # Symbols to autoexport (:DEFAULT tag)

# keep track of the locally created files and of the recalled and locally rfcped files.
my @local_files;

# keep track of the files migrated in castor.
my @remote_files;

# keep track of the test environment (and definition of the defaults).
my %environment = (
                   allowed_stagers => [ 'lxcastordev03', 'lxcastordev04', 'lxc2dev3', 'lxc2dev4']
);

my $time_reference;

# This variable tracks the result of the initial decision to go for it or not (safety mechanism for leftovers)
# It will be used to determine final cleanup behavior;
my $initial_green_light = 0;


# Returns the numeric user ID of the user with given name
sub get_uid_for_username( $ )
{
  my $username = shift;

  my $id_result=`id $username`;
  chomp($id_result);

  if($id_result =~ m/uid=(\d+)/) {
    return $1;
  } else {
    die("Failed to get uid for username \"$username\"\n");
  }
}


# Returns the numeric group ID of the user with given name
sub get_gid_for_username( $ )
{
  my $username = shift;

  my $id_result=`id $username`;
  chomp($id_result);

  if($id_result =~ m/gid=(\d+)/) {
    return $1;
  } else {
    die("Failed to get uid for username \"$username\"\n");
  }
}


# Based on the output of rmGetNodes, this subroutine returns the hostnames of the
# disk-servers sorted alphabetically.
sub get_disk_servers()
{
  my @servers = split ( /,/, $environment{diskservers});
  chomp(@servers);

  return @servers;
}

# Elapsed time since first call which implicitly sets the reference moment
sub elapsed_time ()
{
    if ( defined $time_reference ) {
	return time() - $time_reference;
    } else {
	$time_reference = time();
	chomp $time_reference;
	return 0;
    }
}

# Set the user name in the environment
sub read_config ( $ )
{   
    my $config_file = shift;
    my $local_host = `hostname -s`;
    chomp $local_host;
    $environment{hostname} = $local_host;
    my @per_host_vars = ( 'username', 'file_size', 'file_number', 'castor_directory',
                          'migration_timeout', 'poll_interval', 'tapepool', 'svcclass', 
		             	  'local_transfermanagerd', 'local_rechandlerd', 'local_rhd',
			              'local_rmmasterd', 'local_stagerd', 'local_tapegatewayd',
                          'local_logprocessord', 'testsuite_config_location', 'instance_name',
                          'cns_name', 'run_testsuite', 'diskservers', 'vmgr_schema');
                          
    my @global_vars   = ( 'dbDir' , 'tstDir', 'adminList', 'originalDbSchema',
                          'originalDropSchema', 'originalSetPermissionsSQL', 
                          'castor_single_subdirectory', 'castor_dual_subdirectory',
                          'vdqmScript', 'stagerUpgrade', 'stagerUpgrade_1', 
                          'stagerUpgrade_2', 'stagerUpgrade_3', 'VDQMUpgrade',
                          'nsScript');
    for my $i ( @per_host_vars ) {
        $environment{$i}=getConfParam('TAPETEST', $i.'_'.$local_host, $config_file );
    }
    for my $i ( @global_vars ) {
        $environment{$i}=getConfParam('TAPETEST', $i, $config_file );
    }
}

# Dies with an appropriate message if the specfied file or directory does not
# exist
sub check_file_exists ( $$ )
{
    my ($descriptive_name, $filename) = (shift, shift);

    die("$descriptive_name \"$filename\" does not exist\n") if ! -e $filename;
}

# Check the sanity of the environment, die if anything is wrong
sub check_environment ( ) 
{
    my $pass = 0;
    for my $i ( @{$environment{allowed_stagers}} ) {
        if ( $environment{hostname} eq $i ) {
            $pass = 1;
        }
    }
    if ( !$pass ) { die "Wrong host"; }

    check_file_exists("Database schema directory", $environment{checkout_location}.'/'.$environment{dbDir});
    check_file_exists("Stager schema", $environment{checkout_location}.'/'.$environment{dbDir}.'/'.$environment{originalDbSchema});
    check_file_exists("Drop stager-database script", $environment{checkout_location}.'/'.$environment{dbDir}.'/'.$environment{originalDropSchema});
    check_file_exists("Test-script directory", $environment{checkout_location}.'/'.$environment{tstDir});
}

# Extract enviroment variable (mainly for client application)
sub get_environment ( $ )
{
    my $vname = shift;
    if ( defined $environment{$vname} ) {
        return $environment{$vname}
    } else {
        die "Variable $vname not found in environment";
    }
}

# Set enviroment variable (mainly for client application)
sub set_environment ( $$ )
{
    my ( $vname, $value ) = (shift, shift);
    if ( defined $vname && defined $value) {
        $environment{$vname} = $value;
    } else {
        die "Variable name $vname or value $value undefined";
    }
}

# Register remote file
# keep track of a remote file that can be subsequently checked, modified, 
# sabotaged (to test robustness) and eventually dropped (also automatically)
# here, we just record the name
sub register_remote ( $$ )
{
    my ( $filename, $type ) =  ( shift, shift );
    if ( ! defined $type ) { $type = "file"; } 
    my %file_record = ( 'name' => $filename, 'type' => $type );
    my $file_index = push @remote_files,  \%file_record;
    return $file_index - 1;
}

# Check remote monitor has found anything
sub poll_rm_readyness ( $$ )
{
    my ( $interval, $timeout ) = ( shift, shift );
    my $start_time = time();
    while ( ($start_time + $timeout) >= time()) {
        my $query=`rmGetNodes`;
        if ( $query =~ /name:/ ) {
	    print "RM ready after ".(time() - $start_time)."s.\n";
            return;
        }
	sleep ( $interval );
    }
    die "Timeout in poll_rm_readyness";
 
}

# Check fileservers ready with timeout
sub poll_fileserver_readyness ( $$ )
{
    my ( $interval, $timeout ) = ( shift, shift );
    my $start_time = time();
    while ( ($start_time + $timeout) >= time()) {
        my $query=`su $environment{username} -c \"stager_qry -s\"`;
        if ( $query =~ /FILESYSTEM_PRODUCTION/ ) {
	    print "Fileservers ready after ".(time() - $start_time)."s.\n";
            return;
        }
	sleep ( $interval );
    }
    die "Timeout in poll_fileserver_readyness";
}

# Clean disk servers of files (to be done after dropping the stager DB).
sub clean_fileservers ()
{
    print "Cleaning leftover files on the files servers. Creating /tmp/rfiod for testsuite. Setting permissions on it.\n";
    print `rmGetNodes  | grep name | perl -p -e 's/^.*name: //'  | xargs -i ssh root\@{} rm /srv/castor/*/*/* 2>&1`;
    print `rmGetNodes  | grep name | perl -p -e 's/^.*name: //'  | xargs -i ssh root\@{} mkdir -p /tmp/rfiod 2>&1`;
    print `rmGetNodes  | grep name | perl -p -e 's/^.*name: //'  | xargs -i ssh root\@{} chmod a+rw /tmp/rfiod 2>&1`;
    print `rmGetNodes  | grep name | perl -p -e 's/^.*name: //'  | xargs -i ssh root\@{} "perl -p -i -e \'s|(RFIOD.*PathWhiteList).*\$|\\\$1 /var/tmp/rfiod /tmp/rfiod|\' /etc/castor/castor.conf" 2>&1`;
    print "Done\n";
}

# create a local seed file, returning the index to the file.
# Take 1 parameter: the size in 
sub make_seed ( $ )
{
    my $size = shift;
    ((defined $size)  || ($size < 0)) or die "In $package_name::make_seed: size not defined";
    my $file_name = `mktemp`;
    chomp $file_name;
    my $megs = int ($size/(1024*1024));
    my $kilos = int (($size%(1024*1024))/1024);
    my $bytes = $size % 1024;
    my $kflags="";
    my $bflags="";
    
    print "t=".elapsed_time()."s. Creating seed file $file_name\n";
    my $starttime = time();
    if ($megs != 0) {
	`dd if=/dev/urandom of=$file_name bs=1M count=$megs 2>&1`;
	$kflags=" oflag=append conv=notrunc";
	$bflags=" oflag=append conv=notrunc";
    }
    if ($kilos != 0) {
	`dd if=/dev/urandom of=$file_name bs=1K count=$kilos $kflags 2>&1`;
	$bflags=" oflag=append conv=notrunc";
    }
    if ($bytes != 0) {
	`dd if=/dev/urandom of=$file_name bs=1 count=$bytes $bflags 2>&1`;
    }
    
    # Check that everything went fine
    if (!-e $file_name) {
	die "In $package_name::make_seed: file $file_name not created";
    }
    if (! (-s $file_name) == $size) {
	die "In $package_name::make_seed: file $file_name created with wrong size";
    }
    my $endtime = time();
    my $interval = ($endtime - $starttime);
    if ($interval == 0) { $interval = 1; }
    print "t=".elapsed_time()."s. Seed file created in ".$interval."s. Speed=".
	($size/(1024*1024)/$interval)."MB/s.\n";
    
    # Hand over the file to the user
    print `chown $environment{'username'} $file_name`;
    
    # We call it good enough
    my $checksum = `adler32 $file_name 2>&1`;
    chomp ($checksum);
    my %file_entry = ( 'name' => $file_name, 
		       'size' => $size,
                     'adler32' => $checksum);
    
    my $file_index = push @local_files, \%file_entry;
    $file_index --;
    return $file_index;
}


# Create a local file from seed, with its name prepended, and recompute the checksum
# return index to the file in the local files array.
#
sub make_localfile ( $$ )
{
    my ($seed_index, $file_name) = ( shift, shift );
    open NEW_FILE, "> $file_name" or die "In $package_name::make_localfile: failed to open $file_name for writing: $!";
    print NEW_FILE "$file_name";
    close NEW_FILE;
    `dd if=$local_files[$seed_index]->{'name'} of=$file_name bs=1M oflag=append conv=notrunc 2>&1`;
    my $checksum = `adler32 $file_name 2>&1`;
    chomp ($checksum);
    my $size =  ( -s $file_name );
    my %file_entry = ( 'name' => $file_name, 
		       'size' => $size,
		       'adler32' => $checksum);
    my $file_index = push @local_files, \%file_entry;
    $file_index --;
    print "t=".elapsed_time()."s. Created local file $file_name\n";
    return $file_index;
}


# Utility functions for polling states of files.

# Returns true if the file is of size zero or migrated (m bit), false otherwise
# Dies on the first problem.
sub check_migrated_in_ns ( $ )
{
    my $file_name = shift;
    my $nsls=`nsls -l $file_name`;
    if ($nsls=~ /^([\w-]+)\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)/ ) {
        return ( ('m' eq substr ($1,0,1)) || ($5 == 0) );
    } else {
        warn "Failed to find file $file_name in ns. Assuming m bit.";
        return 1;
    }
}

# Returns true if the file is declared as invalid in the stager
sub check_invalid ( $ )
{
    my $file_name = shift;
    my $stager_qry=`su $environment{username} -c \"stager_qry -M $file_name\"`;
    return ( $stager_qry=~ /INVALID|No such file or directory/ || $stager_qry=~ /^$/);
}

# Returns true if the file is recalled
sub check_recalled_or_fully_migrated ( $ )
{
    my $file_name = shift;
    my $stager_qry=`su $environment{username} -c \"stager_qry -M $file_name\"`;
    return $stager_qry=~ /STAGED/;
}

# Utility functions for breaking the files

# Missing structures: local to the file, and in-db
sub remove_castorfile( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "BEGIN
          DELETE FROM CastorFile cf
           WHERE cf.lastKnownFileName = :NSNAME;
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed castorfile for file: $name\n";
}

sub remove_second_segment ( $$ )
{
    my ($dbh, $name) = (shift, shift);
    `su $environment{username} -c \"nsdelsegment --copyno=2 $name\"`;
    print "t=".elapsed_time()."s. Removed from ns second segment for file: $name\n";
}

sub remove_diskcopy( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varCastorFileId NUMBER;
        BEGIN
          SELECT cf.Id
            INTO varCastorFileId
            FROM CastorFile cf
           WHERE cf.lastKnownFileName = :NSNAME;
          DELETE FROM diskcopy dc
           WHERE dc.castorFile = varCastorFileId;
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed diskcopy for file: $name\n";
}

sub remove_filesystem( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varCastorFileId  NUMBER;
          varFakeFSId      NUMBER;
        BEGIN
          SELECT cf.Id
            INTO varCastorFileId
            FROM CastorFile cf
           WHERE cf.lastKnownFileName = :NSNAME;
          SELECT ids_seq.nextval INTO varFakeFSId FROM DUAL;
          UPDATE diskcopy dc
             SET dc.fileSystem = varFakeFSId
           WHERE dc.castorFile = varCastorFileId;
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed filesystem reference for file: $name\n";
}

sub remove_fileclass( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varFakeFCId  NUMBER;
        BEGIN
          SELECT ids_seq.nextval INTO varFakeFCId FROM DUAL;
          UPDATE CastorFile cf
             SET cf.fileClass = varFakeFCId
           WHERE cf.lastKnownFileName = :NSNAME;
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed fileclass reference for file: $name\n";
}

sub remove_serviceclass( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varFakeSCId  NUMBER;
        BEGIN
          SELECT ids_seq.nextval INTO varFakeSCId FROM DUAL;
          UPDATE CastorFile cf
             SET cf.svcClass = varFakeSCId
           WHERE cf.lastKnownFileName = :NSNAME;
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed serviceclass reference for file: $name\n";
}

sub remove_migration_mount( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varMigrationMountIds \"numList\";
        BEGIN
          SELECT mm.id
            BULK COLLECT INTO varMigrationMountIds
            FROM MigrationMount mm
           INNER JOIN MigrationJob mj ON mj.tapegatewayrequestid = mm.tapegatewayrequestid
           INNER JOIN Castorfile cf ON cf.id = mj.castorFile
           WHERE cf.lastKnownFileName = :NSNAME;
          DELETE FROM MigrationMount mm
           WHERE mm.id IN (SELECT * FROM TABLE (varMigrationMountIds));
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed migration mount(s) for file: $name\n";
}

sub remove_tapepool( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varMigrationMountIds \"numList\";
          varFakeTPId  NUMBER;
        BEGIN
          SELECT ids_seq.nextval INTO varFakeTPId FROM DUAL;
          SELECT mm.id
            BULK COLLECT INTO varMigrationMountIds
            FROM MigrationMount mm
           INNER JOIN MigrationJob mj ON mj.tapeGatewayRequestId = mm.tapeGatewayRequestId
           INNER JOIN Castorfile cf ON cf.id = mj.castorFile
           WHERE cf.lastKnownFileName = :NSNAME;
          UPDATE Stream s
             SET s.tapePool = varFakeTPId
           WHERE s.id IN (SELECT * FROM TABLE (varStreamIds));
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed tapepool reference for file: $name\n";
}

sub remove_segment( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varRecallJobIds \"numList\";
        BEGIN
          SELECT rc.id
            BULK COLLECT INTO varRecallJobIds
            FROM RecallJob rj
           INNER JOIN Castorfile cf ON cf.id = rj.castorFile
           WHERE cf.lastKnownFileName = :NSNAME;
          DELETE FROM Segment seg
           WHERE seg.copy IN (SELECT * FROM TABLE (varRecallJobIds));
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed segement for file: $name\n";
}

sub remove_ns_entry( $$ )
{
    my ($dbh, $name) = (shift, shift);
    `su $environment{username} -c \"nsrm $name\"`;
    print "t=".elapsed_time()."s. Removed ns entry for file: $name\n";
}

# Broken parameters: local to the file and on outside services
sub corrupt_checksum( $$ )
{
    my ($dbh, $name) = (shift, shift);
    `su $environment{username} -c \"nssetchecksum -n AD -k deadbeef $name\"`;
    print "t=".elapsed_time()."s. Corrupted checksum in ns for file: $name\n";
}

sub corrupt_size( $$ )
{
    my ($dbh, $name) = (shift, shift);
    `su $environment{username} -c \"nssetfsize -x 1337 $name\"`;
    print "t=".elapsed_time()."s. Corrupted size in ns for file: $name\n";
}

sub corrupt_segment( $$ )
{
    my ($dbh, $name) = (shift, shift);
    `su $environment{username} -c \"nssetsegment -s 1 -c 1 -d $name\"`;
    print "t=".elapsed_time()."s. Corrupted segment in ns for file: $name\n";
}

# System-wide breakings (triggered during the lifecycle if the file)
sub break_diskserver( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "BEGIN
          DELETE FROM DiskServer ds
           WHERE ds.id IN (
                SELECT fs.diskServer
                  FROM FileSystem fs
                 INNER JOIN DiskCopy dc ON dc.fileSystem = fs.id
                 INNER JOIN Castorfile cf ON cf.id = dc.castorFile
                 WHERE cf.lastKnownFileName = :NSNAME);
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Deleteed diskserver for file: $name\n";
}

sub error_injector ( $$$ )
{
    my ( $dbh, $index, $stage ) = ( shift, shift, shift );
    my %file = %{$remote_files[$index]};
    
    if (defined $file{breaking_type} && 
        $file{breaking_type}=~ /on ${stage}$/ && 
	!$file{breaking_done}) {
        # Missing structures: local to the file, and in-db
        if ($file{breaking_type} =~ /^missing castorfile/) {
             remove_castorfile($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing diskcopy/) {
             remove_diskcopy($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing filesystem/) {
             remove_filesystem($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing fileclass/) {
             remove_fileclass($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing serviceclass/) {
             remove_serviceclass($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing migration mount/) {
             remove_migration_mount($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing tapepool/) {
             remove_tapepool($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing segment/) {
             remove_segment($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing ns entry/) {
             remove_ns_entry($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        # Broken parameters: local to the file and on outside services
        } elsif ($file{breaking_type} =~ /^wrong checksum/) {
             corrupt_checksum($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^wrong size/) {
             corrupt_size($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^wrong segment/) {
             corrupt_segment($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing second tape segment in ns/) {
             remove_second_segment($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        # System-wide breakings (triggered during the lifecycle if the file)
        } elsif ($file{breaking_type} =~ /^broken diskserver/) {
             break_diskserver($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        }
    }
}

# remote files statuses:
# => rfcped -> partially migrated -> migrated => invalidation requested -> on tape => being recalled -> recalled
# 
# all self-transtions (->) are followed up in the same loop, with corresponding checks...
# all test-program initiated transitions (=>) are initiated from a different function...
# rfcped is created by rfcp_localfile
# invalidation requested is generated by cleanup_migrated
# being recalled is generated by stager_reget_from_tape.


# rfcp file to either single or dual copy directory and push all the
# know properties of the local file to the remote file created for the occasion
sub rfcp_localfile ( $$ )
{
    my ( $local_index, $is_dual_copy ) = ( shift, shift );
    my $dest = $environment{castor_directory};
    if ( $is_dual_copy ) { 
        $dest .= $environment{castor_dual_subdirectory};
    } else {
        $dest .= $environment{castor_single_subdirectory};
    }
    my $local = $local_files[$local_index]->{name};
    my $remote = $dest;
    if ( $local =~ /\/([^\/]*)$/ ) {
        $remote .= "/".$1;
    } else {
        die "Wrong file path in rfcp_localfile";
    }
    my $rfcp_ret=`su $environment{username} -c \"STAGE_SVCCLASS=$environment{svcclass} rfcp $local $remote\" 2>&1`;
    my %remote_entry = ( 'name' => $remote, 
                         'type' => "file",
                         'size' => $local_files[$local_index]->{size},
                         'adler32' => $local_files[$local_index]->{adler32},
                         'status' => 'rfcped' );
    push @remote_files, \%remote_entry;
    print "t=".elapsed_time()."s. rtcp'ed $local => $remote:\n";
    for ( split /^/, $rfcp_ret ) {
	   if ( ! /bytes in/ ) {
	       print $_."\n";
	   }
    }
}

# rfcp file, but give it a special flag that decides where it will break from the beginning. Then all the following handling
# functions will be able to inject the problem if they have to.
# finally, manage the error handling for the rfcp step (if needed).
sub rfcp_localfile_break ( $$$$ )
{
    my ($dbh, $local_index, $is_dual_copy, $breaking_type) = (shift, shift, shift, shift );
    my $dest = $environment{castor_directory};
    if ( $is_dual_copy ) { 
        $dest .= $environment{castor_dual_subdirectory};
    } else {
        $dest .= $environment{castor_single_subdirectory};
    }
    my $local = $local_files[$local_index]->{name};
    my $remote = $dest;
    if ( $local =~ /\/([^\/]*)$/ ) {
        $remote .= "/".$1;
    } else {
        die "Wrong file path in rfcp_localfile";
    }
    my $rfcp_ret=`su $environment{username} -c \"STAGE_SVCCLASS=$environment{svcclass} rfcp $local $remote\" 2>&1`;
    my %remote_entry = ( 'name' => $remote, 
                         'type' => "file",
                         'size' => $local_files[$local_index]->{size},
                         'adler32' => $local_files[$local_index]->{adler32},
                         'status' => 'rfcped' );
    if (defined $breaking_type) {
        $remote_entry{breaking_type} = $breaking_type;
        $remote_entry{breaking_done} = 0;
    }
    # Push new element in array and record index
    my $remote_index = (push @remote_files, \%remote_entry) -1;
    print "t=".elapsed_time()."s. rtcp'ed $local => $remote with error attached: ".$breaking_type."\n";
    for ( split /^/, $rfcp_ret ) {
	if ( ! /bytes in/ ) {
	    print $_."\n";
	}
    }
    
    # File creation is done. We might have to deal with breaking the file already
    error_injector ( $dbh, $remote_index, "rfcp" );
}

# Remove staged files from the stager so they can be recalled.
# Inject errors at this stage if requested.
sub cleanup_migrated ( $ )
{
    my $dbh = shift;
    for ( my $i =0; $i < scalar (@remote_files); $i++ ) {
	my %f = %{$remote_files[$i]};
	if ( $f{type} eq "file" && $f{status} eq "migrated" ) {
	    `su $environment{username} -c \"STAGE_SVCCLASS=$environment{svcclass} stager_rm -M $f{name}\"`;
	    $remote_files[$i]->{status} = "invalidation requested";
	    print "t=".elapsed_time()."s. Removed $f{name} (was migrated) from stager.\n";
            
            error_injector ( $dbh, $i, "invalidation" );
	}
    }
}

# Stager_get file
sub stager_reget_from_tape ( $ )
{
    my $dbh = shift;
    for ( my $i =0; $i < scalar (@remote_files); $i++ ) {
	my %f = %{$remote_files[$i]};
	if ( $f{type} eq "file" && $f{status} eq "on tape" ) {
	    `su $environment{username} -c \"STAGE_SVCCLASS=$environment{svcclass} stager_get -M $f{name}\"`;
	    $remote_files[$i]->{status} = "being recalled";
	    print "t=".elapsed_time()."s. Initiated recall for $f{name}.\n";
            
            error_injector ( $dbh, $i, "reget" );
	}
    }    
}

# Check remote entries: check presence (should be always true) and then status of all files listed in remote files list
# returns true is any file has changed status, allowig a caller function to tiem out on "nothing moves anymore"
sub check_remote_entries ( $ )
{
    my $dbh = shift;
    my $changed_entries = 0;
    for  my $i ( 0 .. scalar (@remote_files) - 1 ) {
        my %entry = %{$remote_files[$i]};
        # check presence
        my $nslsresult=`nsls -d $entry{name} 2>&1`;
        if ( $nslsresult =~ /No such file or directory$/ && 
             !( defined $entry{breaking_type} && 
                $entry{breaking_done} && 
                $entry{breaking_type} =~ /^missing ns entry/ ) 
             ) {
            die "Entry not found in name server: $entry{name}";
        }
	undef $nslsresult;
        # if it's a file we can do something. 
        if ( $entry{type} eq "file" ) {
	    # check the migration status and compare checksums.
            if ( $entry{status} eq "rfcped" ) {
                if ( check_migrated_in_ns ( $entry{name} ) ) {
		    # Report the newly detected migration.
		    print "t=".elapsed_time()."s. File ".$entry{name}." now partially migrated to tape (has 'm' bit).\n";
                    $remote_files[$i]->{status} = "partially migrated";
		    # Validate the checksum.
		    my $remote_checksum_string = `su $environment{username} -c \"nsls --checksum $entry{name}\"`;
		    chomp $remote_checksum_string;
		    my ( $remote_checksum, $local_checksum );
		    if ( $remote_checksum_string =~ /AD\s+([[:xdigit:]]+)\s/ ) {
			$remote_checksum = $1;
		    } else {
			print "ERROR: Failed to interpret remote checksum: $remote_checksum_string.\n";
		    }
		    if ( $entry{adler32} =~ /adler32\(.*\) = \d+\, 0x([[:xdigit:]]+)/ ) {
			$local_checksum = $1;
		    } else {
			print "ERROR: Failed to interpret locally stored checksum: $entry{adler32}.\n";
		    }
		    if ( defined $remote_checksum && ( $local_checksum ne $remote_checksum) ) {
			print "ERROR: checksum mismatch beween remote and locally stored for $entry{name}: $local_checksum != $remote_checksum\n";
		    }
                    $changed_entries ++;
                    error_injector ( $dbh, $i, "partial migration" );
                }
	    # check the number of tape copies after the full migration
	    } elsif ( $entry{status} eq "partially migrated" ) {
		if ( check_recalled_or_fully_migrated ( $entry{name} ) ) {		    
		    # Report the newly detected migration.
		    print "t=".elapsed_time()."s. File ".$entry{name}." now fully migrated to tape (from stager's point of view).\n";
		    $remote_files[$i]->{status} = "migrated";
		    # Check that dual tape copies got mirgated as expected (and on different tapes)
		    $nslsresult = `su $environment{username} -c \"nsls --class $entry{name}\"`;
		    if ( $nslsresult =~ /^\s+(\d+)\s+\/castor/ ) {
			my $class = $1;
			my $nslistclass_result = `su $environment{username} -c \"nslistclass --id=$class\"`;
			if ( $nslistclass_result =~ /NBCOPIES\s+(\d+)/ ) {
			    my $expected_copynb = $1;
			    my $found_copynb = `su $environment{username} -c \"nsls -T $entry{name} 2>/dev/null | wc -l\"`;
			    if ( $expected_copynb != $found_copynb ) {
				print "ERROR: Unexpected number of copies for $entry{name}. Expected: $expected_copynb, found:$found_copynb\n";
				print "stager record: ".`su $environment{username} -c \"stager_qry -M $entry{name}\"`;
				print "TODO: handle this state as partially migrated\n";
			    }
			} else {
			    print "ERROR: Failed to extract copynb for file class $class while processing file $entry{name}.\n";
			}
		    } else {
			print "ERROR: Failed to extract class for file ".$entry{name}."\n";
		    }
		    $changed_entries ++;
                    error_injector ( $dbh, $i, "migrated" );
                    }
	    # check the invalidation status
	    } elsif ($entry{status} eq "invalidation requested" ) {
		if ( check_invalid $entry{name} )  {
		    print "t=".elapsed_time()."s. File ".$entry{name}." reported as invalid by the stager.\n";
		    $remote_files[$i]->{status} = "on tape";
		    $changed_entries ++;
                    error_injector ( $dbh, $i, "on tape" );
		}
	    # check the recall status, rfcp in and compare checksums.
            } elsif ($entry{status} eq "being recalled" ) {
		if ( check_recalled_or_fully_migrated ( $entry{name} ) ) {
		    print "t=".elapsed_time()."s. File ".$entry{name}." now recalled to disk.\n";
		    $remote_files[$i]->{status} = "recalled";
		    # rfcp in the file, check size and checksum.
		    my $local_copy=`mktemp`;
		    chomp $local_copy;
		    # hand over the file to the user
		    `chown $environment{username} $local_copy`;
		    # rfcp the recalled copy
		    `su $environment{username} -c \"STAGE_SVCCLASS=$environment{svcclass} rfcp $entry{name} $local_copy\"`;
		    # compute checksum, get size and get rid of file
		    my $local_size = ( -s $local_copy );
		    my $local_checksum_string = `adler32 $local_copy 2>&1`;
		    my $local_checksum;
		    if ( $local_checksum_string =~ /adler32\(.*\) = \d+\, 0x([[:xdigit:]]+)/ ) {
			$local_checksum = $1;
		    }
		    my $stored_checksum;
		    if ( $entry{adler32} =~ /adler32\(.*\) = \d+\, 0x([[:xdigit:]]+)/ ) {
			$stored_checksum = $1;
		    }
		    if ( $stored_checksum ne $local_checksum ) {
			print "ERROR: checksum mismatch beween locally recalled file and stored value for $entry{name}: ".
			    "$local_checksum != $stored_checksum\n";
		    }
		    if ( $local_size != $entry{size} ) {
			print "ERROR: size mismatch beween locally recalled file and stored value for $entry{name}: ".
			    "$local_size != ".$entry{size}."\n";
		    }
		    unlink $local_copy;
		    print "t=".elapsed_time()."s. rfcped ".$entry{name}." back and checked it.\n";
		    $changed_entries ++;
                    error_injector ( $dbh, $i, "recalled" );
                }
	    }
	}
    }
    return $changed_entries;
}

# unblock_stuck_files
# Caller got enough and timed out. We will iterate the files as in check_remote_entries and kill whatever has a pending move.
# Report status of files as screen printouts.
# No return values, but update the pending files repository as the calles will repoll in this list (finding it empty if all went well).
sub unblock_stuck_files ()
{
    my $dbh;
    my @dropped_files_indexes;
    if (!defined $dbh) { $dbh = open_db(); }
    print_migration_and_recall_structures( $dbh );
    print "Will unblock stuck files.\n";
    for  my $i ( 0 .. scalar (@remote_files) - 1 ) {
        my %entry = %{$remote_files[$i]};
        # check presence
        my $nslsresult=`nsls -d $entry{name} 2>&1`;
        if ( $nslsresult =~ /No such file or directory$/ && 
             !( defined $entry{breaking_type} && 
                $entry{breaking_done} && 
                $entry{breaking_type} =~ /^missing ns entry/ ) ) {
            die "Entry not found in name server: $entry{name}";
        }
	undef $nslsresult;
        # if it's a file we can do something. 
        if ( $entry{type} eq "file" ) {
	    # check the migration status and compare checksums.
            if (0 && $entry{status} =~ /^(rfcped|partially migrated|being recalled)$/) {
                print "t=".elapsed_time()."s. File ".$entry{name}." still in ".$entry{status}." state.\n";
                my $nsid = `su $environment{username} -c \"nsls -i $entry{name}\"`;
                if ($nsid =~ /^\s+(\d+)/ ) {
                    print "t=".elapsed_time()."s. NSFILE=".$1."\n";
                }
                my $stager_status = `su $environment{username} -c \"stager_qry -M $entry{name}\"`;
                print "t=".elapsed_time()."s. stager status=".$stager_status."\n";
                if (defined $entry{breaking_type} && $entry{breaking_done}) {
                    print "t=".elapsed_time()."s. This file WAS broken by:".$entry{breaking_type}."\n";
                }
                # Kill tapecopies, un queue them from stream, massage diskcopies and castorfile, stager_rm file, poll the completion of stager_rm, nsrm on top for safety...
                # Step one, artificially declare the job done on migrations. Given the way tests work (no moves), we suppose we can find the file by last_known_name in castor file table.
                if (!defined $dbh) { $dbh = open_db(); }
                my $stmt = $dbh->prepare ("DECLARE
                                  varCastorFileId     NUMBER;
                                  varMigrationJobIds  \"numList\";
                                  varRecallJobIds     \"numList\";
                                  varDiskCopyIds      \"numList\";
                                BEGIN
                                  SELECT cf.Id INTO varCastorFileId
                                    FROM CastorFile cf
                                   WHERE cf.lastKnownFileName = :NSNAME
                                     FOR UPDATE;
                                     
                                  SELECT mj.Id
                                    BULK COLLECT INTO varMigrationJobIds
                                    FROM MigrationJob mj
                                   WHERE mj.CastorFile = varCastorFileId
                                     FOR UPDATE;
                                     
                                  SELECT rj.Id
                                    BULK COLLECT INTO varRecallJobIds
                                    FROM RecallJob rj
                                   WHERE rj.CastorFile = varCastorFileId
                                     FOR UPDATE;
                                     
                                  SELECT dc.Id
                                    BULK COLLECT INTO varDiskCopyIds
                                    FROM DiskCopy dc
                                   WHERE dc.CastorFile = varCastorFileId
                                     FOR UPDATE;
                                     
                                  DELETE FROM MigrationJob mj
                                   WHERE mj.id in (SELECT * FROM TABLE (varMigrationJobIds));
                                   
                                  DELETE FROM RecallJob rj
                                   WHERE rj.id in (SELECT * FROM TABLE (varRecallJobIds));
                                                                     UPDATE DiskCopy dc
                                     SET dc.status = 0 -- DISKCOPY_STAGED
                                   WHERE dc.id IN (SELECT * FROM TABLE (varDiskCopyIds));
                                   
                                  COMMIT;
                                END;");
                $stmt->bind_param (":NSNAME", $entry{name});
                $stmt->execute();
                # File is now "migrated" dump it from stager...
                `su $environment{username} -c \"stager_rm -M $entry{name}\"`;
                # ...and from ns
                `su $environment{username} -c \"nsrm $entry{name}\"`;
                print "t=".elapsed_time()."s. FAILURE: File ".$entry{name}.
                  " forcedfully dropped. Previous state was :".
                  $entry{status}."\n";
                $remote_files[$i]->{status} = "deleted stuck file (was ".$entry{status}.")";
                push ( @dropped_files_indexes, $i ); 
            # check the invalidation status
	    } elsif ($entry{status} =~ /^(rfcped|partially migrated|being recalled|invalidation requested)$/ ) {
		if (!defined $dbh) { $dbh = open_db(); }
		if ( !check_invalid $entry{name} )  {
                    print "t=".elapsed_time()."s. File ".$entry{name}." still in ".$entry{status}." state.\n";
                    my $nsid = `su $environment{username} -c \"nsls -i $entry{name}\"`;
                    if ($nsid =~ /^\s+(\d+)/ ) {
                        print "t=".elapsed_time()."s. NSFILE=".$1."\n";
                    }
                    my $stager_status = `su $environment{username} -c \"stager_qry -M $entry{name}\"`;
                    print "t=".elapsed_time()."s. stager status=".$stager_status."\n";
                    if (defined $entry{breaking_type} && $entry{breaking_done}) {
                        print "t=".elapsed_time()."s. This file WAS broken by:".$entry{breaking_type}."\n";
                    }
                    print "Full information:\n".Dumper (\%entry);
                    print_file_info ($dbh, $entry{name});
                    my $stmt = $dbh->prepare ("DECLARE
                                      varCastorFileId     NUMBER;
                                      varMigrationJobIds  \"numList\";
                                      varRecallJobIds     \"numList\";
                                      varDiskCopyIds      \"numList\";
                                    BEGIN
                                      BEGIN
                                        SELECT cf.Id INTO varCastorFileId
                                          FROM CastorFile cf
                                         WHERE cf.lastKnownFileName = :NSNAME
                                           FOR UPDATE;
                                      EXCEPTION WHEN NO_DATA_FOUND THEN
                                        varCastorFileId := NULL;
                                      END;
                                      
                                      IF varCastorFileId IS NOT NULL THEN
                                        SELECT mj.Id
                                          BULK COLLECT INTO varMigrationJobIds
                                          FROM MigrationJob mj
                                         WHERE mj.CastorFile = varCastorFileId
                                           FOR UPDATE;
                                        
                                        SELECT rj.Id
                                          BULK COLLECT INTO varRecallJobIds
                                          FROM RecallJob rj
                                         WHERE rj.CastorFile = varCastorFileId
                                           FOR UPDATE;
                                           
                                        SELECT dc.Id
                                          BULK COLLECT INTO varDiskCopyIds
                                          FROM DiskCopy dc
                                         WHERE dc.CastorFile = varCastorFileId
                                           FOR UPDATE;
                                           
                                        DELETE FROM MigrationJob mj
                                         WHERE mj.id IN (SELECT * FROM TABLE (varMigrationJobIds));
                                         
                                        DELETE FROM RecallJob rj
                                         WHERE rj.id IN (SELECT * FROM TABLE (varRecallJobIds));
                                         
                                        DELETE FROM DiskCopy dc
                                         WHERE dc.id IN (SELECT * FROM TABLE (varDiskCopyIds));
                                         
                                        DELETE FROM CastorFile cf
                                         WHERE cf.id = varCastorFileId;
                                         
                                        COMMIT;
                                      END IF;
                                    END;");
                    $stmt->bind_param(":NSNAME", $entry{name});
                    $stmt->execute();
                    `su $environment{username} -c \"nsrm $entry{name}\"`;
                    print "t=".elapsed_time()."s. FAILURE: File ".$entry{name}." dropped via DB.\n";
                    $remote_files[$i]->{status} = "nuked from DB after stuck rfcp";
                    push ( @dropped_files_indexes, $i ); 
                }
            }
	}
    }
    # Drop the files from the tracking list, we don't care for them anymore, they're failed.
    # highest index first order drop to have no index shifting effect in the array.
    if ( defined $dbh ) { $dbh->disconnect(); }
    foreach my $i ( reverse ( @dropped_files_indexes ) ) {
        splice ( @remote_files, $i, 1 );
    }
}

# Get the number of files for which a move is expected.
sub count_to_be_moved ()
{
    my $ret = 0;
    for ( @remote_files ) {
        if ( ($_->{type} eq "file" ) && ($_ ->{status} =~ 
					 /rfcped|partially migrated|invalidation requested|being recalled/ ) ) {
            $ret++;
        }
    }
    return $ret;
}

# Check that all the files have migrated 
sub poll_moving_entries ( $$$$ )
{
    my ( $dbh, $poll_interval, $timeout, $options ) = ( shift, shift, shift, shift );
    my $starttime = time();
    while ( count_to_be_moved() > 0 && ((time() - $starttime) < $timeout) ) {
        if ( check_remote_entries ( $dbh ) ) {
            print "t=".elapsed_time()."s. Saw at least one new migration...\n";
            $starttime = time();
        }
	if ( $options =~ /cleanup_migrated/ ) {
	    cleanup_migrated( $dbh );
	}
	if ( $options =~ /stager_reget_from_tape/ ) {
	    stager_reget_from_tape( $dbh );
	}
        sleep ( $poll_interval );
    }
    # All the expected movements completed. Done successfully
    if (count_to_be_moved() == 0 ) {
	print "t=".elapsed_time()."s. All expected moves completed.\n";
        return;
    }
    # Some file are blocked. Hurray! We found errors. (It's the whole point of testing)
    # Log'em, unblock'en (if possible) clean'em up  and carry on.
    unblock_stuck_files();
    # Sleep a little bit to let the stager finish possible pending removals.
    sleep (5);
    if (count_to_be_moved() == 0 ) {
	print "t=".elapsed_time()."s. WARNING. All files successfully unblocked after timeout. Carrying on.\n";
        return;
    }
    warn "Timeout with ".count_to_be_moved()." files to be migrated after $timeout s.";
}

# Returns the value of the specified ORASTAGERCONFIG parameter.
#
# This subroutine prints an error message and aborts the entire tes script if
# it fails to get the value of the specified parameter.
#
# @param  paramName The name of the parameter whose value is to be retrieved.
# @return           The value of the parameter.
sub getOrastagerconfigParam ( $ )
{
  my $paramName = $_[0];

  my $foundParamValue = 0;
  my $paramValue = "";

  open(CONFIG, "</etc/castor/ORASTAGERCONFIG")
    or die "Failed to open /etc/castor/ORASTAGERCONFIG: $!";

  while(<CONFIG>) {
    chomp;
    if(m/^DbCnvSvc\s+$paramName\s+(\w+)/) {
      $paramValue = $1;
      $foundParamValue = 1;
    }
  }

  close CONFIG;

  die("ABORT: Failed to get ORASTAGERCONFIG parameter: paramName=$paramName\n")
    if(! $foundParamValue);

  return $paramValue;
}

# Returns the value of the specified ORAVDQMCONFIG parameter.
#
# This subroutine prints an error message and aborts the entire tes script if
# it fails to get the value of the specified parameter.
#
# @param  paramName The name of the parameter whose value is to be retrieved.
# @return           The value of the parameter.
sub getOraVdqmConfigParam ( $ )
{
  my $paramName = $_[0];
  my $foundParamValue = 0;
  my $paramValue = "";

  open(CONFIG, "</etc/castor/ORAVDQMCONFIG")
    or die "Failed to open /etc/castor/ORAVDQMCONFIG: $!";

  while(<CONFIG>) {
    chomp;
    if(m/^DbCnvSvc\s+$paramName\s+(\w+)/) {
      $paramValue = $1;
      $foundParamValue = 1;
    }
  }
  close CONFIG;
  die("ABORT: Failed to get ORAVDQMCONFIG parameter: paramName=$paramName\n")
    if(! $foundParamValue);
  return $paramValue;
}
# Returns the triplet of username, password, dbname for NS DB.
#
# This subroutine prints an error message and aborts the entire tes script if
# it fails to get the value of the specified parameter.
#
# @return           The triplet of username, password, dbname.
sub getOraCNSLoginInfo ( $ )
{
  open(CONFIG, "</etc/castor/NSCONFIG")
    or die "Failed to open /etc/castor/NSCONFIG: $!";

  while(<CONFIG>) {
    chomp;
    if(m/^([^\/]+)\/([^@]+)@(.*)/) {
        close CONFIG;
        return ( $1, $2, $3 );
    }
  }

  close CONFIG;

  die("ABORT: Failed to get NS Db credentials\n");
  return 0;
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
    close ORACFG;
    my $dbh= DBI->connect('dbi:Oracle:'.$db ,$u, $p,
      { RaiseError => 1, AutoCommit => 0}) 
      or die "Failed to connect to DB as ".$u.'\@'.$db;
    return $dbh;
}


# Returns the value of the specified parameter from the specified file.
#
# This subroutine prints an error message and aborts the entire test script if
# it fails to get the value of the specified parameter.
#
# @param  paramCategory The catagory of the parameter whose value is to be
#                       retrieved.
# @param  paramName     The name of the parameter whose value is to be
#                       retrieved.
# @param  paramFile     The configuration file's name
# @return               The value of the parameter.
sub getConfParam ( $ $ $  )
{
    my $paramCategory = $_[0];
    my $paramName     = $_[1];
    my $confFilename  = $_[2];
    
    open(CONFIG, $confFilename)
        or die "Failed to open $confFilename: $!";
    
    while(<CONFIG>) {
        chomp;
        if(m/^$paramCategory\s+$paramName\s+([^\s]+$)/) {
            close CONFIG;
            return $1;
        }
    }
    close CONFIG;
    die("ABORT: Failed to get $confFilename parameter: category=$paramCategory, name=$paramName\n");
}

# Returns the value of the specified /etc/castor/castor.conf parameter.
#
# This subroutine prints an error message and aborts the entire test script if
# it fails to get the value of the specified parameter.
#
# @param  paramCategory The catagory of the parameter whose value is to be
#                       retrieved.
# @param  paramName     The name of the parameter whose value is to be
#                       retrieved.
# @return               The value of the parameter.
sub getCastorConfParam ( $ $ )
{
    my ( $category, $parameter ) = ( shift, shift );
    return getConfParam ( $category, $parameter, "/etc/castor/castor.conf");
}


# Returns true if the daemon with specfied name is running else false.
#
# @param  daemonName The name of the daemon.
# @return            True if the daemon is runn ing, else false.
sub daemonIsRunning ( $ )
{
  my $daemonName = $_[0];

  my $psResult = `ps -e | grep $daemonName`;
  chomp($psResult);

  return($psResult =~ m/^\s*\d+\s+\?\s+\d\d:\d\d:\d\d\s+$daemonName$/ || $psResult =~ m/^\s*\d+\s+\?\s+\d\d:\d\d:\d\d\s+valgrind.*$daemonName$/);
}


# Tries to kill the specified daemon within the specified time-out period.  If
# the daemon is dead within the time-out period, then this subroutine returns
# 0, else this subroutine prints an error message and returns a value of 1.
#
# If the daemon process to be killed is already dead when this subroutine is
# called, then this method will succeed immediately.
#
# @param  processName The process name of the daemon to be killed.
# @param  timeOutSecs The time-out period in seconds.
# @return             0 on success and 1 on failure.
sub killDaemonWithTimeout ( $$ )
{
  my $processName = $_[0];
  my $timeOutSecs = $_[1];

  `/sbin/service $processName stop`;
  # In case we are running valgrind, let's not use gloves.
  `ps auxf | grep $processName | grep valgrind | grep -v grep | grep -v tail |  cut -c 10-15 | xargs kill 2> /dev/null`;

  if (daemonIsRunning($processName)) {
      sleep (1);
  } else {
      return 0;
  }

  if (daemonIsRunning($processName)) {
      `killall $processName`;
  } else {
      return 0;
  }

  if (daemonIsRunning($processName)) {
      sleep (1);
  } else {
      return 0;
  }
  if (daemonIsRunning($processName)) {
      `killall -9 $processName`;
  }

  my $startTime  = time();
  my $timeOutTime = $startTime + $timeOutSecs;

  while(1) {
    if(!&daemonIsRunning($processName)) {
      return 0; # Success
    }

    if(time() >= $timeOutTime) {
      die ("Failed to kill $processName\n");
      return 1; # Failure
    }
    sleep(1);
  }
}

# Start demons (from config file parameters)
sub startDaemons ()
{
    my %castor_deamons_locations =
	(
         'transfermanagerd' => './castor/scheduler/transfermanagerd',
	 'rechandlerd' => './castor/tape/rechandler/rechandlerd',
	 'rhd'         => './castor/rh/rhd',
	 'rmmasterd'   => './castor/monitoring/rmmaster/rmmasterd',
#         'stagerd'     => 'valgrind --log-file=/var/log/castor/stagerd-valgrind.%p --trace-children=yes ./castor/stager/daemon/stagerd',
#	 'stagerd'     => 'valgrind --log-file=/var/log/castor/stagerd-valgrind.%p --leak-check=full --track-origins=yes --suppressions=./tools/castor.supp --trace-children=yes ./castor/stager/daemon/stagerd',
         'stagerd'     => './castor/stager/daemon/stagerd',
	 'tapegatewayd'=> './castor/tape/tapegateway/tapegatewayd',
         'logprocessord' => './logprocessor/logprocessord'
	 );

    # Simply start all of them, demons not needed will jsut not start.
    for ( keys %castor_deamons_locations) {
        # First make sure the daemon is dead
        killDaemonWithTimeout ( $_, 2 );
        if ($environment{"local_".$_} =~ /True/) {
	    my $local_command = $castor_deamons_locations{$_};
            my $checkout_location=$environment{checkout_location};
            `( cd $checkout_location; LD_LIBRARY_PATH=\`find ./ -name "*.so*" | perl -p -e \'s|[^/]*\$|\n|\' | sort | uniq | tr \"\n\" \":\" | perl -p -e \'s/:\$/\n/\'\` $local_command )`;
	} else {
	    `service $_ start`;
	}        
    }
}

# Start one deamon (from config file parameters)
sub startSingleDaemon ( $ )
{
    my %castor_deamons_locations =
	(
         'transfermanagerd' => './castor/scheduler/transfermanagerd',
	 'rechandlerd' => './castor/tape/rechandler/rechandlerd',
	 'rhd'         => './castor/rh/rhd',
	 'rmmasterd'   => './castor/monitoring/rmmaster/rmmasterd',
	 'stagerd'     => 'valgrind --log-file=/var/log/castor/stagerd-valgrind.%p --suppressions=./tools/castor.supp --trace-children=yes ./castor/stager/daemon/stagerd',
#'./castor/stager/daemon/stagerd',
	 'tapegatewayd'=> './castor/tape/tapegateway/tapegatewayd',
         'logprocessord' => './logprocessor/logprocessord'
	 );

    # Simply start all of them, demons not needed will jsut not start.
    for ( shift ) {
        # First make sure the daemon is dead
        killDaemonWithTimeout ( $_, 2 );
        if ($environment{"local_".$_} =~ /True/) {
	    my $local_command = $castor_deamons_locations{$_};
            my $checkout_location=$environment{checkout_location};
            `( cd $checkout_location; LD_LIBRARY_PATH=\`find ./ -name "*.so*" | perl -p -e \'s|[^/]*\$|\n|\' | sort | uniq | tr \"\n\" \":\" | perl -p -e \'s/:\$/\n/\'\` $local_command )`;
	} else {
	    `service $_ start`;
	}        
    }
}

# Creates and calls an external script that calls sqlplus with the script name as a parameter
sub executeSQLPlusScript ( $$$$$ )
{
    my ( $u, $p , $db, $f, $title ) = ( shift,  shift,  shift,  shift, shift );
    if ( ! -e $f ) { die "ABORT: $f: file not found."; }
    my $tmpScript = `mktemp`;  # This creates and empty 0600 file.
    chomp $tmpScript;
    `echo "WHENEVER SQLERROR EXIT FAILURE;" > $tmpScript`;
    #`echo "SET ECHO ON;" >> $tmpScript`;
    `echo "CONNECT $u/$p\@$db" >> $tmpScript`;
    `echo >> $tmpScript`;
    `cat $f >> $tmpScript`;
    `echo >> $tmpScript`;
    `echo "EXIT;" >> $tmpScript`;
    print ("$title\n");
    my $result = `'sqlplus' /NOLOG \@$tmpScript`;
    unlink $tmpScript;
    print("\n");
    print("$title RESULT\n");
    print("===================\n");
    my @result_array = split(/\n/, $result);
    $result = "";
    for (@result_array) {
        # Pass on the boring "everything's fine" messages.
        if (! /^((Package(| body)|Function|Table|Index|Trigger|Procedure|View|\d row(|s)) (created|altered|updated)\.|)$/) {
            $result .= $_."\n";
        }
    }
    if ( $result =~/ERROR/ ) {
        print $result;
        die "Error encountered in SQL script";
    } else {
        print "SUCCESS\n";
    }
}


# Creates and calls an external script that calls sqlplus with the script name as a parameter
sub executeSQLPlusScriptNoFilter ( $$$$$ )
{
    my ( $u, $p , $db, $f, $title ) = ( shift,  shift,  shift,  shift, shift );
    if ( ! -e $f ) { die "ABORT: $f: file not found."; }
    my $tmpScript = `mktemp`;  # This creates and empty 0600 file.
    chomp $tmpScript;
    `echo "WHENEVER SQLERROR EXIT FAILURE;" > $tmpScript`;
    #`echo "SET ECHO ON;" >> $tmpScript`;
    `echo "CONNECT $u/$p\@$db" >> $tmpScript`;
    `echo >> $tmpScript`;
    `cat $f >> $tmpScript`;
    `echo >> $tmpScript`;
    `echo "EXIT;" >> $tmpScript`;
    print ("$title\n");
    my $result = `'sqlplus' /NOLOG \@$tmpScript`;
    unlink $tmpScript;
    print("\n");
    print("$title RESULT\n");
    print("===================\n");
    print $result;
}
# Switch to tapegatewayd (in DB, stop demons before, massages the DB as required)
sub stopAndSwitchToTapeGatewayd ( $ )
{
    my $dbh = shift;

    # Stop the mighunter, rechandler, tapegateway, rtcpclientd
    for my $i ('mighunterd', 'rechandlerd', 'tapegatewayd', 'rtcpclientd') {
        if (killDaemonWithTimeout ($i, 5)) {
            die "Failed to stop a daemon. Call an exorcist.";
        }
    }    
    #$dbh->do("UPDATE castorconfig SET value='tapegatewayd' WHERE class='tape' AND key='interfaceDaemon'");
    #$dbh->commit();
    # Migrate to tapegateway by script.
    my $dbUser   = &getOrastagerconfigParam("user");
    my $dbPasswd = &getOrastagerconfigParam("passwd");
    my $dbName   = &getOrastagerconfigParam("dbName");
    my $checkout_location   = $environment{checkout_location};
    my $dbDir               = $environment{dbDir};
    my $switch_to_tapegateway = $environment{switchoverToTapeGateway};
    
    my $switch_to_tapegateway_full_path = $checkout_location.'/'.$dbDir.'/'.$switch_to_tapegateway;
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $switch_to_tapegateway_full_path,
                           "Switching to Tape gateway");
}

# Switch to tapegatewayd (in DB, stop demons before, massages the DB as required)
sub stopAndSwitchToTapeGatewaydFromCheckout ( $$ )
{
    my ( $dbh, $checkout_location ) = ( shift, shift );

    # Stop the mighunter, rechandler, tapegateway, rtcpclientd
    for my $i ('mighunterd', 'rechandlerd', 'tapegatewayd', 'rtcpclientd') {
        if (killDaemonWithTimeout ($i, 5)) {
            die "Failed to stop a daemon. Call an exorcist.";
        }
    }    
    #$dbh->do("UPDATE castorconfig SET value='tapegatewayd' WHERE class='tape' AND key='interfaceDaemon'");
    #$dbh->commit();
    # Migrate to tapegateway by script.
    my $dbUser   = &getOrastagerconfigParam("user");
    my $dbPasswd = &getOrastagerconfigParam("passwd");
    my $dbName   = &getOrastagerconfigParam("dbName");
    my $dbDir               = $environment{dbDir};
    my $switch_to_tapegateway = "switchToTapegatewayd.sql";
    
    my $switch_to_tapegateway_full_path = $checkout_location.'/'.$dbDir.'/'.$switch_to_tapegateway;
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $switch_to_tapegateway_full_path,
                           "Switching to Tape gateway");
}

# get_db_version: pull out the db version in order to use the proper schema in queries
sub get_db_version ( $ )
{
    my $dbh = shift;
    my $sth = $dbh -> prepare ("SELECT COUNT(*) FROM user_objects WHERE object_name = 'CASTORVERSION'");
    $sth->execute();
    my @row = $sth->fetchrow_array();
    if ( $row[0] == 0 ) { return "No Version"; }
    $sth = $dbh -> prepare ("SELECT schemaversion FROM CastorVersion");
    $sth->execute();
    @row = $sth->fetchrow_array();
    return $row[0];
}

# check_leftovers : find wether there are any leftover unmigrated data in the stager
sub check_leftovers ( $ )
{
    my $dbh = shift;
    my $sth = $dbh -> prepare ("SELECT count (*) FROM TABS");
    $sth -> execute();
    my @row = $sth->fetchrow_array();
    if ($row[0] == 0) {
        return 0;
    }
    
    my $schemaversion = get_db_version ( $dbh );
    if ( $schemaversion =~ /^2_1_1[23]/ ) {
        $sth = $dbh -> prepare("SELECT count (*) from ( 
                              SELECT dc.id from diskcopy dc where
                                dc.status NOT IN ( 0, 7, 4 )
                              UNION ALL
                              SELECT mj.id from MigrationJob mj)");
                              # Discopy_staged diskcopy_invalid diskcopy_failed
    } elsif ( $schemaversion =~ /^2_1_11/ ) {
        $sth = $dbh -> prepare("SELECT count (*) from ( 
                              SELECT dc.id from diskcopy dc where
                                dc.status NOT IN ( 0, 7, 4 )
                              UNION ALL
                              SELECT tc.id from TapeCopy tc)");
    } elsif ( $schemaversion =~ /No Version/ ) {
        $sth = $dbh->prepare("SELECT 0 FROM DUAL");
        warn "Could no find schema version. Assuming no leftover.";
    } else {
        die "Unexpected schema version: $schemaversion";
    }
    $sth -> execute ();
    @row = $sth->fetchrow_array();
    return $row[0];
}

# check_leftovers_poll_timeout: loop poll
sub check_leftovers_poll_timeout ( $$$ )
{
    my ( $dbh, $interval, $timeout ) = ( shift, shift, shift );
    my $start_time = time();
    while (check_leftovers($dbh)) {
        if (time()> $start_time + $timeout) {
            return 1;
        }
    }
    return 0;
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

# print_leftovers
sub print_leftovers ( $ )
{
    my $dbh = shift;
    my $schemaversion = get_db_version ( $dbh );
    if ( $schemaversion =~ /^2_1_1[23]/ ) {
        # Print by castofile with corresponding tapecopies
        my $sth = $dbh -> prepare ("SELECT cf.lastknownfilename, dc.id, dc.status, mj.id, mj.status,
                                    rj.id, rj.status
                                  FROM castorfile cf
                                  LEFT OUTER JOIN diskcopy dc ON dc.castorfile = cf.id
                                  LEFT OUTER JOIN migrationJob mj ON mj.castorfile = cf.id
                                  LEFT OUTER JOIN recallJob rj ON rj.castorfile = cf.id
                                 WHERE dc.status NOT IN ( 0, 7, 4)"); # Discopy_staged diskcopy_invalid diskcopy_failed
        $sth -> execute();
        while ( my @row = $sth->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print( "Remaining catorfile for $row[0]\n\twith diskcopy (id=$row[1], ".
               "status=$row[2]), migrationJob (id=$row[3], status=$row[4]) ".
               "and recallJob (id=$row[5], status=$row[6])\n" );
        }
    } elsif ($schemaversion =~ /^2_1_11/ ) {
        my $sth = $dbh -> prepare ("SELECT cf.lastknownfilename, dc.id, dc.status, tc.id, tc.status 
                                  FROM castorfile cf
                                  LEFT OUTER JOIN diskcopy dc ON dc.castorfile = cf.id
                                  LEFT OUTER JOIN tapecopy tc ON tc.castorfile = cf.id
                                 WHERE dc.status NOT IN ( 0, 7, 4)"); # Discopy_staged diskcopy_invalid diskcopy_failed
        $sth -> execute();
        while ( my @row = $sth->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print( "Remaining catorfile for $row[0]\n\twith diskcopy (id=$row[1], ".
               "status=$row[2]) and tapecopy (id=$row[3], status=$row[4])\n" );
        }
    } elsif ( $schemaversion =~ /No Version/ ) {
        warn "Could no find schema version. Assuming no leftover.";
    } else {
        die "Unexpected schema version: $schemaversion";
    }
    if ( $schemaversion =~ /^2_1_1[23]/ ) {
        # print any other migration and recall jobs not covered previously
        my $sth = $dbh -> prepare ("SELECT cf.lastknownfilename, dc.id, dc.status, mj.id, mj.status, rj.id, rj.status
                                 FROM castorfile cf
                                 RIGHT OUTER JOIN diskcopy dc ON dc.castorfile = cf.id
                                 RIGHT OUTER JOIN migrationJob mj ON mj.castorfile = cf.id
                                 RIGHT OUTER JOIN recallJob rj ON rj.castorfile = cf.id
                                 WHERE dc.status IS NULL OR dc.status IN ( 0 )");
        $sth -> execute();
        while ( my @row = $sth->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print( "Remaining tape job for $row[0]\n\twith diskcopy (id=$row[1], ".
                   "status=$row[2]), migrationJob (id=$row[3], status=$row[4]), recallJob (id=$row[5], status=$row[6])\n" );
        }
    }  elsif ($schemaversion =~ /^2_1_11/ ) {
        my $sth = $dbh -> prepare ("SELECT cf.lastknownfilename, dc.id, dc.status, tc.id, tc.status 
                                 FROM castorfile cf
                                 LEFT OUTER JOIN diskcopy dc ON dc.castorfile = cf.id
                                 LEFT OUTER JOIN tapecopy tc ON tc.castorfile = cf.id
                                 WHERE dc.status NOT IN ( 0, 7, 4)");
        $sth -> execute();
        while ( my @row = $sth->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print( "Remaining tapecopy for $row[0]\n\twith diskcopy (id=$row[1], ".
                   "status=$row[2]) and tapecopy (id=$row[3], status=$row[4])\n" );
        }    
    } elsif ( $schemaversion =~ /No Version/ ) {
        warn "Could no find schema version. Assuming no leftover.";
    } else {
        die "Unexpected schema version: $schemaversion";
    }    
}

# print_file_info
sub print_file_info ( $$ )
{
    my ( $dbh, $filename ) = ( shift, shift );
    my $schemaversion = get_db_version ( $dbh );
    # Print as many infomation as can be regarding this file
    # Bypass stager and just dump from the DB.
    # First dump general file-related info
    my $stmt = $dbh -> prepare ("SELECT cf.lastknownfilename, cf.nsHost, cf.fileId, cf.id, fc.name
                                   FROM castorfile cf
                                   LEFT OUTER JOIN fileClass fc ON fc.id = cf.fileClass
                                  WHERE cf.lastKnownFileName = :FILENAME");
    $stmt->bind_param (":FILENAME", $filename);
    $stmt->execute();
    while ( my @row = $stmt->fetchrow_array() ) {
        nullize_arrays_undefs ( \@row );
        print  "Remaining catorfile for $row[0], NSID=$row[2] on SRV=$row[1] (DB id=$row[3], fileclass=$row[4]\n";
    }
    # Then dump the disk-related info
    $stmt = $dbh -> prepare ("SELECT dc.id, dc.status
                                  FROM castorfile cf
                                  LEFT OUTER JOIN diskcopy dc ON dc.castorfile = cf.id
                                 WHERE cf.lastKnownFileName = :FILENAME");
    $stmt->bind_param (":FILENAME", $filename);
    $stmt->execute();
    while ( my @row = $stmt->fetchrow_array() ) {
        nullize_arrays_undefs ( \@row );
        print  "    dc.id=$row[0], dc.status=$row[1]\n";
    }
    # Then dump the migration-related info
    if ( $schemaversion =~ /^2_1_13/ ) {
        $stmt = $dbh -> prepare ("SELECT mj.id, mj.status, mjtp.name, mm.id, mm.status, mm.vid, tp.name
                                      FROM castorfile cf
                                      INNER JOIN migrationJob mj on mj.castorfile = cf.id
                                      LEFT OUTER JOIN tapepool mjtp on mjtp.id = mj.tapepool
                                      LEFT OUTER JOIN migrationMount mm on mm.mountTransactionId = mj.mountTransactionId
                                      LEFT OUTER JOIN TapePool tp on tp.id = mm.tapepool
                                     WHERE cf.lastKnownFileName = :FILENAME");
        $stmt->bind_param (":FILENAME", $filename);
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    mj.id=$row[0], mj.status=$row[1] mjtp.name=$row[2] mm.id=$row[3] mm.status=$row[4] mm.vid=$row[5] tp.name=$row[6]\n";
        }
        # Finally dump the recall-related into
        $stmt = $dbh -> prepare ("SELECT rj.id, rj.status, rj.vid, rj.fseq, rj.nbmounts, rj.nbretrieswithinmount, rm.status
                                      FROM castorfile cf
                                      INNER JOIN recallJob rj on rj.castorfile = cf.id
                                      LEFT OUTER JOIN recallMount rm on rm.vid = rj.vid
                                     WHERE cf.lastKnownFileName = :FILENAME");
        $stmt->bind_param (":FILENAME", $filename);
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    rj.id=$row[0], rj.status=$row[1] rj.vid=$row[2] rj.nbmounts=$row[3] rj.nbretrieswithinmount=$row[4] rm.status=$row[5]\n";
        }
    } elsif ( $schemaversion =~ /^2_1_12/ ) {
        $stmt = $dbh -> prepare ("SELECT mj.id, mj.status, mjtp.name, mm.id, mm.status, mm.vid, tp.name, seg.id, seg.status, seg.errorcode, seg.errmsgtxt, seg.tape 
                                      FROM castorfile cf
                                      INNER JOIN migrationJob mj on mj.castorfile = cf.id
                                      LEFT OUTER JOIN tapepool mjtp on mjtp.id = mj.tapepool
                                      LEFT OUTER JOIN migrationMount mm on mm.tapegatewayRequestId = mj.tapegatewayRequestId
                                      LEFT OUTER JOIN TapePool tp on tp.id = mm.tapepool
                                      LEFT OUTER JOIN Segment seg on seg.copy = mj.id
                                     WHERE cf.lastKnownFileName = :FILENAME");
        $stmt->bind_param (":FILENAME", $filename);
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    mj.id=$row[0], mj.status=$row[1] mjtp.name=$row[2] mm.id=$row[3] mm.status=$row[4] mm.vid=$row[5] tp.name=$row[6] seg.id=$row[7] seg.status=$row[8] seg.errorcode=$row[9] seg.errmsgtxt=$row[10] seg.tape=$row[11]\n";
        }
        # Finally dump the recall-related into
        $stmt = $dbh -> prepare ("SELECT rj.id, rj.status, seg.id, seg.status, t.id, t.tpmode, t.status, seg.errorcode, seg.errmsgtxt, seg.tape
                                      FROM castorfile cf
                                      INNER JOIN recallJob rj on rj.castorfile = cf.id
                                      LEFT OUTER JOIN Segment seg on seg.copy = rj.id
                                      LEFT OUTER JOIN Tape t on t.id = seg.tape
                                     WHERE cf.lastKnownFileName = :FILENAME");
        $stmt->bind_param (":FILENAME", $filename);
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    rj.id=$row[0], rj.status=$row[1] seg.id=$row[2] seg.status=$row[3] t.id=$row[4] t.tpmode=$row[5] t.status=$row[6] seg.errorcode=$row[7] seg.errmsgtxt=$row[8] seg.tape=$row[9]\n";
        }
    } elsif ($schemaversion =~ /^2_1_11/ ) {
        $stmt = $dbh -> prepare ("SELECT tc.id, tc.status, tctp.name, s.id, s.status, s.vid, tp.name, seg.id, seg.status, seg.errorcode, seg.errmsgtxt, seg.tape 
                                      FROM castorfile cf
                                      INNER JOIN tapecopy tc on tc.castorfile = cf.id
                                      LEFT OUTER JOIN tapepool tctp on tctp.id = tc.tapepool
                                      LEFT OUTER JOIN stream2tapecopy sttc on sttc.child = tc.id
                                      LEFT OUTER JOIN stream s on sttc.parent = s.id
                                      LEFT OUTER JOIN TapePool tp on tp.id = s.tapepool
                                      LEFT OUTER JOIN Segment seg on seg.copy = tc.id
                                     WHERE cf.lastKnownFileName = :FILENAME
                                       AND tc.status not in (4,8)");
        $stmt->bind_param (":FILENAME", $filename);
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    tc.id=$row[0], tc.status=$row[1] tctp.name=$row[2] s.id=$row[3] s.status=$row[4] s.vid=$row[5] tp.name=$row[6] seg.id=$row[7] seg.status=$row[8] seg.errorcode=$row[9] seg.errmsgtxt=$row[10] seg.tape=$row[11]\n";
        }
        # Finally dump the recall-related into
        $stmt = $dbh -> prepare ("SELECT tc.id, tc.status, seg.id, seg.status, t.id, t.tpmode, t.status, seg.errorcode, seg.errmsgtxt, seg.tape
                                      FROM castorfile cf
                                      INNER JOIN tapecopy tc on tc.castorfile = cf.id
                                      LEFT OUTER JOIN Segment seg on seg.copy = rj.id
                                      LEFT OUTER JOIN Tape t on t.id = seg.tape
                                     WHERE cf.lastKnownFileName = :FILENAME
                                       AND tc.status in (4,8)");
        $stmt->bind_param (":FILENAME", $filename);
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    tc.id=$row[0], tc.status=$row[1] seg.id=$row[2] seg.status=$row[3] t.id=$row[4] t.tpmode=$row[5] t.status=$row[6] seg.errorcode=$row[7] seg.errmsgtxt=$row[8] seg.tape=$row[9]\n";
        }
    } else { 
        warn "Could no find schema version. Skipping migration and recall structures display.";
    }
}

# print_migration_and_recall_structures
sub print_migration_and_recall_structures ( $ )
{
    my ( $dbh ) = (shift);
    my $schemaversion = get_db_version ( $dbh );
    if ( $schemaversion =~ /^2_1_13/ ) {
        # print all the migration mounts (with tapepool info).
        print "Migration mounts snapshot:\n";
        my $stmt = $dbh -> prepare ("SELECT mm.id, mm.status, mm.mounttransactionid, mm.vid, tp.name
                                FROM MigrationMount mm
                                LEFT OUTER JOIN TapePool tp ON tp.id = mm.tapepool");
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    mm.id=$row[0], mm.status=$row[1] mm.mounttransactionid=$row[2] mm.vid=$row[3] tp.name=$row[4]\n";
        }
    } elsif ( $schemaversion =~ /^2_1_12/ ) {
        # print all the migration mounts (with tapepool info).
        print "Migration mounts snapshot:\n";
        my $stmt = $dbh -> prepare ("SELECT mm.id, mm.status, mm.tapegatewayrequestid, mm.vid, tp.name
                                FROM MigrationMount mm
                                LEFT OUTER JOIN TapePool tp ON tp.id = mm.tapepool");
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    mm.id=$row[0], mm.status=$row[1] mm.tapegatewayrequestid=$row[2] mm.vid=$row[3] tp.name=$row[4]\n";
        }
    } elsif ($schemaversion =~ /^2_1_11/ ) {
        # print all the streams (with tapepool info).
        print "Streams snapshot:\n";
        my $stmt = $dbh -> prepare ("SELECT s.id, s.status, s.tapegatewayrequestid, t.vid, tp.name
                                FROM Stream s
                                LEFT OUTER JOIN Tape t on t.id = s.tape
                                LEFT OUTER JOIN TapePool tp ON tp.id = s.tapepool");
        $stmt->execute();
        while ( my @row = $stmt->fetchrow_array() ) {
            nullize_arrays_undefs ( \@row );
            print  "    mm.id=$row[0], s.status=$row[1] s.tapegatewayrequestid=$row[2] t.vid=$row[3] tp.name=$row[4]\n";
        }
        
    } else { 
        warn "Could no find schema version. Skipping migrationa and recall structures display.";
    }
    if ($schemaversion =~ /^2.1.13/ ) {
      my $stmt =  $dbh -> prepare ("SELECT rm.id, rm.mountTransactionId, rm.vid, rm.label, rm.density,
                                            rm.recallgroup, rm.starttime, rm.status, rm.lastvdqmpingtime,
                                            rm.lastprocessedfseq
                                  FROM recallMount rm");
      $stmt->execute();
      while ( my @row = $stmt->fetchrow_array() ) {
          nullize_arrays_undefs ( \@row );
          print  "    rm.id=$row[0], rm.mountTransactionId=$row[1], rm.vid=$row[2], rm.label=$row[3], rm.density=$row[4],\n".
                 "          rm.recallgroup=$row[5], rm.starttime=$row[6], rm.status=$row[7], rm.lastvdqmpingtime=$row[8]\n".
                 "          rm.lastprocessedfseq=$row[9]\n";
      }
    } elsif ($schemaversion =~ /^2_1_1[12]/ ) {
      my $stmt =  $dbh -> prepare ("SELECT t.id, t.vid, t.status
                                  FROM tape t");
      $stmt->execute();
      while ( my @row = $stmt->fetchrow_array() ) {
          nullize_arrays_undefs ( \@row );
          print  "    t.id=$row[0], t.vid=$row[1] t.status=$row[2]\n";
      }
    } else {
       warn "Could no find schema version. Skipping migration and recall structures display.";
    }
}

sub upgradeStagerDb ( $ )
{
    my $checkout_location = shift;
    
    my $vdqm_host = getCastorConfParam('STAGER', 'HOST');

    # Print error message and abort if the user is not root
    my $uid = POSIX::getuid;
    my $gid = POSIX::getgid;
    if($uid != 0 || $gid !=0) {
        print("ABORT: This script must be ran as root\n");
        exit(-1);
    }
    
    my $stager_upgrade_script     = $environment{stagerUpgrade};

    my $originalUpgradeStagerFullpath=$checkout_location.'/'.$stager_upgrade_script;
            
    # check upgrade scripts existance
    die "ABORT: $originalUpgradeStagerFullpath does not exist\n"
       if ! -e $originalUpgradeStagerFullpath;  
               
    # Stop any daemon accessing the database
    killDaemonWithTimeout('transfermanagerd' , 2);
    killDaemonWithTimeout('rechandlerd' , 2);
    killDaemonWithTimeout('rhd'         , 2);
    killDaemonWithTimeout('rmmasterd'   , 2);
    killDaemonWithTimeout('stagerd'     , 2);
    killDaemonWithTimeout('tapegatewayd', 2);

    my $stageUid = &get_uid_for_username('stage');
    my $stageGid = &get_gid_for_username('stage');
    my $adminList = $environment{adminList};
    my $instanceName = $environment{instance_name};
    my $CNSName = $environment{cns_name};
    my ( $NsDbUser, $NsDbPasswd, $NsDbName ) = &getOraCNSLoginInfo ();
        
    my $hacked_creation= `mktemp`;
    chomp $hacked_creation;
    `cat $originalUpgradeStagerFullpath > $hacked_creation`;
    `sed -i s/^ACCEPT/--ACCEPT/ $hacked_creation`;
    `sed -i s/^PROMPT/--PROMPT/ $hacked_creation`;
    `sed -i s/^UNDEF/--UNDEF/ $hacked_creation`;
    `sed -i s/\\&stageUid/$stageUid/g $hacked_creation`;
    `sed -i s/\\&stageGid/$stageGid/g $hacked_creation`;
    `sed -i s/\\&adminList/$adminList/g $hacked_creation`;
    `sed -i s/\\&instanceName/$instanceName/g $hacked_creation`;
    `sed -i s/\\&stagerNsHost/$CNSName/g $hacked_creation`;
    `sed -i s/\\&cnsUser/$NsDbUser/g $hacked_creation`;
    `sed -i s/\\&cnsPasswd/$NsDbPasswd/g $hacked_creation`;
    `sed -i s/\\&cnsDbName/$NsDbName/g $hacked_creation`;

    # Run the upgrade script
    my $dbUser   = &getOrastagerconfigParam("user");
    my $dbPasswd = &getOrastagerconfigParam("passwd");
    my $dbName   = &getOrastagerconfigParam("dbName");
    executeSQLPlusScriptNoFilter ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation,
                           "Upgrading Stager's schema");
    unlink $hacked_creation;
}

sub postUpgradeStagerDb ( $ )
{
    my $checkout_location = shift;
    
    my $vdqm_host = getCastorConfParam('STAGER', 'HOST');

    # Print error message and abort if the user is not root
    my $uid = POSIX::getuid;
    my $gid = POSIX::getgid;
    if($uid != 0 || $gid !=0) {
        print("ABORT: This script must be ran as root\n");
        exit(-1);
    }
    
    my $stager_upgrade_script_1   = $environment{stagerUpgrade_1};
    my $stager_upgrade_script_2   = $environment{stagerUpgrade_2};
    my $stager_upgrade_script_3   = $environment{stagerUpgrade_3};
    my $stager_upgrade_script_4   = $environment{stagerUpgrade_4};

    my $originalUpgradeStagerFullpath_1 =$checkout_location.'/'.$stager_upgrade_script_1;
    my $originalUpgradeStagerFullpath_2 =$checkout_location.'/'.$stager_upgrade_script_2;
    my $originalUpgradeStagerFullpath_3 =$checkout_location.'/'.$stager_upgrade_script_3;
    my $originalUpgradeStagerFullpath_4 =$checkout_location.'/'.$stager_upgrade_script_4;
            
    # check upgrade scripts existance
    die "ABORT: $originalUpgradeStagerFullpath_1 does not exist\n"
       if ! -e $originalUpgradeStagerFullpath_1;   
    die "ABORT: $originalUpgradeStagerFullpath_2 does not exist\n"
       if ! -e $originalUpgradeStagerFullpath_2;
    die "ABORT: $originalUpgradeStagerFullpath_3 does not exist\n"
       if ! -e $originalUpgradeStagerFullpath_3;         
    die "ABORT: $originalUpgradeStagerFullpath_4 does not exist\n"
       if ! -e $originalUpgradeStagerFullpath_4;     
               
    # Stop any daemon accessing the database
    killDaemonWithTimeout('transfermanagerd' , 2);
    killDaemonWithTimeout('rechandlerd' , 2);
    killDaemonWithTimeout('rhd'         , 2);
    killDaemonWithTimeout('rmmasterd'   , 2);
    killDaemonWithTimeout('stagerd'     , 2);
    killDaemonWithTimeout('tapegatewayd', 2);

    my $stageUid = &get_uid_for_username('stage');
    my $stageGid = &get_gid_for_username('stage');
    my $adminList = $environment{adminList};
    my $instanceName = $environment{instance_name};
    my $CNSName = $environment{cns_name};
    my ( $NsDbUser, $NsDbPasswd, $NsDbName ) = &getOraCNSLoginInfo ();
    
    my $hacked_creation= `mktemp`;
    chomp $hacked_creation;
    `cat $originalUpgradeStagerFullpath_1 > $hacked_creation`;
    `sed -i s/^ACCEPT/--ACCEPT/ $hacked_creation`;
    `sed -i s/^PROMPT/--PROMPT/ $hacked_creation`;
    `sed -i s/^UNDEF/--UNDEF/ $hacked_creation`;
    `sed -i s/\\&stageUid/$stageUid/g $hacked_creation`;
    `sed -i s/\\&stageGid/$stageGid/g $hacked_creation`;
    `sed -i s/\\&adminList/$adminList/g $hacked_creation`;
    `sed -i s/\\&instanceName/$instanceName/g $hacked_creation`;
    `sed -i s/\\&stagerNsHost/$CNSName/g $hacked_creation`;
    `sed -i s/\\&cnsUser/$NsDbUser/g $hacked_creation`;
    `sed -i s/\\&cnsPasswd/$NsDbPasswd/g $hacked_creation`;
    `sed -i s/\\&cnsDbName/$NsDbName/g $hacked_creation`;

    # Run the upgrade script
    # Run the upgrade script
    my $dbUser   = &getOrastagerconfigParam("user");
    my $dbPasswd = &getOrastagerconfigParam("passwd");
    my $dbName   = &getOrastagerconfigParam("dbName");
    executeSQLPlusScriptNoFilter ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation,
                           "Applying 2.1.12-0 postupgrade on stager db");
    unlink $hacked_creation;
    
    $hacked_creation= `mktemp`;
    chomp $hacked_creation;
    `cat $originalUpgradeStagerFullpath_2 > $hacked_creation`;
    `sed -i s/^ACCEPT/--ACCEPT/ $hacked_creation`;
    `sed -i s/^PROMPT/--PROMPT/ $hacked_creation`;
    `sed -i s/^UNDEF/--UNDEF/ $hacked_creation`;
    `sed -i s/\\&stageUid/$stageUid/g $hacked_creation`;
    `sed -i s/\\&stageGid/$stageGid/g $hacked_creation`;
    `sed -i s/\\&adminList/$adminList/g $hacked_creation`;
    `sed -i s/\\&instanceName/$instanceName/g $hacked_creation`;
    `sed -i s/\\&stagerNsHost/$CNSName/g $hacked_creation`;
    `sed -i s/\\&cnsUser/$NsDbUser/g $hacked_creation`;
    `sed -i s/\\&cnsPasswd/$NsDbPasswd/g $hacked_creation`;
    `sed -i s/\\&cnsDbName/$NsDbName/g $hacked_creation`;

    # Run the upgrade script
    executeSQLPlusScriptNoFilter ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation,
                           "Applying upgrade to 2.1.12-1 on stager db");
    unlink $hacked_creation;
    
    $hacked_creation= `mktemp`;
    chomp $hacked_creation;
    `cat $originalUpgradeStagerFullpath_3 > $hacked_creation`;
    `sed -i s/^ACCEPT/--ACCEPT/ $hacked_creation`;
    `sed -i s/^PROMPT/--PROMPT/ $hacked_creation`;
    `sed -i s/^UNDEF/--UNDEF/ $hacked_creation`;
    `sed -i s/\\&stageUid/$stageUid/g $hacked_creation`;
    `sed -i s/\\&stageGid/$stageGid/g $hacked_creation`;
    `sed -i s/\\&adminList/$adminList/g $hacked_creation`;
    `sed -i s/\\&instanceName/$instanceName/g $hacked_creation`;
    `sed -i s/\\&stagerNsHost/$CNSName/g $hacked_creation`;
    `sed -i s/\\&cnsUser/$NsDbUser/g $hacked_creation`;
    `sed -i s/\\&cnsPasswd/$NsDbPasswd/g $hacked_creation`;
    `sed -i s/\\&cnsDbName/$NsDbName/g $hacked_creation`;

    # Run the upgrade script
    executeSQLPlusScriptNoFilter ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation,
                           "Applying upgrade to 2.1.12-4 on stager db");
    unlink $hacked_creation;

    $hacked_creation= `mktemp`;
    chomp $hacked_creation;
    `cat $originalUpgradeStagerFullpath_4 > $hacked_creation`;
    `sed -i s/^ACCEPT/--ACCEPT/ $hacked_creation`;
    `sed -i s/^PROMPT/--PROMPT/ $hacked_creation`;
    `sed -i s/^UNDEF/--UNDEF/ $hacked_creation`;
    `sed -i s/\\&stageUid/$stageUid/g $hacked_creation`;
    `sed -i s/\\&stageGid/$stageGid/g $hacked_creation`;
    `sed -i s/\\&adminList/$adminList/g $hacked_creation`;
    `sed -i s/\\&instanceName/$instanceName/g $hacked_creation`;
    `sed -i s/\\&stagerNsHost/$CNSName/g $hacked_creation`;
    `sed -i s/\\&cnsUser/$NsDbUser/g $hacked_creation`;
    `sed -i s/\\&cnsPasswd/$NsDbPasswd/g $hacked_creation`;
    `sed -i s/\\&cnsDbName/$NsDbName/g $hacked_creation`;

    # Run the upgrade script
    executeSQLPlusScriptNoFilter ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation,
                           "Applying upgrade to 2.1.12-HEAD on stager db");
    unlink $hacked_creation;
}

sub upgradeVDQMDb ( $ )
{
    my $checkout_location = shift;
    
    my $vdqm_host = getCastorConfParam('VDQM', 'HOST');

    # Print error message and abort if the user is not root
    my $uid = POSIX::getuid;
    my $gid = POSIX::getgid;
    if($uid != 0 || $gid !=0) {
        print("ABORT: This script must be ran as root\n");
        exit(-1);
    }
    
    my $stager_upgrade_script     = $environment{VDQMUpgrade};

    my $originalUpgradeVDQMFullpath=$checkout_location.'/'.$stager_upgrade_script;
    
    # check upgrade script's existance
    die "ABORT: $originalUpgradeVDQMFullpath does not exist\n"
       if ! -e $originalUpgradeVDQMFullpath;
       
    # Stop any daemon accessing the database
    killDaemonWithTimeout('vdqmd' , 2);

    # Run the upgrade script
    my $dbUser   = &getOraVdqmConfigParam("user");
    my $dbPasswd = &getOraVdqmConfigParam("passwd");
    my $dbName   = &getOraVdqmConfigParam("dbName");
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $originalUpgradeVDQMFullpath,
                           "Upgrading VDQM's schema");
    # As the name server is not running on the 
}


sub reinstall_vdqm_db_from_checkout ( $ )
{
    my $checkout_location = shift;
    
    my $vdqm_host = getCastorConfParam('VDQM', 'HOST');

    # Print error message and abort if the user is not root
    my $uid = POSIX::getuid;
    my $gid = POSIX::getgid;
    if($uid != 0 || $gid !=0) {
        print("ABORT: This script must be ran as root\n");
        exit(-1);
    }
    
    my $dbDir               = $environment{dbDir};
    my $originalDropSchema  = $environment{originalDropSchema};
    my $vdqmScript          = $environment{vdqmScript};
    my $originalDropSchemaFullpath=$checkout_location.'/'.$dbDir.'/'.$originalDropSchema;
    my $originalDbSchemaFullpath=$checkout_location.'/'.$vdqmScript;

    die "ABORT: $originalDropSchema does not exist\n"
       if ! -e $originalDropSchemaFullpath;
    
    die "ABORT: $originalDbSchemaFullpath does not exist\n"
        if ! -e $originalDbSchemaFullpath;
    
    # Make sure we're running on the proper machine.
    my $host = `uname -n`;
    die('ABORT: This script is only made to be run on host $stager_host\n')
      if ( ! $host =~ /^$vdqm_host($|\.)/i );

    # Ensure all of the daemons accessing the stager-database are dead
    killDaemonWithTimeout('vdqmd' , 2);

    my $dbUser   = &getOraVdqmConfigParam("user");
    my $dbPasswd = &getOraVdqmConfigParam("passwd");
    my $dbName   = &getOraVdqmConfigParam("dbName");
    
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $originalDropSchemaFullpath,
                           "Dropping VDQM schema");

    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $originalDbSchemaFullpath, "Re-creating VDQM schema");
}

sub reinstall_ns_db_from_checkout ( $ )
{
    my $checkout_location = shift;
    
    my $ns_host = getCastorConfParam('CNS', 'HOST');
    # Drastic test to prevent the running of this dangerous tool
    # ouside of a dev environement
    die "reinstall_ns_db_from_checkout should not run on $ns_host" unless $ns_host =~ /lxc2dev3/;

    # Print error message and abort if the user is not root
    my $uid = POSIX::getuid;
    my $gid = POSIX::getgid;
    if($uid != 0 || $gid !=0) {
        print("ABORT: This script must be ran as root\n");
        exit(-1);
    }
    
    my $dbDir               = $environment{dbDir};
    my $originalDropSchema  = $environment{originalDropSchema};
    my $nsScript            = $environment{nsScript};
    my $originalDropSchemaFullpath=$checkout_location.'/'.$dbDir.'/'.$originalDropSchema;
    my $originalDbSchemaFullpath=$checkout_location.'/'.$nsScript;

    
    die "ABORT: $originalDropSchema does not exist\n"
       if ! -e $originalDropSchemaFullpath;
    
    die "ABORT: $originalDbSchemaFullpath does not exist\n"
        if ! -e $originalDbSchemaFullpath;  
    
    # Make sure we're running on the proper machine.
    my $host = `uname -n`;
    die('ABORT: This script is only made to be run on host $ns_host\n')
      unless ( $host =~ /^$ns_host($|\.)/i );

    # Ensure all of the daemons accessing the stager-database are dead
    killDaemonWithTimeout('nsd' , 2);

    my ( $dbUser, $dbPasswd, $dbName ) = &getOraCNSLoginInfo ();
    
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $originalDropSchemaFullpath,
                           "Dropping NS schema");
                           
    my $vmgr_schema = $environment{vmgr_schema};
    my $hacked_creation= `mktemp`;
    chomp $hacked_creation;
    `cat $originalDbSchemaFullpath > $hacked_creation`;
    `sed -i s/^ACCEPT/--ACCEPT/ $hacked_creation`;
    `sed -i s/^PROMPT/--PROMPT/ $hacked_creation`;
    `sed -i s/^UNDEF/--UNDEF/ $hacked_creation`;
    `sed -i s/\\&vmgrSchema\./$vmgr_schema/g $hacked_creation`;

    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation, "Re-creating NS schema");
    unlink $hacked_creation;
}
sub reinstall_stager_db_from_checkout ( $ )
{
    my $checkout_location = shift;
    
    my $stager_host = getCastorConfParam('STAGER', 'HOST');

    # Print error message and abort if the user is not root
    my $uid = POSIX::getuid;
    my $gid = POSIX::getgid;
    if($uid != 0 || $gid !=0) {
        print("ABORT: This script must be ran as root\n");
        exit(-1);
    }
    
    my $dbDir               = $environment{dbDir};
    my $originalDropSchema  = $environment{originalDropSchema};
    my $originalDbSchema    = $environment{originalDbSchema};
    my $tstDir              = $environment{tstDir};
    my $originalSetPermissionsSQL = $environment{originalSetPermissionsSQL};

    my $originalDropSchemaFullpath=$checkout_location.'/'.$dbDir.'/'.$originalDropSchema;
    my $originalDbSchemaFullpath=$checkout_location.'/'.$dbDir.'/'.$originalDbSchema;
    my $originalSetPermissionSQLFullpath=$checkout_location.'/'.$tstDir.'/'.$originalSetPermissionsSQL;

    die "ABORT: $originalDropSchema does not exist\n"
       if ! -e $originalDropSchemaFullpath;
    
    die "ABORT: $originalDbSchema does not exist\n"
        if ! -e $originalDbSchemaFullpath;

    die "ABORT: $originalSetPermissionSQLFullpath does not exist\n"
        if ! -e $originalSetPermissionSQLFullpath;
    
    # Make sure we're running on the proper machine.
    my $host = `uname -n`;
    die('ABORT: This script is only made to be run on host $stager_host\n')
      if ( ! $host =~ /^$stager_host($|\.)/i );

    my @diskServers = get_disk_servers();
    my $nbDiskServers = @diskServers;

    print("Found the following disk-servers: ");
    foreach(@diskServers) {
      print("$_ ");
    }
    print("\n");

    warn("WARNING: Reinstall requires at least 2 disk-servers")
      if($nbDiskServers < 2);

    # This is where the final go/no go decsion is taken. 
    # Record that we got green light for stager DB wiping.
    $initial_green_light = 1;

    # Ensure all of the daemons accessing the stager-database are dead
    killDaemonWithTimeout('transfermanagerd' , 2);
    killDaemonWithTimeout('rechandlerd' , 2);
    killDaemonWithTimeout('rhd'         , 2);
    killDaemonWithTimeout('rmmasterd'   , 2);
    killDaemonWithTimeout('stagerd'     , 2);
    killDaemonWithTimeout('tapegatewayd', 2);

    # Destroy the shared memory segment
    print "Removing shared memory segment\n";
    `ipcrm -M 0x00000946`;

    # Print shared memory usage
    print "Current shared memory usage:\n";
    {
       my $pm = `ps axh | cut -c 1-5 | egrep '[0-9]*' | xargs -i pmap -d {}`;
       my  $current_proc;
       foreach ( split (/^/, $pm) ) { 
          if ( /^\d+:\s+/ ) { $current_proc = $_ } 
          elsif ( /\[\s+shmid=/ ) { 
             print $current_proc; 
             print $_; 
          } 
       }
    }
    # Ensure there is no leftover in the DB
    my $dbh = open_db ();
    if ( check_leftovers ( $dbh ) ) {
        print_leftovers ( $dbh );
        $dbh->disconnect();
        die ("Found leftovers in the stager's DB. Stopping here.");
    }
    $dbh->disconnect();
    
    my $dbUser   = &getOrastagerconfigParam("user");
    my $dbPasswd = &getOrastagerconfigParam("passwd");
    my $dbName   = &getOrastagerconfigParam("dbName");

    my ( $NsDbUser, $NsDbPasswd, $NsDbName ) = &getOraCNSLoginInfo ();
    
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $originalDropSchemaFullpath,
                           "Dropping schema");
    
    my $stageUid = &get_uid_for_username('stage');
    my $stageGid = &get_gid_for_username('stage');
    my $adminList = $environment{adminList};
    my $instanceName = $environment{instance_name};
    my $CNSName = $environment{cns_name};
    
    my $hacked_creation= `mktemp`;
    chomp $hacked_creation;
    `cat $originalDbSchemaFullpath > $hacked_creation`;
    `sed -i s/^ACCEPT/--ACCEPT/ $hacked_creation`;
    `sed -i s/^PROMPT/--PROMPT/ $hacked_creation`;
    `sed -i s/^UNDEF/--UNDEF/ $hacked_creation`;
    `sed -i s/\\&stageUid/$stageUid/g $hacked_creation`;
    `sed -i s/\\&stageGid/$stageGid/g $hacked_creation`;
    `sed -i s/\\&adminList/$adminList/g $hacked_creation`;
    `sed -i s/\\&instanceName/$instanceName/g $hacked_creation`;
    `sed -i s/\\&stagerNsHost/$CNSName/g $hacked_creation`;
    `sed -i s/\\&cnsUser/$NsDbUser/g $hacked_creation`;
    `sed -i s/\\&cnsPasswd/$NsDbPasswd/g $hacked_creation`;
    `sed -i s/\\&cnsDbName/$NsDbName/g $hacked_creation`;

    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation, "Re-creating schema");
    unlink $hacked_creation;
    
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $originalSetPermissionSQLFullpath, "Creating additional permissions for testsuite");

    # Restart some daemons (not all yet)
    startSingleDaemon ( 'transfermanagerd' );
    startSingleDaemon ( 'rechandlerd' );
    startSingleDaemon ( 'rhd' );
    startSingleDaemon ( 'rmmasterd' );
    startSingleDaemon ( 'stagerd' );
    #startSingleDaemon ( 'tapegatewayd' );
}

sub configure_headnode_2111 ( )
{
    my @diskServers = get_disk_servers();   

    my $rmGetNodesResult = `rmGetNodes | egrep 'name:'`;
    print("\n");
    print("rmGetNodes RESULTS\n");
    print("==================\n");
    print($rmGetNodesResult);
    
    # Fill database with the standard set-up for a dev-box
    print `nslistclass | grep NAME | awk '{print \$2}' | xargs -i enterFileClass --Name {} --GetFromCns`;
    
 print `enterSvcClass --Name default --DiskPools default --DefaultFileSize 10485760 --FailJobsWhenNoSpace yes --NbDrives 1 --TapePool stager_dev03_3 --MigratorPolicy defaultMigrationPolicy --StreamPolicy defaultStreamPolicy --gcpolicy default`;
    print `enterSvcClass --Name dev --DiskPools extra --DefaultFileSize 10485760 --FailJobsWhenNoSpace yes  --gcpolicy default`;
    print `enterSvcClass --Name diskonly --DiskPools extra --ForcedFileClass temp --DefaultFileSize 10485760 --Disk1Behavior yes --FailJobsWhenNoSpace yes  --gcpolicy default`;
    
    print `rmAdminNode -r -R -n $diskServers[0]`;
    print `rmAdminNode -r -R -n $diskServers[1]`;
    sleep 10;
    print `moveDiskServer default $diskServers[0]`;
    print `moveDiskServer extra $diskServers[1]`;
    
    # Add a tape-pool to $environment{svcclass} service-class
    my $tapePool = get_environment('tapepool');
    print `modifySvcClass --Name $environment{svcclass} --AddTapePool $tapePool --MigratorPolicy defaultMigrationPolicy --StreamPolicy defaultStreamPolicy`;
    
    # Set the number of drives on the default and dev service-classes to desired number for each
    print `modifySvcClass --Name default --NbDrives 1`;
    print `modifySvcClass --Name dev     --NbDrives 2`;
}

sub configure_headnode_2112 ( )
{
    my @diskServers = get_disk_servers();
    my $nbDiskServers = @diskServers;
    poll_rm_readyness ( 1, 15 );
    print "Sleeping extra 15 seconds\n";
    sleep (15);
    
    my $rmGetNodesResult = `rmGetNodes | egrep 'name:'`;
    print("\n");
    print("rmGetNodes RESULTS\n");
    print("==================\n");
    print($rmGetNodesResult);
    
    # Fill database with the standard set-up for a dev-box
    print `nslistclass | grep NAME | awk '{print \$2}' | xargs -i enterfileclass {}`;

    print `enterdiskpool default`;
    print `enterdiskpool extra`;
    
    print `entersvcclass --diskpools default --defaultfilesize 10485760 --failjobswhennospace yes default`;
    my $main_tapepool=`vmgrlistpool  | head -1  | awk '{print \$1}' | tr -d "\\n"`;
    print `entertapepool --nbdrives 2 --minamountdata 0 --minnbfiles 0 --maxfileage 0 $main_tapepool`;
    print `entersvcclass --diskpools extra --defaultfilesize 10485760 --failjobswhennospace yes dev`;
    print `entersvcclass --diskpools extra --forcedfileclass temp --defaultfilesize 10485760 --disk1behavior yes --failjobswhennospace yes diskonly`;
    
    print `rmAdminNode -r -R -n $diskServers[0]`;
    print `rmAdminNode -r -R -n $diskServers[1]`;
    sleep 10;
    print `moveDiskServer default $diskServers[0]`;
    print `moveDiskServer extra $diskServers[1]`;

    # Add the tapecopy routing rules
    my $single_copy_dir = $environment{castor_directory} . $environment{castor_single_subdirectory};
    my $dual_copy_dir = $environment{castor_directory} . $environment{castor_dual_subdirectory};
    #my $single_file_class_number = `nsls -d --class $single_copy_dir |  awk '{print \$1}'`;
    #my $single_copy_class = `printfileclass  | perl -e 'while (<>) { if (/\\s*(\\w+)\\s+(\\w+)\\s+(\\w+)\\s+(\\w+)/ && \$2 eq $single_file_class_number) { print \$1 }}'`;
    my  $single_copy_class = 'largeuser';
    #my $dual_file_class_number = `nsls -d --class $dual_copy_dir |  awk '{print \$1}'`;
    #my $dual_copy_class = `printfileclass  | perl -e 'while (<>) { if (/\\s*(\\w+)\\s+(\\w+)\\s+(\\w+)\\s+(\\w+)/ && \$2 eq $dual_file_class_number) { print \$1 }}'`;
    my $dual_copy_class ='test2';
    my $svcclass = $environment{svcclass};
    my $tapepool = $environment{tapepool};
    print `entertapepool --nbdrives 2 --minamountdata 0 --minnbfiles 0 --maxfileage 0 $tapepool`;
    print `entermigrationroute $single_copy_class 1:$tapepool`;
    print `entermigrationroute $dual_copy_class 1:$tapepool 2:$tapepool`;
    
    # Add a single copy default-default route using first pool name
    my $default_pool = `vmgrlistpool | head -1  | awk '{print \$1}'`;
    print `entermigrationroute largeuser 1:$default_pool`;
}

sub configure_headnode_2113 ( )
{
    my @diskServers = get_disk_servers();
    my $nbDiskServers = @diskServers;
    poll_rm_readyness ( 1, 15 );
    print "Sleeping extra 15 seconds\n";
    sleep (15);
    
    my $rmGetNodesResult = `rmGetNodes | egrep 'name:'`;
    print("\n");
    print("rmGetNodes RESULTS\n");
    print("==================\n");
    print($rmGetNodesResult);
    
    # Fill database with the standard set-up for a dev-box
    # Re-create the name server's file classes first:
    print `nsenterclass --name=temp --id=58`;
    print `nsenterclass --name=largeuser --id=95 --nbcopies=1`;
    print `nsenterclass --name=test2 --id=27 --nbcopies=2`;
    
    # populate the stager.
    print `enterfileclass -a`;

    print `enterdiskpool default`;
    print `enterdiskpool extra`;
    
    print `entersvcclass --diskpools default --defaultfilesize 10485760 --failjobswhennospace yes  --gcpolicy default default`;
    my $main_tapepool=`vmgrlistpool  | head -1  | awk '{print \$1}' | tr -d "\\n"`;
    print `entertapepool --nbdrives 2 --minamountdata 0 --minnbfiles 0 --maxfileage 0 $main_tapepool`;
    print `entersvcclass --diskpools extra --defaultfilesize 10485760 --failjobswhennospace yes  --gcpolicy default dev`;
    print `entersvcclass --diskpools extra --forcedfileclass temp --defaultfilesize 10485760 --disk1behavior yes --failjobswhennospace yes  --gcpolicy default diskonly`;
    
    print `rmAdminNode -r -R -n $diskServers[0]`;
    print `rmAdminNode -r -R -n $diskServers[1]`;
    sleep 10;
    print `moveDiskServer default $diskServers[0]`;
    print `moveDiskServer extra $diskServers[1]`;

    # Add the tapecopy routing rules
    my $single_copy_dir = $environment{castor_directory} . $environment{castor_single_subdirectory};
    my $dual_copy_dir = $environment{castor_directory} . $environment{castor_dual_subdirectory};
    #my $single_file_class_number = `nsls -d --class $single_copy_dir |  awk '{print \$1}'`;
    #my $single_copy_class = `printfileclass  | perl -e 'while (<>) { if (/\\s*(\\w+)\\s+(\\w+)\\s+(\\w+)\\s+(\\w+)/ && \$2 eq $single_file_class_number) { print \$1 }}'`;
    my  $single_copy_class = 'largeuser';
    #my $dual_file_class_number = `nsls -d --class $dual_copy_dir |  awk '{print \$1}'`;
    #my $dual_copy_class = `printfileclass  | perl -e 'while (<>) { if (/\\s*(\\w+)\\s+(\\w+)\\s+(\\w+)\\s+(\\w+)/ && \$2 eq $dual_file_class_number) { print \$1 }}'`;
    my $dual_copy_class ='test2';
    my $svcclass = $environment{svcclass};
    my $tapepool = $environment{tapepool};
    print `entertapepool --nbdrives 2 --minamountdata 0 --minnbfiles 0 --maxfileage 0 $tapepool`;
    print `entermigrationroute $single_copy_class 1:$tapepool`;
    print `entermigrationroute $dual_copy_class 1:$tapepool 2:$tapepool`;
    
    # Add a single copy default-default route using first pool name
    my $default_pool = `vmgrlistpool | head -1  | awk '{print \$1}'`;
    print `entermigrationroute largeuser 1:$default_pool`;
}

sub reinstall_stager_db()
{
    reinstall_stager_db_from_checkout ( $environment{checkout_location} );
}

sub cleanup () {
    print "t=".elapsed_time()."s. In CastorTapeTests::cleanup: cleaning up files\n";
    for (@local_files) {
        print "t=".elapsed_time()."s. Cleanup: removing ".$_->{name}."\n";
        unlink $_->{name};
    }
    #use Data::Dumper;
    #print Dumper \@remote_files;
    for ( my $i =scalar ( @remote_files ) - 1 ; $i >= 0; $i-- ) {
        #reverse order remotees removal to removes directories in the end.
        print "t=".elapsed_time()."s. Cleanup: removing ".$remote_files[$i]->{name}."\n";
	if ($remote_files[$i]->{type} eq "file") {
            `su $environment{username} -c \"STAGE_SVCCLASS=$environment{svcclass} stager_rm -M $remote_files[$i]->{name}\"`;
        }
        `su $environment{username} -c \"nsrm $remote_files[$i]->{name}\"`;
    }
}

sub spawn_testsuite ()
{
    my ( $pid, $rd, $wr );
    # create a pipe for parent-child communication.
    pipe ($rd, $wr);
    if ( $pid = fork) { # parent part: just return child's pid and 
        close ($wr);
        return ($pid, $rd);
    } else { # child part: just call the testsuite, return the standard output in the 
        die "Cannot fork: $!" unless defined $pid;
        close ($rd);
        print print "t=".elapsed_time()."s.About to run testsuite\n";
        print $wr `( cd ../testsuite/; su $environment{username} -c \"py.test --configfile=$environment{testsuite_config_location} --verbose --rfio --client --root --tape 2>&1\")`;
        print "t=".elapsed_time()."s. testsuite run complete\n";
        close ($wr);
        POSIX::_exit(0);    
    }
}

sub wait_testsuite ( $$ )
{
    my ( $pid, $rd ) = ( shift, shift );
    my $ret = "";
    print "t=".elapsed_time()."s. Waiting for testsuite run completion...\n";
    while (my $l = <$rd>) {
        $ret .= $l;
    }
    close ($rd);
    waitpid ( $pid, 0 );
    return $ret;
}

# Final cleanup of the library.
# So far, get rid of leftover local files
END {
    print "t=".elapsed_time()."s. In CastorTapeTests::END: calling cleanup\n";
    cleanup();
    print "t=".elapsed_time()."s. Cleanup complete. Printing leftovers.\n";
    my $dbh = open_db();
    print_leftovers ($dbh);
    print "t=".elapsed_time()."s. End of leftovers\n";
    if ( $initial_green_light ) {
        print "t=".elapsed_time()."s. Nuking leftovers, if any.\n";
        my $stmt;
        my $schemaversion = get_db_version ( $dbh );
        if ( $schemaversion =~ /^2_1_13/ ) {
            $stmt = $dbh->prepare("BEGIN
                                    DELETE FROM MigrationJob;
                                    DELETE FROM RecallJob;
                                    DELETE FROM DiskCopy;
                                    COMMIT;
                                  END;");
        } elsif ( $schemaversion =~ /^2_1_12/ ) {
            $stmt = $dbh->prepare("BEGIN
                                    DELETE FROM MigrationJob;
                                    DELETE FROM RecallJob;
                                    DELETE FROM Segment;
                                    DELETE FROM DiskCopy;
                                    COMMIT;
                                  END;");
        } elsif ( $schemaversion =~ /^2_1_11/ ) {
            $stmt = $dbh->prepare("BEGIN
                                    DELETE FROM Stream2TapeCopy;
                                    DELETE FROM Segment;
                                    DELETE FROM DiskCopy;
                                    DELETE FROM TapeCopy;
                                    COMMIT;
                                  END;");
        } elsif ( $schemaversion =~ /No Version/ ) {
            warn "Could no find schema version. Assuming no leftover.";
            $stmt = $dbh->prepare("SELECT 1 FROM DUAL");
        } else {
            die "Unexpected schema version: $schemaversion";
        }        
        $stmt->execute();
    }
    $dbh->disconnect();
}
# # create a local 
1;                            # this should be your last line
