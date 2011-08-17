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
                   genBackupFileName 
                   getNbTapeCopiesInStagerDb 
                   insertCastorFile 
                   deleteCastorFile 
                   setFileClassNbCopies 
                   overrideCheckPermission 
                   insertTapeCopy 
                   deleteTapeCopy 
                   deleteAllStreamsTapeCopiesAndCastorFiles 
                   executeSQLPlusScript 
                   executeSQLPlusScriptNoError 
                   stopAndSwitchToTapeGatewayd 
                   stopAndSwitchToRtcpclientd 
                   getLdLibraryPathFromSrcPath
                   check_leftovers
                   check_leftovers_poll_timeout
                   nullize_arrays_undefs
                   print_leftovers
                   print_file_info
                   reinstall_stager_db
                   getOrastagerconfigParam
                   cleanup
		   elapsed_time
                   spawn_testsuite
                   wait_testsuite
                   ); # Symbols to autoexport (:DEFAULT tag)

# keep track of the locally created files and of the recalled and locally rfcped files.
my @local_files;

# keep track of the files migrated in castor.
my @remote_files;

# keep track of the test environment (and definition of the defaults).
my %environment = (
    allowed_stagers => [ 'lxcastordev03', 'lxcastordev04' ]
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


# Based on the output of bhosts, this subroutine returns the hostnames of the
# disk-servers sorted alphabetically.
sub get_disk_servers()
{
  my @servers = `bhosts | egrep -v 'HOST_NAME|closed' | awk '{print \$1;}' | sort`;
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
			  'local_transfermanagerd',
			  'local_mighunterd',
			  'local_rechandlerd',
			  'local_rhd',
			  'local_rmmasterd',
			  'local_rtcpclientd',
			  'local_stagerd',
			  'local_tapegatewayd',
                          'local_expertd',
                          'local_logprocessord',
                          'testsuite_config_location',
                          'instance_name',
                          'cns_name',
                          'run_testsuite');
    my @global_vars   = ( 'dbDir' , 'tstDir', 'adminList', 'originalDbSchema',
                          'originalDropSchema', 'originalSetPermissionsSQL', 
                          'castor_single_subdirectory', 'castor_dual_subdirectory', 
                          'switchoverToTapeGateway', 'switchoverToRtcpClientd');
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
    print "Cleaning leftover files on the files servers\n";
    print `rmGetNodes  | grep name | perl -p -e 's/^.*name: //'  | xargs -i ssh root\@{} rm /srv/castor/*/*/* 2>&1`;
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
    return ( $stager_qry=~ /INVALID/ || $stager_qry=~ /^$/);
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

sub remove_stream( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varStreamIds \"numList\";
        BEGIN
          SELECT s.id
            BULK COLLECT INTO varStreamIds
            FROM Stream s
           INNER JOIN Stream2TapeCopy sttc ON s.id = sttc.parent
           INNER JOIN TapeCopy tc ON tc.id = sttc.child
           INNER JOIN Castorfile cf ON cf.id = tc.castorFile
           WHERE cf.lastKnownFileName = :NSNAME;
          DELETE FROM Stream2TapeCopy sttc 
           WHERE sttc.parent IN (SELECT * FROM TABLE (varStreamIds));
          DELETE FROM Stream s
           WHERE s.id IN (SELECT * FROM TABLE (varStreamIds));
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed stream for file: $name\n";
}

sub remove_tapepool( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varStreamIds \"numList\";
          varFakeTPId  NUMBER;
        BEGIN
          SELECT ids_seq.nextval INTO varFakeTPId FROM DUAL;
          SELECT s.id
            BULK COLLECT INTO varStreamIds
            FROM Stream s
           INNER JOIN Stream2TapeCopy sttc ON s.id = sttc.parent
           INNER JOIN TapeCopy tc ON tc.id = sttc.child
           INNER JOIN Castorfile cf ON cf.id = tc.castorFile
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
          varTapeCopyIds \"numList\";
        BEGIN
          SELECT tc.id
            BULK COLLECT INTO varTapeCopyIds
            FROM TapeCopy tc
           INNER JOIN Castorfile cf ON cf.id = tc.castorFile
           WHERE cf.lastKnownFileName = :NSNAME;
          DELETE FROM Segment seg
           WHERE seg.copy IN (SELECT * FROM TABLE (varTapeCopyIds));
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed segement for file: $name\n";
}

sub remove_tape( $$ )
{
    my ($dbh, $name) = (shift, shift);
    my $stmt = $dbh->prepare(
       "DECLARE
          varTapeIds \"numList\";
        BEGIN
          SELECT s.tape
            BULK COLLECT INTO varTapeIds
            FROM Stream s
           INNER JOIN Stream2TapeCopy sttc ON s.id = sttc.parent
           INNER JOIN TapeCopy tc ON sttc.child = tc.id
           INNER JOIN Castorfile cf ON cf.id = tc.castorFile
           WHERE cf.lastKnownFileName = :NSNAME;
          DELETE FROM Tape t
           WHERE t.id IN (SELECT * FROM TABLE (varTapeIds));
          COMMIT;
        END;");
    $stmt->bind_param(":NSNAME", $name);
    $stmt->execute();
    print "t=".elapsed_time()."s. Removed tape for file: $name\n";
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
        } elsif ($file{breaking_type} =~ /^missing stream/) {
             remove_stream($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing tapepool/) {
             remove_tapepool($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing segment/) {
             remove_segment($dbh, $file{name});
             $remote_files[$index]->{breaking_done} = 1;
        } elsif ($file{breaking_type} =~ /^missing tape/) {
             remove_tape($dbh, $file{name});
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
                                  varCastorFileId NUMBER;
                                  varTapeCopyIds  \"numList\";
                                  varDiskCopyIds  \"numList\";
                                BEGIN
                                  SELECT cf.Id INTO varCastorFileId
                                    FROM CastorFile cf
                                   WHERE cf.lastKnownFileName = :NSNAME
                                     FOR UPDATE;
                                     
                                  SELECT tc.Id
                                    BULK COLLECT INTO varTapeCopyIds
                                    FROM TapeCopy tc
                                   WHERE tc.CastorFile = varCastorFileId
                                     FOR UPDATE;
                                  
                                  SELECT dc.Id
                                    BULK COLLECT INTO varDiskCopyIds
                                    FROM DiskCopy dc
                                   WHERE dc.CastorFile = varCastorFileId
                                     FOR UPDATE;
                                     
                                  DELETE FROM Stream2TapeCopy sttc
                                   WHERE sttc.child in (SELECT * FROM TABLE (varTapeCopyIds));
                                   
                                  DELETE FROM TapeCopy tc
                                   WHERE tc.id in (SELECT * FROM TABLE (varTapeCopyIds));
                                   
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
                                      varCastorFileId NUMBER;
                                      varTapeCopyIds  \"numList\";
                                      varDiskCopyIds  \"numList\";
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
                                        SELECT tc.Id
                                          BULK COLLECT INTO varTapeCopyIds
                                          FROM TapeCopy tc
                                         WHERE tc.CastorFile = varCastorFileId
                                           FOR UPDATE;
                                        
                                        SELECT dc.Id
                                          BULK COLLECT INTO varDiskCopyIds
                                          FROM DiskCopy dc
                                         WHERE dc.CastorFile = varCastorFileId
                                           FOR UPDATE;
                                           
                                        DELETE FROM Stream2TapeCopy sttc
                                         WHERE sttc.child IN (SELECT * FROM TABLE (varTapeCopyIds));
                                         
                                        DELETE FROM TapeCopy tc
                                         WHERE tc.id IN (SELECT * FROM TABLE (varTapeCopyIds));
                                         
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
    die "Timeout with ".count_to_be_moved()." files to be migrated after $timeout s.";
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
	 'mighunterd'  => './castor/tape/mighunter/mighunterd',
	 'rechandlerd' => './castor/tape/rechandler/rechandlerd',
	 'rhd'         => './castor/rh/rhd',
	 'rmmasterd'   => './castor/monitoring/rmmaster/rmmasterd',
	 'rtcpclientd' => './rtcopy/rtcpclientd',
#         'stagerd'     => 'valgrind --log-file=/var/log/castor/stagerd-valgrind.%p --trace-children=yes ./castor/stager/daemon/stagerd',
#	 'stagerd'     => 'valgrind --log-file=/var/log/castor/stagerd-valgrind.%p --leak-check=full --track-origins=yes --suppressions=./tools/castor.supp --trace-children=yes ./castor/stager/daemon/stagerd',
         'stagerd'     => './castor/stager/daemon/stagerd',
	 'tapegatewayd'=> './castor/tape/tapegateway/tapegatewayd',
         'expertd'     => './expert/expertd',
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
	 'mighunterd'  => './castor/tape/mighunter/mighunterd',
	 'rechandlerd' => './castor/tape/rechandler/rechandlerd',
	 'rhd'         => './castor/rh/rhd',
	 'rmmasterd'   => './castor/monitoring/rmmaster/rmmasterd',
	 'rtcpclientd' => './rtcopy/rtcpclientd',
	 'stagerd'     => 'valgrind --log-file=/var/log/castor/stagerd-valgrind.%p --suppressions=./tools/castor.supp --trace-children=yes ./castor/stager/daemon/stagerd',
#'./castor/stager/daemon/stagerd',
	 'tapegatewayd'=> './castor/tape/tapegateway/tapegatewayd',
         'expertd'     => './expert/expertd',
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


# Returns a back-up filename based on the specified original filename.
#
# Please note that this subroutine does not create or modify any files.
#
# The back-up filename is the original plus a unique extension.
#
# @param  originalFilename The full path-name of the original file.
# @return                  The full path-name of the back-up copy.
sub genBackupFileName ( $ ) 
{
  my $originalFilename = $_[0];

  my $uuid = `uuidgen`;
  chomp($uuid);
  my $date = `date | sed 's/ /_/g;s/:/_/g'`;
  chomp($date);
  my $hostname = `hostname`;
  chomp($hostname);
  my $backupFilename = "${originalFilename}_${uuid}_${hostname}_${date}_backup";

  return($backupFilename);
}



# Returns the number of tape-copies in the stager-database.
#
# @param dbh The handle to the stager-database.
# @return    The number of tape-copies in the stager database.
sub getNbTapeCopiesInStagerDb ( $ ) 
{
  my $dbh = $_[0];

  my $stmt = "SELECT COUNT(*) FROM TapeCopy";
  my $rows = $dbh->selectall_arrayref($stmt);
  my $nbTapeCopies = $$rows[0][0];

  return($nbTapeCopies);
}


# Inserts a row into the CastorFile table and returns its database ID.
#
# This subroutine does not insert a row into the id2type table.
#
# @param  dbh                The handle to the stager-database.
# @param  nsHost             The name-server hostname.
# @param  fileId
# @param  fileSize
# @param  creationTime
# @param  lastUpdateTime
# @param  lastAccessTime
# @param  lastKnownFilename
# @param  svcClassName
# @param  fileClassName
# @return The database ID of the newly inserted CastorFile.
sub insertCastorFile ( $$$$$$$$$$ )
{
  my $dbh               = $_[0];
  my $nsHost            = $_[1];
  my $fileId            = $_[2];
  my $fileSize          = $_[3];
  my $creationTime      = $_[4];
  my $lastUpdateTime    = $_[5];
  my $lastAccessTime    = $_[6];
  my $lastKnownFilename = $_[7];
  my $svcClassName      = $_[8];
  my $fileClassName     = $_[9];

  my $stmt = "
    DECLARE
      varSvcClassId   NUMBER(38) := NULL;
      varFileClassId  NUMBER(38) := NULL;
      varCastorFileId NUMBER(38) := NULL;
    BEGIN
      /* Deteremine the database IDs of the service and file classes */
      SELECT id INTO varSvcClassId  FROM SvcClass  WHERE name = :SVCCLASSNAME;
      SELECT id INTO varFileClassId FROM FileClass WHERE name = :FILECLASSNAME;

      INSERT INTO CastorFile(id, nsHost, fileId, fileSize, creationTime,
                  lastUpdateTime, lastAccessTime, lastKnownFilename, svcClass,
                  fileClass)
           VALUES (ids_seq.NEXTVAL, :NSHOST, :FILEID, :FILESIZE,
                  :CREATIONTIME, :LASTUPDATETIME, :LASTACCESSTIME,
                  :LASTKNOWNFILENAME, varSvcClassId, varFileClassId)
        RETURNING id INTO varCastorFileId;

      INSERT INTO Id2Type(id, type) VALUES(varCastorFileId, 2);

      :CASTORFILEID := varCastorFileId;
    END;";

  # The castor-file database ID will be the return value
  my $castorFileId;

  my $sth = $dbh->prepare($stmt);
  $sth->bind_param_inout(":SVCCLASSNAME",\$svcClassName,2048)
    or die $sth->errstr;
  $sth->bind_param_inout(":FILECLASSNAME",\$fileClassName,2048)
    or die $sth->errstr;
  $sth->bind_param_inout(":NSHOST",\$nsHost,2048) or die $sth->errstr;
  $sth->bind_param_inout(":FILEID",\$fileId,20) or die $sth->errstr;
  $sth->bind_param_inout(":FILESIZE",\$fileSize,20) or die $sth->errstr;
  $sth->bind_param_inout(":CREATIONTIME",\$creationTime,20) or die $sth->errstr;
  $sth->bind_param_inout(":LASTUPDATETIME",\$lastUpdateTime,20)
    or die $sth->errstr;
  $sth->bind_param_inout(":LASTACCESSTIME",\$lastAccessTime,20)
    or die $sth->errstr;
  $sth->bind_param_inout(":LASTKNOWNFILENAME",\$lastKnownFilename,2048)
    or die $sth->errstr;
  $sth->bind_param_inout(":CASTORFILEID",\$castorFileId,20) or die $sth->errstr;
  $sth->execute();

  return $castorFileId;
}


# Deletes the row from the CastorFile table with the specified database ID.
#
# @param dbh          The handle to the stager-database.
# @param castorFileId The castor database ID.
sub deleteCastorFile ( $$ )
{
  my $dbh          = $_[0];
  my $castorFileId = $_[1];

  my $stmt = "
    DECLARE
      varCastorFileId NUMBER(38) := :CASTORFILEID;
    BEGIN
      DELETE FROM CastorFile WHERE id = varCastorFileId;
      DELETE FROM Id2Type    WHERE id = varCastorFileId;
    END;";

  my $sth = $dbh->prepare($stmt);
  $sth->bind_param_inout(":CASTORFILEID", \$castorFileId, 20) or die $sth->errstr;
  $sth->execute();
}


# Sets the nbCopies attribute of the specified file-class to the specified
# value.
#
# @param dbh           The handle to the stager-database.
# @param fileClassName The name of the file-class.
# @param nbCopies      The new number of tape-copies.
sub setFileClassNbCopies ( $$$ )
{
  my $dbh           = $_[0];
  my $fileClassName = $_[1];
  my $nbCopies      = $_[2];

  my $stmt = "
    BEGIN
      UPDATE FileClass
         SET nbCopies = :NBCOPIES
       WHERE name = :FILECLASSNAME;
    END;";

  my $sth = $dbh->prepare($stmt);
  $sth->bind_param_inout(":NBCOPIES", \$nbCopies, 20)
    or die $sth->errstr;
  $sth->bind_param_inout(":FILECLASSNAME", \$fileClassName, 2048)
    or die $sth->errstr;
  $sth->execute();
}


# Override checkPermission
#
# @param dbh           The handle to the stager-database.
sub overrideCheckPermission ( $ )
{
  my $dbh = $_[0];

  my $stmt = "
    CREATE OR REPLACE FUNCTION checkPermission(
      reqSvcClass IN VARCHAR2,
      reqEuid     IN NUMBER,
      reqEgid     IN NUMBER,
      reqTypeI    IN NUMBER)
    RETURN NUMBER AS
    BEGIN
      RETURN 0;
    END;";

  my $sth = $dbh->prepare($stmt);
  $sth->execute();
}


# Inserts a row into the TapeCopy table and returns the database ID.
#
# @param dbh          The handle to the stager-database.
# @param castorFileId The database ID of the associated castor-file.
# @param status       The initial status of the tape-copy.
# @return             The database ID of the newly inserted tape-copy.
sub insertTapeCopy ( $$$ )
{
  my $dbh          = $_[0];
  my $castorFileId = $_[1];
  my $status       = $_[2];

  my $stmt = "
    DECLARE
      varTapeCopyId NUMBER(38) := NULL;
    BEGIN
      INSERT INTO TapeCopy(id, copyNb, castorFile, status)
           VALUES (ids_seq.nextval, 1, :CASTORFILEID, :STATUS)
        RETURNING id INTO varTapeCopyId;

      INSERT INTO Id2Type(id, type) VALUES (varTapeCopyId, 30);

      :TAPECOPYID := varTapeCopyId;
    END;";

  # The tape-copy database ID will be the return value
  my $tapeCopyId;

  my $sth = $dbh->prepare($stmt);
  $sth->bind_param_inout(":CASTORFILEID", \$castorFileId, 20) or die $sth->errstr;
  $sth->bind_param_inout(":STATUS"      , \$status      , 20) or die $sth->errstr;
  $sth->bind_param_inout(":TAPECOPYID"  , \$tapeCopyId  , 20) or die $sth->errstr;
  $sth->execute();

  return $tapeCopyId;
}


# Inserts the specified number of rows into the TapeCopy table all pointing to
# the specified castor-file and all with the specified status.
#
# @param dbh          The handle to the stager-database.
# @param castorFileId The database ID of the associated castor-file.
# @param status       The initial status of the tape-copies.
# @param nbTapeCopies The number of rows to insert into the TapeCopy table.
sub insertTapeCopies ( $$$$ )
{
  my $dbh          = $_[0];
  my $castorFileId = $_[1];
  my $status       = $_[2];
  my $nbTapeCopies = $_[3];

  my $stmt = "
    DECLARE
      varTapeCopyId NUMBER(38) := NULL;
    BEGIN
      FOR i IN 1 .. :NBTAPECOPIES LOOP
        INSERT INTO TapeCopy(id, copyNb, castorFile, status)
             VALUES (ids_seq.nextval, 1, :CASTORFILEID, :STATUS)
          RETURNING id INTO varTapeCopyId;

        INSERT INTO Id2Type(id, type) VALUES (varTapeCopyId, 30);
      END LOOP;
    END;";

  # The tape-copy database ID will be the return value
  my $tapeCopyId;

  my $sth = $dbh->prepare($stmt);
  $sth->bind_param_inout(":NBTAPECOPIES", \$nbTapeCopies, 20)
    or die $sth->errstr;
  $sth->bind_param_inout(":CASTORFILEID", \$castorFileId, 20)
    or die $sth->errstr;
  $sth->bind_param_inout(":STATUS"      , \$status      , 20)
    or die $sth->errstr;
  $sth->execute();

  return $tapeCopyId;
}


# Deletes the row from the TapeCopy table with the specified database ID.
#
# @param dbh        The handle to the stager-database.
# @param tapeCopyId The castor database ID.
sub deleteTapeCopy ( $$ )
{
  my $dbh        = $_[0];
  my $tapeCopyId = $_[1];

  my $stmt = "
    DECLARE
      varTapeCopyId NUMBER(38) := :TAPECOPYID;
    BEGIN
      DELETE FROM Stream2TapeCopy WHERE child = varTapeCopyId;
      DELETE FROM TapeCopy        WHERE id    = varTapeCopyId;
      DELETE FROM Id2Type         WHERE id    = varTapeCopyId;
    END;";

  my $sth = $dbh->prepare($stmt);
  $sth->bind_param_inout(":TAPECOPYID", \$tapeCopyId, 20) or die $sth->errstr;
  $sth->execute();
}


# Deletes all Stream2TapeCopy, Stream, TapeCopy and CastorFile rows from the
# stager database.
#
# TO BE USED WITH CAUTION.
#
# @param dbh The handle to the stager-database.
sub deleteAllStreamsTapeCopiesAndCastorFiles ( $ )
{
  my $dbh        = $_[0];

  my $stmt = "
    BEGIN
      DELETE FROM Stream2TapeCopy;
      DELETE FROM Stream;
      DELETE FROM TapeCopy;
      DELETE FROM CastorFile;
      DELETE FROM Id2Type WHERE type = 26; /* OBJ_STREAM     */
      DELETE FROM Id2Type WHERE type = 30; /* OBJ_TAPECOPY   */
      DELETE FROM Id2Type WHERE type =  2; /* OBJ_CASTORFILE */
    END;";

  my $sth = $dbh->prepare($stmt);
  print("Deleting all streams, tape-copies and castor-files\n");
  $sth->execute();
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
sub executeSQLPlusScriptNoError ( $$$$$ )
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
    } else {
        print "SUCCESS\n";
    }
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
    $sth = $dbh -> prepare("SELECT count (*) from ( 
                              SELECT dc.id from diskcopy dc where
                                dc.status NOT IN ( 0, 7, 4 )
                              UNION ALL
                              SELECT tc.id from tapecopy tc)"); # Discopy_staged diskcopy_invalid diskcopy_failed
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
    # Print by castofile with corresponding tapecopies
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

# print_file_info
sub print_file_info ( $$ )
{
    my ( $dbh, $filename ) = ( shift, shift );
    # Print as many infomation as can be regarding this file
    # Bypass stager and just dump from the DB.
    # First dump general file-related info
    my $stmt = $dbh -> prepare ("SELECT cf.lastknownfilename, cf.nsHost, cf.fileId, cf.id, 
                                        sc.name, fc.name
                                   FROM castorfile cf
                                   LEFT OUTER JOIN svcClass sc ON sc.id = cf.svcClass
                                   LEFT OUTER JOIN fileClass fc ON fc.id = cf.fileClass
                                  WHERE cf.lastKnownFileName = :FILENAME");
    $stmt->bind_param (":FILENAME", $filename);
    $stmt->execute();
    while ( my @row = $stmt->fetchrow_array() ) {
        nullize_arrays_undefs ( \@row );
        print  "Remaining catorfile for $row[0], NSID=$row[2] on SRV=$row[1] (DB id=$row[3], svcclass=$row[4], fileclass=$row[5]\n";
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
    $stmt = $dbh -> prepare ("SELECT tc.id, tc.status, s.id, s.status, t.vid, t.tpmode, t.status, tp.name, seg.id, seg.status, seg.errorcode, seg.errmsgtxt, seg.tape 
                                  FROM castorfile cf
                                  LEFT OUTER JOIN TapeCopy tc on tc.castorfile = cf.id
                                  LEFT OUTER JOIN Stream2Tapecopy sttc on sttc.child = tc.id
                                  LEFT OUTER JOIN Stream s on s.id = sttc.parent
                                  LEFT OUTER JOIN Tape t on t.id = s.tape
                                  LEFT OUTER JOIN TapePool tp on tp.id = s.tapepool
                                  LEFT OUTER JOIN Segment seg on seg.copy = tc.id
                                 WHERE cf.lastKnownFileName = :FILENAME
                                   AND tc.status NOT IN ( 4 ) -- TAPECOPY_TO_BE RECALLED");
    $stmt->bind_param (":FILENAME", $filename);
    $stmt->execute();
    while ( my @row = $stmt->fetchrow_array() ) {
        nullize_arrays_undefs ( \@row );
        print  "    tc.id=$row[0], tc.status=$row[1] s.id=$row[2] s.status=$row[3] t.vid=$row[4] t.tpmode=$row[5] t.status=$row[6] tp.name=$row[7] seg.id=$row[8] seg.status=$row[9] seg.errorcode=$row[10] seg.errmsgtxt=$row[11] seg.tape=$row[12]\n";
    }
    # Finally dump the recall-related into
    $stmt = $dbh -> prepare ("SELECT tc.id, tc.status, seg.id, seg.status, t.id, t.tpmode, t.status, seg.errorcode, seg.errmsgtxt, seg.tape
                                  FROM castorfile cf
                                  LEFT OUTER JOIN TapeCopy tc on tc.castorfile = cf.id
                                  LEFT OUTER JOIN Segment seg on seg.copy = tc.id
                                  LEFT OUTER JOIN Tape t on t.id = seg.tape
                                 WHERE cf.lastKnownFileName = :FILENAME
                                   AND tc.status IN ( 4 ) -- TAPECOPY_TO_BE RECALLED");
    $stmt->bind_param (":FILENAME", $filename);
    $stmt->execute();
    while ( my @row = $stmt->fetchrow_array() ) {
        nullize_arrays_undefs ( \@row );
        print  "    tc.id=$row[0], tc.status=$row[1] seg.id=$row[2] seg.status=$row[3] t.id=$row[4] t.tpmode=$row[5] t.status=$row[6] seg.errorcode=$row[7] seg.errmsgtxt=$row[8] seg.tape=$row[9]\n";
    }
}

sub reinstall_stager_db()
{
    my $stager_host = getCastorConfParam('STAGER', 'HOST');

    # Print error message and abort if the user is not root
    my $uid = POSIX::getuid;
    my $gid = POSIX::getgid;
    if($uid != 0 || $gid !=0) {
        print("ABORT: This script must be ran as root\n");
        exit(-1);
    }

    my $checkout_location   = $environment{checkout_location};
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

    die("ABORT: Reinstall requires at least 2 disk-servers")
      if($nbDiskServers < 2);

    # This is where the final go/no go decsion is taken. Record that we got green light.
    $initial_green_light = 1;

    # Ensure all of the daemons accessing the stager-database are dead
    killDaemonWithTimeout('transfermanagerd' , 2);
    killDaemonWithTimeout('mighunterd'  , 2);
    killDaemonWithTimeout('rechandlerd' , 2);
    killDaemonWithTimeout('rhd'         , 2);
    killDaemonWithTimeout('rmmasterd'   , 2);
    killDaemonWithTimeout('rtcpclientd' , 2);
    killDaemonWithTimeout('stagerd'     , 2);
    killDaemonWithTimeout('tapegatewayd', 2);


    # Re-create mighunterd daemon scripts
    print("Re-creating mighunterd daemon scripts\n");
    `echo "DAEMON_COREFILE_LIMIT=unlimited" >  /etc/sysconfig/mighunterd`;
    `echo 'SVCCLASSES="default dev"'        >> /etc/sysconfig/mighunterd`;
    `echo 'MIGHUNTERD_OPTIONS="-t 5"'       >  /etc/sysconfig/mighunterd.default`;
    `echo 'MIGHUNTERD_OPTIONS="-t 5"'       >  /etc/sysconfig/mighunterd.dev`;
  
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
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $hacked_creation, "Re-creating schema");
    unlink $hacked_creation;
    
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $originalSetPermissionSQLFullpath, "Creating additional permissions for testsuite");

    # Restart some daemons (not all yet)
    startSingleDaemon ( 'transfermanagerd' );
    #startSingleDaemon ( 'mighunterd' );
    startSingleDaemon ( 'rechandlerd' );
    startSingleDaemon ( 'rhd' );
    startSingleDaemon ( 'rmmasterd' );
    #startSingleDaemon ( 'rtcpclientd' );
    startSingleDaemon ( 'stagerd' );
    #startSingleDaemon ( 'tapegatewayd' );

    poll_rm_readyness ( 1, 15 );
    print "Sleeping extra 15 seconds\n";
    sleep (15);
    
    my $rmGetNodesResult = `rmGetNodes | egrep 'name:'`;
    print("\n");
    print("rmGetNodes RESULTS\n");
    print("==================\n");
    print($rmGetNodesResult);
    
    # Fill database with the standard set-up for a dev-box
    `nslistclass | grep NAME | awk '{print \$2}' | xargs -i enterFileClass --Name {} --GetFromCns`;
    
    `enterSvcClass --Name default --DiskPools default --DefaultFileSize 10485760 --FailJobsWhenNoSpace yes --NbDrives 1 --TapePool stager_dev03 --MigratorPolicy defaultMigrationPolicy --StreamPolicy defaultStreamPolicy`;
    `enterSvcClass --Name dev --DiskPools extra --DefaultFileSize 10485760 --FailJobsWhenNoSpace yes`;
    `enterSvcClass --Name diskonly --DiskPools extra --ForcedFileClass temp --DefaultFileSize 10485760 --Disk1Behavior yes --FailJobsWhenNoSpace yes`;
    
    `rmAdminNode -r -R -n $diskServers[0]`;
    `rmAdminNode -r -R -n $diskServers[1]`;
    sleep 10;
    `moveDiskServer default $diskServers[0]`;
    `moveDiskServer extra $diskServers[1]`;
    
    # Add a tape-pool to $environment{svcclass} service-class
    my $tapePool = get_environment('tapepool');
    `modifySvcClass --Name $environment{svcclass} --AddTapePool $tapePool --MigratorPolicy defaultMigrationPolicy --StreamPolicy defaultStreamPolicy`;
    
    # Set the number of drives on the default and dev service-classes to desired number for each
    `modifySvcClass --Name default --NbDrives 1`;
    `modifySvcClass --Name dev     --NbDrives 2`;
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

# Switch from rtcpclientd (in DB, stops demons before, massages the DB as required)
sub stopAndSwitchToRtcpclientd ( $ )
{
    my $dbh = shift;

    # Stop the mighunter, rechandler, tapegateway, rtcpclientd
    for my $i ('mighunterd', 'rechandlerd', 'tapegatewayd', 'rtcpclientd') {
        if (killDaemonWithTimeout ($i, 5)) {
            die "Failed to stop a daemon. Call an exorcist.";
        }
    }
    #$dbh->do("UPDATE castorconfig SET value='rtcpclientd' WHERE class='tape' AND key='interfaceDaemon'");
    #$dbh->commit();
    # Migrate to rtcpclientd by script.
    my $dbUser   = &getOrastagerconfigParam("user");
    my $dbPasswd = &getOrastagerconfigParam("passwd");
    my $dbName   = &getOrastagerconfigParam("dbName");
    my $checkout_location   = $environment{checkout_location};
    my $dbDir               = $environment{dbDir};
    my $switch_to_rtcpclientd = $environment{switchoverToRtcpClientd};
    
    my $switch_to_rtcpclientd_full_path = $checkout_location.'/'.$dbDir.'/'.$switch_to_rtcpclientd;
    executeSQLPlusScript ( $dbUser, $dbPasswd, $dbName, 
                           $switch_to_rtcpclientd_full_path,
                           "Switching to RtcpClientd");
}


# Returns the LD_LIBRARY_PATH locating all of the *.so* files found in the
# specified root CASTOR source path
#
# @param  srcPath The root CASTOR source path containing the *.so* files
# @return         The LD_LIBRARY_PATH
sub getLdLibraryPathFromSrcPath ( $ ) {
  my $srcPath = $_[0];

  my $ldLibraryPath = `find $srcPath -follow -name '*.so*' | sed 's|/[^/]*\$||' | sort | uniq | xargs | sed 's/ /:/g'`;
  chomp($ldLibraryPath);

  return $ldLibraryPath;
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
        print $wr `( cd ../testsuite/; su $environment{username} -c \"py.test --configfile=$environment{testsuite_config_location} --verbose --rfio --client --root 2>&1\")`;
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
        my $stmt = $dbh->prepare("BEGIN
                                    DELETE FROM Stream2TapeCopy;
                                    DELETE FROM DiskCopy;
                                    DELETE FROM TapeCopy;
                                    COMMIT;
                                  END;");
        $stmt->execute();
    }
    $dbh->disconnect();
}
# # create a local 
1;                            # this should be your last line
