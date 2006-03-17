#!/usr/bin/perl -w
#/******************************************************************************
# *                      listFileClass
# *
# * This file is part of the Castor project.
# * See http://castor.web.cern.ch/castor
# *
# * Copyright (C) 2003  CERN
# * This program is free software; you can redistribute it and/or
# * modify it under the terms of the GNU General Public License
# * as published by the Free Software Foundation; either version 2
# * of the License, or (at your option) any later version.
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# *
# * @(#)$RCSfile: C2-C2FileMover.pl,v $ $Revision: 1.1 $ $Release$ $Date: 2006/03/17 15:56:56 $ $Author: sponcec3 $
# *
# * Lists existing service classes
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/
use strict;
use diagnostics;
use lib "/usr/lib/perl/CASTOR";
use castor_tools;

my $nsHost = "cnslhcb.cern.ch";
my $svcClassId = 123;

# usage function
sub usage {
  printf("Usage : %s [-h|--help] <fileName>\n", $0);
  printf("  where the given file lists fileids of files to copy\n");
}

# first parse the options
my $help;
if (0 == Getopt::Long::GetOptions ('h|help' => \$help)) {
  usage();
  exit(1);
}

if ($help) {
  usage();
  exit(0);
}

# Check the arguments
if ($#ARGV != 0) {
  print("Error : wrong number of arguments\n");
  usage();
  exit;
}

# get the fileId list
my $line;
my $fileName = $ARGV[0];
open(FILE, "< $fileName") or die("Could not open file '$fileName' : $!.");

# connect to DB
my $dbh = DBI->connect('dbi:Oracle:','castor_stager@castorstager','castor');
my $dbh2 = DBI->connect('dbi:Oracle:','castor_stager@c2lhcbstgdb','castor');

# Create needed function in the DB
my $dbquery6;
$dbquery6 .= <<EOF;
CREATE OR REPLACE PROCEDURE bestFileSystemForJob_DBI
(minFree IN NUMBER, rMountPoint OUT VARCHAR2,
 rDiskServer OUT VARCHAR2) AS
 fileSystems castor."strList";
 machines castor."strList";
 minFreeList castor."cnumList";
BEGIN
  minFreeList(1):=minFree;
  bestFileSystemForJob(fileSystems, machines, minFreeList, rMountPoint, rDiskServer);
END;
EOF
my $sth6 = $dbh2->prepare("$dbquery6");
$sth6->execute;

# Loop over the files
my $CastorFileName;
foreach $CastorFileName (<FILE>) {
    printf ("$CastorFileName\n");
    chomp($CastorFileName);
# Get fileId
    my $line;
    my $comm = 'nsls -i ' . $CastorFileName . ' | awk \'{print $1;}\'';
    #printf "$comm\n";
    chomp($line = `$comm`);
    #printf ("fileid is $line\n");

# Get details about the file from first DB
    my $dbquery;
    $dbquery .= <<EOF;
    SELECT CastorFile.fileSize, FileClass.name,
    DiskCopy.path, FileSystem.mountPoint, DiskServer.name
	FROM Castorfile, DiskCopy, FileSystem, DiskServer, FileClass
	WHERE CastorFile.fileid = :fileId
	AND CastorFile.nsHost = :nsHost
	AND CastorFile.id = DiskCopy.castorfile
	AND DiskCopy.fileSystem = FileSystem.id
	AND FileSystem.diskServer = DiskServer.id
	AND DiskCopy.status = 0
	AND FileSystem.status = 0
	AND DiskServer.status = 0
	AND FileClass.id = CastorFile.FileClass
	AND ROWNUM < 2
EOF
	my $sth = $dbh->prepare("$dbquery");
    $sth->bind_param_inout(":fileId",\$line,0);
    $sth->bind_param_inout(":nsHost",\$nsHost,2048);
    $sth->execute;
    my @row = $sth->fetchrow_array();

# Reserve space somewhere to put the file
    my $dbquery2;
    my $mountPoint;
    my $diskServer;
    my $fsId;
    $dbquery2 .= <<EOF;
    DECLARE
	dsId NUMBER;
        dev NUMBER;
    BEGIN
	bestFileSystemForJob_DBI(:fileSize,:mountPoint,:diskServer);
    SELECT FileSystem.id, FileSystem.fsDeviation,
    DiskServer.id INTO :fsId, dev, dsId
	FROM FileSystem, DiskServer
	WHERE FileSystem.diskServer = DiskServer.id
	AND FileSystem.mountPoint = :mountPoint
	AND DiskServer.name = :diskServer;
    updateFsFileOpened(dsId, :fsId, dev, :fileSize);
    END;
EOF
	my $sth2 = $dbh2->prepare("$dbquery2");
    $sth2->bind_param_inout(":fsId",\$fsId,0);
    $sth2->bind_param_inout(":fileSize",\$row[0],0);
    $sth2->bind_param_inout(":mountPoint",\$mountPoint,2048);
    $sth2->bind_param_inout(":diskServer",\$diskServer,2048);
    $sth2->execute();
    if $sth->err() continue ;
#printf("Got $diskServer://$mountPoint\n");

# Create new Castorfile
    my $dbquery4;
    my $cid;
    my $unused;
    $dbquery4 .= <<EOF;
    DECLARE
	fcid NUMBER;
    BEGIN
	select id INTO fcid from FileClass where name = :fileClass;
    selectCastorFile(:fileId,:nsHost,:svcClass,fcid,
		     :fileSize,:fileName,:cid,:unused);
    END;
EOF
	my $sth4 = $dbh2->prepare("$dbquery4");
    $sth4->bind_param_inout(":fileId",\$line,0);
    $sth4->bind_param_inout(":nsHost",\$nsHost,2048);
    $sth4->bind_param_inout(":svcClass",\$svcClassId,0);
    $sth4->bind_param_inout(":fileClass",\$row[1],0);
    $sth4->bind_param_inout(":fileSize",\$row[0],0);
    $sth4->bind_param_inout(":fileName",\$CastorFileName,0);
    $sth4->bind_param_inout(":cid",\$cid,0);
    $sth4->bind_param_inout(":unused",\$unused,0);
    $sth4->execute;

# Create new Diskcopy
    my $newPath;
    my $dbquery5;
    $dbquery5 .= <<EOF;
    DECLARE
	dcId NUMBER;
    BEGIN
	SELECT ids_seq.nextval INTO dcId FROM DUAL;
    buildPathFromFileId(:fileId, :nsHost, dcId, :newpath);
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
	VALUES (:newpath, dcId, :fsId, :cid, 0, getTime());
    INSERT INTO Id2Type (id, type) VALUES (dcId, 5);
    END;
EOF
	my $sth5 = $dbh2->prepare("$dbquery5");
    $sth5->bind_param_inout(":fileId",\$line,0);
    $sth5->bind_param_inout(":nsHost",\$nsHost,2048);
    $sth5->bind_param_inout(":newpath",\$newPath,0);
    $sth5->bind_param_inout(":cid",\$cid,0);
    $sth5->bind_param_inout(":fsId",\$fsId,0);
    $sth5->execute;
#printf ("new location is $diskServer://$mountPoint$newPath\n");

# Realize the transfer
    printf ("Transfering $row[4]://$row[3]$row[2] into $diskServer://$mountPoint$newPath\n");
    printf ("ssh root\@$row[4] 'scp $row[3]$row[2] $diskServer:$mountPoint$newPath'\n");


# Cleanup reservation in the DB and commit
    my $dbquery3;
    $dbquery3 .= <<EOF;
    BEGIN
	updateFSFileClosed(:fsId,:fileSize,:fileSize);
    commit;
    END;
EOF
	my $sth3 = $dbh2->prepare("$dbquery3");
    $sth3->bind_param_inout(":fileSize",\$row[0],0);
    $sth3->bind_param_inout(":fsId",\$fsId,0);
    $sth3->execute;

}
close(FILE);
