-----------------------------------------------------------------------------
-- This file provides useful statements to investigate basic configuration --
-- of svcClasses, fileSystems, diskServers, diskPools, and tapePools       --
-----------------------------------------------------------------------------

-- Status of fileSystems and correspondent diskServers/diskPools:
SELECT fs.id, fs.totalsize/1048576000 as totsize, fs.free/1048576000 as free, (fs.totalsize-fs.free)*100.0/fs.totalsize as occupancy,
       fs.minfreespace, fs.maxfreespace, fs.status as fsStatus, diskserver.name as diskserver, fs.mountpoint,
       diskserver.status as dsStatus, diskpool.name as diskpool
  FROM filesystem fs, diskserver, diskpool
 WHERE fs.diskserver = diskserver.id AND fs.diskpool = diskpool.id
 ORDER BY diskpool;

SELECT * FROM diskserver;
SELECT * FROM diskpool;
SELECT * FROM fileclass;

-- configuration of SvcClasses and associated DiskPools&TapePools:
SELECT svcclass.*, diskpool.name as diskpool FROM svcclass, diskpool2svcclass, diskpool
 WHERE diskpool2svcclass.child = svcclass.id and diskpool2svcclass.parent = diskpool.id;

SELECT svcclass.*, tapepool.name as tapepool FROM svcclass, svcclass2tapepool, tapepool
 WHERE svcclass2tapepool.parent = svcclass.id and svcclass2tapepool.child = tapepool.id;

-- number of fileSystems per diskPool
SELECT dp.name as diskpool, count(*) as nbFS 
  FROM diskpool dp, filesystem fs
 WHERE fs.diskpool = dp.id GROUP BY dp.name;
