-----------------------------------------------------------------------------
-- This file provides useful statements to investigate basic configuration --
-- of svcClasses, fileSystems, diskServers, diskPools, and tapePools       --
-----------------------------------------------------------------------------

-- Status of fileSystems and correspondent diskServers/diskPools:
SELECT fs.id, fs.totalsize/1048576000 as totsize, fs.free/1048576000 as free, fs.minfreespace,
       fs.maxfreespace, fs.mountpoint, fs.status, diskserver.name as diskserver,
       diskserver.status as dsStatus, diskpool.name as diskpool
  FROM filesystem fs, diskserver, diskpool
 WHERE fs.diskserver = diskserver.id AND fs.diskpool = diskpool.id;


SELECT * FROM diskserver;
SELECT * FROM diskpool;

-- configuration of SvcClasses and associated DiskPools&TapePools:
SELECT svcclass.*, diskpool.name as diskpool FROM svcclass, diskpool2svcclass, diskpool
 WHERE diskpool2svcclass.child = svcclass.id and diskpool2svcclass.parent = diskpool.id;

SELECT dp.name as diskpool, count(*) as nbFS FROM diskpool dp, filesystem fs
 WHERE fs.diskpool(+) = dp.id group by dp.name;

SELECT svcclass.*, tapepool.name as tapepool FROM svcclass, svcclass2tapepool, tapepool
 WHERE svcclass2tapepool.parent = svcclass.id and svcclass2tapepool.child = tapepool.id;
