-----------------------------------------------------------------------------
-- This file provides useful statements to investigate basic configuration --
-- of svcClasses, fileSystems, diskServers, diskPools, and tapePools       --
-----------------------------------------------------------------------------

-- Status of fileSystems and correspondent diskServers/diskPools:
select fs.id, fs.totalsize/1048576000 as totsize, fs.free/1048576000 as free, fs.mountpoint, fs.status,
       diskserver.name as diskserver, diskserver.status as dsStatus, diskpool.name as diskpool
  from filesystem fs, diskserver, diskpool
 WHERE fs.diskserver = diskserver.id AND fs.diskpool = diskpool.id;


select * from diskserver;
select * from diskpool;

-- configuration of SvcClasses and associated DiskPools&TapePools:
select svcclass.*, diskpool.name as diskpool from svcclass, diskpool2svcclass, diskpool
where diskpool2svcclass.child = svcclass.id and diskpool2svcclass.parent = diskpool.id;

select svcclass.*, tapepool.name as tapepool from svcclass, svcclass2tapepool, tapepool
where svcclass2tapepool.parent = svcclass.id and svcclass2tapepool.child = tapepool.id;

