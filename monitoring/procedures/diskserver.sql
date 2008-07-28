CREATE OR REPLACE PROCEDURE Proc_DiskServerStat AS

    TYPE first IS TABLE OF CASTOR_STAGER.diskserver.name%TYPE;
    TYPE secnd IS TABLE OF NUMBER;
    TYPE thrd  IS TABLE OF CASTOR_STAGER.filesystem.mountpoint%TYPE;

    a first;
    b thrd;
    c secnd;
    d secnd;

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_DiskServerStat';

    SELECT * BULK COLLECT INTO a, b, c, d
      FROM (
	select 	diskserver.name,
		filesystem.MOUNTPOINT,
		diskcopy.status,
		count(*)
	from 	castor_stager.diskcopy, castor_stager.diskserver, castor_stager.filesystem
	where 	diskcopy.filesystem=filesystem.id
		and diskserver.id=filesystem.diskserver
	group by diskserver.name, filesystem.MOUNTPOINT, diskcopy.status
  );

      forall marker in a.first..a.last
        insert into CASTOR_STAGER.monitoring_DiskServerStat values(mytime, a(marker), b(marker), c(marker), d(marker));

END Proc_DiskServerStat;

