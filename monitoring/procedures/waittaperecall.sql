CREATE OR REPLACE PROCEDURE Proc_waittaperecall AS

    TYPE first   IS TABLE OF CASTOR_STAGER.svcclass.name%TYPE;
    TYPE secnd   IS TABLE OF CASTOR_STAGER.diskcopy.creationtime%TYPE;
    TYPE third   IS TABLE OF CASTOR_STAGER.filesystem.mountpoint%TYPE;
    TYPE forth   IS TABLE OF CASTOR_STAGER.castorfile.lastknownfilename%TYPE;
    TYPE fifth   IS TABLE OF CASTOR_STAGER.diskserver.name%TYPE;
    TYPE sixth   IS TABLE OF CASTOR_STAGER.diskcopy.path%TYPE;

    a first;
    b secnd;
    c third;
    d forth;
    e fifth;
    f sixth;

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_waittaperecall';

    SELECT * BULK COLLECT INTO a, b, c, d, e, f
      FROM (
        select svc.name,
               dc.creationtime,
               fs.mountpoint,
               cf.lastknownfilename,
               ds.name ade,
               dc.path
        from CASTOR_STAGER.castorfile cf,
             CASTOR_STAGER.diskserver ds,
             CASTOR_STAGER.diskcopy dc,
             CASTOR_STAGER.svcclass svc,
             CASTOR_STAGER.filesystem fs,
             CASTOR_STAGER.diskpool2svcclass dp2svc
        where dc.status = '2' 
              AND dc.creationtime < ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-48*3600)
              AND dc.filesystem = fs.id
	      AND fs.diskserver = ds.id 
              AND dp2svc.parent = fs.diskpool
              AND dp2svc.child = svc.id
              AND cf.id = dc.castorfile
        order by svc.name
      );
 
      forall marker in a.first..a.last
        insert into CASTOR_STAGER.monitoring_waittaperecall values(mytime, a(marker), b(marker), c(marker), d(marker), e(marker), f(marker));
      
END Proc_waittaperecall;
/ 
