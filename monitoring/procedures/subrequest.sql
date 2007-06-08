CREATE OR REPLACE PROCEDURE Proc_SubRequest AS

    TYPE first   IS TABLE OF CASTOR_STAGER.svcclass.name%TYPE;
    TYPE secnd   IS TABLE OF NUMBER;

    a first;
    b secnd;
    c secnd;
    d secnd;
    e secnd; 
    f secnd;
    g secnd;
    h secnd; 	
    i secnd; 
    j secnd; 
    k secnd;
    l secnd; 

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_SubRequest';

    SELECT * BULK COLLECT INTO a, b, c, d, e, f, g, h, i, j, k, l 
      FROM (
	select  svc.name, 
	        nvl(sum(decode(sr.status,0,1,0)),0),
		nvl(sum(decode(sr.status,1,1,0)),0),	
		nvl(sum(decode(sr.status,2,1,0)),0),
		nvl(sum(decode(sr.status,3,1,0)),0),
		nvl(sum(decode(sr.status,4,1,0)),0),
		nvl(sum(decode(sr.status,5,1,0)),0),
		nvl(sum(decode(sr.status,6,1,0)),0),
		nvl(sum(decode(sr.status,7,1,0)),0),
		nvl(sum(decode(sr.status,8,1,0)),0),
		nvl(sum(decode(sr.status,9,1,0)),0),
		nvl(sum(decode(sr.status,10,1,0)),0)
	from castor_stager.svcclass svc, castor_stager.diskpool2svcclass dp2svc, castor_stager.filesystem fs,
                      castor_stager.diskcopy dc, castor_stager.subrequest sr
                 where svc.id=dp2svc.child
                       and dp2svc.parent=fs.diskpool
                       and fs.id=dc.filesystem
                       and sr.diskcopy=dc.id
                 group by svc.name
  );
 
      forall marker in a.first..a.last
        insert into CASTOR_STAGER.monitoring_SubRequest values(mytime, a(marker), b(marker), c(marker), d(marker), e(marker), f(marker), g(marker), h(marker), i(marker), j(marker), k(marker), l(marker));
      
END Proc_SubRequest;
/ 
