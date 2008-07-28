CREATE OR REPLACE PROCEDURE Proc_TAPECOPY AS

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

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_TAPECOPY';

    SELECT * BULK COLLECT INTO a, b, c, d, e, f, g, h
      FROM (

 	select name,
		nvl(sum(decode(tpcp.status,0,1,0)),0),
		nvl(sum(decode(tpcp.status,1,1,0)),0),
		nvl(sum(decode(tpcp.status,2,1,0)),0),
		nvl(sum(decode(tpcp.status,3,1,0)),0),
		nvl(sum(decode(tpcp.status,4,1,0)),0),
		nvl(sum(decode(tpcp.status,5,1,0)),0),
		nvl(sum(decode(tpcp.status,6,1,0)),0)
	from castor_stager.svcclass svc,
             castor_stager.tapecopy tpcp,
             castor_stager.castorfile fid
        where tpcp.castorfile(+)=fid.id and fid.svcclass(+)=svc.id
                 group by svc.name

      );

      forall marker in a.first..a.last
        insert into CASTOR_STAGER.monitoring_TAPECOPY values(mytime, a(marker), b(marker), c(marker), d(marker), e(marker), f(marker), g(marker), h(marker));

END Proc_TAPECOPY;

