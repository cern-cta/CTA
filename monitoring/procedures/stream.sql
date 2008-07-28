CREATE OR REPLACE PROCEDURE Proc_STREAM AS

    TYPE first   IS TABLE OF CASTOR_STAGER.svcclass.name%TYPE;
    TYPE secnd   IS TABLE OF NUMBER;

    a first;
    b secnd;
    c secnd;
    d secnd;
    e secnd;
    f secnd;
    g secnd;

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_STREAM';

    SELECT * BULK COLLECT INTO a, b, c, d, e, f, g
      FROM (
	select  svc.name,
	        nvl(sum(decode(st.status,0,1,0)),0),
		nvl(sum(decode(st.status,1,1,0)),0),
		nvl(sum(decode(st.status,2,1,0)),0),
		nvl(sum(decode(st.status,3,1,0)),0),
		nvl(sum(decode(st.status,4,1,0)),0),
		nvl(sum(decode(st.status,5,1,0)),0)
    	from castor_stager.svcclass svc,
           castor_stager.svcclass2tapepool svc2tppool,
           castor_stager.tapepool tppool,
           castor_stager.stream st
    	where svc2tppool.parent(+)=svc.id
             and svc2tppool.child=tppool.id(+)
             and st.tapepool(+)=tppool.id
    	group by svc.name
  );

      forall marker in a.first..a.last
        insert into CASTOR_STAGER.monitoring_STREAM values(mytime, a(marker), b(marker), c(marker), d(marker), e(marker), f(marker), g(marker));

END Proc_STREAM;


