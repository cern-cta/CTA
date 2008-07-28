CREATE OR REPLACE PROCEDURE Proc_Tape AS

    TYPE secnd   IS TABLE OF NUMBER;

    b secnd;
    c secnd;
    d secnd;
    e secnd;
    f secnd;
    g secnd;
    h secnd;
    i secnd;

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_Tape';

    SELECT * BULK COLLECT INTO b, c, d, e, f, g, h, i
      FROM (
	select 	nvl(sum(decode(status,0,1,0)),0),
		nvl(sum(decode(status,1,1,0)),0),
		nvl(sum(decode(status,2,1,0)),0),
		nvl(sum(decode(status,3,1,0)),0),
		nvl(sum(decode(status,4,1,0)),0),
		nvl(sum(decode(status,5,1,0)),0),
		nvl(sum(decode(status,6,1,0)),0),
		nvl(sum(decode(status,7,1,0)),0)
	from castor_stager.tape
  );

      forall marker in b.first..b.last
        insert into CASTOR_STAGER.monitoring_Tape values(mytime, 'clusterwide', b(marker), c(marker), d(marker), e(marker), f(marker), g(marker),
h(marker), i(marker));

END Proc_Tape;

