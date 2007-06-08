CREATE OR REPLACE PROCEDURE Proc_Segment AS

    TYPE secnd   IS TABLE OF NUMBER;

    b secnd;
    c secnd;
    d secnd;
    e secnd; 
    f secnd;

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_Segment';

    SELECT * BULK COLLECT INTO b, c, d, e, f
      FROM (
	select 	nvl(sum(decode(status,0,1,0)),0),
		nvl(sum(decode(status,5,1,0)),0),	
		nvl(sum(decode(status,6,1,0)),0),
		nvl(sum(decode(status,7,1,0)),0),
		nvl(sum(decode(status,8,1,0)),0)
	from castor_stager.segment
  );
 
      forall marker in b.first..b.last
        insert into CASTOR_STAGER.monitoring_Segment values(mytime, 'clusterwide', b(marker), c(marker), d(marker), e(marker), f(marker)); 
      
END Proc_Segment;
/ 
