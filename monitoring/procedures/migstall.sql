CREATE OR REPLACE PROCEDURE Proc_MigStall AS

    TYPE first   IS TABLE OF NUMBER;
    TYPE secnd   IS TABLE OF VARCHAR(2048);

    s secnd;
    a first;
    b first;
    c first;
    d first;
    e first;
    f first;

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_MigStall';

    SELECT * BULK COLLECT INTO s, a, b, c, d, e, f
      FROM (
      	select svc.name,
             sum(case when dc.CREATIONTIME > TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-3600) then 1 else 0 end),
             sum(case when dc.CREATIONTIME between TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-3600)
                           and TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*6)) then 1 else 0 end),
             sum(case when dc.CREATIONTIME between TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*6))
                           and TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*2*6)) then 1 else 0 end),
             sum(case when dc.CREATIONTIME between TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*2*6))
                           and TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*4*6)) then 1 else 0 end) ,
             sum(case when dc.CREATIONTIME between  TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*4*6))
                           and TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*8*6)) then 1 else 0 end) ,
             sum(case when dc.CREATIONTIME < TO_CHAR((sysdate-TO_DATE('01011970','ddmmyyyy'))*24*60*60-(3600*8*6)) then 1 else 0 end)
         from castor_stager.castorfile cf,
              castor_stager.diskcopy dc,
              castor_stager.svcclass svc
         where dc.status = 10 and dc.castorfile=cf.id and cf.svcclass=svc.id group by svc.name
      );


      forall marker in s.first..s.last
        insert into CASTOR_STAGER.monitoring_MigStall values(mytime, s(marker), a(marker), b(marker), c(marker), d(marker), e(marker), f(marker));

END Proc_MigStall;

