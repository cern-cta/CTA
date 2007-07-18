CREATE OR REPLACE PROCEDURE Proc_TABLESIZE AS

    TYPE first   IS NUMBER;
    a first;
    b first;
    c first;
    d firest;
    e first;

    mytime DATE := SYSDATE;

    BEGIN

    execute immediate 'truncate table castor_stager.monitoring_TABLESIZE';

    SELECT * BULK COLLECT INTO a
      FROM (
       SELECT count(*)
        FROM CASTOR_STAGER.subrequest
      );
 
    SELECT * BULK COLLECT INTO b
      FROM (
       SELECT count(*)
        FROM CASTOR_STAGER.diskcopy
      );

    SELECT * BULK COLLECT INTO c
      FROM (
       SELECT count(*)
        FROM CASTOR_STAGER.tapecopy
      );

    SELECT * BULK COLLECT INTO d
      FROM (
       SELECT count(*)
        FROM CASTOR_STAGER.castorfile
      );

    SELECT * BULK COLLECT INTO e
      FROM (
       SELECT count(*)
        FROM CASTOR_STAGER.id2type
      );
    

      forall marker in a.first..a.last
        insert into CASTOR_STAGER.monitoring_TABLESIZE values(mytime, a(marker), b(marker), c(marker), d(marker), e(marker));
      
END Proc_TABLESIZE;
/ 
