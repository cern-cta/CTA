CREATE OR REPLACE PROCEDURE Proc_MetaRecallRunning AS

  TYPE first IS TABLE OF CASTOR_STAGER.svcclass.name%TYPE;
  TYPE secnd IS TABLE OF NUMBER;

  a first;
  b secnd;
  c secnd;
  d secnd;
  e secnd;

  mytime DATE := SYSDATE;

  BEGIN

  execute immediate 'truncate table castor_stager.monitoring_MetaRecallRunning';

  SELECT * BULK COLLECT INTO a, b, c, d, e
    FROM (
      SELECT name, min(creationtime), max(creationtime) maxage, avg(creationtime) avgage, sum(marker) total
        FROM (
          SELECT svcClass.name, decode(creationTime, NULL, 0, gettime() - creationtime) creationtime, decode(creationTime, NULL, 0, 1) marker
            FROM (
              SELECT Request.svcClassName svcClass, DiskCopy.creationtime
                FROM DiskCopy, SubRequest, castorfile,
                     (SELECT id, svcClassName FROM StageGetRequest UNION ALL
                      SELECT id, svcClassName FROM StagePrepareToGetRequest UNION ALL
                      SELECT id, svcClassName FROM StageUpdateRequest UNION ALL
                      SELECT id, svcClassName FROM StageRepackRequest) Request
               WHERE SubRequest.diskcopy = diskcopy.id
                 AND DiskCopy.castorfile = castorfile.id
                 AND DiskCopy.status = 2
                 AND SubRequest.status = 4
                 AND SubRequest.parent = 0
                 AND SubRequest.request = Request.id
            ) requests
       RIGHT JOIN SvcClass ON SvcClass.name = Requests.svcClass
      ) GROUP BY name);

  FORALL f IN a.first..a.last
    INSERT INTO CASTOR_STAGER.monitoring_MetaRecallRunning VALUES (mytime, a(f), b(f), c(f), d(f), e(f));
END Proc_MetaRecallRunning;