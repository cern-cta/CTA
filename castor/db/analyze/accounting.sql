/**************/
/* Accounting */
/**************/

/* The table holding accounting information */
CREATE TABLE NbRequestsPerUser (euid INTEGER PRIMARY KEY, nb NUMBER);

/* The table holding The accounting limits */
CREATE TABLE MaxNbRequestsPerUser (euid INTEGER PRIMARY KEY, nb NUMBER);
INSERT INTO MaxNbRequestsPerUser VALUES (0, 1000);

/* triggers updating the accounting info when a subrequest arrives */
CREATE OR REPLACE TRIGGER tr_NbRequestsPerUser_NewSubReq
BEFORE INSERT ON SubRequest FOR EACH ROW
DECLARE
  userId INTEGER;
  maxNbReq INTEGER;
  currentNbReq INTEGER;
BEGIN
  SELECT euid INTO userId
    FROM (SELECT euid from StageGetRequest WHERE id = :new.request UNION ALL
          SELECT euid from StagePrepareToGetRequest WHERE id = :new.request UNION ALL
          SELECT euid from StageUpdateRequest WHERE id = :new.request UNION ALL
          SELECT euid from StagePrepareToUpdateRequest WHERE id = :new.request UNION ALL
          SELECT euid from StagePutRequest WHERE id = :new.request UNION ALL
          SELECT euid from StagePrepareToPutRequest WHERE id = :new.request) Request;
  BEGIN
    SELECT nb INTO maxNbReq FROM MaxNbRequestsPerUser WHERE euid = userId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The user has no max, use default
    SELECT nb INTO maxNbReq FROM MaxNbRequestsPerUser WHERE euid = 0;
  END;
  BEGIN
    SELECT nb INTO currentNbReq FROM NbRequestsPerUser WHERE euid = userId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- create the request accounting
    INSERT INTO NbRequestsPerUser VALUES (userId, 0);
    currentNbReq := 0;
  END;
  IF currentNbReq >= maxNbReq THEN
     raise_application_error(-20102, 'Too many requests going on. User access denied.');
  ELSE
    UPDATE NbRequestsPerUser SET nb = nb + 1 WHERE euid = userId;
  END IF;
END;

/* triggers updating the accounting info when a subrequest was scheduled */
CREATE OR REPLACE TRIGGER tr_NbRequestsPerUser_SubReqEnd
AFTER UPDATE of status ON SubRequest
FOR EACH ROW WHEN (new.status IN (6, 7)) -- READY, FAILED
BEGIN
  UPDATE NbRequestsPerUser SET nb = nb - 1
   WHERE euid = (SELECT euid
                   FROM (SELECT euid from StageGetRequest WHERE id = :old.request UNION ALL
                         SELECT euid from StagePrepareToGetRequest WHERE id = :old.request UNION ALL
                         SELECT euid from StageUpdateRequest WHERE id = :old.request UNION ALL
                         SELECT euid from StagePrepareToUpdateRequest WHERE id = :old.request UNION ALL
                         SELECT euid from StagePutRequest WHERE id = :old.request UNION ALL
                         SELECT euid from StagePrepareToPutRequest WHERE id = :old.request));
END;

