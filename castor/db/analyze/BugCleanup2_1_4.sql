-- For logging
--DROP TABLE CleanupLogTable;
CREATE TABLE CleanupLogTable (fac NUMBER, message VARCHAR2(256), logDate NUMBER);

-- Cleanup of subrequests stuck in status WAITSUBREQ and
-- which are waiting on failed requests due to bug in the
-- handling of manual job kills
DECLARE
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
 INSERT INTO CleanupLogTable VALUES (8, 'Cleaning SubRequests stuck in WAITSUBREQ 0% done', getTime());
 COMMIT;
 SELECT count(s1.id) INTO totalCount
   FROM SubRequest s1, SubRequest s2
  WHERE s1.status = 5
    AND s1.parent = s2.id
    AND s2.status = 9;
 -- fail subrequests
 UPDATE SubRequest SET status = 7 -- FAILED
  WHERE id IN (SELECT s1.id FROM SubRequest s1, SubRequest s2
                WHERE s1.status = 5 AND s1.parent = s2.id
                  AND s2.status = 9);
 COMMIT;
 UPDATE CleanupLogTable
    SET message = 'SubRequests stuck in WAITSUBREQ were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
  WHERE fac = 8;
 COMMIT;
END;

-- Optional steps follow
INSERT INTO CleanupLogTable VALUES (16, 'Shrinking tables', getTime());
COMMIT;

-- Shrinking space in the tables that have just been cleaned up.
-- This only works if tables have ROW MOVEMENT enabled, and it's
-- not a big problem if we don't do that. Newer Castor versions
-- do enable this feature in all involved tables.
ALTER TABLE Id2Type SHRINK SPACE CASCADE;
ALTER TABLE Client SHRINK SPACE CASCADE;
ALTER TABLE StagePrepareToPutRequest SHRINK SPACE CASCADE;
ALTER TABLE StagePrepareToGetRequest SHRINK SPACE CASCADE;
ALTER TABLE StagePutDoneRequest SHRINK SPACE CASCADE;
ALTER TABLE StageRmRequest SHRINK SPACE CASCADE;
ALTER TABLE StageGetRequest SHRINK SPACE CASCADE;
ALTER TABLE StagePutRequest SHRINK SPACE CASCADE;
ALTER TABLE SubRequest SHRINK SPACE CASCADE;

UPDATE CleanupLogTable
   SET message = 'All tables were shrunk', logDate = getTime()
 WHERE fac = 16;
COMMIT;

-- To provide a summary of the performed cleanup
SELECT * FROM CleanupLogTable;
