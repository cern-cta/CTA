CREATE OR REPLACE FUNCTION Ops_getTapePoolSvcClasses(
  inTapePoolId NUMBER)
RETURN VARCHAR2
AS
  varSvcClasses      VARCHAR2(2048) := '';
  varIsFirstSvcClass BOOLEAN        := true;
BEGIN
  FOR recName IN (
    SELECT SvcClass.name
      FROM SvcClass
     INNER JOIN SvcClass2TapePool
        ON (SvcClass.id = SvcClass2TapePool.parent)
     WHERE SvcClass2TapePool.child = inTapePoolId)
  LOOP
    IF varIsFirstSvcClass THEN
      IF LENGTH(varSvcClasses) + LENGTH(recName.name) > 2048 THEN
        RETURN 'TOO MANY CHARS';
      END IF;

      varSvcClasses := recName.name;
      varIsFirstSvcClass := FALSE;
    ELSE
      IF LENGTH(varSvcClasses) + 1 + LENGTH(recName.name) > 2048 THEN
        RETURN 'TOO MANY CHARS';
      END IF;

      varSvcClasses := varSvcClasses || ' ' || recName.name;
    END IF;
  END LOOP;

  RETURN varSvcClasses;
END Ops_getTapePoolSvcClasses;
/

CREATE OR REPLACE VIEW Ops_StreamStats AS
    WITH Now AS (
           SELECT getTime() time
             FROM Dual),
         StreamVid AS (
           SELECT Stream.id            streamId,
                  NVL(Tape.vid, 'N/A') vid
             FROM Stream
             LEFT OUTER JOIN Tape
               ON (Stream.tape = Tape.id))
  SELECT Stream.id                              streamId,
         Stream.status                          streamStatus,
         StreamVid.vid,
         TapePool.name                          tapePoolName,
         Ops_getTapePoolSvcClasses(TapePool.id) svcClassNames,
         NVL(stats.nbTapeCopies       , 0)      nbTapeCopies,
         NVL(stats.totalBytes         , 0)      totalBytes,
         NVL(stats.ageOfOldestTapeCopy, 0)      ageOfOldestTapeCopy
    FROM Stream
   INNER JOIN StreamVid
      ON (Stream.id = StreamVid.streamId)
   INNER JOIN TapePool
      ON (Stream.tapePool = TapePool.id)
    LEFT OUTER JOIN (
           SELECT Stream2TapeCopy.parent           streamId,
                  count(distinct TapeCopy.id)      nbTapeCopies,
                  sum(NVL(CastorFile.fileSize, 0)) totalBytes,
                  (select time from Now) -
                    min(NVL(Diskcopy.creationtime, (select time from Now)))
                                                   ageOfOldestTapeCopy
             FROM Stream2TapeCopy
            INNER JOIN TapeCopy
               ON (Stream2TapeCopy.child = TapeCopy.id)
             LEFT OUTER JOIN CastorFile
               ON (TapeCopy.castorFile = CastorFile.id)
             LEFT OUTER JOIN DiskCopy
               ON (TapeCopy.castorFile = Diskcopy.castorFile)
            GROUP BY Stream2TapeCopy.parent
         ) Stats
      ON (Stream.id = Stats.streamId)
   ORDER BY Stream.id;
