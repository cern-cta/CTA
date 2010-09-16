CREATE OR REPLACE VIEW StreamStats AS
  SELECT Stream.id                         streamId,
         Stream.status                     streamStatus,
         TapePool.name                     tapePoolName,
         SvcClass.name                     svcClassName,
         NVL(stats.nbTapeCopies       , 0) nbTapeCopies,
         NVL(stats.totalBytes         , 0) totalBytes,
         NVL(stats.ageOfOldestTapeCopy, 0) ageOfOldestTapeCopy
    FROM Stream
   INNER JOIN TapePool
      ON (Stream.tapePool = TapePool.id)
   INNER JOIN SvcClass2TapePool
      ON (TapePool.id = SvcClass2TapePool.child)
   INNER JOIN SvcClass
      ON (SvcClass2TapePool.parent = SvcClass.id)
    LEFT OUTER JOIN (
           SELECT Stream2TapeCopy.parent           streamId,
                  count(distinct TapeCopy.id)      nbTapeCopies,
                  NVL(sum(CastorFile.fileSize), 0) totalBytes,
                  NVL(getTime() - min(Diskcopy.creationtime), 0)
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
