-- This script updates NbTapeCopiesInFS with correct values.
-- It should not be used while migration is running
DECLARE
  nb NUMBER;
BEGIN
  FOR dsIt IN (SELECT * FROM DiskServer) LOOP
    dbms_output.put_line(CONCAT('Starting Diskserver ', dsIt.name));
    FOR fsIt IN (SELECT * FROM FileSystem WHERE diskserver = dsIt.id) LOOP
      --dbms_output.put_line(CONCAT('  Dealing with FileSystem ', fsIt.mountPoint));
      FOR countIt IN (SELECT /*+ FIRST_ROWS */ Stream2TapeCopy.parent stream, count(*) nbt
                        FROM DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
                       WHERE DiskCopy.filesystem = fsIt.id
                         AND DiskCopy.castorfile = CastorFile.id
                         AND DiskCopy.status = 10 -- CANBEMIGR
                         AND TapeCopy.castorfile = Castorfile.id
                         AND TapeCopy.status = 2
                         AND Stream2TapeCopy.child = TapeCopy.id
                       group by (Stream2TapeCopy.parent)) LOOP
        UPDATE NbTapeCopiesInFs SET NbTapeCopies = countIt.nbt WHERE fs = fsIt.id AND Stream = countIt.stream;
        COMMIT;
      END LOOP;
    END LOOP;
    --dbms_output.put_line(CONCAT('End of Diskserver ', dsIt.name));
  END LOOP;
END;
