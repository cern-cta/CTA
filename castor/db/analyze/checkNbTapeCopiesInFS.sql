-- This script checks whether the NbTapeCopiesInFs content is in sync with reality
DECLARE
  nbt number;
BEGIN
  FOR fsIt IN (SELECT filesystem.*, DiskServer.name FROM FileSystem, DiskPool2SvcClass, SvcClass, DiskServer
                WHERE filesystem.diskpool = DiskPool2SvcClass.parent
                  and DiskPool2SvcClass.child = SvcClass.id
                  and SvcClass.name = 'ITDC'
                  and diskServer.id = FileSystem.diskserver) LOOP
      FOR countIt IN (SELECT /*+ FIRST_ROWS */ Stream2TapeCopy.parent stream, count(*) nbt
                        FROM DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
                       WHERE DiskCopy.filesystem = fsIt.id
                         AND DiskCopy.castorfile = CastorFile.id
                         AND DiskCopy.status = 10 -- CANBEMIGR
                         AND TapeCopy.castorfile = Castorfile.id
                         AND TapeCopy.status = 2
                         AND Stream2TapeCopy.child = TapeCopy.id
                       group by (Stream2TapeCopy.parent)) LOOP
        select NbTapeCopies into nbt from NbTapeCopiesInFs WHERE fs = fsIt.id AND Stream = countIt.stream;
        dbms_output.put_line(CONCAT(CONCAT(CONCAT(fsIt.name,'  '),fsIt.mountPoint),CONCAT(CONCAT('  In theory : ', TO_CHAR(nbt)), CONCAT(', in practice : ', TO_CHAR(countIt.nbt)))));
      END LOOP;
  END LOOP;
END;
