/***** TO BE MERGED INTO stager_2.1.13-6_to_2.1.14-0.sql *****/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_0', '2_1_14_X', 'NON TRANSPARENT');
COMMIT;

/* For deleteDiskCopy */
CREATE GLOBAL TEMPORARY TABLE DeleteDiskCopyHelper
  (dcId INTEGER CONSTRAINT PK_DDCHelper_dcId PRIMARY KEY, rc INTEGER)
  ON COMMIT PRESERVE ROWS;


ALTER TABLE CastorFile ADD (tapeStatus INTEGER, nsOpenTime NUMBER);
DELETE FROM ObjStatus WHERE object='DiskCopy' AND field='status'
                        AND statusName IN ('DISKCOPY_STAGED', 'DISKCOPY_CANBEMIGR');
BEGIN setObjStatusName('DiskCopy', 'status', 0, 'DISKCOPY_VALID'); END;
/

DECLARE
  remCF INTEGER;
BEGIN
  FOR cf IN (SELECT unique castorFile, status, nbCopies
               FROM DiskCopy, CastorFile, FileClass
              WHERE DiskCopy.castorFile = CastorFile.id
                AND CastorFile.fileClass = FileClass.id
                AND DiskCopy.status IN (0, 10)) LOOP
    UPDATE CastorFile
       SET tapeStatus = DECODE(cf.status,
                               10, dconst.CASTORFILE_NOTONTAPE,
                               DECODE(cf.nbCopies,
                                      0, dconst.CASTORFILE_DISKONLY,
                                      dconst.CASTORFILE_ONTAPE))
     WHERE id = cf.castorFile;
  END LOOP;
  COMMIT;
  SELECT COUNT(*) INTO remCF FROM CastorFile WHERE tapestatus IS NULL;

XXX cleanup files with no diskcopy ???

  IF remCF > 0 THEN
    raise_application_error(-20001, 'Found ' || TO_CHAR(remCF) || ' CastorFile with no DiskCopy !');
  END IF;
END;
/

DROP TRIGGER tr_DiskCopy_Online;
DROP PROCEDURE getDiskCopiesForJob;
DROP PROCEDURE processPrepareRequest;
DROP PROCEDURE recreateCastorFile;

UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_X';
COMMIT;
