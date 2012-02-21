/* Verify that the script is running against the correct schema and version */
WHENEVER SQLERROR EXIT FAILURE

DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'STAGER'
     AND release LIKE '2_1_12_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please check that you are applying this post-upgrade script to the proper release.');
END;
/

----------------
-- migrations --
----------------

SET serveroutput ON;

DECLARE
  nbTC NUMBER;
BEGIN
  -- loop over files to be migrated
  FOR file IN (
    SELECT DC.castorFile, DC.diskcopysize, CastorFile.fileid FROM
     (SELECT DiskCopy.castorFile AS castorFile, MigrationJob.castorFile AS mj, DiskCopy.diskcopysize
        FROM DiskCopy, MigrationJob
       WHERE DiskCopy.status = 10
         AND MigrationJob.castorFile(+) = DiskCopy.castorFile) DC, CastorFile, Cns_file_metadata@remoteNS
      WHERE DC.mj is NULL
        AND DC.castorFile = CastorFile.id
        AND CastorFile.fileid = Cns_file_metadata.fileid) LOOP
    BEGIN
      -- get the number of tape copies to create
      SELECT nbCopies INTO nbTC FROM FileClass, CastorFile
        WHERE CastorFile.id = file.castorFile AND CastorFile.fileClass = FileClass.id;
      FOR i IN 1..nbTC LOOP
        -- trigger migration of the ith copy
        initMigration(file.castorFile, file.diskcopysize, NULL, NULL, i, tconst.MIGRATIONJOB_PENDING);
      END LOOP;
      COMMIT;
    EXCEPTION WHEN OTHERS THEN
      -- ignore it, and continue with the other files
      dbms_output.put_line('Fileid ' || TO_CHAR(file.fileid) || ' failed with error ' || TO_CHAR(SQLCODE) || ' : ' || SQLERRM);
    END;
  END LOOP;
END;
/

-------------
-- recalls --
-------------

-- cleanup segment table (should have been done in upgrade script)
DELETE FROM Segment;
COMMIT;

-- for each ongoing recall, cancel the current recall, and trigger a new one
DECLARE
  dcids "numList";
BEGIN
  DELETE FROM DiskCopy WHERE status = 2 RETURNING id BULK COLLECT INTO dcids;
  UPDATE SubRequest SET status = 1
   WHERE status IN (4, 5) -- WAITTAPERECALL, WAITSUBREQ
     AND diskcopy IN (SELECT * FROM TABLE(dcids));
  COMMIT;
END;
/
