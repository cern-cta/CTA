-------------------------------------------------------------------------
-- This file gives solutions to problems you may find out when running --
-- currentStatus.sql                                                   --
-------------------------------------------------------------------------

-- Many old subrequest in status 9 : FAILED_FINISHED -> clean them
--------------------------------------------------------------
BEGIN
 LOOP
   -- do it 100 by 100 in order to not slow down the normal requests
   FOR sr IN (SELECT id FROM SubRequest WHERE status = 9 and ROWNUM <= 100) LOOP
     archiveSubReq(sr.id);
   END LOOP;
   -- commit and wait 5 seconds between each bunch of 100
   COMMIT;
   DBMS_LOCK.sleep(seconds => 5.0);
 END LOOP;
 EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;

-- Many old subrequest in status 6 : READY -> clean them
--------------------------------------------------------
BEGIN
 LOOP
   -- do it 100 by 100 in order to not slow down the normal requests
   FOR sr IN (SELECT id FROM SubRequest
               WHERE creationtime < getTime() - 10000
                 AND status = 6 and ROWNUM <= 100) LOOP
     archiveSubReq(sr.id);
   END LOOP;
   -- commit and wait 5 seconds between each bunch of 100
   COMMIT;
   DBMS_LOCK.sleep(seconds => 5.0);
 END LOOP;
 EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;

-- some requests have all there subrequests FINISHED
----------------------------------------------------
-- TAKE CARE : DO NOT USE IF THERE ARE FINISHED SUBREQUESTS IN OTHER SITUATIONS
BEGIN
 LOOP
   -- do it 100 by 100 in order to not slow down the normal requests
   FOR sr IN (SELECT id FROM SubRequest
               WHERE creationtime < getTime() - 10000
                 AND status = 8 and ROWNUM <= 100) LOOP
     archiveSubReq(sr.id);
   END LOOP;
   -- commit and wait 5 seconds between each bunch of 100
   COMMIT;
   DBMS_LOCK.sleep(seconds => 5.0);
 END LOOP;
 EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;

-- SubRequests waiting on nothing (mostly non existing parents -> restart them with no parent
---------------------------------------------------------------------------------------------
UPDATE SubRequest SET status = 1, parent = NULL
 WHERE status = 5 AND (parent NOT IN (SELECT id FROM SubRequest) OR parent IS NULL);

-- Tapes in status 6 : FAILED -> look why
-----------------------------------------
-- 1- List segments for these tapes AND see their status
SELECT s.status, s.errorcode, s.errmsgtxt, COUNT(s.id) FROM subrequest sr, tapecopy tc, tape t, segment s
 WHERE sr.creationtime < gettime() - 10000 AND sr.status = 4
   AND tc.castorfile = sr.castorfile
   AND s.copy = tc.id AND s.tape = t.id
 GROUP BY s.status, s.errorcode, s.errmsgtxt;
-- 2- If any segment in status 6, look whether the files still exist in nameserver
SELECT unique c.fileid, c.id FROM subrequest sr, tapecopy tc, tape t, segment s, Diskcopy d, castorfile c
 WHERE sr.creationtime < gettime() - 10000 AND sr.status = 4
   AND tc.castorfile = sr.castorfile
   AND s.copy = tc.id AND s.tape = t.id
   AND d.castorfile = sr.castorfile
   AND c.id = d.castorfile;
-- Then in a shell : for f in <list of fileids>; do nsGetPath castor $f; done
-- 3- For those that report No such file or directory, call cleanLostFiles
DECLARE
  rawfiles "numList" := "numList"();   -- <-- put your castorfile ids here
  files castor."cnumList";
BEGIN
  FOR i in rawfiles.FIRST .. rawfiles.LAST LOOP
    files(i):=rawfiles(i);
  end LOOP;
  filesClearedProc(files);
  COMMIT;
END;
