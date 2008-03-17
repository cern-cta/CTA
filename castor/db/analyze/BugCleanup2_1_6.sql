-- For logging

-- drop the table if it exists

DECLARE
 t varchar2(2048);
BEGIN
	SELECT table_name INTO t FROM user_tables WHERE table_name = 'CLEANUPLOGTABLE';
	execute immediate 'DROP TABLE CleanupLogTable';
EXCEPTION WHEN no_data_found THEN
   	NULL;
END;

-- create/recreate the table

CREATE TABLE CleanupLogTable (fac NUMBER, message VARCHAR2(256), logDate NUMBER);

-- Cleanup for diskcopies in STAGIN without associated TAPECOPY. 

DECLARE 
    cfIds "numList";
    sIds "numList";
    totalIds NUMBER;
BEGIN
	INSERT INTO CleanupLogTable VALUES (10, 'Cleaning STAGIN diskcopy without tapecopy 0 done', getTime());
	COMMIT; 
        -- info for log   
	SELECT count(*) INTO totalIds FROM diskcopy WHERE castorfile NOT IN ( SELECT castorfile FROM tapecopy) AND status=2;  
	-- diskcopy as invalid
	UPDATE diskcopy SET status=7 WHERE castorfile NOT IN ( SELECT castorfile FROM tapecopy) AND status=2 RETURNING castorfile BULK COLLECT INTO cfIds;
	-- restart the subrequests
	UPDATE subrequest SET status=0 WHERE status IN (4,5) AND castorfile MEMBER OF cfIds;  
        COMMIT;
        -- log 
        UPDATE CleanupLogTable SET message = 'STAGIN diskcopy without tapecopy: cleaned - ' || TO_CHAR(totalIds) || ' entries', logDate = getTime() WHERE fac = 10;
	COMMIT;
END;


-- Cleanup for orphan segment.
DECLARE
	sIds "numList";
 	totalIds NUMBER;
BEGIN
	INSERT INTO CleanupLogTable VALUES (11, 'Cleaning segments without tapecopy 0 done', getTime());
	COMMIT;
        -- info for log
	SELECT count(*) INTO totalIds FROM segment WHERE status IN (6, 8) AND copy NOT IN( SELECT id FROM tapecopy);

	-- delete segments
	DELETE FROM segment WHERE status IN (6, 8) AND copy NOT IN (SELECT id FROM tapecopy) RETURNING id BULK COLLECT INTO sIds;
	DELETE FROM Id2type WHERE id MEMBER OF sIds;
	COMMIT;
        -- log
        UPDATE CleanupLogTable SET message = 'Segments without tapecopy: cleaned - ' || TO_CHAR(totalIds) || ' entries', logDate = getTime() WHERE fac = 11;
	COMMIT;
END; 


-- Optional steps follow
INSERT INTO CleanupLogTable VALUES (16, 'Shrinking tables', getTime());
COMMIT;

-- Shrinking space in the tables that have just been cleaned up.
-- This only works if tables have ROW MOVEMENT enabled, and it's
-- not a big problem if we don't do that. Newer Castor versions
-- do enable this feature in all involved tables.

ALTER TABLE Segment SHRINK SPACE CASCADE;
ALTER TABLE id2type SHRINK SPACE CASCADE;

UPDATE CleanupLogTable
   SET message = 'All tables were shrunk', logDate = getTime()
 WHERE fac = 16;
COMMIT;

-- To provide a summary of the performed cleanup
SELECT * FROM CleanupLogTable;
