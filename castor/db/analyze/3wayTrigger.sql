/* GC trigger code with 3-trigger version to workaround mutating table error */

/* This solution is suitable to amy case where a FOR EACH ROW trigger needs to call
   a procedure which can potentially modify the same table for which the trigger has
   been fired. */

CREATE OR REPLACE PACKAGE fileSystem_status AS
  type ridArray is table of rowid index by binary_integer;
  
  newRows ridArray;
  empty ridArray;
END;

CREATE OR REPLACE TRIGGER tr_FileSystem_Update_before
BEFORE UPDATE ON FileSystem
BEGIN
  fileSystem_status.newRows := fileSystem_status.empty;
END;

CREATE OR REPLACE TRIGGER tr_FileSystem_Update_row
AFTER UPDATE ON FileSystem
FOR EACH ROW
BEGIN
  fileSystem_status.newRows( fileSystem_status.newRows.count+1 ) := :new.rowid;
END;

/*
 * Trigger launching garbage collection whenever needed
 * Note that we only launch it when at least 10 gigs are to be deleted
 */

CREATE OR REPLACE TRIGGER tr_FileSystem_Update
AFTER UPDATE ON FileSystem
DECLARE
  freeSpace NUMBER;
  fsId NUMBER;
  jobid NUMBER;
  minFSpace NUMBER;
  maxFSpace NUMBER;
BEGIN
  FOR i IN 1 .. fileSystem_status.newRows.count LOOP
    -- compute the actual free space taking into account reservations (reservedSpace)
    -- and already running GC processes (spaceToBeFreed)
    SELECT id, free + deltaFree - reservedSpace + spaceToBeFreed, minFreeSpace, maxFreeSpace
      INTO fsId, freeSpace, minFSpace, maxFSpace
      FROM FileSystem
     WHERE rowid = fileSystem_status.newRows(i);
    -- shall we launch a new GC?
    IF minFSpace > freeSpace AND 
       -- is it really worth launching it? (some other GCs maybe are already running
       -- so we accept it only if it will free more than 5 Gb)
       maxFSpace > freeSpace + 5000000000 THEN
      -- here we spawn a job to do the real work. This ensures that
      -- the current update does not fail if GC fails
      DBMS_JOB.SUBMIT(jobid,'garbageCollectFS(' || fsId || ');');
    END IF;
  END LOOP;
END;

