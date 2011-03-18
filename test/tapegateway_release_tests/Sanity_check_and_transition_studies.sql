-- Safe transitions from rtcpclientd to tapegateway.

-- Transitions are conditioned by success of a complete sanity check of the DB.

-- Check for the trivial. Level one, invalid states (we could have constraints on those ones...)
-- All queries should come out empty.
select status, count(*) from tapecopy where status not in ( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ) group by status;

-- In case of rtcpclientd => gateway
select status, count(*) from tape where status not in ( 0, 1, 2, 3, 4, 5, 6, 7, 8 ) group by status;
-- In case of gateway => rtcpclientd
select status, count(*) from tape where status not in ( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ) group by status;

select tpmode, count(*) from tape where tpmode not in ( 0, 1 ) group by tpmode;

select status, count(*) from segment where status not in ( 0, 5, 6, 7, 8 ) group by status;
select status, count(*) from subrequest where status not in ( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 ) group by status;
select getnextstatus, count(*) from subrequest where getnextstatus not in ( 0, 1, 2 ) group by getnextstatus;

-- In case of rtcpclientd => gateway
select status, count(*) from stream where status not in ( 0, 1, 2, 3, 4, 5, 6, 7 ) group by status;
-- In case of gateway => rtcpclientd
select status, count(*) from stream where status not in ( 0, 1, 2, 3, 4, 5, 6, 7, 8 ) group by status;

select querytype, count(*) from queryparameter where querytype not in ( 0, 1, 2, 3, 4, 5, 6 ) group by querytype;
select status, count(*) from filesystem where status not in ( 0, 1, 2 ) group by status;
select adminStatus, count(*) from filesystem where adminStatus not in ( 0, 1, 2, 3 ) group by adminStatus;
select status, count(*) from diskserver where status not in ( 0, 1, 2 ) group by status;
select adminStatus, count(*) from diskserver where adminStatus not in ( 0, 1, 2, 3 ) group by adminStatus;
select status, count(*) from diskcopy where status not in ( 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11 ) group by status;
select gcType, count(*) from diskcopy where gcType not in ( 0, 1, 2, 3, 4 ) group by gcType;

-- Second pass (tape targeted)
-- We start from one table and left outer join to find if the startup element is orhaned or not.
-- The tables of the tape system that we will use are: tapecopy, tape, stream, segment.
-- The big join for tapecopies in rtcpclientd. We do not take the shortcut to join tapecopy.castorfile to diskcopy.castorfile, but we'll look for orphaned diskcopies separately.
-- As tapecopy can be orphaned from diskcopy from both lack of diskcopy and lack of castorfile. We take the latter as a killer. Ophaned diskcopies can be tackled in another sequence.

-- This join is recall oriented tapecopy => segment => tape.tpmode = read.
select tapecopy.id tc_id, tapecopy.status tc_stat, castorfile.id cf_id, castorfile.lastknownfilename cf_name, castorfile.fileid, svcclass.name svcclass, fileclass.name fileclass, d1.id dc_id, d1.status dc_status, 
       d1.path dp_path, 
       d2.id dc2_id, d2.status dc2_status, 
       filesystem.mountpoint fs_mountpoint, diskserver.name ds_name, diskpool.name diskpool,
       segment.id seg_id, segment.status seg_stat, tape.id tp_id, tape.status tp_stat, tape.tpmode tp_mode, tape.vid tp_vid
  from tapecopy
  left outer join castorfile on castorfile.id = tapecopy.castorfile
  left outer join svcclass on castorfile.svcclass = svcclass.id
  left outer join fileclass on castorfile.fileclass = fileclass.id
  left outer join diskcopy d1 on d1.castorfile = castorfile.id
  left outer join diskcopy d2 on d2.castorfile = tapecopy.castorfile
  left outer join filesystem on filesystem.id = d1.filesystem
  left outer join diskpool on diskpool.id = filesystem.diskpool
  left outer join diskserver on diskserver.id = filesystem.diskserver
  left outer join segment on segment.copy = tapecopy.id
  left outer join tape on tape.id = segment.tape
  where rownum <= 10
  AND NOT (
    -- We don't cover migration related states here. Failed (6) is a shared state.
     (tapecopy.status IN (0, 1, 2, 3, 5, 7, 9)) OR
    -- tapecopy TOBERECALLED (4) => castorfile is valid, diskcopy is NULL
    -- stream2tapecopy.child IS NULL, stream IS NULL, tapepool IS NULL (as a consequence), tape depends on the state of the stream
    -- segment is NOT NULL and 
     (tapecopy.status = 4 AND castorfile.id IS NOT NULL AND d1.id IS NULL
        AND svcclass.id IS NOT NULL AND (segment.id IS NOT NULL OR Segment.status IN (100)) and filesystem.id IS NOT NULL and diskserver.id is not NULL 
        and diskpool.id IS NOT NULL AND tape.id IS NULL)
  );
  
-- This join is migration oriented tapecopy ( => segment ) => stream2tapecopy => stream ( => tapepool ) => tape.tpmode = write (or NULL).
-- Pare
select tapecopy.id tc_id, tapecopy.status tc_status, castorfile.lastknownfilename cf_name, svcclass.name svcclass, fileclass.name fileclass, d1.id d1_id, d1.status d1_stat,
       d2.id d2_id, d2.status d2_stat,
       filesystem.mountpoint, diskserver.name, diskpool.name diskpool,
       segment.id seg_id, segment.status seg_stat, segment.tape seg_tape, tseg.vid seg_tp_vid, tseg.status seg_tp_stat, stream.id stream, stream.status str_stat, tapepool.name tapepool, 
       tape.id tape, tape.status tp_status, tape.tpmode tp_mode
  from tapecopy
  left outer join castorfile on castorfile.id = tapecopy.castorfile     
  left outer join svcclass on castorfile.svcclass = svcclass.id
  left outer join fileclass on castorfile.fileclass = fileclass.id
  left outer join diskcopy d1 on d1.castorfile = castorfile.id
  left outer join diskcopy d2 on d2.castorfile = tapecopy.castorfile
  left outer join filesystem on filesystem.id = d1.filesystem
  left outer join diskpool on diskpool.id = filesystem.diskpool
  left outer join diskserver on diskserver.id = filesystem.diskserver
  left outer join segment on segment.copy = tapecopy.id
  left outer join tape tseg on segment.tape = tseg.id
  left outer join stream2tapecopy on stream2tapecopy.child = tapecopy.id
  left outer join stream on stream2tapecopy.parent = stream.id
  left outer join tape on tape.id = stream.tape
  left outer join tapepool on stream.tapepool = tapepool.id
  where (segment.id is not null) and rownum <= 100
  AND NOT (
     -- Exclude recall states
     (tapecopy.status IN (4)) OR
     -- tapecopy CREATED (0)=migration => castorfile is valid, diskcopy is valid, diskcopy status is CANBEMIGR (10), classes are not null, tape is NULL, segment is NULL, servers, filesystems are valid
     (tapecopy.status = 0 AND castorfile.id IS NOT NULL AND d1.id IS NOT NULL and d1.status = 10 and fileclass.id IS NOT NULL
        AND svcclass.id IS NOT NULL AND segment.id IS NULL and filesystem.id IS NOT NULL and diskserver.id is not NULL 
        and diskpool.id IS NOT NULL) OR
    -- tapecopy TOBEMIGRATED (1) => castorfile is valid, diskcopy is valid, diskcopy status is CANBEMIGR (10), classes are not null, segment is NULL, servers, filesystems are valid,
    -- stream2tapecopy.child IS NULL, stream, tape, tapepool are NULL (as a consequence)
     (tapecopy.status = 1 AND castorfile.id IS NOT NULL AND d1.id IS NOT NULL and d1.status = 10 and fileclass.id IS NOT NULL
        AND svcclass.id IS NOT NULL AND segment.id IS NULL and filesystem.id IS NOT NULL and diskserver.id is not NULL 
        and diskpool.id IS NOT NULL AND stream2tapecopy.child IS NULL) OR
    -- tapecopy WAITINSTREAMS (2) => castorfile is valid, diskcopy is valid, diskcopy status is CANBEMIGR (10), classes are not null, segment is NULL or retied (8), servers, filesystems are valid,
    -- stream2tapecopy.child IS NOT NULL, stream IS NOT NULL, tapepool IS NOT NULL (as a consequence), tape depends on the state of the stream.
     (tapecopy.status = 2 AND castorfile.id IS NOT NULL AND d1.id IS NOT NULL and d1.status = 10 and fileclass.id IS NOT NULL
        AND svcclass.id IS NOT NULL AND (segment.id IS NULL OR Segment.status = 8) and filesystem.id IS NOT NULL and diskserver.id is not NULL 
        and diskpool.id IS NOT NULL AND Stream.id IS NOT NULL AND Stream.status IN (5,0,7,6,4) AND tapepool.id IS NOT NULL and tape.id IS NULL) OR
     (tapecopy.status = 2 AND castorfile.id IS NOT NULL AND d1.id IS NOT NULL and d1.status = 10 and fileclass.id IS NOT NULL
        AND svcclass.id IS NOT NULL AND (segment.id IS NULL OR Segment.status = 8) and filesystem.id IS NOT NULL and diskserver.id is not NULL 
        and diskpool.id IS NOT NULL AND Stream.id IS NOT NULL AND Stream.status IN (8,1,2,3) AND tapepool.id IS NOT NULL and tape.id IS NOT NULL) OR
    -- Failed tapecopies (6) => castorfile is valid, diskcopy is valid, diskcopy status is CANBEMIGR (10), classes are not null, segment is NOT NULL and retied (8), servers, filesystems are valid,
    -- stream2tapecopy.child IS NULL, stream IS NULL, tapepool IS NULL (as a consequence), tape(stream) IS NULL.
     (tapecopy.status = 6 AND castorfile.id IS NOT NULL AND d1.id IS NOT NULL and d1.status = 10 and fileclass.id IS NOT NULL
        AND svcclass.id IS NOT NULL AND (segment.id IS NOT NULL AND Segment.status = 8) and filesystem.id IS NOT NULL and diskserver.id is not NULL 
        and diskpool.id IS NOT NULL AND Stream.id IS NULL)
  )
  
--     and not (segment.status = 0 /* SEGMENT_UNPROCESSED */ 
--              and tapecopy.status = 4 /* TAPECOPY_TOBERECALLED */ 
--              and diskcopy.status = 2 /* DISKCOPY_WAITTAPERECALL */ 
--              and tape.tpmode = 0 
--              and tape.status in (1 /* TAPE_PENDING */, 
--                                  2 /* TAPE_DRIVE */, 
--                                  3 /* TAPE_WAITMOUNT */, 
--                                  4 /* TAPE_MOUNTED */, 
--                                  8 /* TAPE_WAITPOLICY */)) 
--     and not (segment.status = 6 /* SEGMENT_FAILED */ 
--              and ((tape.tpmode = 0 
--                    and tapecopy.status = 4 /* TAPECOPY_TOBERECALLED */) 
--                   or (tape.tpmode = 1 and tapecopy.status = 3 /* TAPECOPY_SELECTED */)));

-- Considered bulk migrate procedure. Might be improved based on the fine grained 
-- sanity check queries.
--
-- The major reset procedure
-- Tape copies go back to to be migrated and we restart from basically scratch for most of the states.
-- From TAPECOPY_CREATED, leave.
-- From TAPECOPY_TOBEMIGRATED, leave.
-- From TAPECOPY_WAITINSTREAMS, leave.
-- From TAPECOPY_SELECTED, move back to TAPECOPY_TOBEMIGRATED, remove VID, no segments expected.
-- From TAPECOPY_TOBERECALLED, leave.
-- From TAPECOPY_STAGED, reset to TOBEMIGRATED. (will be remigrated by rtcpclientd.)
-- From TAPECOPY_FAILED, leave.
-- From TAPECOPY_WAITPOLICY, leave.
-- From TAPECOPY_REC_RETRY, move to TAPECOPY_TOBERECALLED, segment is left as is.
-- From TAPECOPY_MIG_RETRY, move back to TO BE MIGRATED.

-- Streams 
-- From STREAM_PENDING, Leave as is
-- From STREAM_WAITDRIVE, Set to pending
-- From STREMA_WAITMOUNT, set to pending
-- From STREAM_RUNING, set to pending
-- From STREAM_WAITSPACE, Leave as is.
-- From STREAM_CREATED, Leave as is.
-- From STREAM_STOPPED, Leave as is.
-- From STREAM_WAITPOLICY, Leave as is.
-- From STREAM_TOBESENTTOVDQM, we have a busy tape attached and they have to be free.

-- Tapes
-- From TAPE_UNSED, Leave as is.
-- From TAPE_PENDING, Leave as is.
-- From TAPE_WAITDRIVE, reset to PENDING
-- From TAPE_WAITMOUNT, reset to PENDING
-- From TAPE_MOUNTED, reset to PENDING
-- From TAPE_FINISHED, Leave as is. (Assuming it is an end state).
-- From TAPE_FAILED, Leave as is.
-- From TAPE_UNKNOWN, Leave as is. (Assuming it is an end state).
-- From TAPE_WAITPOLICY, Leave as is. (For the rechandler to take).

-- Segments (the gateway is not going to use)
-- From SEGMENT_UNPROCESSED, Leave as is.
-- From SEGMENT_FILECOPIED, (apparently unused)
-- From SEGMENT_FAILED, Reset to unprocessed.
-- From SEGMENT_SELECTED, Move to unprocessed
-- From SEGMENT_RETRIED, Delete segment from the database.