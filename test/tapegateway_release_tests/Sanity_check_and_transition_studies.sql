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
select tapecopy.id, tapecopy.status, castorfile.lastknownfilename, svcclass.name, fileclass.name, diskcopy.id , diskcopy.status, 
       filesystem.mountpoint, diskserver.name, diskpool.name,
       segment.id, segment.status, tape.id, tape.status, tape.tpmode
  from tapecopy
  left outer join castorfile on castorfile.id = tapecopy.castorfile
  left outer join svcclass on castorfile.svcclass = svcclass.id
  left outer join fileclass on castorfile.fileclass = fileclass.id
  left outer join diskcopy on diskcopy.castorfile = castorfile.id
  left outer join filesystem on filesystem.id = diskcopy.filesystem
  left outer join diskpool on diskpool.id = filesystem.diskpool
  left outer join diskserver on diskserver.id = filesystem.diskserver
  left outer join segment on segment.copy = tapecopy.id
  left outer join tape on tape.id = segment.tape
  where (segment.id is not null) and rownum <= 10
   and not ( -- Negative logic: disregard everything that is allowed. Disregard all migration related states as well.
    (tapecopy.status == tapecopy_created        
   );
  
-- This join is migration oriented tapecopy ( => segment ) => stream2tapecopy => stream ( => tapepool ) => tape.tpmode = write (or NULL).
-- Pare
select tapecopy.id tapecopy, tapecopy.status, castorfile.lastknownfilename, svcclass.name svcclass, fileclass.name fileclass, diskcopy.id diskcopy, diskcopy.status, 
       filesystem.mountpoint, diskserver.name, diskpool.name diskpool,
       segment.id segment, segment.status, segment.tape seg_tape, stream.id stream, tapepool.name tapepool, 
       tape.id tape, tape.status, tape.tpmode
  from tapecopy
  left outer join castorfile on castorfile.id = tapecopy.castorfile     
  left outer join svcclass on castorfile.svcclass = svcclass.id
  left outer join fileclass on castorfile.fileclass = fileclass.id
  left outer join diskcopy on diskcopy.castorfile = castorfile.id
  left outer join filesystem on filesystem.id = diskcopy.filesystem
  left outer join diskpool on diskpool.id = filesystem.diskpool
  left outer join diskserver on diskserver.id = filesystem.diskserver
  left outer join segment on segment.copy = tapecopy.id
  left outer join stream2tapecopy on stream2tapecopy.child = tapecopy.id
  left outer join stream on stream2tapecopy.parent = stream.id
  left outer join tape on tape.id = stream.tape
  left outer join tapepool on stream.tapepool = tapepool.id
  where (segment.id is not null) and rownum <= 10;
  
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