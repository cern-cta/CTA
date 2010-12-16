-- Safe transitions from rtcpclientd to tapegateway.

-- Transitions are conditioned by success of a complete sanity check of the DB.

-- Check for the trivial. Level one, invalid states (we could have constraints on those ones...)
select status, count(*) from tapecopy where status not in ( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ) group by status;


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