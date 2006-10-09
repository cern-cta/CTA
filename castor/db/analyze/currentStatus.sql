-------------------------------------------------------------------------
-- This file gives useful statements to get a clear idea of what is to --
-- be cleaned up in the database. Solutions to the different problems  --
-- are given in solutions.sql.                                         --
-------------------------------------------------------------------------

-- distribution of running subrequests per type
-----------------------------------------------
select type, count(*) from Id2Type, subrequest where subrequest.request = id2type.id and status <= 6 group by type;

-- distribution of old subrequests (more than 10000s)
-----------------------------------------------------
select status, count(*) from subrequest where creationtime < getTime() - 10000 group by status;
-- Typical problems :
--   - many subrequest in status 9 : FAILED_FINISHED. See solutions.sql for cleaning
--   - many subrequest in status 6 : READY Go to next step to find out why
--   - many subrequest in status 8 : FINISHED Go to next step to find out why

-- What are the FINISHED subrequest waiting on ?
SELECT s1.status, count(*) from subrequest s1, subrequest s2
 WHERE s2.creationtime < getTime() - 10000
   and s1.request = s2.request and s1.status != 8 and s2.status = 8 group by s1.status;
-- Typical output :
--   - some in status 4 (WAITAPERECALL) : some other part of the request is waiting for tape recall
--     we will see on next next step how tape recalls are doing
--   - some in status 5 (WAITSUBREQ) : some other part of the request is waiting on a waiting file
--     we will see on next step what they are waiting on
--   - some in status 6 (READY) : these requests are READY for too long now. See solutions.sql for cleaning
--   - nothing : remaining FINISHED subrequests are within requests where everything is over. See
--     solutions.sql to clean that up.

-- What are WAITSUBREQ subrequests waiting on ?
SELECT p.status, count(*) from subrequest c, subrequest p
 WHERE c.creationtime < getTime() - 10000
   and c.parent = p.id(+) and c.status = 5 group by p.status;
-- Typical output :
--   - status 4 (WAITTAPERECALL) : waiting on a tape recall
--     we will see on next next step how the tape recalls are doing
--   - status "null" : waiting on nothing : restart them (see solutions.sql)

-- What are the status of old tapecopies for tape recalls
select t.* from subrequest s, tapecopy t
 where s.creationtime < gettime() - 10000 and s.status = 4
   and t.castorfile = s.castorfile;
-- Typical output :
--   - all in status 4 (TOBERECALLED) : all are waiting for the stream/tape.
--     we will see in next step how the tapes are doing

-- What are the status of tapes for tape recalls
select unique t.* from subrequest sr, tapecopy tc, tape t, segment s
 where sr.creationtime < gettime() - 10000 and sr.status = 4
   and tc.castorfile = sr.castorfile
   and s.copy = tc.id and s.tape = t.id;
-- Typical problem :
--   - some in status 6 (FAILED) : these tapes had errors, see solutions.sql for cleaning



-- distribution of old DiskCopies (more than 10000s)
select status, count(*) from diskcopy where creationtime < getTime() - 10000 group by status;

-- What are the WAITDisk2DiskCopy ones ?
select * from diskcopy where creationtime < getTime() - 10000 and status = 1;