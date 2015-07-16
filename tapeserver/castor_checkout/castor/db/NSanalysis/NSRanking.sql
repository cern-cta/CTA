-----------
-- setup --
-----------
SET linesize 1000
SET pagesize 1000
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET ECHO OFF
SET VERIFY OFF
SET MARKUP HTML ON ENTMAP OFF SPOOL ON
SET SERVEROUTPUT ON
COLUMN fullName FORMAT A150
COLUMN avgFileSize FORMAT A12 JUSTIFY RIGHT
COLUMN totalSize FORMAT A11 JUSTIFY RIGHT
COLUMN lostPart FORMAT A8 JUSTIFY RIGHT
COLUMN sizeOnTape FORMAT A11 JUSTIFY RIGHT
COLUMN sizeOfDiskOnlyData FORMAT A18 JUSTIFY RIGHT

@@NSRankingCode.sql

---------
-- run --
---------
--@@NSRankingPath.sql /castor/cern.ch/alice 4
@@NSRankingPath.sql /castor/cern.ch/atlas 4
@@NSRankingPath.sql /castor/cern.ch/cms 4
@@NSRankingPath.sql /castor/cern.ch/lhcb 4
@@NSRankingPath.sql /castor/cern.ch/compass 4
@@NSRankingPath.sql /castor/cern.ch/na48 4
@@NSRankingPath.sql /castor/cern.ch/user 4
--@@NSRankingPath.sql /castor/cern.ch 3

---------------
-- tape data --
---------------
--@@NSRankingTape.sql tapes

-------------
-- cleanup --
-------------
@@NSRankingCleanup.sql
