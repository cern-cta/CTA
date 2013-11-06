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
COLUMN timeToMigrate FORMAT A22 JUSTIFY RIGHT
COLUMN timeSpentInTapeMarks FORMAT A22 JUSTIFY RIGHT
COLUMN tapeMarksPart FORMAT A14 JUSTIFY RIGHT
COLUMN avgFileSize FORMAT A12 JUSTIFY RIGHT
COLUMN totalSize FORMAT A11 JUSTIFY RIGHT
COLUMN timeToRecall FORMAT A22 JUSTIFY RIGHT
COLUMN lostTime FORMAT A22 JUSTIFY RIGHT
COLUMN lostPart FORMAT A8 JUSTIFY RIGHT
COLUMN sizeOnTape FORMAT A11 JUSTIFY RIGHT
COLUMN sizeOfDiskOnlyData FORMAT A18 JUSTIFY RIGHT

@@NSRankingCode.sql

---------
-- run --
---------
@@NSRankingAllPeriods.sql /castor/cern.ch/alice
@@NSRankingAllPeriods.sql /castor/cern.ch/atlas
@@NSRankingAllPeriods.sql /castor/cern.ch/cms
@@NSRankingAllPeriods.sql /castor/cern.ch/lhcb
@@NSRankingAllPeriods.sql /castor/cern.ch/compass
@@NSRankingAllPeriods.sql /castor/cern.ch/na48
@@NSRankingAllPeriods.sql /castor/cern.ch/user
@@NSRankingAllPeriods.sql /castor/cern.ch

---------------
-- tape data --
---------------
@@NSRankingTape.sql tapes

-------------
-- cleanup --
-------------
@@NSRankingCleanup.sql
