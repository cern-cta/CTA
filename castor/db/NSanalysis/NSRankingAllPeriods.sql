---------------------------------
-- First the all times ranking --
---------------------------------
COL dirTableToUse new_value dirs;
select 'dirs' dirTableToUse from dual;
@NSRankingPath.sql &1 global All

------------------------
-- The weekly ranking --
------------------------
select 'wdirs' dirTableToUse from dual;
@NSRankingPath.sql &1 lastweek Week
@PROMPT 
-------------------------
-- The monthly ranking --
-------------------------
select 'mdirs' dirTableToUse from dual;
@NSRankingPath.sql &1 lastmonth Month

------------------------
-- The yearly ranking --
------------------------
select 'ydirs' dirTableToUse from dual;
@NSRankingPath.sql &1 lastyear Year
