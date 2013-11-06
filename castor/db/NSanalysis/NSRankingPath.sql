COL fileName new_value spoolFile;
select SUBSTR('&1',INSTR('&1','/',-1)+1)||'.'||SUBSTR('&_USER',INSTR('&_USER','NS_')+3)||'.&2' fileName from dual;

PROMPT 'writing to &spoolFile'
SPOOL '&spoolFile'
PROMPT
PROMPT
PROMPT <H1>Statistics for &1</H1>
PROMPT
PROMPT <H2>Shortcuts</H2>
PROMPT <A href='#migrations'>Worst directories for migration</A>
PROMPT <A href='#recalls'>Worst directories for recalls</A>
PROMPT <A href='#tape'>Biggest directories in terms of space on tape</A>
PROMPT <A href='#disk'>Biggest directories in terms of disk only space</A>
PROMPT
PROMPT

ALTER SESSION SET statistics_level=all;

PROMPT <A name=migrations><H1>Worst directories for migrations</H1></A>
PROMPT
BEGIN worstMigrGeneric('&3','&1'); END; -- filling the temporary table
/
SELECT * FROM WorstMigTempTable;
DECLARE
  nbr INTEGER;
BEGIN
 SELECT COUNT(*) INTO nbr FROM WorstMigTempTable;
 IF nbr = 0 THEN
   dbms_output.put_line('No activity<br><br>');
 END IF;
END;
/
DELETE FROM WorstMigTempTable;
COMMIT; -- also emptying the temporary table
PROMPT

PROMPT <A name=recalls><H1>Worst directories for recalls</H1></A>
PROMPT
BEGIN worstRecallGeneric('&3','&1'); END; -- filling the temporary table
/
SELECT * FROM WorstRecallTempTable;
DECLARE
  nbr INTEGER;
BEGIN
 SELECT COUNT(*) INTO nbr FROM WorstRecallTempTable;
 IF nbr = 0 THEN
   dbms_output.put_line('No activity<br><br>');
 END IF;
END;
/
DELETE FROM WorstRecallTempTable;
COMMIT; -- also emptying the temporary table
PROMPT

PROMPT <A name=tape><H1>Biggest directories in terms of space on tape</H1></A>
PROMPT
BEGIN biggestOnTapeGeneric('&3','&1'); END; -- filling the temporary table
/
SELECT * FROM biggestOnTapeTempTable;
DECLARE
  nbr INTEGER;
BEGIN
 SELECT COUNT(*) INTO nbr FROM biggestOnTapeTempTable;
 IF nbr = 0 THEN
   dbms_output.put_line('No activity<br><br>');
 END IF;
END;
/
DELETE FROM BiggestOnTapeTempTable;
COMMIT; -- also emptying the temporary table
PROMPT

PROMPT <A name=disk><H1>Biggest directories in terms of disk only space</H1></A>
PROMPT
BEGIN biggestOnDiskGeneric('&3','&1'); END; -- filling the temporary table
/
SELECT * FROM biggestOnDiskTempTable;
DECLARE
  nbr INTEGER;
BEGIN
 SELECT COUNT(*) INTO nbr FROM biggestOnDiskTempTable;
 IF nbr = 0 THEN
   dbms_output.put_line('No activity<br><br>');
 END IF;
END;
/
DELETE FROM BiggestOnDiskTempTable;
COMMIT; -- also emptying the temporary table
PROMPT

ALTER SESSION SET statistics_level=basic;

SPOOL OFF
