COL fileName new_value spoolFile;
select '&1..'||SUBSTR('&_USER',11) fileName from dual;

PROMPT 'writing to &spoolFile'
SPOOL '&spoolFile'
PROMPT
PROMPT
PROMPT <H1>Statistics for &1</H1>
PROMPT
PROMPT

PROMPT <H1>Tapes with many files</H1>
PROMPT
SELECT * FROM (SELECT VID, nbFiles, size2char(dataSize) DataOnTape, size2char(avgFileSize) averageFileSize,
                      size2Char(stdDevFileSize) FileSizeStandardDeviation, sec2date(avgFileLastModTime) AverageModificationTime,
                      duration2char(stdDevFileLastModTime) ModificationTimeStdDev,
                      sec2date(minFileLastModTime) oldestLastModificationTime, sec2date(maxFileLastModTime) newestLastModificationTime
                 FROM tapeData ORDER BY nbFiles DESC) WHERE ROWNUM < 30;
PROMPT

PROMPT <H1>Tapes with large time dispersion</H1>
PROMPT
SELECT * FROM (SELECT VID, nbFiles, size2char(dataSize) DataOnTape, size2char(avgFileSize) averageFileSize,
                      size2Char(stdDevFileSize) FileSizeStandardDeviation, sec2date(avgFileLastModTime) AverageModificationTime,
                      duration2char(stdDevFileLastModTime) ModificationTimeStdDev,
                      sec2date(minFileLastModTime) oldestLastModificationTime, sec2date(maxFileLastModTime) newestLastModificationTime
                 FROM tapeData ORDER BY stdDevFileLastModTime DESC) WHERE ROWNUM < 30;
PROMPT

SPOOL OFF
