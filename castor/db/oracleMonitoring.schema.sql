/*******************************************************************
 * @(#)$RCSfile: oracleMonitoring.schema.sql,v $ $Revision: 1.6 $ $Date: 2009/07/05 13:46:14 $ $Author: waldron $
 * Schema creation code for Monitoring tables
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement for table MonDiskCopyStats */
CREATE TABLE MonDiskCopyStats
  (timestamp DATE, interval NUMBER, diskServer VARCHAR2(255), mountPoint VARCHAR2(255), dsStatus VARCHAR2(50), fsStatus VARCHAR2(50), available VARCHAR2(2), status VARCHAR2(100), totalFileSize NUMBER, nbFiles NUMBER);

/* SQL statement for table MonWaitTapeMigrationStats */
CREATE TABLE MonWaitTapeMigrationStats
  (timestamp DATE, interval NUMBER, tapePool VARCHAR2(255), status VARCHAR2(10), minWaitTime NUMBER, maxWaitTime NUMBER, avgWaitTime NUMBER, minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, bin_LT_1 NUMBER, bin_1_To_6 NUMBER, bin_6_To_12 NUMBER, bin_12_To_24 NUMBER, bin_24_To_48 NUMBER, bin_GT_48 NUMBER, totalFileSize NUMBER, nbFiles NUMBER);

/* SQL statement for table MonWaitTapeRecallStats  */
CREATE TABLE MonWaitTapeRecallStats
  (timestamp DATE, interval NUMBER, svcClass VARCHAR2(255), minWaitTime NUMBER, maxWaitTime NUMBER, avgWaitTime NUMBER, minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, bin_LT_1 NUMBER, bin_1_To_6 NUMBER, bin_6_To_12 NUMBER, bin_12_To_24 NUMBER, bin_24_To_48 NUMBER, bin_GT_48 NUMBER, totalFileSize NUMBER, nbFiles NUMBER);

