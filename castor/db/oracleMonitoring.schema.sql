/*******************************************************************
 * @(#)$RCSfile: oracleMonitoring.schema.sql,v $ $Revision: 1.4 $ $Date: 2009/06/30 14:13:40 $ $Author: itglp $
 * Schema creation code for Monitoring tables
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement for table MonDiskCopyStats */
CREATE TABLE MonDiskCopyStats
  (timestamp DATE, interval NUMBER, diskServer VARCHAR2(255), mountPoint VARCHAR2(255), dsStatus VARCHAR2(20), fsStatus VARCHAR2(20), available VARCHAR2(2), status VARCHAR2(100), totalSize NUMBER, nbFiles NUMBER);

/* SQL statement for table MonWaitTapeMigrationStats */
CREATE TABLE MonWaitTapeMigrationStats
  (timestamp DATE, interval NUMBER, svcClass VARCHAR2(255), status VARCHAR2(10), minFileAge NUMBER, maxFileAge NUMBER, avgFileAge NUMBER, minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, bin_LT_1 NUMBER, bin_1_To_6 NUMBER, bin_6_To_12 NUMBER, bin_12_To_24 NUMBER, bin_24_To_48 NUMBER, bin_GT_48 NUMBER, totalSize NUMBER, nbFiles NUMBER);

/* SQL statement for table MonWaitTapeRecallStats  */
CREATE TABLE MonWaitTapeRecallStats
  (timestamp DATE, interval NUMBER, svcClass VARCHAR2(255), minFileAge NUMBER, maxFileAge NUMBER, avgFileAge NUMBER, minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, bin_LT_1 NUMBER, bin_1_To_6 NUMBER, bin_6_To_12 NUMBER, bin_12_To_24 NUMBER, bin_24_To_48 NUMBER, bin_GT_48 NUMBER, totalSize NUMBER, nbFiles NUMBER);

