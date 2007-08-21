/* SQL statements for type RepackSubRequest */
CREATE TABLE RepackSubRequest (vid VARCHAR2(2048), xsize INTEGER, status NUMBER, cuuid VARCHAR2(2048), filesMigrating NUMBER, filesStaging NUMBER, files NUMBER, filesFailed NUMBER, submitTime INTEGER, id INTEGER PRIMARY KEY, requestID INTEGER, filesStaged NUMBER ) INITRANS 50 PCTFREE 50;

/* SQL statements for type RepackSegment */
CREATE TABLE RepackSegment (fileid INTEGER, compression NUMBER, segsize INTEGER, filesec INTEGER, copyno NUMBER, blockid INTEGER, fileseq INTEGER, id INTEGER PRIMARY KEY, vid INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type RepackRequest */
CREATE TABLE RepackRequest (machine VARCHAR2(2048), userName VARCHAR2(2048), creationTime NUMBER, serviceclass VARCHAR2(2048), pid INTEGER, command NUMBER, pool VARCHAR2(2048), stager VARCHAR2(2048), groupid NUMBER, userid NUMBER, id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;


CREATE TABLE CastorVersion (schemaVersion VARCHAR2(20), release VARCHAR2(20));
INSERT INTO CastorVersion VALUES ('-', '2_1_4_1');

/*******************************************************************
 *
 * @(#)RCSfile: oracleTrailer.sql,v  Revision: 1.3  Release Date: 2007/07/03 16:29:05  Author: itglp 
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* A small table used to cross check code and DB versions */
UPDATE CastorVersion SET schemaVersion = '2_1_4_0';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_typeId on Id2Type (type, id);

/* get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;

