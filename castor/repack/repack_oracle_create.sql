/* SQL statements for type RepackSubRequest */
CREATE TABLE RepackSubRequest (vid VARCHAR2(2048), xsize INTEGER, status NUMBER, cuuid VARCHAR2(2048), filesMigrating NUMBER, filesStaging NUMBER, files NUMBER, filesFailed NUMBER, id INTEGER PRIMARY KEY, requestID INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type RepackSegment */
CREATE TABLE RepackSegment (fileid INTEGER, compression NUMBER, segsize INTEGER, filesec INTEGER, copyno NUMBER, blockid INTEGER, fileseq INTEGER, id INTEGER PRIMARY KEY, vid INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type RepackRequest */
CREATE TABLE RepackRequest (machine VARCHAR2(2048), userName VARCHAR2(2048), creationTime NUMBER, serviceclass VARCHAR2(2048), pid INTEGER, command NUMBER, pool VARCHAR2(2048), stager VARCHAR2(2048), groupid NUMBER, userid NUMBER, id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;


/* SQL statements for Castor_Version */
CREATE TABLE castor_version (version VARCHAR2(2048));

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_typeId on Id2Type (type, id);
