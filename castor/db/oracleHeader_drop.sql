/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code          */

/* A small table used to cross check code and DB versions */
DROP TABLE CastorVersion;

/* SQL statements for object types */
DROP INDEX I_Id2Type_typeId;
DROP TABLE Id2Type;

/* Sequence for indices */
DROP SEQUENCE ids_seq;

/* SQL statements for requests status */
DROP INDEX I_newRequests_typeId;
DROP TABLE newRequests;

/* support tables - check oracleTrailer_create.sql */
DROP TABLE NbTapeCopiesInFS;
DROP TABLE LockTable;

