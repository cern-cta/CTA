/* SQL statements for type RepackRequest */
CREATE TABLE RepackRequest (machine VARCHAR2(2048), userName VARCHAR2(2048), creationTime INTEGER, pool VARCHAR2(2048), pid INTEGER, svcclass VARCHAR2(2048), stager VARCHAR2(2048), userId NUMBER, groupId NUMBER, retryMax INTEGER, reclaim NUMBER, finalPool VARCHAR2(2048), id INTEGER CONSTRAINT I_RepackRequest_Id PRIMARY KEY, command INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RepackSegment */
CREATE TABLE RepackSegment (fileid INTEGER, segsize INTEGER, compression NUMBER, filesec NUMBER, copyno NUMBER, blockid INTEGER, fileseq INTEGER, errorCode NUMBER, errorMessage VARCHAR2(2048), id INTEGER CONSTRAINT I_RepackSegment_Id PRIMARY KEY, repacksubrequest INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RepackSubRequest */
CREATE TABLE RepackSubRequest (vid VARCHAR2(2048), xsize INTEGER, filesMigrating INTEGER, filesStaging INTEGER, files INTEGER, filesFailed INTEGER, cuuid VARCHAR2(2048), submitTime INTEGER, filesStaged INTEGER, filesFailedSubmit INTEGER, retryNb INTEGER, id INTEGER CONSTRAINT I_RepackSubRequest_Id PRIMARY KEY, repackrequest INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

