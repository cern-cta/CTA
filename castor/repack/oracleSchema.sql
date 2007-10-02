/* SQL statements for type RepackRequest */
CREATE TABLE RepackRequest (machine VARCHAR2(2048), userName VARCHAR2(2048), creationTime INTEGER, pool VARCHAR2(2048), pid INTEGER, svcclass VARCHAR2(2048), command NUMBER, stager VARCHAR2(2048), userId NUMBER, groupId NUMBER, retryMax INTEGER, id INTEGER CONSTRAINT I_RepackRequest_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RepackSegment */
CREATE TABLE RepackSegment (fileid INTEGER, segsize INTEGER, compression NUMBER, filesec NUMBER, copyno NUMBER, blockid INTEGER, fileseq INTEGER, id INTEGER CONSTRAINT I_RepackSegment_Id PRIMARY KEY, repacksubrequest INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RepackSubRequest */
CREATE TABLE RepackSubRequest (vid VARCHAR2(2048), xsize INTEGER, status NUMBER, filesMigrating NUMBER, filesStaging NUMBER, files NUMBER, filesFailed NUMBER, cuuid VARCHAR2(2048), submitTime INTEGER, filesStaged NUMBER, filesFailedSubmit NUMBER, retryNb INTEGER, id INTEGER CONSTRAINT I_RepackSubRequest_Id PRIMARY KEY, repackrequest INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

