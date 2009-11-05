
ALTER TABLE TapeCopy ADD nbRetry NUMBER;
ALTER TABLE TapeCopy ADD errorCode NUMBER;
ALTER TABLE Tape ADD density VARCHAR(2048);
ALTER TABLE Tape ADD label VARCHAR(2048);
ALTER TABLE Tape ADD dgn VARCHAR(2048);
ALTER TABLE Tape ADD devtype VARCHAR(2048);


CREATE TABLE TapeGatewayRequest (accessMode NUMBER, startTime INTEGER, lastVdqmPingTime INTEGER, vdqmVolReqId NUMBER, nbRetry NUMBER, lastF
seq NUMBER, id INTEGER CONSTRAINT PK_TapeGatewayRequest_Id PRIMARY KEY, streamMigration INTEGER, tapeRecall INTEGER, status INTEGER) INITRA
NS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeGatewayRequest', 'status', 0, 'TO_BE_RESOLVED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeGatewayRequest', 'status', 1, 'TO_BE_SENT_TO_VDQM');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeGatewayRequest', 'status', 2, 'WAITING_TAPESERVER');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeGatewayRequest', 'status', 3, 'ONGOING');

/* SQL statements for type TapeGatewaySubRequest */
CREATE TABLE TapeGatewaySubRequest (fseq NUMBER, id INTEGER CONSTRAINT PK_TapeGatewaySubRequest_Id PRIMARY KEY, tapecopy INTEGER, request I
NTEGER, diskcopy INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;


-- when rtcpclientd will disappear
--DROP PROCEDURE anyTapeCopyForStream;
--DROP PROCEDURE streamsToDo;
--DROP PROCEDURE segmentsForTape;
--DROP PROCEDURE anySegmentsForTape;
--DROP PROCEDURE failedSegments;
--DROP PROCEDURE checkFileForRepack;
--DROP PROCEDURE rtcpclientdCleanUp;









