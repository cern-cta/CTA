/*******************************************************************
 *
 * @(#)$RCSfile$ $Revision$ $Date$ $Author$
 *
 * This file contains is an SQL script used to test polices of the tape gateway
 * on a disposable stager schema, which is supposed to be initially empty.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/
 
 /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */
 /*
  * D O   N O    U S E    O N    A   P R O D   O R   D E V    S T A G E R
  *
  * Only intended for throwaway schema on devdb10
  */
   /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */
   
CREATE OR REPLACE PACKAGE BODY TG_POLICY_TESTING AS

	/* Putting things together */
	PROCEDURE Test1 IS
	BEGIN
		ClearLog;
		ClearData;
		PopulateData;
		LoopGetNextFile;
		ClearData;
	END;
	

	/* Procedure to insert a log line. Log line id implicitely added by a trigger */
	PROCEDURE LogLine ( Line IN VARCHAR2) IS
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		INSERT INTO TG_test_policies_logs (Line) VALUES (Line); COMMIT;
	END;

	/* Flush the logs */
	PROCEDURE ClearLog IS
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		DELETE FROM TG_test_policies_logs;
		COMMIT;
	END;
	
	/* Procedure to cleanup the data from the tests, based on names of objects (dummy-something) */
	PROCEDURE ClearData IS
	BEGIN
		/*DELETE FROM TG_test_policies_logs;*/
		LogLine ('Cleanup...');
		DECLARE /* Cleanup block */
			dropped_ids "numList";
		BEGIN  /* Cleanup block */
			/* Reverse order cleanup to respect constrains */
			/* Delete tapecopies */
			SELECT t.id BULK COLLECT INTO dropped_ids from tapecopy t INNER JOIN castorfile c 
				ON t.castorfile = c.id WHERE c.nshost LIKE 'dummyns' FOR UPDATE;
			DELETE FROM tapecopy t WHERE t.id IN (SELECT * FROM TABLE(dropped_ids));
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
      /* Delete tapegatewayrequest */
			DELETE FROM TapeGatewayRequest tgr WHERE tgr.Stream
				in (SELECT UNIQUE parent FROM Stream2TapeCopy sttc 
					WHERE sttc.child IN (SELECT t.id FROM TapeCopy t INNER JOIN CastorFile c
						ON t.CastorFile = c.id WHERE c.nshost LIKE 'dummyns'))
				RETURNING tgr.id BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete stream2tapecopy */
			DELETE FROM Stream2TapeCopy sttc WHERE sttc.child IN (SELECT t.id FROM TapeCopy t INNER JOIN CastorFile c
				ON t.CastorFile = c.id WHERE c.nshost LIKE 'dummyns'); /* collect is just to test */
			/* Delete dependant stream migrations (TG requests) */
			DELETE FROM TapeGatewayRequest tgr WHERE tgr.Stream IN (SELECT UNIQUE s.id FROM
				Stream s WHERE s.TapePool IN (SELECT UNIQUE tp.id FROM TapePool tp 
					WHERE tp.name LIKE 'dummyTapePool'));
			/* Delete streams */
			DELETE FROM Stream s WHERE s.TapePool IN (SELECT UNIQUE tp.id FROM TapePool tp WHERE tp.name LIKE 'dummyTapePool')
				RETURNING s.id BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete Tapes */
			DELETE FROM Tape t WHERE t.VID LIKE 'dummyTape%' RETURNING  t.id BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete diskcopies */
			DELETE FROM diskcopy d WHERE d.path LIKE '/tmp/dummy%' RETURNING d.id
				BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete the FileSystems */
			DELETE FROM FileSystem fs WHERE fs.MountPoint LIKE '/dummy/%'
				RETURNING fs.id BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete the DiskServers */
			DELETE FROM DiskServer ds WHERE ds.Name LIKE 'dummyDiskServer%'
				RETURNING ds.id BULK COLLECT INTO dropped_ids;  
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));	
			/* Delete tapepools */
			DELETE FROM SvcClass2TapePool sttp WHERE sttp.child IN (SELECT UNIQUE t.id
				FROM TapePool t WHERE t.name LIKE 'dummyTapePool');
			DELETE FROM tapepool t WHERE t.name LIKE 'dummyTapePool' RETURNING t.id
				BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete diskpools */
			DELETE FROM DiskPool2SvcClass dpts WHERE dpts.parent IN (SELECT UNIQUE d.id
				FROM diskpool d WHERE d.name LIKE 'dummyDiskPool');		
			DELETE FROM diskpool d WHERE d.name LIKE 'dummyDiskPool' RETURNING d.id
				BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete castor files */
			DELETE FROM castorfile c WHERE c.nshost LIKE 'dummyns' RETURNING c.id
				BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete file classes */
			DELETE FROM FILECLASS f WHERE f.name LIKE 'dummyFileClass' RETURNING f.id
				BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
			/* Delete Service classes */
			DELETE FROM SVCCLASS s WHERE s.name  LIKE 'dummyServiceClass' RETURNING s.id
				BULK COLLECT INTO dropped_ids;
			DELETE FROM id2type i WHERE i.id IN (SELECT * FROM TABLE(dropped_ids));
		END; /* Cleaup block */
		COMMIT;
		LogLine ('Cleanup done.');
	END; /* Procedure CleanLog */
	
	/* Procedure to create a dataset to play with */
	PROCEDURE PopulateData IS
		/* Create a minimal set of data to play with */
		/* Create global variables only for singleton types */
		now NUMBER:= getTime();
		sc SVCCLASS%ROWTYPE;
		fc FILECLASS%ROWTYPE;
		dp DISKPOOL%ROWTYPE;
		tp TAPEPOOL%ROWTYPE;
	BEGIN /* Data creation block */
		LogLine ('Data creation: ServiceClass');
		/* Create service class */
		sc.name := 'dummyServiceClass';
		sc.nbdrives := 5;
		sc.defaultfilesize := 1024;
		sc.forcedfileclass := 234;
		SELECT ids_seq.nextval INTO sc.id FROM dual;
		INSERT INTO svcclass VALUES sc;
		INSERT INTO id2type (id, type) VALUES ( sc.id, castorobjtypes.svcclass );
		COMMIT;
		LogLine ('FileClass');  
		/* Create file class */
		fc.name := 'dummyFileClass';
		fc.nbcopies := 2;
		SELECT ids_seq.nextval INTO fc.id FROM dual;
		INSERT INTO fileclass VALUES fc;
		INSERT INTO id2type (id, type) VALUES ( fc.id, castorobjtypes.fileclass );
		COMMIT;
		LogLine ('Castorfiles');
		/* Create the castorfiles */
		DECLARE
			cf CastorFile%ROWTYPE;
		BEGIN
			cf.nshost := 'dummyns';
			cf.filesize := 1000;
			cf.creationtime := now;
			cf.lastaccesstime := now;
			cf.lastupdatetime := now;
			cf.svcclass := sc.id;
			cf.fileclass := fc.id;
			DECLARE
				i number;
			BEGIN
				FOR i in 1..10 LOOP
					SELECT ids_seq.nextval INTO cf.id FROM dual;
					cf.fileid := 666000000+i;
					cf.lastknownfilename := 'filenumber' || cf.fileid;
					INSERT INTO  CASTORFILE	VALUES cf;
					INSERT INTO ID2TYPE (ID, TYPE) VALUES (cf.id, castorobjtypes.castorfile);
				END LOOP;
			END;
		END;
		COMMIT;
		LogLine ('DiskPool');
		/* Create the diskpool */
		dp.name := 'dummyDiskPool';
		select ids_seq.nextval into dp.id FROM dual;
		INSERT INTO diskpool VALUES dp;
		INSERT INTO ID2TYPE (ID, TYPE) VALUES ( dp.id, castorobjtypes.diskpool );
		/* Connect the DiskPool to ServiceClass */
		INSERT INTO DiskPool2SvcClass ( parent, child ) VALUES ( dp.id, sc.id );
		COMMIT;
		LogLine ('TapePool');
		/* Create the tapepool */
		tp.name := 'dummyTapePool';
		select ids_seq.nextval into tp.id FROM dual;
		INSERT INTO tapepool VALUES tp;
		INSERT INTO ID2TYPE (ID, TYPE) VALUES ( tp.id, castorobjtypes.tapepool ); 
		/* Connect the TapePool to Service Class */
		INSERT INTO SvcClass2TapePool ( parent, child ) VALUES ( sc.id, tp.id );
		COMMIT;
		LogLine ('DiskServer');
		DECLARE
			ds DiskServer%ROWTYPE;
		BEGIN
			SELECT ids_seq.nextval INTO ds.id FROM dual;
			ds.Name := 'dummyDiskServer' || ds.id;
			ds.Status := CastorStatus.DiskServer_Production;
			INSERT INTO DiskServer VALUES ds;
			INSERT INTO ID2TYPE (ID, TYPE) VALUES ( ds.id, castorobjtypes.DiskServer );
			COMMIT;
		END;
		LogLine ('FileSystem');
		DECLARE
			fs FileSystem%ROWTYPE;
		BEGIN
			SELECT ids_seq.nextval INTO fs.id FROM dual;
			SELECT d.id INTO fs.DiskServer FROM DiskServer d WHERE d.Name LIKE 'dummyDiskServer%' 
				AND ROWNUM < 2 ORDER BY d.id;
			fs.MountPoint := '/dummy/' || fs.id;
			fs.DiskPool := dp.id;
			fs.Status := CASTORSTATUS.Filesystem_Production;
			INSERT INTO FileSystem VALUES fs;
			INSERT INTO ID2TYPE (ID, TYPE) VALUES ( fs.id, castorobjtypes.FileSystem );
			COMMIT;
		END;
		LogLine ('DiskCopy');
		/* Create disk copies for all files, then 2 disk copies for 2 of them, 3 for another one */
		DECLARE /* Disk copy creation block */
			CURSOR c_cf IS SELECT cf.* FROM castorfile cf, fileclass fc WHERE cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass';
			CURSOR c_cf2 IS SELECT cf.* FROM castorfile cf, fileclass fc WHERE cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass'
				 AND rownum < 4 ORDER BY cf.id ASC;
			CURSOR c_cf3 is SELECT cf.* FROM castorfile cf, fileclass fc WHERE cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass'
				 AND rownum < 4 ORDER BY cf.id DESC;
		BEGIN
			/* One disk copy for everyone */
			for cf in c_cf LOOP
				DECLARE
					dc DiskCopy%ROWTYPE;
				BEGIN
					select ids_seq.nextval into dc.id from dual;
					dc.path:= '/tmp/dummy' || dc.id;
					dc.castorfile:= cf.id;
					dc.Status := CastorStatus.DiskCopy_CanBeMigr;
					/* Give it a file system */
					SELECT fs.id INTO dc.FileSystem FROM FileSystem fs WHERE fs.MountPoint LIKE '/dummy/%' 
						AND ROWNUM = 1 ORDER BY id;
					INSERT into diskcopy values dc;
					INSERT into id2type (id, type) values (dc.id, castorobjtypes.diskcopy);
				END;
			END LOOP;
			for cf in c_cf2 LOOP
				DECLARE
					dc DiskCopy%ROWTYPE;
				BEGIN
					select ids_seq.nextval into dc.id from dual;
					dc.path:= '/tmp/dummy2' || dc.id;
					dc.castorfile:= cf.id;
					dc.Status := CastorStatus.DiskCopy_CanBeMigr;
					/* Give it a file system */
					SELECT fs.id INTO dc.FileSystem FROM FileSystem fs WHERE fs.MountPoint LIKE '/dummy/%' 
						AND ROWNUM = 1 ORDER BY id;
					INSERT into diskcopy values dc;
					INSERT into id2type (id, type) values (dc.id, castorobjtypes.diskcopy);
				END;
			END LOOP;
			for cf in c_cf3 LOOP
				DECLARE
					dc DiskCopy%ROWTYPE;
				BEGIN
					select ids_seq.nextval into dc.id from dual;
					dc.path:= '/tmp/dummy3a' || dc.id;
					dc.castorfile:= cf.id;
					dc.Status := CastorStatus.DiskCopy_CanBeMigr;
					/* Give it a file system */
					SELECT fs.id INTO dc.FileSystem FROM FileSystem fs WHERE fs.MountPoint LIKE '/dummy/%' 
						AND ROWNUM = 1 ORDER BY id;
					INSERT into diskcopy values dc;
					INSERT into id2type (id, type) values (dc.id, castorobjtypes.diskcopy);
					select ids_seq.nextval into dc.id from dual;
					dc.path:= '/tmp/dummy3b' || dc.id;
					dc.castorfile:= cf.id;
					/* Give it a file system */
					SELECT fs.id INTO dc.FileSystem FROM FileSystem fs WHERE fs.MountPoint LIKE '/dummy/%' 
						AND ROWNUM = 1 ORDER BY id;
					INSERT into diskcopy values dc;
					INSERT into id2type (id, type) values (dc.id, castorobjtypes.diskcopy);
				END;
			END LOOP;
			COMMIT;
		END; /* disk copies creation */
		LogLine ('TapeCopy');
		DECLARE /* Tape copy creation block */
			/* Reusing tc from above */
			CURSOR c_cf is SELECT cf.* FROM castorfile cf, fileclass fc WHERE cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass';
			CURSOR c_cf2 is SELECT cf.* FROM castorfile cf, fileclass fc WHERE cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass'
				 AND (ROWNUM < 2 OR (ROWNUM >=4 AND ROWNUM < 5)) ORDER BY cf.id ASC;
			CURSOR c_cf3 is SELECT cf.* from castorfile cf, fileclass fc WHERE cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass'
				 AND (ROWNUM < 2 OR (ROWNUM >=4 AND ROWNUM < 5)) ORDER BY cf.id DESC;
		BEGIN
			/* One tape copy for everyone */
			for cf in c_cf LOOP
				DECLARE
					tc TapeCopy%ROWTYPE;
				BEGIN
					SELECT ids_seq.nextval INTO tc.id FROM dual;
					tc.castorfile := cf.id;
					SELECT count (*) INTO tc.copynb FROM tapecopy tdb WHERE tdb.castorfile = cf.id;
					tc.copynb := tc.copynb + 1;
					tc.Status := CastorStatus.TapeCopy_WaitInStreams;
					INSERT into tapecopy VALUES tc;
					INSERT into id2type ( id, type ) VALUES ( tc.id, castorobjtypes.tapecopy );
				END;
			END LOOP;
			/* Some get a second one */
			FOR cf IN c_cf2  LOOP
				DECLARE
					tc TapeCopy%ROWTYPE;
				BEGIN
					SELECT ids_seq.nextval into tc.id FROM dual;
					tc.castorfile := cf.id;
					SELECT COUNT (*) INTO tc.copynb FROM tapecopy tdb WHERE tdb.castorfile = cf.id;
					tc.copynb := tc.copynb + 1;
					tc.Status := CastorStatus.TapeCopy_WaitInStreams;
					INSERT into tapecopy VALUES tc;
					INSERT into id2type ( id, type ) VALUES ( tc.id, castorobjtypes.tapecopy );
				END;
			END LOOP;
			/* Others get a second or third one */
			FOR cf IN c_cf3  LOOP
				DECLARE
					tc TapeCopy%ROWTYPE;
				BEGIN
					SELECT ids_seq.nextval INTO tc.id FROM dual;
					tc.castorfile := cf.id;
					SELECT COUNT (*) INTO tc.copynb FROM tapecopy tdb WHERE tdb.castorfile = cf.id;
					tc.copynb := tc.copynb + 1;
					tc.Status := CastorStatus.TapeCopy_WaitInStreams;
					INSERT into tapecopy VALUES tc;
					INSERT into id2type ( id, type ) VALUES ( tc.id, castorobjtypes.tapecopy );
				END;
			END LOOP;
			COMMIT;
		END; /* Tape copies creation */
		LogLine ('Tape');
		/* Create one tape  */
		DECLARE
			t Tape%ROWTYPE;
		BEGIN
			FOR i IN 1..5 LOOP
				SELECT ids_seq.nextval INTO t.id FROM DUAL;
				t.VID := 'dummyTape' || t.id;
				INSERT INTO Tape VALUES t;
				INSERT INTO id2type ( id, type ) VALUES ( t.id, castorobjtypes.Tape );
				COMMIT;
			END LOOP;
		END; /* Tape creation */
		LogLine ('Stream');
		DECLARE 
		BEGIN /* Streams creation block */
			FOR i IN 1..10 LOOP
				DECLARE
					str Stream%ROWTYPE;
				BEGIN
					SELECT ids_seq.nextval INTO str.id FROM dual;
					str.TapePool := tp.id;
					BEGIN
						SELECT t.id INTO str.Tape FROM Tape t WHERE t.VID LIKE 'dummyTape%' AND t.id NOT IN (SELECT UNIQUE s.Tape FROM Stream s) AND ROWNUM = 1;
					EXCEPTION WHEN NO_DATA_FOUND THEN
						NULL; /* Null tape for the stream is also OK */
					END;
					INSERT INTO STREAM VALUES str;
					INSERT INTO id2type ( id, type ) VALUES ( str.id, castorobjtypes.stream );
					COMMIT;
				END;
			END LOOP;
		END; /* Creation of streams */
		LogLine ('Stream2TapeCopy');
		DECLARE
			/* Iterate on all our tape copies */
			/* reuse the above tape copy tc */
			CURSOR c_tc IS SELECT t.* FROM tapecopy t, castorfile cf, fileclass fc WHERE t.castorfile = cf.id AND cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass';
			CURSOR c_str IS SELECT str.* FROM stream str WHERE str.TapePool IN (SELECT UNIQUE tp.id FROM TapePool tp WHERE tp.name LIKE 'dummyTapePool');
		BEGIN /* Creation of stream2tapecopy */
			FOR tclocal IN c_tc LOOP
				FOR strlocal IN c_str LOOP
					INSERT INTO Stream2TapeCopy ( Parent, Child ) VALUES ( strlocal.id, tclocal.id );
				END LOOP;
			END LOOP;
		END;
		COMMIT;
/* 		UPDATE tapecopy SET status = 3 WHERE id IN (SELECT t.id FROM tapecopy t, castorfile cf, fileclass fc WHERE t.castorfile = cf.id AND cf.fileclass = fc.id AND fc.name LIKE 'dummyFileClass'); */ 
		LogLine ('TapeGatewayRequest');
		DECLARE
			CURSOR c_str IS SELECT s.* FROM Stream s WHERE s.Tape IS NOT NULL;
		BEGIN /* TapeGatewayRequest creation */
			FOR str IN c_str LOOP
				DECLARE
					tgr TapeGatewayRequest%ROWTYPE;
				BEGIN
					SELECT ids_seq.nextval INTO tgr.id FROM DUAL;
					SELECT ids_seq.nextval INTO tgr.VDQMVolReqId FROM DUAL;
					tgr.Stream := str.id;
					INSERT INTO TapeGatewayRequest VALUES tgr;
					INSERT INTO id2type ( id, type ) VALUES ( tgr.id, castorobjtypes.TapeGatewayRequest );
					COMMIT;
				END;
			END LOOP;
		END;
		NULL; /* For the moment */
		LogLine('Initialization complete');
	END; /* PopulateData */
	
	/* Start calling the policy */
	PROCEDURE LoopGetNextFile IS
		CURSOR c_streams_id IS SELECT UNIQUE tgr.VDQMVolReqId id FROM TapeGatewayRequest tgr WHERE
			tgr.Stream in (SELECT UNIQUE sttc.parent FROM Stream2TapeCopy sttc  WHERE 
				sttc.child IN (SELECT t.id FROM TapeCopy t INNER JOIN CastorFile c
					ON t.CastorFile = c.id WHERE c.nshost LIKE 'dummyns'));
		ret INTEGER;
		outVid VARCHAR2(2048);
		outputFile castorTape.FileToMigrateCore_cur;
	BEGIN
		LogLine ('Will loop-call the policies for all streams');
		FOR c_s_iter IN c_streams_id LOOP	
			LOOP
				tg_getFileToMigrate (c_s_iter.id, ret, outVid, outputFile);
				LogLine ('TapeGatewayRequest(VDQM req ID): '|| c_s_iter.id || ' ret=' || ret ||' outVid=' || outVid /*|| '  ' || outPutfile*/);
				IF ret = 0 THEN
					DECLARE
						ftm castorTape.FileToMigrateCore;
					BEGIN
						LOOP
							FETCH outputFile INTO ftm;
							EXIT WHEN outputFile%NOTFOUND;
							LogLine ('\t\tFileToMigrate: Id=' || ftm.fileId || ' nshost: ' || ftm.nsHost || ' lastModTime=' || ftm.LastModificationTime
								|| ' DiskServer=' || ftm.DiskServer || ' mountPoint=' || ftm.mountPoint || ' path=' || ftm.path || ' last known=' 
								|| ftm.lastKnownFileName || ' fseq=' || ftm.fseq || ' fileSize=' || ftm.fileSize || ' fileTransactionId=' || ftm.fileTransactionId);
						END LOOP;
						CLOSE outputFile;
					END;
				END IF;
			EXIT WHEN ret != 0; END LOOP;
		END LOOP;
	END;
END; /* PACKAGE BODY TG_POLICY_TESTING; */
/
