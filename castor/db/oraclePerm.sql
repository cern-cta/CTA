/* This file contains SQL code that is not generated automatically */
/* and is inserted at the end of the generated code           */

CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER, tapeCopyStatus IN NUMBER,
                                                  newTapeCopyStatus IN NUMBER, newStreamStatus IN NUMBER,
                                                  diskServerName OUT VARCHAR, mountPoint OUT VARCHAR,
                                                  path OUT VARCHAR, diskCopyId OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER) AS
BEGIN
 SELECT rh_DiskServer.name, rh_FileSystem.mountPoint, rh_DiskCopy.path, rh_DiskCopy.id,
   rh_CastorFile.id, rh_CastorFile.fileId, rh_CastorFile.nsHost, rh_CastorFile.fileSize, rh_TapeCopy.id
  INTO diskServerName, mountPoint, path, diskCopyId, castorFileId, fileId, nsHost, fileSize, tapeCopyId
  FROM rh_DiskServer, rh_FileSystem, rh_DiskCopy, rh_CastorFile, rh_TapeCopy, rh_Stream2TapeCopy
  WHERE rh_DiskServer.id = rh_FileSystem.diskserver
    AND rh_FileSystem.id = rh_DiskCopy.filesystem
    AND rh_DiskCopy.castorfile = rh_CastorFile.id
    AND rh_TapeCopy.castorfile = rh_Castorfile.id
    AND rh_Stream2TapeCopy.child = rh_TapeCopy.id
    AND rh_Stream2TapeCopy.parent = streamId
    AND rh_TapeCopy.status = tapeCopyStatus
    AND ROWNUM < 2
  ORDER by rh_FileSystem.weight DESC;
 UPDATE rh_TapeCopy SET status = newTapeCopyStatus WHERE id = tapeCopyId;
 UPDATE rh_Stream SET status = newStreamStatus WHERE id = streamId;
END;

