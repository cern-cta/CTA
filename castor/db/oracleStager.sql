/* This file contains SQL code that is not generated automatically */
/* and is inserted at the end of the generated code           */

/* Indexes realted to CastorFiles */
drop index I_rh_DiskCopyCastorfileIndex;
drop index I_rh_TapeCopyCastorfileIndex;
drop index I_rh_SubRequestCastorfileIndex;
create index I_rh_DiskCopyCastorfileIndex on rh_DiskCopy (castorFile);
create index I_rh_TapeCopyCastorfileIndex on rh_TapeCopy (castorFile);
create index I_rh_SubRequestCastorfileIndex on rh_SubRequest (castorFile);

/* PL/SQL method implementing bestTapeCopyForStream */
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
    AND rh_Stream2TapeCopy.parent = rh_TapeCopy.id
    AND rh_Stream2TapeCopy.child = streamId
    AND rh_TapeCopy.status = tapeCopyStatus
    AND ROWNUM < 2
  ORDER by rh_FileSystem.weight DESC;
 UPDATE rh_TapeCopy SET status = newTapeCopyStatus WHERE id = tapeCopyId;
 UPDATE rh_Stream SET status = newStreamStatus WHERE id = streamId;
END;

/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR,
                                                     mountPoint OUT VARCHAR, path OUT VARCHAR,
                                                     diskCopyId OUT INTEGER) AS
 fileSystemId NUMBER;
BEGIN
SELECT rh_DiskServer.name, rh_FileSystem.mountPoint, rh_DiskCopy.path, rh_DiskCopy.id
 INTO diskServerName, mountPoint, path, diskCopyId
 FROM rh_DiskServer, rh_FileSystem, rh_DiskPool2SvcClass, rh_StageInRequest,
      rh_SubRequest, rh_TapeCopy, rh_Segment, rh_DiskCopy, rh_CastorFile
  WHERE rh_Segment.id = segmentId
    AND rh_Segment.copy = rh_TapeCopy.id
    AND rh_SubRequest.castorfile = rh_TapeCopy.castorfile
    AND rh_DiskCopy.castorfile = rh_TapeCopy.castorfile
    AND rh_Castorfile.id = rh_TapeCopy.castorfile
    AND rh_StageInRequest.id = rh_SubRequest.request
    AND rh_StageInRequest.svcclass = rh_DiskPool2SvcClass.child
    AND rh_FileSystem.diskpool = rh_DiskPool2SvcClass.parent
    AND rh_FileSystem.free > rh_CastorFile.fileSize
    AND ROWNUM < 2
  ORDER by rh_FileSystem.weight DESC;
UPDATE rh_DiskCopy SET fileSystem = fileSystemId WHERE id = diskCopyId;
END;
