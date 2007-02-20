/******************************************************************************
 *                      IJobSvc.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: IJobSvc.h,v $ $Revision: 1.5 $ $Release$ $Date: 2007/02/20 17:55:56 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_IJOBSVC_H
#define CASTOR_IJOBSVC_H 1

#include "castor/Constants.h"
#include "castor/stager/DiskCopyStatusCodes.h"

/* Forward declarations for the C world */
struct C_IService_t;
struct C_IClient_t;
struct C_IObject_t;
struct C_IAddress_t;
struct Cstager_IJobSvc_t;
struct Cstager_Tape_t;
struct Cstager_Stream_t;
struct Cstager_Request_t;
struct Cstager_Segment_t;
struct Cstager_DiskCopy_t;
struct Cstager_DiskPool_t;
struct Cstager_SvcClass_t;
struct Cstager_TapePool_t;
struct Cstager_CastorFile_t;
struct Cstager_DiskServer_t;
struct Cstager_SubRequest_t;
struct Cstager_FileSystem_t;
struct Cstager_TapeCopyForMigration_t;
struct Cstager_DiskCopyForRecall_t;

/**
 * Dynamic cast from IService
 */
struct Cstager_IJobSvc_t*
Cstager_IJobSvc_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int Cstager_IJobSvc_delete(struct Cstager_IJobSvc_t* svcs);

/**
 * Selects the next request the job service should deal with.
 * Selects a Request in START status and move its status
 * PROCESSED to avoid double processing.
 * @param jobSvc the IJobSvc used
 * @param request the request to process
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_requestToDo
(struct Cstager_IJobSvc_t* jobSvc,
 struct Cstager_Request_t** request);

/**
 * Handles the start of a Get or Update job.
 * Schedules the corresponding SubRequest on a given
 * FileSystem and returns the DiskCopy to use for data
 * access.
 * Note that deallocation of the DiskCopy is the
 * responsability of the caller.
 * Depending on the available DiskCopies for the file
 * the SubRequest deals with, we have different cases :
 *  - no DiskCopy at all and file is not of size 0 :
 * a DiskCopy is created with status DISKCOPY_WAITTAPERECALL.
 * Null pointer is returned
 *  - no DiskCopy at all and file is of size 0 :
 * a DiskCopy is created with status DISKCOPY_WAIDISK2DISKCOPY.
 * This diskCopy is returned and the emptyFile content is
 * set to true.
 *  - one DiskCopy in DISKCOPY_WAITTAPERECALL, DISKCOPY_WAITFS
 * or DISKCOPY_WAITDISK2DISKCOPY status :
 * the SubRequest is linked to the one recalling and
 * put in SUBREQUEST_WAITSUBREQ status. Null pointer is
 * returned.
 *  - no valid (STAGE*, WAIT*) DiskCopy on the selected
 * FileSystem but some in status DISKCOPY_STAGEOUT or
 * DISKCOPY_STAGED on other FileSystems : a new DiskCopy
 * is created with status DISKCOPY_WAITDISK2DISKCOPY.
 * It is returned and the sources parameter is filed
 * with the DiskCopies found on the non selected FileSystems.
 *  - one DiskCopy on the selected FileSystem in
 * DISKCOPY_STAGEOUT or DISKCOPY_STAGED status :
 * the SubRequest is ready, the DiskCopy is returned and
 * sources remains empty.
 * @param jobSvc the IJobSvc used
 * @param subreq  the SubRequest to consider
 * @param fileSystem the selected FileSystem
 * @param sources this is a list of DiskCopies that
 * can be used as source of a Disk to Disk copy. This
 * list is never empty when diskCopy has status
 * DISKCOPY_DISK2DISKCOPY and always empty otherwise.
 * Note that the DiskCopies returned in sources must be
 * deallocated by the caller.
 * @param sourcesNb the length of the sources list
 * @param emptyFile 1 if the resulting diskCopy
 * deals with the recall of an empty file, 0 in all other cases
 * @param diskCopy the DiskCopy to use for the data access or
 * a null pointer if the data access will have to wait
 * and there is nothing more to be done. Even in case
 * of a non null pointer, the data access will have to
 * wait for a disk to disk copy if the returned DiskCopy
 * is in DISKCOPY_WAITDISKTODISKCOPY status. This
 * disk to disk copy is the responsability of the caller.
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_getUpdateStart
(struct Cstager_IJobSvc_t* jobSvc,
 struct Cstager_SubRequest_t* subreq,
 struct Cstager_FileSystem_t* fileSystem,
 struct Cstager_DiskCopyForRecall_t*** sources,
 unsigned int* sourcesNb,
 int *emptyFile,
 struct Cstager_DiskCopy_t** diskCopy);

/**
 * Handles the start of a Put job.
 * Links the DiskCopy associated to the SubRequest to
 * the given FileSystem and updates the DiskCopy status
 * to DISKCOPY_STAGEOUT.
 * Note that deallocation of the DiskCopy is the
 * responsability of the caller.
 * @param jobSvc the IJobSvc used
 * @param subreq  the SubRequest to consider
 * @param fileSystem the selected FileSystem
 * @param diskCopy the DiskCopy to use for the data access
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_putStart
(struct Cstager_IJobSvc_t* jobSvc,
 struct Cstager_SubRequest_t* subreq,
 struct Cstager_FileSystem_t* fileSystem,
 struct Cstager_DiskCopy_t** diskCopy);

/**
 * Handles the start of a PutDone job.
 * Actually only returns the DiskCopy associated to the SubRequest
 * Note that deallocation of the DiskCopy is the
 * responsability of the caller.
 * If a null  DiskCopy is returned then the PutDone
 * was put in waiting mode and the caller should stop
 * processing it
 * @param jobSvc the IJobSvc used
 * @param subreqId the if of the SubRequest to consider
 * @param diskCopy the DiskCopy to use for the data access or
 * 0 if the PutDone should wait
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_putDoneStart
(struct Cstager_IJobSvc_t* jobSvc,
 u_signed64 subreqId,
 struct Cstager_DiskCopy_t** diskCopy);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param jobSvc the IJobSvc used
 * @return the error message
 */
const char* Cstager_IJobSvc_errorMsg(struct Cstager_IJobSvc_t* jobSvc);

/**
 * Retrieves a SvcClass from the database based on its name.
 * Caller is in charge of the deletion of the allocated object
 * @param jobSvc the IJobSvc used
 * @param svcClass the SvcClass object returned, or 0 if none found
 * @param name the name of the SvcClass
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_selectSvcClass(struct Cstager_IJobSvc_t* jobSvc,
                                      struct Cstager_SvcClass_t** svcClass,
                                      const char* name);

/**
 * Retrieves a FileSystem from the database based on its
 * mount point and diskServer name.
 * Caller is in charge of the deletion of the allocated
 * objects, including the DiskServer Object
 * @param jobSvc the IJobSvc used
 * @param mountPoint the mountPoint of the FileSystem
 * @param diskServer the name of the disk server hosting this file system
 * @param fileSystem the FileSystem linked to its DiskServer, or 0 if none found
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_selectFileSystem
(struct Cstager_IJobSvc_t* jobSvc,
 struct Cstager_FileSystem_t** fileSystem,
 const char* mountPoint,
 const char* diskServer);

/**
 * Updates database after successful completion of a
 * disk to disk copy. This includes setting the DiskCopy
 * status to DISKCOPY_STAGED and setting the SubRequest
 * status to SUBREQUEST_READY.
 * Changes are commited
 * @param jobSvc the IJobSvc used
 * @param diskcopyId the id of the new DiskCopy
 * @param status the status of the new DiskCopy
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_disk2DiskCopyDone
(struct Cstager_IJobSvc_t* jobSvc,
 u_signed64 diskCopyId,
 enum Cstager_DiskCopyStatusCodes_t status);

/**
 * Prepares a file for migration, when needed.
 * This is called both when a stagePut is over and when a
 * putDone request is processed.
 * In the case of a stagePut that in part of a PrepareToPut,
 * it actually does not prepare the file for migration
 * but only updates its size in DB and name server.
 * Otherwise (stagePut with no prepare and putDone),
 * it also updates the filesystem free space and creates
 * the needed TapeCopies according to the FileClass of the
 * castorFile.
 * @param jobSvc the IJobSvc used
 * @param subreq The SubRequest handling the file to prepare
 * @param fileSize The actual size of the castor file 
 * @param timeStamp To know if the fileSize is still valid
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_prepareForMigration
(struct Cstager_IJobSvc_t* jobSvc,
 struct Cstager_SubRequest_t* subreq,
 u_signed64 fileSize, u_signed64 timeStamp);

/**
 * Informs the stager the a Get or Update SubRequest
 * (without write) was finished successfully.
 * The SubRequest and potentially the corresponding
 * Request will thus be removed from the DataBase
 * @param jobSvc the IJobSvc used
 * @param subReqId the id of the finished SubRequest
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_getUpdateDone
(struct Cstager_IJobSvc_t* jobSvc,
 u_signed64 subReqId);

/**
 * Informs the stager the a Get or Update SubRequest
 * (without write) failed.
 * The SubRequest's status will thus be set to FAILED
 * @param jobSvc the IJobSvc used
 * @param subReqId the id of the failing SubRequest
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_getUpdateFailed
(struct Cstager_IJobSvc_t* jobSvc,
 u_signed64 subReqId);

/**
 * Informs the stager the a Put or PutDone SubRequest failed.
 * The SubRequest's status will thus be set to FAILED
 * @param jobSvc the IJobSvc used
 * @param subReqId the id of the failing SubRequest
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IJobSvc_errorMsg
 */
int Cstager_IJobSvc_putFailed
(struct Cstager_IJobSvc_t* jobSvc,
 u_signed64 subReqId);


#endif // CASTOR_IJOBSVC_H
