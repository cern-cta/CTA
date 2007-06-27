/******************************************************************************
 *                      IStagerSvc.h
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ISTAGERSVC_H
#define CASTOR_ISTAGERSVC_H 1

#include "castor/Constants.h"
#include "castor/stager/DiskCopyStatusCodes.h"

/* Forward declarations for the C world */
struct C_IService_t;
struct C_IClient_t;
struct C_IObject_t;
struct C_IAddress_t;
struct Cstager_IStagerSvc_t;
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
struct Cstager_IStagerSvc_t*
Cstager_IStagerSvc_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int Cstager_IStagerSvc_delete(struct Cstager_IStagerSvc_t* svcs);

/**
 * Selects the next SubRequest the stager should deal with.
 * Selects a SubRequest in START, RESTART or RETRY status
 * and move its status to SUBREQUEST_WAITSCHED to avoid
 * double processing.
 * The selection is restricted to SubRequest associated to
 * requests of a given set of types.
 * @param stgSvc the IStagerSvc used
 * @param types the list of accepted types for the request
 * associated to the returned subrequest
 * @param nbTypes the number of types in the list
 * @param subreq the SubRequest to process
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_subRequestToDo
(struct Cstager_IStagerSvc_t* stgSvc,
 enum C_ObjectsIds* types,
 unsigned int nbTypes,
 struct Cstager_SubRequest_t** subreq);

/**
 * Selects the next request the stager should deal with.
 * Selects a Request in START status and move its status
 * PROCESSED to avoid double processing.
 * The selection is restricted to Request of a given set
 * of types.
 * @param stgSvc the IStagerSvc used
 * @param types the list of accepted types for the request
 * @param nbTypes the number of types in the list
 * @param request the request to process
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_requestToDo
(struct Cstager_IStagerSvc_t* stgSvc,
 enum C_ObjectsIds* types,
 unsigned int nbTypes,
 struct Cstager_Request_t** request);

/**
 * Decides whether a SubRequest should be scheduled.
 * Looks at all diskCopies for the file a SubRequest
 * deals with and depending on them, decides whether
 * to schedule the SubRequest. In case it can be scheduled,
 * also returns a list of diskcopies available to the
 * subrequest.
 * The scheduling decision is taken this way :
 * 0: no scheduling, the SubRequest is set to WAIT and it
      is linked to the parent SubRequest, which we're waiting
      on (e.g. TapeRecall, Disk2DiskCopy,..)
 * 1: schedule + list of avail sources, a DiskCopy was
      found and the SubRequest needs to be rescheduled.
 * 2: schedule + no sources, a disk2disk copy is necessary
 * 3: no schedule, no DiskCopy anywhere found, we need a
      Tape recall.
 * 4: no schedule, no DiskCopy anywhere found, there is no
      space left on the diskpool
 * @param subreq the SubRequest to consider
 * @param sources this is a list of DiskCopies that
 * can be used by the subrequest.
 * Note that the DiskCopies returned in sources must be
 * deallocated by the caller.
 * @return 0,1,2,3,4
 * @exception Exception in case of error
*/
int Cstager_IStagerSvc_isSubRequestToSchedule
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq,
 struct Cstager_DiskCopyForRecall_t*** sources,
 unsigned int* sourcesNb);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param stgSvc the IStagerSvc used
 * @return the error message
 */
const char* Cstager_IStagerSvc_errorMsg(struct Cstager_IStagerSvc_t* stgSvc);

/**
 * Retrieves a SvcClass from the database based on its name.
 * Caller is in charge of the deletion of the allocated object
 * @param stgSvc the IStagerSvc used
 * @param svcClass the SvcClass object returned, or 0 if none found
 * @param name the name of the SvcClass
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectSvcClass(struct Cstager_IStagerSvc_t* stgSvc,
                                      struct Cstager_SvcClass_t** svcClass,
                                      const char* name);

/**
 * Retrieves a FileClass from the database based on its name.
 * Caller is in charge of the deletion of the allocated object
 * @param stgSvc the IStagerSvc used
 * @param fileClass the FileClass object returned, or 0 if none found
 * @param name the name of the FileClass
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectFileClass(struct Cstager_IStagerSvc_t* stgSvc,
                                       struct Cstager_FileClass_t** fileClass,
                                       const char* name);

/**
 * Retrieves a CastorFile from the database based on its fileId.
 * Caller is in charge of the deletion of the allocated object
 * @param stgSvc the IStagerSvc used
 * @param castorFile the CastorFile object returned, or 0 if none found
 * @param fileId the fileId of the CastorFile
 * @param nsHost the name server to use
 * @param svcClass the service class of the castor file.
 * Used only in case of creation of a new castor file.
 * @param fileSize the size fo the castor file.
 * Used only in case of creation of a new castor file.
 * @param fileName the name of the castor file at the time
 * if this call. This will go to the DB as lastKnownFileName.
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectCastorFile
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_CastorFile_t** castorFile,
 const u_signed64 fileId, const char* nsHost,
 u_signed64 svcClass, u_signed64 fileClass,
 u_signed64 fileSize, const char* fileName);

/**
 * Updates a SubRequest status in the DB, including
 * the answered flag that is set to 1 and tells
 * whether the request to which it belongs still
 * has some other SubRequests that were not processed.
 * By not processed we mean that their "answered" flag
 * is not set AND their status is neither READY neither
 * FINISHED nor one of the FAILED* status.
 * The two operations are executed atomically.
 * The update is commited before returning.
 * This method should only be called when the calling
 * process is answering to the client. In other cases,
 * the updateRep method should be used.
 * @param stgSvc the IStagerSvc used
 * @param subreq the SubRequest to update
 * @param result 1 is there are still SubRequests in
 * SUBREQUEST_START status within the same request, 0 if
 * there are none
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_updateAndCheckSubRequest
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq,
 int* result);

/**
 * Recreates a castorFile.
 * Depending on the context, this method cleans up the
 * database when a castor file is recreated or gets
 * the unique DiskCopy of a castor file.
 * When called in the context of a Put inside a
 * PrepareToPut, the method returns the unique DiskCopy
 * associated to the castorFile. This DiskCopy can be
 * either in WAITFS, WAITFS_SCHEDULING or STAGEOUT
 * status and is linked to the SubRequest.
 * In all others cases, the method first
 * checks whether the recreation is possible.
 * A recreation is considered to be possible if
 * no TapeCopy of the given file is in TAPECOPY_SELECTED
 * status and no DiskCopy of the file is in either
 * WAITFS, WAITFS_SCHEDULING, WAITTAPERECALL or
 * WAITDISK2DISKCOPY status. When recreation is not
 * possible, a null pointer is returned.
 * Else, all DiskCopies for the given file are marked
 * INVALID (that is those not in DISKCOPY_FAILED and
 * DISKCOPY_DELETED status) and all TapeCopies are
 * deleted. A new DiskCopy is then created in
 * DISKCOPY_WAITFS status, linked to the given
 * SubRequest returned.
 * Note that everything is commited and that the caller
 * is responsible for the deletion of the returned
 * DiskCopy (if any)
 * @param stgSvc the IStagerSvc used
 * @param castorFile the file to recreate
 * @param subreq the SubRequest recreating the file
 * @param DiskCopy the new DiskCopy in DISKCOPY_WAITFS status
 * or null if recreation is not possible
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_recreateCastorFile
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_CastorFile_t* castorFile,
 struct Cstager_SubRequest_t* subreq,
 struct Cstager_DiskCopyForRecall_t** diskCopy);

/**
 * Updates a filesystem state (e.g : weight,
 * fsdeviation) to take into account the opening of
 * a new job.
 * @param stgSvc the IStagerSvc used
 * @param fileSystem the file system mount point
 * @param diskServer the name of the diskserver
 * where the filesystem resides
 * @param fileSize the (supposed) size of the file
 * to be written by the job
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_updateFileSystemForJob
(struct Cstager_IStagerSvc_t* stgSvc,
 char* fileSystem,
 char* diskServer,
 u_signed64 fileSize);

/**
 * Archives a SubRequest
 * The SubRequest and potentially the corresponding
 * Request will thus be removed from the DataBase
 * @param stgSvc the IStagerSvc used
 * @param subReqId the id of the SubRequest to archive
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_archiveSubReq
(struct Cstager_IStagerSvc_t* stgSvc,
 u_signed64 subReqId);

/*
 * Implements a single file stageRelease.
 * It throws a Busy exception in case the file is
 * used by any request or is waiting for migration.
 * Otherwise, it marks all the copies of the file
 * as candidate for the garbage collection.
 * @param stgSvc the IStagerSvc used
 * @param fileId the fileId of the CastorFile
 * @param nsHost the name server to use
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_stageRelease
(struct Cstager_IStagerSvc_t* stgSvc,
 const u_signed64 fileId,
 const char* nsHost);

/*
 * Implements a single file stageRm.
 * It throws a Busy exception in case the file is
 * used by any request or is waiting for migration.
 * Otherwise, it marks all the copies of the file
 * as candidate for the garbage collection.
 * @param stgSvc the IStagerSvc used
 * @param fileId the fileId of the CastorFile
 * @param nsHost the name server to use
 * @param svcClassId the svcClass where to perform
 * the rm operation; 0 for all svcClasses.
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_stageRm
(struct Cstager_IStagerSvc_t* stgSvc,
 const u_signed64 fileId,
 const char* nsHost,
 const u_signed64 svcClassId);

/*
 * Updates the gcWeight of all copies a a given file.
 * @param stgSvc the IStagerSvc used
 * @param fileId the fileId of the CastorFile
 * @param nsHost the name server to use
 * @param weight the new gcWeight for the file
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_setFileGCWeight
(struct Cstager_IStagerSvc_t* stgSvc,
 const u_signed64 fileId,
 const char* nsHost,
 const float weight);

/**
 * Creates a candidate for a recall. This includes TapeCopy with
 * its Segment(s), a DiskCopy and a SubRequest in WAITTAPERECALL.
 * @param subreq the subreq of the file to recall
 * @param euid the user id
 * @param egid the group id of the user
 * @return -1 in case of an error.
 */
int Cstager_IStagerSvc_createRecallCandidate
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq,
 const unsigned long euid,
 const unsigned long egid);

/**
 * Retrieves a DiskPool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param fsSvc the IFSSvc used
 * @param diskPool the DiskPool object returned, or 0 if none found
 * @param name the name of the DiskPool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectDiskPool
(struct Cstager_IStagerSvc_t* fsSvc,
 struct Cstager_DiskPool_t** diskPool,
 const char* name);

/**
 * Retrieves a TapePool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param fsSvc the IFSSvc used
 * @param tapePool the TapePool object returned, or 0 if none found
 * @param name the name of the TapePool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectTapePool
(struct Cstager_IStagerSvc_t* fsSvc,
 struct Cstager_TapePool_t** tapePool,
 const char* name);

#endif /* CASTOR_ISTAGERSVC_H */
