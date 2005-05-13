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

/// Forward declarations for the C world
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
 * Check if there still are any segments waiting for a given tape.
 * Before a tape is physically mounted, the VidWorker process will
 * check if there still are Segments entries waiting for this tape.
 * If not, the tape request is cancelled. If there is at least one
 * matching entry, the matching catalog entries Tape status should be
 * updated to TAPE_WAITMOUNT before return.
 * TAPE_WAITMOUNT indicates that the tape request will continue
 * mounting the tape and the matching Segments entries should normally
 * wait for this tape to be mounted. This means that if the CASTOR
 * file has multiple tape copies, the tape requests for the other
 * copies should be cancelled unless there are outstanding requests
 * for other files that reside on that tape.
 * @param stgSvc the IStagerSvc used
 * @param searchItem the tape information used for the search
 * @return >0 : at least one tapeCopy found. 0 : no Tapecopy found
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_anySegmentsForTape(struct Cstager_IStagerSvc_t* stgSvc,
                                          struct Cstager_Tape_t* searchItem);

/*
 * Get the array of segments currently waiting for a given tape.
 * Search the catalog for all eligible segments that
 * are waiting for the given tape VID to become ready.
 * The matching Segments entries must have the status
 * SEGMENT_UNPROCESSED.
 * Before return this function atomically updates the
 * matching catalog entries Tape status to TAPE_MOUNTED
 * and the segments to SEGMENT_SELECTED.
 * @param stgSvc the IStagerSvc used
 * @param searchItem the tape information used for the search
 * @param segmentArray array with pointers to all waiting segments
 * @param nbItems number of items in the array
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_segmentsForTape
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_Tape_t* searchItem,
 struct Cstager_Segment_t*** segmentArray,
 int* nbItems);

/**
 * Finds the best filesystem for a given segment.
 * Looks for a filesystem where to write the segment content
 * once it will be retrieved from tape. This file system
 * must have enough space and the one with the biggest weight
 * will be taken (if any).
 * If a filesystem is chosen, then the link with the only
 * DiskCopy available for the CastorFile the segment belongs
 * to is created.
 * @param stgSvc the IStagerSvc used
 * @param segment the segment we are dealing with
 * @param diskCopy the only DiskCopy available for the CastorFile the
 * segment belongs too. A DiskCopyForRecall is actually returned
 * that contains additionnal information. The Castorfile associated
 * is also created
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_bestFileSystemForSegment
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_Segment_t* segment,
 struct Cstager_DiskCopyForRecall_t** diskCopy);

/**
 * Check if there still is any tapeCopy waiting for a stream.
 * The matching TapeCopies entry must have the status
 * TAPECOPY_WAITINSTREAM. If there is at least one, the Stream
 * status is updated to STREAM_WAITMOUNT before return. This
 * indicates that the stream will continue mounting the tape.
 * @param stgSvc the IStagerSvc used
 * @param searchItem the stream information used for the search
 * @return >0 : at least one TapeCopy is waiting. 0 : no TapeCopy is waiting
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_anyTapeCopyForStream
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_Stream_t* searchItem);

/*
 * Get the best TapeCopy currently waiting for a given Stream.
 * Search the catalog for the best eligible TapeCopy that
 * is waiting for the given Stream to become ready.
 * The matching TapeCopy entry must have the status
 * TAPECOPY_WAITINSTREAMS and will be changed to status SELECTED.
 * Before return this function atomically updates the
 * matching catalog entry Stream status to STREAM_RUNNING.
 * @param stgSvc the IStagerSvc used
 * @param searchItem the Stream information used for the search
 * @param autocommit whether to commit the changes
 * @param tapecopy the best waiting tapecopy or 0 if none found
 * In this later case, the serrno is set to ENOENT
 * @param autocommit whether to commit the changes
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_bestTapeCopyForStream
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_Stream_t* searchItem,
 struct Cstager_TapeCopyForMigration_t** tapeCopy);

/*
 * Gets the streams associated to the given TapePool
 * and link them to the pool. Takes a lock on the
 * returned streams in the database and does not
 * commit.
 * @param stgSvc the IStagerSvc used
 * @param tapePool the tapePool to handle
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_streamsForTapePool
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_TapePool_t* tapePool);

/**
 * Updates the database when a file recall is successfully over.
 * This includes updating the DiskCopy status to DISKCOPY_STAGED
 * (note that it is guaranteed that there is a single
 * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
 * It also includes updating the status of the corresponding
 * SubRequest to SUBREQUEST_RESTART and updating the status of
 * the SubRequests waiting on this recall to SUBREQUEST_RESTART
 * @param stgSvc the IStagerSvc used
 * @param tapeCopy the TapeCopy that was just recalled
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_fileRecalled
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_TapeCopy_t* tapeCopy);

/**
 * Updates the database when a file recall failed.
 * This includes updating the DiskCopy status to DISKCOPY_FAILED
 * (note that it is garanted that there is a single
 * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
 * It also includes updating the status of the corresponding
 * SubRequest to SUBREQUEST_FAILED and updating the status of
 * the SubRequests waiting on this recall to SUBREQUEST_FAILED
 * @param stgSvc the IStagerSvc used
 * @param tapeCopy the TapeCopy that was just recalled
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_fileRecallFailed
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_TapeCopy_t* tapeCopy);

/**
 * Get an array of the tapes to be processed.
 * This method searches the request catalog for all tapes that are
 * in TAPE_PENDING status. It atomically updates the status to
 * TAPE_WAITVDQM and returns the corresponding Tape objects.
 * This means that a subsequent call to this method will not return
 * the same entries. Objects may be present n times in the returned
 * vector of tapes. The rtcpclientd will notice multiple identical
 * requests and only submit one of them to VDQM.
 * @param stgSvc the IStagerSvc used
 * @param tapeArray output array of pointers to tape objects
 * @param nbItems output number of items returned in the tapeArray
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_tapesToDo(struct Cstager_IStagerSvc_t* stgSvc,
                                 struct Cstager_Tape_t*** tapeArray,
                                 int *nbItems);

/**
 * Get an array of the streams to be processed.
 * This method searches the catalog for all streams that are
 * in STREAM_PENDING status. It atomically updates the status to
 * STREAM_WAITDRIVE and returns the corresponding Stream objects.
 * This means that a subsequent call to this method will not return
 * the same entries.
 * @param stgSvc the IStagerSvc used
 * @param streamArray output array of pointers to stream objects
 * @param nbItems output number of items returned in the tapeArray
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_streamsToDo(struct Cstager_IStagerSvc_t* stgSvc,
                                   struct Cstager_Stream_t*** streamArray,
                                   int *nbItems);

/**
 * Retrieves a tape from the database based on its vid,
 * side and tpmode. If no tape is found, creates one.
 * Note that this method creates a lock on the row of the
 * given tape and does not release it. It is the
 * responsability of the caller to commit the transaction.
 * @param stgSvc the IStagerSvc used
 * @param tape the tape object returned
 * @param vid the vid of the tape
 * @param side the side of the tape
 * @param tpmode the tpmode of the tape
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectTape(struct Cstager_IStagerSvc_t* stgSvc,
                                  struct Cstager_Tape_t** tape,
                                  const char* vid,
                                  const int side,
                                  const int tpmode);

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
 *   - if no diskCopy is found, return true (scheduling
 * for tape recall) and sources stays empty.
 *   - if some diskcopies are found but all in WAIT*
 * status, return false (no schedule) and link the SubRequest
 * to the one we're waiting on + set its status to
 * SUBREQUEST_WAITSUBREQ. Sources stays empty.
 *   - if some diskcopies are found in STAGED/STAGEOUT
 * status, return true and list them in sources.
 * @param stgSvc the IStagerSvc used
 * @param subreq the SubRequest to consider
 * @param sources this is a list of DiskCopies that
 * can be used by the subrequest.
 * Note that the DiskCopies returned in sources must be
 * deallocated by the caller.
 * @param sourcesNb the length of the sources list
 * @return whether to schedule it. 1 for YES, 0 for NO,
 * -1 for an error. In the later case serrno is set to
 * the corresponding error code and a detailed error
 * message can be retrieved by calling Cstager_IStagerSvc_errorMsg
 * @exception Exception in case of error
 */
int Cstager_IStagerSvc_isSubRequestToSchedule
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq,
 struct Cstager_DiskCopyForRecall_t*** sources,
 unsigned int* sourcesNb);

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
 * @param stgSvc the IStagerSvc used
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
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_getUpdateStart
(struct Cstager_IStagerSvc_t* stgSvc,
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
 * @param stgSvc the IStagerSvc used
 * @param subreq  the SubRequest to consider
 * @param fileSystem the selected FileSystem
 * @param diskCopy the DiskCopy to use for the data access
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_putStart
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq,
 struct Cstager_FileSystem_t* fileSystem,
 struct Cstager_DiskCopy_t** diskCopy);

/**
 * Handles the start of a PutDone job.
 * Actually only returns the DiskCopy associated to the SubRequest
 * Note that deallocation of the DiskCopy is the
 * responsability of the caller.
 * @param stgSvc the IStagerSvc used
 * @param subreqId the if of the SubRequest to consider
 * @param diskCopy the DiskCopy to use for the data access
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_putDoneStart
(struct Cstager_IStagerSvc_t* stgSvc,
 u_signed64 subreqId,
 struct Cstager_DiskCopy_t** diskCopy);

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
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectCastorFile
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_CastorFile_t** castorFile,
 const u_signed64 fileId, const char* nsHost,
 u_signed64 svcClass, u_signed64 fileClass, u_signed64 fileSize);

/**
 * Retrieves a FileSystem from the database based on its
 * mount point and diskServer name.
 * Caller is in charge of the deletion of the allocated
 * objects, including the DiskServer Object
 * @param stgSvc the IStagerSvc used
 * @param mountPoint the mountPoint of the FileSystem
 * @param diskServer the name of the disk server hosting this file system
 * @param fileSystem the FileSystem linked to its DiskServer, or 0 if none found
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectFileSystem
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_FileSystem_t** fileSystem,
 const char* mountPoint,
 const char* diskServer);

/**
 * Retrieves a DiskPool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param stgSvc the IStagerSvc used
 * @param diskPool the DiskPool object returned, or 0 if none found
 * @param name the name of the DiskPool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectDiskPool
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_DiskPool_t** diskPool,
 const char* name);

/**
 * Retrieves a TapePool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param stgSvc the IStagerSvc used
 * @param tapePool the TapePool object returned, or 0 if none found
 * @param name the name of the TapePool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectTapePool
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_TapePool_t** tapePool,
 const char* name);

/**
 * Retrieves a DiskServer from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param stgSvc the IStagerSvc used
 * @param diskServer the DiskServer object returned, or 0 if none found
 * @param name the name of the DiskServer
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectDiskServer
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_DiskServer_t** diskServer,
 const char* name);

/**
 * Retrieves the TapeCopies from the database that have
 * status TAPECOPY_CREATED or TAPECOPY_TOBEMIGRATED and
 * have a castorFile linked to the right SvcClass.
 * Changes their status to TAPECOPY_WAITINSTREAMS.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param stgSvc the IStagerSvc used
 * @param svcClass the SvcClass we select on
 * @param tapeCopyArray output array of pointers to tapeCopy objects
 * @param nbItems output number of items returned in the tapeCopyArray
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectTapeCopiesForMigration
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SvcClass_t* svcClass,
 struct Cstager_TapeCopy_t*** tapeCopyArray,
 int *nbItems);

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
 * Updates database after successful completion of a
 * disk to disk copy. This includes setting the DiskCopy
 * status to DISKCOPY_STAGED and setting the SubRequest
 * status to SUBREQUEST_READY.
 * Changes are commited
 * @param stgSvc the IStagerSvc used
 * @param diskcopyId the id of the new DiskCopy
 * @param status the status of the new DiskCopy
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_disk2DiskCopyDone
(struct Cstager_IStagerSvc_t* stgSvc,
 u_signed64 diskCopyId,
 enum Cstager_DiskCopyStatusCodes_t status);

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
 * @param stgSvc the IStagerSvc used
 * @param subreq The SubRequest handling the file to prepare
 * @param fileSize The actual size of the castor file
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_prepareForMigration
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq,
 u_signed64 fileSize);

/**
 * resets a stream by either deleting it or setting
 * its status to STREAM_PENDING depending on whether
 * there are TapeCopies in status WAITINSTREAMS status.
 * Also deletes all links to TapeCopies for this stream
 * @param stgSvc the IStagerSvc used
 * @param stream the stream to reset
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_resetStream
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_Stream_t* stream);

/**
 * Selects a machine and FileSystem for a given job.
 * Note that the caller is responsible for freeing
 * the returned strings : mountPoint and diskServer
 * @param stgSvc the IStagerSvc used
 * @param fileSystems the list of allowed filesystems
 * according to job requirements (given by id). This
 * is the fileSystems' mountPoint, the corresponding
 * machines are given by parameter machines.
 * A null array means that any filesystem is eligible
 * @param machines the machines on which the filesystems
 * in parameter fileSystems reside.
 * A null array means that any machine is eligible. in such
 * a case, fileSystems has to be null.
 * @param minFree the minimum free space needed on each
 * filesystem to be selected. This is filesystem dependent
 * if filesystems are given (due to possible reservations
 * of the scheduler).
 * If no filesystem are given, this array must have
 * exactely one item, used for all filesystems.
 * @param fileSystemsNb the length of the arrays
 * fileSystems, machines and minFree when they are not
 * null (and if filesystems are given for minFree)
 * @mountPoint the selected fileSystem's mountPoint
 * @diskServer the diskServer on which the selected
 * fileSystem resides.
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_bestFileSystemForJob
(struct Cstager_IStagerSvc_t* stgSvc,
 char** fileSystems, char** machines,
 u_signed64* minFree, unsigned int fileSystemsNb,
 char** mountPoint, char** diskServer);

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
 * Informs the stager the a Get or Update SubRequest
 * (without write) was finished successfully.
 * The SubRequest and potentially the corresponding
 * Request will thus be removed from the DataBase
 * @param stgSvc the IStagerSvc used
 * @param subReqId the id of the finished SubRequest
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_getUpdateDone
(struct Cstager_IStagerSvc_t* stgSvc,
 u_signed64 subReqId);

/**
 * Informs the stager the a Get or Update SubRequest
 * (without write) failed.
 * The SubRequest's status will thus be set to FAILED
 * @param stgSvc the IStagerSvc used
 * @param subReqId the id of the failing SubRequest
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_getUpdateFailed
(struct Cstager_IStagerSvc_t* stgSvc,
 u_signed64 subReqId);

/**
 * Informs the stager the a Put SubRequest failed.
 * The SubRequest's status will thus be set to FAILED
 * @param stgSvc the IStagerSvc used
 * @param subReqId the id of the failing SubRequest
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_putFailed
(struct Cstager_IStagerSvc_t* stgSvc,
 u_signed64 subReqId);

/*
 * Get an array of segments that are in SEGMENT_FAILED
 * status. This method does not take any lock on the segments
 * and thus may return twice the same segments in two
 * different threads calling it at the same time
 * @param stgSvc the IStagerSvc used
 * @param segmentArray array with all failed segments
 * @param nbItems number of items in the array
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_failedSegments
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_Segment_t*** segmentArray,
 int* nbItems);

/*
 * Implements a single file stageRm.
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
int Cstager_IStagerSvc_stageRm
(struct Cstager_IStagerSvc_t* stgSvc,
 const u_signed64 fileId,
 const char* nsHost);

#endif // CASTOR_ISTAGERSVC_H
