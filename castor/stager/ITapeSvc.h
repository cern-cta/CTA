/******************************************************************************
 *                      ITapeSvc.h
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
 * @(#)$RCSfile: ITapeSvc.h,v $ $Revision: 1.8 $ $Release$ $Date: 2007/04/02 15:26:08 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ITAPESVC_H
#define CASTOR_ITAPESVC_H 1

#include "castor/Constants.h"
#include "castor/stager/DiskCopyStatusCodes.h"

/// Forward declarations for the C world
struct C_IService_t;
struct C_IClient_t;
struct C_IObject_t;
struct C_IAddress_t;
struct Cstager_ITapeSvc_t;
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
struct Cstager_ITapeSvc_t*
Cstager_ITapeSvc_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int Cstager_ITapeSvc_delete(struct Cstager_ITapeSvc_t* svcs);

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
 * @param tpSvc the ITapeSvc used
 * @param searchItem the tape information used for the search
 * @return >0 : at least one tapeCopy found. 0 : no Tapecopy found
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_anySegmentsForTape(struct Cstager_ITapeSvc_t* tpSvc,
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
 * @param tpSvc the ITapeSvc used
 * @param searchItem the tape information used for the search
 * @param segmentArray array with pointers to all waiting segments
 * @param nbItems number of items in the array
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_segmentsForTape
(struct Cstager_ITapeSvc_t* tpSvc,
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
 * @param tpSvc the ITapeSvc used
 * @param segment the segment we are dealing with
 * @param diskCopy the only DiskCopy available for the CastorFile the
 * segment belongs too. A DiskCopyForRecall is actually returned
 * that contains additionnal information. The Castorfile associated
 * is also created
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_bestFileSystemForSegment
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_Segment_t* segment,
 struct Cstager_DiskCopyForRecall_t** diskCopy);

/**
 * Check if there still is any tapeCopy waiting for a stream.
 * The matching TapeCopies entry must have the status
 * TAPECOPY_WAITINSTREAM. If there is at least one, the Stream
 * status is updated to STREAM_WAITMOUNT before return. This
 * indicates that the stream will continue mounting the tape.
 * @param tpSvc the ITapeSvc used
 * @param searchItem the stream information used for the search
 * @return >0 : at least one TapeCopy is waiting. 0 : no TapeCopy is waiting
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_anyTapeCopyForStream
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_Stream_t* searchItem);

/*
 * Get the best TapeCopy currently waiting for a given Stream.
 * Search the catalog for the best eligible TapeCopy that
 * is waiting for the given Stream to become ready.
 * The matching TapeCopy entry must have the status
 * TAPECOPY_WAITINSTREAMS and will be changed to status SELECTED.
 * Before return this function atomically updates the
 * matching catalog entry Stream status to STREAM_RUNNING.
 * @param tpSvc the ITapeSvc used
 * @param searchItem the Stream information used for the search
 * @param autocommit whether to commit the changes
 * @param tapecopy the best waiting tapecopy or 0 if none found
 * In this later case, the serrno is set to ENOENT
 * @param autocommit whether to commit the changes
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_bestTapeCopyForStream
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_Stream_t* searchItem,
 struct Cstager_TapeCopyForMigration_t** tapeCopy);

/*
 * Gets the streams associated to the given TapePool
 * and link them to the pool. Takes a lock on the
 * returned streams in the database and does not
 * commit.
 * @param tpSvc the ITapeSvc used
 * @param tapePool the tapePool to handle
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_streamsForTapePool
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_TapePool_t* tapePool);

/**
 * Updates the database when a file recall is successfully over.
 * This includes updating the DiskCopy status to DISKCOPY_STAGED
 * (note that it is guaranteed that there is a single
 * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
 * It also includes updating the status of the corresponding
 * SubRequest to SUBREQUEST_RESTART and updating the status of
 * the SubRequests waiting on this recall to SUBREQUEST_RESTART
 * @param tpSvc the ITapeSvc used
 * @param tapeCopy the TapeCopy that was just recalled
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_fileRecalled
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_TapeCopy_t* tapeCopy);

/**
 * Updates the database when a file recall failed.
 * This includes updating the DiskCopy status to DISKCOPY_FAILED
 * (note that it is garanted that there is a single
 * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
 * It also includes updating the status of the corresponding
 * SubRequest to SUBREQUEST_FAILED and updating the status of
 * the SubRequests waiting on this recall to SUBREQUEST_FAILED
 * @param tpSvc the ITapeSvc used
 * @param tapeCopy the TapeCopy that was just recalled
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_fileRecallFailed
(struct Cstager_ITapeSvc_t* tpSvc,
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
 * @param tpSvc the ITapeSvc used
 * @param tapeArray output array of pointers to tape objects
 * @param nbItems output number of items returned in the tapeArray
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_tapesToDo(struct Cstager_ITapeSvc_t* tpSvc,
                                 struct Cstager_Tape_t*** tapeArray,
                                 int *nbItems);

/**
 * Get an array of the streams to be processed.
 * This method searches the catalog for all streams that are
 * in STREAM_PENDING status. It atomically updates the status to
 * STREAM_WAITDRIVE and returns the corresponding Stream objects.
 * This means that a subsequent call to this method will not return
 * the same entries.
 * @param tpSvc the ITapeSvc used
 * @param streamArray output array of pointers to stream objects
 * @param nbItems output number of items returned in the tapeArray
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_streamsToDo(struct Cstager_ITapeSvc_t* tpSvc,
                                   struct Cstager_Stream_t*** streamArray,
                                   int *nbItems);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param tpSvc the ITapeSvc used
 * @return the error message
 */
const char* Cstager_ITapeSvc_errorMsg(struct Cstager_ITapeSvc_t* tpSvc);

/**
 * Retrieves the TapeCopies from the database that have
 * status TAPECOPY_CREATED or TAPECOPY_TOBEMIGRATED and
 * have a castorFile linked to the right SvcClass.
 * Changes their status to TAPECOPY_WAITINSTREAMS.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param tpSvc the ITapeSvc used
 * @param svcClass the SvcClass we select on
 * @param tapeCopyArray output array of pointers to tapeCopy objects
 * @param nbItems output number of items returned in the tapeCopyArray
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_selectTapeCopiesForMigration
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_SvcClass_t* svcClass,
 struct Cstager_TapeCopy_t*** tapeCopyArray,
 int *nbItems);

/**
 * resets a stream by either deleting it or setting
 * its status to STREAM_PENDING depending on whether
 * there are TapeCopies in status WAITINSTREAMS status.
 * Also deletes all links to TapeCopies for this stream
 * @param tpSvc the ITapeSvc used
 * @param stream the stream to reset
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_resetStream
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_Stream_t* stream);

/*
 * Get an array of segments that are in SEGMENT_FAILED
 * status. This method does not take any lock on the segments
 * and thus may return twice the same segments in two
 * different threads calling it at the same time
 * @param tpSvc the ITapeSvc used
 * @param segmentArray array with all failed segments
 * @param nbItems number of items in the array
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_failedSegments
(struct Cstager_ITapeSvc_t* tpSvc,
 struct Cstager_Segment_t*** segmentArray,
 int* nbItems);

/**
 * Checks, if the fileid is in a actual repack process.
 * This method is run by the migrator. It looks into the
 * Stager Catalog, if a StageRepackRequest object is assigned to
 * a subrequest for this  file. In this case it returns the 
 * volume name (repackvid field) of the request. 
 * @param tpSvc the ITapeSvc used
 * @param repackvid the found tape name or NULL
 * @param key the castorfile to check
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_ITapeSvc_errorMsg
 */
int Cstager_ITapeSvc_checkFileForRepack
(struct Cstager_ITapeSvc_t* tpSvc, 
 char** repackvid,
 const u_signed64 key);

/**
 * Sends a UDP message to the rmMasterDaemon to inform it
 * of the creation or deletion of a stream on a given
 * machine/filesystem. This method is always successful
 * although there is no guaranty that the UDP package is
 * sent and arrives.
 * @param tpSvc the ITapeSvc used
 * @param diskserver the diskserver where the stream resides
 * @param filesystem the filesystem where the stream resides
 * @param direction the stream direction. Value should be one
 * of O_RDONLY, O_WRONLY, O_RDWR. Any other value will
 * be ignored silently.
 * @param created whether the stream was created or deleted.
 * 0 means deletion, other values mean creation.
 */
void Cstager_ITapeSvc_sendStreamReport
(struct Cstager_ITapeSvc_t* tpSvc,
 char* diskServer,
 char* fileSystem,
 int direction,
 int created);

#endif // CASTOR_ITAPESVC_H

