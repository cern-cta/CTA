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

/// Forward declarations for the C world
struct C_IService_t;
struct Cstager_IStagerSvc_t;
struct Cstager_Tape_t;
struct Cstager_Stream_t;
struct Cstager_Segment_t;
struct Cstager_DiskCopy_t;
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
 * matching catalog entries Tape status to TAPE_MOUNTED.
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
 * @param tapecopy the best waiting tapecopy or 0 if none found
 * In this later case, the serrno is set to ENOENT
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_bestTapeCopyForStream
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_Stream_t* searchItem,
 struct Cstager_TapeCopyForMigration_t** tapeCopy);

/**
 * Updates the database when a file recalled is over.
 * This includes updating the DiskCopy status to DISKCOPY_STAGED
 * (note that it is garanted that there is a single diskcopy in
 * status DISKCOPY_WAITTAPERECALL for this TapeCopy).
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
 * @param stgSvc the IStagerSvc used
 * @param subreq the SubRequest to process 
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_subRequestToDo(struct Cstager_IStagerSvc_t* stgSvc,
                                      struct Cstager_SubRequest_t** subreq);

/**
 * Decides whether a SubRequest should be scheduled.
 * Looks at all diskCopies for the file a SubRequest
 * deals with and depending on them, decides whether
 * to schedule the SubRequest.
 * If no diskCopy is found or some are found and none
 * is in status DISKCOPY_WAITTAPERECALL, we schedule
 * (for tape recall in the first case, for access in
 * the second case).
 * Else (case where a diskCopy is in DISKCOPY_WAITTAPERECALL
 * status), we don't schedule, we link the SubRequest
 * to the recalling SubRequest and we set its status to
 * SUBREQUEST_WAITSUBREQ.
 * @param stgSvc the IStagerSvc used
 * @param subreq the SubRequest to consider
 * @return whether to schedule it. 1 for YES, 0 for NO,
 * -1 for an error. In the later case serrno is set to
 * the corresponding error code and a detailed error
 * message can be retrieved by calling Cstager_IStagerSvc_errorMsg
 * @exception Exception in case of error
 */
int Cstager_IStagerSvc_isSubRequestToSchedule
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq);

/**
 * Schedules a SubRequest on a given FileSystem.
 * Depending on the available DiskCopies for the file
 * the SubRequest deals with, decides what to do.
 * The different cases are :
 *  - no DiskCopy at all : a DiskCopy is created with
 * status DISKCOPY_WAITTAPERECALL.
 *  - one DiskCopy in DISKCOPY_WAITTAPERECALL status :
 * the SubRequest is linked to the one recalling and
 * put in SUBREQUEST_WAITSUBREQ status.
 *  - no DiskCopy on the selected FileSystem but some
 * in status DISKCOPY_STAGOUT or DISKCOPY_STAGED on other
 * FileSystems : a new DiskCopy is created with status
 * DISKCOPY_WAITDISK2DISKCOPY.
 *  - one DiskCopy on the selected FileSystem in
 * DISKCOPY_WAITDISKTODISKCOPY status : the SubRequest
 * has to wait until the end of the copy
 *  - one DiskCopy on the selected FileSystem in
 * DISKCOPY_STAGOUT or DISKCOPY_STAGED status :
 * the SubRequest is ready
 * @param stgSvc the IStagerSvc used
 * @param subreq  the SubRequest to consider
 * @param fileSystem the selected FileSystem
 * @param diskCopy the diskCopy to work on. The DiskCopy and
 * SubRequest status gives the result of the decision
 * that was taken.
 * @return whether to schedule it. 1 for YES, 0 for NO,
 * -1 for an error. In the later case serrno is set to
 * the corresponding error code and a detailed error
 * message can be retrieved by calling Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_scheduleSubRequest
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_SubRequest_t* subreq,
 struct Cstager_FileSystem_t* fileSystem,
 struct Cstager_DiskCopy_t** diskCopy);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param stgSvc the IStagerSvc used
 * @return the error message
 */
const char* Cstager_IStagerSvc_errorMsg(struct Cstager_IStagerSvc_t* stgSvc);

#endif // CASTOR_ISTAGERSVC_H
