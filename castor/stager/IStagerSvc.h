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
struct Cstager_Segment_t;

/**
 * Dynamic cast from IService
 */
struct Cstager_IStagerSvc_t*
Cstager_IStagerSvc_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int Cstager_IStagerSvc_delete(struct Cstager_IStagerSvc_t* svcs);

/*
 * Get the array of segments currently waiting for a given tape.
 * Search the catalog for all eligible tape files (segments) that
 * are waiting for the given tape VID to become ready.
 * The matching Segment entries must have one of the
 * following status values: SEGMENT_UNPROCESSED,
 * SEGMENT_WAITFSEQ, SEGMENT_WAITPATH or SEGMENT_WAITCOPY.
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
int Cstager_IStagerSvc_segmentsForTape (struct Cstager_IStagerSvc_t* stgSvc,
                                        struct Cstager_Tape_t* searchItem,
                                        struct Cstager_Segment_t*** segmentArray,
                                        int* nbItems);

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
 * @return >0 : number of waiting requests found. 0 : no requests found
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_anySegmentsForTape(struct Cstager_IStagerSvc_t* stgSvc,
                                          struct Cstager_Tape_t* searchItem);

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
                                  const std::string vid,
                                  const int side,
                                  const int tpmode);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param stgSvc the IStagerSvc used
 * @return the error message
 */
const char* Cstager_IStagerSvc_errorMsg(struct Cstager_IStagerSvc_t* stgSvc);

#endif // CASTOR_ISTAGERSVC_H
