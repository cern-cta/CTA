/******************************************************************************
 *                castor/stager/IStagerSvc.hpp
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
 * @(#)$RCSfile: IStagerSvc.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2004/10/26 16:47:48 $ $Author: sponcec3 $
 *
 * This class provides methods usefull to the stager to
 * deal with database queries
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_ISTAGERSVC_HPP
#define STAGER_ISTAGERSVC_HPP 1

// Include Files
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"
#include <vector>

namespace castor {

  namespace stager {

    // Forward declaration
    class Tape;
    class Stream;
    class Segment;
    class TapeCopy;
    class DiskCopy;
    class FileSystem;
    class SubRequest;
    class TapeCopyForMigration;
    class DiskCopyForRecall;

    /**
     * This class provides methods usefull to the stager to
     * deal with database queries
     */
    class IStagerSvc : public virtual IService {

    public:

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
       * @param searchItem the tape information used for the search
       * @return >0 : number of waiting requests found. 0 : no requests found
       * @exception in case of error
       */
      virtual int anySegmentsForTape(castor::stager::Tape* searchItem)
        throw (castor::exception::Exception) = 0;

      /*
       * Get the array of segments currently waiting for a given tape.
       * Search the catalog for all eligible segments that
       * are waiting for the given tape VID to become ready.
       * The matching Segments entries must have the status
       * SEGMENT_UNPROCESSED.
       * Before return this function atomically updates the
       * matching catalog entries Tape status to TAPE_MOUNTED.
       * @param searchItem the tape information used for the search
       * @return vector with all waiting segments
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Segment*>
      segmentsForTape(castor::stager::Tape* searchItem)
        throw (castor::exception::Exception) = 0;

      /**
       * Finds the best filesystem for a given segment.
       * Looks for a filesystem where to write the segment content
       * once it will be retrieved from tape. This file system
       * must have enough space and the one with the biggest weight
       * will be taken (if any).
       * If a filesystem is chosen, then the link with the only
       * DiskCopy available for the CastorFile the segment belongs
       * to is created.
       * @param segment the segment we are dealing with
       * @return The only DiskCopy available for the CastorFile the
       * segment belongs too. A DiskCopyForRecall is actually returned
       * that contains additionnal information. The Castorfile associated
       * is also created
       * @exception in case of error
       */
      virtual castor::stager::DiskCopyForRecall* bestFileSystemForSegment
      (castor::stager::Segment *segment)
        throw (castor::exception::Exception) = 0;

      /**
       * Check if there still is any tapeCopy waiting for a stream.
       * The matching TapeCopies entry must have the status
       * TAPECOPY_WAITINSTREAM. If there is at least one, the Stream
       * status is updated to STREAM_WAITMOUNT before return. This
       * indicates that the stream will continue mounting the tape.
       * @param searchItem the stream information used for the search
       * @return whether a Tapecopy is waiting
       * @exception in case of error
       */
      virtual bool anyTapeCopyForStream(castor::stager::Stream* searchItem)
        throw (castor::exception::Exception) = 0;

      /*
       * Get the best TapeCopy currently waiting for a given Stream.
       * Search the catalog for the best eligible TapeCopy that
       * is waiting for the given Stream to become ready.
       * The matching TapeCopy entry must have the status
       * TAPECOPY_WAITINSTREAMS and will be changed to status SELECTED.
       * Before return this function atomically updates the
       * matching catalog entry Stream status to STREAM_RUNNING.
       * @param searchItem the Stream information used for the search
       * @return the best waiting TapeCopy (or 0 if none).
       * @exception in case of error
       */
      virtual castor::stager::TapeCopyForMigration*
      bestTapeCopyForStream(castor::stager::Stream* searchItem)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates the database when a file recalled is over.
       * This includes updating the DiskCopy status to DISKCOPY_STAGED
       * (note that it is garanted that there is a single diskcopy in
       * status DISKCOPY_WAITTAPERECALL for this TapeCopy).
       * It also includes updating the status of the corresponding
       * SubRequest to SUBREQUEST_RESTART and updating the status of
       * the SubRequests waiting on this recall to SUBREQUEST_RESTART
       * @param tapeCopy the TapeCopy that was just recalled
       * @exception in case of error
       */
      virtual void fileRecalled(castor::stager::TapeCopy* tapeCopy)
        throw (castor::exception::Exception) = 0;

      /**
       * Get an array of the tapes to be processed.
       * This method searches the request catalog for all tapes that are
       * in TAPE_PENDING status. It atomically updates the status to
       * TAPE_WAITVDQM and returns the corresponding Tape objects.
       * This means that a subsequent call to this method will not return
       * the same entries. Objects may be present n times in the returned
       * vector of tapes. The rtcpclientd will notice multiple identical
       * requests and only submit one of them to VDQM.
       * @return vector of tapes to be processed
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Tape*> tapesToDo()
        throw (castor::exception::Exception) = 0;

      /**
       * Get an array of the streams to be processed.
       * This method searches the stager catalog for all streams that are
       * in STREAM_PENDING status. It atomically updates the status to
       * STREAM_WAITDRIVE and returns the corresponding STREAM objects.
       * This means that a subsequent call to this method will not return
       * the same entries.
       * @return vector of streams to be processed
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Stream*> streamsToDo()
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a tape from the database based on its vid,
       * side and tpmode. If no tape is found, creates one.
       * Note that this method creates a lock on the row of the
       * given tape and does not release it. It is the
       * responsability of the caller to commit the transaction.
       * @param vid the vid of the tape
       * @param side the side of the tape
       * @param tpmode the tpmode of the tape
       * @return the tape. the return value can never be 0
       * @exception Exception in case of error (no tape found,
       * several tapes found, DB problem, etc...)
       */
      virtual castor::stager::Tape* selectTape(const std::string vid,
                                               const int side,
                                               const int tpmode)
        throw (castor::exception::Exception) = 0;

      /**
       * Selects the next SubRequest the stager should deal with.
       * Selects a SubRequest in START, RESTART or RETRY status
       * and move its status to SUBREQUEST_WAITSCHED to avoid
       * double processing.
       * @return the SubRequest to process
       * @exception Exception in case of error
       */
      virtual castor::stager::SubRequest* subRequestToDo()
        throw (castor::exception::Exception) = 0;

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
       * @param subreq the SubRequest to consider
       * @return whether to schedule it
       * @exception Exception in case of error
       */
      virtual bool isDiskCopyToSchedule
      (castor::stager::SubRequest* subreq)
        throw (castor::exception::Exception) = 0;

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
       * @param subreq  the SubRequest to consider
       * @param fileSystem the selected FileSystem
       * @return the diskCopy to work on. The DiskCopy and
       * SubRequest status gives the result of the decision
       * that was taken.
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskCopy* scheduleDiskCopy
      (castor::stager::SubRequest* subreq,
       castor::stager::FileSystem* fileSystem)
        throw (castor::exception::Exception) = 0;

    }; // end of class IStagerSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ISTAGERSVC_HPP
