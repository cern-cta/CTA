/******************************************************************************
 *                castor/stager/ITapeSvc.hpp
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
 * @(#)$RCSfile: ITapeSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/07/07 14:58:42 $ $Author: itglp $
 *
 * This class provides methods related to tape handling
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_ITAPESVC_HPP
#define STAGER_ITAPESVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include <vector>
#include <string>
#include <list>

namespace castor {

  // Forward declaration
  class IObject;
  class IClient;
  class IAddress;

  namespace stager {

    // Forward declaration
    class Tape;
    class Stream;
    class Request;
    class Segment;
    class TapeCopy;
    class DiskCopy;
    class DiskPool;
    class SvcClass;
    class FileClass;
    class TapePool;
    class FileSystem;
    class DiskServer;
    class SubRequest;
    class CastorFile;
    class GCLocalFile;
    class TapeCopyForMigration;
    class DiskCopyForRecall;

    /**
     * This class provides methods related to tape handling
     */
    class ITapeSvc : public virtual ICommonSvc {

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
       * matching catalog entries Tape status to TAPE_MOUNTED
       * and the segments to SEGMENT_SELECTED.
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
       * The changes in the database are automatically commited
       * @param searchItem the Stream information used for the search
       * @return the best waiting TapeCopy (or 0 if none).
       * @exception in case of error
       */
      virtual castor::stager::TapeCopyForMigration*
      bestTapeCopyForStream(castor::stager::Stream* searchItem)
        throw (castor::exception::Exception) = 0;

      /*
       * Gets the streams associated to the given TapePool
       * and link them to the pool. Takes a lock on the
       * returned streams in the database and does not
       * commit.
       * @param tapePool the tapePool to handle
       * @exception in case of error
       */
      virtual void streamsForTapePool
      (castor::stager::TapePool* tapePool)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates the database when a file recall is successfully over.
       * This includes updating the DiskCopy status to DISKCOPY_STAGED
       * (note that it is guaranteed that there is a single
       * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
       * It also includes updating the status of the corresponding
       * SubRequest to SUBREQUEST_RESTART and updating the status of
       * the SubRequests waiting on this recall to SUBREQUEST_RESTART
       * @param tapeCopy the TapeCopy that was just recalled
       * @exception in case of error
       */
      virtual void fileRecalled(castor::stager::TapeCopy* tapeCopy)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates the database when a file recall failed.
       * This includes updating the DiskCopy status to DISKCOPY_FAILED
       * (note that it is garanted that there is a single
       * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
       * It also includes updating the status of the corresponding
       * SubRequest to SUBREQUEST_FAILED and updating the status of
       * the SubRequests waiting on this recall to SUBREQUEST_FAILED
       * @param tapeCopy the TapeCopy that was just recalled
       * @exception in case of error
       */
      virtual void fileRecallFailed(castor::stager::TapeCopy* tapeCopy)
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
       * Retrieves the TapeCopies from the database that have
       * status TAPECOPY_CREATED or TAPECOPY_TOBEMIGRATED and
       * have a castorFile linked to the right SvcClass.
       * Changes their status to TAPECOPY_WAITINSTREAMS.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param svcClass the SvcClass we select on
       * @return a vector of matching TapeCopies
       * @exception Exception in case of error
       */
      virtual std::vector<castor::stager::TapeCopy*>
      selectTapeCopiesForMigration
      (castor::stager::SvcClass *svcClass)
        throw (castor::exception::Exception) = 0;

      /**
       * resets a stream by either deleting it or setting
       * its status to STREAM_PENDING depending on whether
       * there are TapeCopies in status WAITINSTREAMS status.
       * Also deletes all links to TapeCopies for this stream
       * @param stream the stream to reset
       * @exception Exception throws an Exception in case of error
       */
      virtual void resetStream(castor::stager::Stream* stream)
        throw (castor::exception::Exception) = 0;

      /*
       * Get an array of segments that are in SEGMENT_FAILED
       * status. This method does not take any lock on the segments
       * and thus may return twice the same segments in two
       * different threads calling it at the same time
       * @return vector with all failed segments
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Segment*>
      failedSegments()
        throw (castor::exception::Exception) = 0;

    }; // end of class ITapeSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ITAPESVC_HPP
