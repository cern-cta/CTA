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
 * @(#)$RCSfile: IStagerSvc.hpp,v $ $Revision: 1.18 $ $Release$ $Date: 2004/11/23 15:50:25 $ $Author: sponcec3 $
 *
 * This class provides methods usefull to the stager to
 * deal with database queries
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_ISTAGERSVC_HPP
#define STAGER_ISTAGERSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"
#include <vector>
#include <string>
#include <list>

namespace castor {

  namespace stager {

    // Forward declaration
    class Tape;
    class Stream;
    class Segment;
    class TapeCopy;
    class DiskCopy;
    class SvcClass;
    class FileClass;
    class FileSystem;
    class SubRequest;
    class CastorFile;
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
       * The caller is also responsible for the deletion of the
       * allocated object
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
       * The selection is restricted to SubRequest associated to
       * requests of a given set of types.
       * @param types the list of accepted types for the request
       * associated to the returned subrequest
       * @return the SubRequest to process
       * @exception Exception in case of error
       */
      virtual castor::stager::SubRequest* subRequestToDo
      (std::vector<ObjectsIds> &types)
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
      virtual bool isSubRequestToSchedule
      (castor::stager::SubRequest* subreq)
        throw (castor::exception::Exception) = 0;

      /**
       * Schedules a SubRequest on a given FileSystem and
       * return the DiskCopy to use for data access.
       * Depending on the available DiskCopies for the file
       * the SubRequest deals with, we have different cases :
       *  - no DiskCopy at all : a DiskCopy is created with
       * status DISKCOPY_WAITTAPERECALL. Null pointer is returned
       *  - one DiskCopy in DISKCOPY_WAITTAPERECALL status :
       * the SubRequest is linked to the one recalling and
       * put in SUBREQUEST_WAITSUBREQ status. Null pointer is
       * returned.
       *  - no DiskCopy on the selected FileSystem but some
       * in status DISKCOPY_STAGEOUT or DISKCOPY_STAGED on other
       * FileSystems : a new DiskCopy is created with status
       * DISKCOPY_WAITDISK2DISKCOPY. It is returned and the
       * sources parameter is filed with the DiskCopies found
       * on the non selected FileSystems.
       *  - one DiskCopy on the selected FileSystem in
       * DISKCOPY_WAITDISKTODISKCOPY status : the SubRequest
       * has to wait until the end of the copy.
       * The DiskCopy found is returned, sources remains empty.
       *  - one DiskCopy on the selected FileSystem in
       * DISKCOPY_STAGOUT or DISKCOPY_STAGED status :
       * the SubRequest is ready, the DiskCopy is returned and
       * sources remains empty.
       * @param subreq  the SubRequest to consider
       * @param fileSystem the selected FileSystem
       * @param sources this is a list of DiskCopies that
       * can be used as source of a Disk to Disk copy. If
       * this list is not empty, the Disk to Disk copy must
       * be performed. If it is empty, the copy is performed
       * by someone else and the caller should just wait
       * for its end.
       * @return The DiskCopy to use for the data access or
       * a null pointer if the data access will have to wait
       * and there is nothing more to be done. Even in case
       * of a non null pointer, the data access will have to
       * wait for a disk to disk copy if the returned DiskCopy
       * is in DISKCOPY_WAITDISKTODISKCOPY status. This
       * disk to disk copy is the responsability of the caller
       * if sources is not empty.
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskCopy* scheduleSubRequest
      (castor::stager::SubRequest* subreq,
       castor::stager::FileSystem* fileSystem,
       std::list<castor::stager::DiskCopyForRecall*>& sources)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a SvcClass from the database based on its name.
       * Caller is in charge of the deletion of the allocated object
       * @param name the name of the SvcClass
       * @return the SvcClass, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::SvcClass* selectSvcClass(const std::string name)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a FileClass from the database based on its name.
       * Caller is in charge of the deletion of the allocated object
       * @param name the name of the FileClass
       * @return the FileClass, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::FileClass* selectFileClass(const std::string name)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a CastorFile from the database based on its fileId
       * and name server.
       * Caller is in charge of the deletion of the allocated object
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @return the CastorFile, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::CastorFile* selectCastorFile
      (const u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates a SubRequest status in the DB and tells
       * whether the request to which it belongs still
       * has some SubRequests in SUBREQUEST_START status.
       * The two operations are executed atomically.
       * The update is commited before returning.
       * @param subreq the SubRequest to update
       * @return whether there are still SubRequests in
       * SUBREQUEST_START status within the same request
       * @exception Exception in case of error
       */
      virtual bool updateAndCheckSubRequest
      (castor::stager::SubRequest *subreq)
        throw (castor::exception::Exception) = 0;

    }; // end of class IStagerSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ISTAGERSVC_HPP
