/******************************************************************************
 *                castor/db/ora/OraTapeSvc.hpp
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
 * Implementation of the ITapeSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORATAPESVC_HPP
#define ORA_ORATAPESVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#ifdef ORACDBC
#include "castor/db/newora/OraCommonSvc.hpp"
#else
#include "castor/db/ora/OraCommonSvc.hpp"
#endif
#include "castor/stager/BaseTapeSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the ITapeSvc for Oracle
       */
      class OraTapeSvc : public OraCommonSvc,
                         public virtual castor::stager::BaseTapeSvc {

      public:

        /**
         * default constructor
         */
        OraTapeSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraTapeSvc() throw();

        /**
         * Get the service id
         */
        virtual inline const unsigned int id() const;

        /**
         * Get the service id
         */
        static const unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw ();

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

        /*
         * Get the best TapeCopy currently waiting for a given Stream.
         * Search the catalog for the best eligible TapeCopies that
         * is waiting for the given Stream to become ready.
         * The matching TapeCopies entry must have the status
         * TAPECOPY_WAITINSTREAMS.
         * Before return this function atomically updates the
         * matching catalog entry Stream status to STREAM_RUNNING and
         * the matching catalog entry TapeCopy status to TAPECOPY_SELECTED.
         * @param searchItem the Stream information used for the search
         * @param autocommit whether to commit the changes
         * @return vector with all waiting TapeCopies
         * @exception in case of error
         */
        virtual castor::stager::TapeCopyForMigration*
        bestTapeCopyForStream(castor::stager::Stream*  searchItem)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

        /**
         * Get an array of the streams to be processed.
         * This method searches the catalog for all streams that are
         * in STREAM_PENDING status. It atomically updates the status to
         * STREAM_WAITDRIVE and returns the corresponding Stream objects.
         * This means that a subsequent call to this method will not return
         * the same entries.
         * @return vector of streams to be processed
         * @exception in case of error
         */
        virtual std::vector<castor::stager::Stream*> streamsToDo()
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

        /**
         * resets a stream by either deleting it or setting
         * its status to STREAM_PENDING depending on whether
         * there are TapeCopies in status WAITINSTREAMS status.
         * Also deletes all links to TapeCopies for this stream
         * @param stream the stream to reset
         * @exception Exception throws an Exception in case of error
         */
        virtual void resetStream(castor::stager::Stream* stream)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);



        /**
          * Checks, if the fileid is in a actual repack process.
	  * This method is run by the migrator. It looks into the
	  * Stager Catalog, if a StageRepackRequest object is assigned to
	  * a subrequest for this  file. In this case it returns the 
	  * volume name (repackvid field) of the request. The SubRequest is 
	  * set to ARCHIVED.
	  * @return the name of the tape
	  * @exception in case of an error
          */
        virtual std::string checkFileForRepack 
        (const u_signed64 file)	 throw (castor::exception::Exception);



      private:

        /// SQL statement for function tapesToDo
        static const std::string s_tapesToDoStatementString;

        /// SQL statement object for function tapesToDo
        oracle::occi::Statement *m_tapesToDoStatement;

        /// SQL statement for function streamsToDo
        static const std::string s_streamsToDoStatementString;

        /// SQL statement object for function streamsToDo
        oracle::occi::Statement *m_streamsToDoStatement;

        /// SQL statement for function anyTapeCopyForStream
        static const std::string s_anyTapeCopyForStreamStatementString;

        /// SQL statement object for function anyTapeCopyForStream
        oracle::occi::Statement *m_anyTapeCopyForStreamStatement;

        /// SQL statement for function bestTapeCopyForStream
        static const std::string s_bestTapeCopyForStreamStatementString;

        /// SQL statement object for function bestTapeCopyForStream
        oracle::occi::Statement *m_bestTapeCopyForStreamStatement;

        /// SQL statement for function streamsForTapePool
        static const std::string s_streamsForTapePoolStatementString;

        /// SQL statement object for function streamsForTapePool
        oracle::occi::Statement *m_streamsForTapePoolStatement;

        /// SQL statement for function bestFileSystemForSegment
        static const std::string s_bestFileSystemForSegmentStatementString;

        /// SQL statement object for function bestFileSystemForSegment
        oracle::occi::Statement *m_bestFileSystemForSegmentStatement;

        /// SQL statement for function segmentsForTape
        static const std::string s_segmentsForTapeStatementString;

        /// SQL statement object for function segmentsForTape
        oracle::occi::Statement *m_segmentsForTapeStatement;

        /// SQL statement for function anySegmentsForTape
        static const std::string s_anySegmentsForTapeStatementString;

        /// SQL statement object for function anySegmentsForTape
        oracle::occi::Statement *m_anySegmentsForTapeStatement;

        /// SQL statement for function fileRecalled
        static const std::string s_fileRecalledStatementString;

        /// SQL statement object for function fileRecalled
        oracle::occi::Statement *m_fileRecalledStatement;

        /// SQL statement for function fileRecallFailed
        static const std::string s_fileRecallFailedStatementString;

        /// SQL statement object for function fileRecallFailed
        oracle::occi::Statement *m_fileRecallFailedStatement;

        /// SQL statement for function selectTapeCopiesForMigration
        static const std::string s_selectTapeCopiesForMigrationStatementString;

        /// SQL statement object for function selectTapeCopiesForMigration
        oracle::occi::Statement *m_selectTapeCopiesForMigrationStatement;

        /// SQL statement for function resetStream
        static const std::string s_resetStreamStatementString;

        /// SQL statement object for function resetStream
        oracle::occi::Statement *m_resetStreamStatement;

        /// SQL statement for function failedSegments
        static const std::string s_failedSegmentsStatementString;

        /// SQL statement object for function failedSegments
        oracle::occi::Statement *m_failedSegmentsStatement;

        /// SQL statement for checkFileForRepack
        static const std::string s_checkFileForRepackStatementString;

        /// SQL statement object for function checkFileForRepack
        oracle::occi::Statement *m_checkFileForRepackStatement;

      }; // end of class OraTapeSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORATAPESVC_HPP
