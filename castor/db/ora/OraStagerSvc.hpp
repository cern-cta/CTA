/******************************************************************************
 *                castor/db/ora/OraStagerSvc.hpp
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
 * Implementation of the IStagerSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORASTAGERSVC_HPP
#define ORA_ORASTAGERSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraBaseObj.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/Stream.hpp"
#include "occi.h"

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IStagerSvc for Oracle
       */
      class OraStagerSvc : public BaseSvc,
                           public OraBaseObj,
                           public virtual castor::stager::IStagerSvc {

      public:

        /**
         * default constructor
         */
        OraStagerSvc(const std::string name);

        /**
         * default destructor
         */
        ~OraStagerSvc() throw();

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
        /*
         * Get the array of segments currently waiting for a given tape.
         * Search the catalog for all eligible tape files (segments) that
         * are waiting for the given tape VID to become ready.
         * The matching Segment entries must have one of the
         * following status values: SEGMENT_UNPROCESSED,
         * SEGMENT_WAITFSEQ, SEGMENT_WAITPATH or SEGMENT_WAITCOPY.
         * Before return this function atomically updates the
         * matching catalog entries Tape status to TAPE_MOUNTED.
         * The segments' status stay unchanged
         * @param searchItem the tape information used for the search
         * @return vector with all waiting segments
         * @exception in case of error
         */
        virtual std::vector<castor::stager::Segment*>
        segmentsForTape(castor::stager::Tape* searchItem)
          throw (castor::exception::Exception);

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
         * Retrieves a tape from the database based on its vid,
         * side and tpmode. If no tape is found, creates one
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
        castor::stager::Tape* selectTape(const std::string vid,
                                         const int side,
                                         const int tpmode)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function tapesToDo
        static const std::string s_tapesToDoStatementString;
        
        /// SQL statement object for function tapesToDo
        oracle::occi::Statement *m_tapesToDoStatement;

        /// SQL statement for function streamsToDo
        static const std::string s_streamsToDoStatementString;
        
        /// SQL statement object for function streamsToDo
        oracle::occi::Statement *m_streamsToDoStatement;

        /// SQL statement for function selectTape
        static const std::string s_selectTapeStatementString;
        
        /// SQL statement object for function selectTape
        oracle::occi::Statement *m_selectTapeStatement;

      }; // end of class OraStagerSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORASTAGERSVC_HPP
