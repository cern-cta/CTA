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
 * @(#)$RCSfile: IStagerSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/05/26 10:11:44 $ $Author: sponcec3 $
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
    class Segment;

    /**
     * This class provides methods usefull to the stager to
     * deal with database queries
     */
    class IStagerSvc : public virtual IService {

    public:

      /*
       * Get the array of segments currently waiting for a given tape.
       * Search the catalog for all eligible tape files (segments) that
       * are waiting for the given tape VID to become ready.
       * The matching Segment entries must have one of the
       * following status values: SEGMENT_UNPROCESSED,
       * SEGMENT_WAITFSEQ, SEGMENT_WAITPATH or SEGMENT_WAITCOPY.
       * Before return this function atomically updates the
       * matching catalog entries Tape status to TAPE_MOUNTED
       * and Segment status to:
       * SEGMENT_WAITFSEQ - if no tape fseq or blockid has been decided yet
       * SEGMENT_WAITPATH - if physical disk path has not been decided yet
       * SEGMENT_WAITCOPY - if both tape fseq (or blockid) and path are defined
       * @param searchItem the tape information used for the search
       * @return vector with all waiting segments
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Segment*>
      segmentsForTape(castor::stager::Tape* searchItem)
        throw (castor::exception::Exception) = 0;

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
       * @return whether waiting segments exist
       * @exception in case of error
       */
      virtual bool anySegmentsForTape(castor::stager::Tape* searchItem)
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

    }; // end of class IStagerSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ISTAGERSVC_HPP
