/******************************************************************************
 *                      InputArguments.hpp
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
 * @(#)$RCSfile: InputArguments.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/07/23 12:21:57 $ $Author: waldron $
 *
 * small struct holding the list of arguments passed to stagerjob
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGERJOB_INPUTARGUMENTS_HPP
#define STAGERJOB_INPUTARGUMENTS_HPP 1

// Include Files
#include <string>
#include "Cns_api.h"
#include "Cuuid.h"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward declaration
  class IClient;

  namespace job {

    namespace stagerjob {

      /**
       * Internal enum describing the access mode
       */
      enum AccessMode {
        ReadOnly,
        WriteOnly,
        ReadWrite
      };

      /**
       * Internal struct holding the list of arguments passed to stagerjob
       */
      class InputArguments {

      public:

        /**
         * Constructor initializing an InputArguments object
         * from the command line
         * @param argc the number of arguments on the command line
         */
        InputArguments(int argc, char** argv)
          throw (castor::exception::Exception);

      public:

	/// The Cns invariant of the job
        struct Cns_fileid fileId;

	/// The request uuid of the job being processed
        Cuuid_t requestUuid;

	/// The sub request uuid of the job being processed
        Cuuid_t subRequestUuid;

	/// The request uuid of the job represented as a string
        std::string rawRequestUuid;

	/// The sub request uuid of the job represented as a string
        std::string rawSubRequestUuid;

	/// The protocol used by the mover
        std::string protocol;

	/// The id of the subrequest
        u_signed64 subRequestId;

	/// The type of request e.g 40 (StagePutRequest)
        int type;

	/// The selected diskserver as determined by scheduling
        std::string diskServer;

	/// The select filesystem as determined by scheduling
        std::string fileSystem;

	/// The access mode to open the file e.g. read, write, read/write, etc.
        enum AccessMode accessMode;

	/// The client where to send the response
        castor::IClient* client;

	/// Flag to indicate whether the transfer should be secure
        bool isSecure;

	/// The userid of the user initiating the transfer
	uid_t euid;

	/// The groupid of the user initiating the transfer
	gid_t egid;

	/// The creation time of the original request in seconds since EPOCH
        u_signed64 requestCreationTime;

	/// The location of the file to retrieve containing the diskserver and
	/// filesystem to write too
	std::string resourceFile;

      }; // end of class InputArguments

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_INPUTARGUMENTS_HPP
