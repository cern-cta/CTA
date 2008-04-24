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
 * @(#)$RCSfile: InputArguments.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/04/24 13:13:11 $ $Author: sponcec3 $
 *
 * small struct holding the list of arguments passed to stagerJob
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
       * internal enum describing the access mode
       */
      enum AccessMode {
        ReadOnly,
        WriteOnly,
        ReadWrite
      };

      /**
       * internal struct holding the list of arguments passed to stagerJob
       */
      class InputArguments {

      public:

        /**
         * constructor initializing an InputArguments object
         * from the command line
         * @param argc the number of arguments on the command line
         */
        InputArguments(int argc, char** argv)
          throw (castor::exception::Exception);

      public:
        struct Cns_fileid fileId;
        Cuuid_t requestUuid;
        Cuuid_t subRequestUuid;
        std::string rawRequestUuid;
        std::string rawSubRequestUuid;
        std::string protocol;
        u_signed64 subRequestId;
        int type;
        std::string diskServer;
        std::string fileSystem;
        enum AccessMode accessMode;
        castor::IClient* client;
        bool isSecure;
	uid_t euid;
	gid_t egid;
        u_signed64 requestCreationTime;

      }; // end of class InputArguments

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_INPUTARGUMENTS_HPP
