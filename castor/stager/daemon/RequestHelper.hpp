/******************************************************************************
 *                castor/stager/daemon/RequestHelper.hpp
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
 *
 * Helper class for handling file-oriented user requests
 *
 * @author castor dev team
 *****************************************************************************/


#ifndef STAGER_REQUEST_HELPER_HPP
#define STAGER_REQUEST_HELPER_HPP 1

#include <vector>
#include <iostream>
#include <string>
#include <string.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "serrno.h"
#include <errno.h>

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"

namespace castor {

  namespace stager {

    namespace daemon {

      class RequestHelper {

      public:

        // services needed: database and stager services
        castor::stager::IStagerSvc* stagerService;
        castor::db::DbCnvSvc* dbSvc;
        castor::BaseAddress* baseAddr;

        // NameServer data
        struct Cns_fileid cnsFileid;
        struct Cns_filestatcs cnsFilestat;
        u_signed64 m_stagerOpenTimeInUsec;
      
        // request & co
        castor::stager::SubRequest* subrequest;
        castor::stager::FileRequest* fileRequest;
        castor::stager::SvcClass* svcClass;
        castor::stager::CastorFile* castorFile;

        // resolved username and groupname        
        std::string username;
        std::string groupname;
      
        // for logging purposes
        Cuuid_t subrequestUuid;
        Cuuid_t requestUuid;
      
        timeval tvStart;

        RequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest)
          throw(castor::exception::Exception);

        ~RequestHelper() throw();

        /**
         * Resolves the svcClass if not resolved yet
         * @throw exception in case of any database error
         */
        void resolveSvcClass() throw(castor::exception::Exception);

        /**
         * Checks the existence of the requested file in the NameServer, and creates it if the request allows for
         * creation. Internally sets the fileId and nsHost for the file.
         * @return true if the file has been created
         * @throw exception in case of permission denied or non extisting file and no right to create it
         */
        bool openNameServerFile()
          throw(castor::exception::Exception);

        /**
         * Stats the requested file in the NameServer.
         * @throw exception in case of any NS error except ENOENT. serrno is set accordingly.
         */
        void statNameServerFile()
          throw(castor::exception::Exception);

        /**
         * Gets the castorFile from the db, calling selectCastorFile
         * @throw exception in case of any database error
         */
        void getCastorFile() throw(castor::exception::Exception);

        /**
         * Extracts the username and groupname from uid,gid
         * @throw exception in case of invalid user
         */
        void setUsernameAndGroupname() throw(castor::exception::Exception);

        /**
         * Logs a standard message to DLF including all needed info (e.g. filename, svcClass, etc.)
         * @param level the DLF logging level
         * @param messageNb the message number as defined in DlfMessages.hpp
         * @param fid the fileId structure if needed
         */
        void logToDlf(int level, int messageNb, struct Cns_fileid* fid = 0) throw();

      }; //end RequestHelper class
      
    }//end namespace daemon
    
  }//end namespace stager
  
}//end namespace castor

#endif
