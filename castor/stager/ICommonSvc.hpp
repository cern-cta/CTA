/******************************************************************************
 *                castor/stager/ICommonSvc.hpp
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
 * @(#)$RCSfile: ICommonSvc.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/07/29 12:32:21 $ $Author: mbraeger $
 *
 * This class provides common methods useful to the stager to
 * deal with database queries
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef STAGER_ICOMMONSVC_HPP
#define STAGER_ICOMMONSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
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
    class Request;
    class SvcClass;
    class FileClass;
    class FileSystem;

    /**
     * This class provides common methods useful to the stager to
     * deal with database queries
     */
    class ICommonSvc : public virtual castor::IService {

    public:

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
       * Retrieves a FileSystem from the database based on its
       * mount point and diskServer name. Keeps a lock on it.
       * Caller is in charge of the deletion of the allocated
       * objects, including the DiskServer Object
       * @param mountPoint the mountPoint of the FileSystem
       * @param diskServer the name of the disk server hosting this file system
       * @return the FileSystem linked to its DiskServer, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::FileSystem* selectFileSystem
      (const std::string mountPoint, const std::string diskServer)
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
       * Selects the next request the stager should deal with.
       * Selects a Request in START status and move its status
       * PROCESSED to avoid double processing.
       * The selection is restricted to Request of a given set
       * of types.
       * @param types the list of accepted types for the request
       * @return the Request to process
       * @exception Exception in case of error
       */
      virtual castor::stager::Request* requestToDo
      (std::vector<ObjectsIds> &types)
        throw (castor::exception::Exception) = 0;
        
	    /**
	     * helper method to commit
	     */
      virtual void commit() = 0;
      
      /**
	     * helper method to rollback
	     */
      virtual void rollback() = 0;

    }; // end of class ICommonSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ICOMMONSVC_HPP
