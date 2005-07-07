/******************************************************************************
 *                castor/stager/IFSSvc.hpp
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
 * @(#)$RCSfile: IFSSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/07/07 14:58:42 $ $Author: itglp $
 *
 * This class provides stager methods related to File System handling
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_IFSSVC_HPP
#define STAGER_IFSSVC_HPP 1

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
    class DiskPool;
    class TapePool;
    class DiskServer;

    /**
     * This class provides stager methods related to File System handling
	 */
    class IFSSvc : public virtual ICommonSvc {

    public:
      /**
       * Retrieves a DiskPool from the database based on name.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param name the name of the disk pool
       * @return the DiskPool object or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskPool* selectDiskPool
      (const std::string name)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a TapePool from the database based on name.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param name the name of the tape pool
       * @return the TapePool object or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::TapePool* selectTapePool
      (const std::string name)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a DiskServer from the database based on name.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param name the name of the disk server
       * @return the DiskServer object or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskServer* selectDiskServer
      (const std::string name)
        throw (castor::exception::Exception) = 0;

    }; // end of class IFSSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_IFSSVC_HPP
