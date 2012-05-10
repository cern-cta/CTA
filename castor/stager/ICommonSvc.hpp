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
 * @(#)$RCSfile: ICommonSvc.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2009/03/26 10:59:48 $ $Author: itglp $
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
       * Selects the next request a stager service should deal with.
       * Selects a Request in START status and deletes it from the
       * NewRequests helper table to avoid double processing.
       * @param service the stager service that will process the Request
       * @return the Request to process
       * @exception Exception in case of error
       */
      virtual castor::stager::Request* requestToDo(std::string service)
        throw (castor::exception::Exception) = 0;

    }; // end of class ICommonSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ICOMMONSVC_HPP
