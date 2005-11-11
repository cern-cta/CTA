/******************************************************************************
 *                      IQuerySvc.hpp
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
 * @(#)$RCSfile: IQuerySvc.hpp,v $ $Revision: 1.12 $ $Release$ $Date: 2005/11/11 10:32:26 $ $Author: itglp $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef QUERY_IQUERYSVC_HPP
#define QUERY_IQUERYSVC_HPP 1

// Include Files
#include "castor/stager/ICommonSvc.hpp"
#include "castor/stager/RequestQueryType.hpp"
#include "castor/exception/Exception.hpp"
#include <string>
#include <list>

namespace castor {

  namespace stager {

    // Forward declaration
    class DiskCopyInfo;
    class CastorFile;
    
  }

  namespace query {

    /**
     * This class provides methods usefull to the query
     * handler to with database queries
     */
    class IQuerySvc : public virtual castor::stager::ICommonSvc {

    public:

      /**
       * Gets all DiskCopies for a given file.
       * The caller is responsible for the deallocation of
       * the returned objects
       * @param fileId the fileId identifying the file
       * @param nsHost the name server host for this file
       * @param svcClassId the Id of the service class we're using
       * @return the list of DiskCopies available
       * @exception in case of error
       */
      virtual std::list<castor::stager::DiskCopyInfo*>*
      diskCopies4File (std::string fileId,
                       std::string nsHost,
                       u_signed64 svcClassId)
        throw (castor::exception::Exception) = 0;


      /**
       * Gets all DiskCopies for a given request by reqId or userTag.
       * For the GETNEXT requests, gets the newly staged DiskCopies.
       * This is meaningful for a PrepareToGet request of several files:
       * once correspondent DiskCopies are in STAGED status, they're
       * returned and the correspondent subRequests are flagged as
       * already returned, so that the function returns a given
       * DiskCopy only once.
       * @param reqType the request type
       * @param param the query param, either a requestId or userTag 
       * @param svcClassId the Id of the service class we're using
       * @return the list of DiskCopies available; an empty list if
       * none is available, NULL if the reqId or the userTag have not
       * been found.
       * @exception in case of error
       */
      virtual std::list<castor::stager::DiskCopyInfo*>*
       diskCopies4Request (castor::stager::RequestQueryType reqType,
                           std::string param,
                           u_signed64 svcClassId)
         throw (castor::exception::Exception) = 0;


      /**
       * Selects the next request the query service should deal with.
       * Selects a Request in START status and move its status to
       * PROCESSED to avoid double processing.
       * @return the Request to process
       * @exception Exception in case of error
       */
      virtual castor::stager::Request* requestToDo()
        throw (castor::exception::Exception) = 0;

    }; // end of class IQuerySvc

  } // end of namespace query

} // end of namespace castor

#endif // QUERY_IQUERYSVC_HPP
