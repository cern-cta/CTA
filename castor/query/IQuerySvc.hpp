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
#include "castor/query/DiskPoolQueryType.hpp"
#include "castor/exception/Exception.hpp"
#include <string>
#include <list>
#include <vector>

namespace castor {

  namespace stager {

    // Forward declaration
    class StageQueryResult;
    class CastorFile;
    
  }

  namespace query {
    
    // Forward declaration
    class DiskPoolQueryResponse;

    /**
     * This class provides methods usefull to the query
     * handler to with database queries
     */
    class IQuerySvc : public virtual castor::stager::ICommonSvc {

    public:

      /**
       * Gets all DiskCopies for a given file name.
       * The caller is responsible for the deallocation of
       * the returned objects.
       * Note that this function only makes use of the
       * lastKnownFileName stored in the stager database
       * for each file, that is in no way accurate. In case
       * a renaming took place between the last request for
       * this file and the call to this function, no
       * DiskCopy will be returned for the new name while
       * the old name will still be known
       * @param fileName the fileName to be used for the query
       * @param svcClassId the Id of the service class we're using
       * @param euid the uid of the requestor
       * @param guid the gid of the requestor
       * @return the list of DiskCopies available
       * @exception in case of error
       */
      virtual std::list<castor::stager::StageQueryResult*>*
      diskCopies4FileName (std::string fileName,
                           u_signed64 svcClassId,
                           unsigned euid,
                           unsigned egid)
        throw (castor::exception::Exception) = 0;

      /**
       * Gets all DiskCopies for a given file.
       * The caller is responsible for the deallocation of
       * the returned objects
       * @param fileId the fileId identifying the file
       * @param nsHost the name server host for this file
       * @param svcClassId the Id of the service class we're using
       * @param euid the uid of the requestor
       * @param guid the gid of the requestor
       * @param fileName the current name of the file
       * @return the list of DiskCopies available
       * @exception in case of error
       */
      virtual std::list<castor::stager::StageQueryResult*>*
      diskCopies4File (u_signed64 fileId,
                       std::string nsHost,
                       u_signed64 svcClassId,
                       unsigned euid,
                       unsigned egid,
                       std::string& fileName)
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
      virtual std::list<castor::stager::StageQueryResult*>*
      diskCopies4Request (castor::stager::RequestQueryType reqType,
			  std::string param,
			  u_signed64 svcClassId)
	throw (castor::exception::Exception) = 0;

      /**
       * Lists diskpools and give details on their machine/filesystems
       * @param svcClass the Service class to consider, or empty string if none
       * @param euid the uid of the user issuing the command
       * @param egid the gid of the user issuing the command
       * @param detailed flag to indicate whether the diskserver and
       * filesystem related information should be included in response
       * @param queryType type of query to run (default, available space
       * or total space)
       * @return a vector of diskpool descriptions. Note that the caller
       * is responsible for freeing the allocated memory
       * @exception Exception in case of error
       */
      virtual std::vector<castor::query::DiskPoolQueryResponse*>*
      describeDiskPools(std::string svcClass,
			unsigned long euid,
			unsigned long egid,
			bool detailed,
                        enum castor::query::DiskPoolQueryType queryType)
	throw (castor::exception::Exception) = 0;

      /**
       * Give details on the machines/filesystems of a given diskpool
       * @param diskPool the DiskPool name
       * @param svcClass the Service class to consider, or empty string if none
       * @param detailed flag to indicate whether the diskserver and
       * filesystem related information should be included in response
       * @param queryType type of query to run (default, available space
       * or total space)
       * @return the descriptions of the DiksPool. Note that the caller
       * is responsible for freeing the allocated memory
       * @exception Exception in case of error
       */
      virtual castor::query::DiskPoolQueryResponse*
      describeDiskPool(std::string diskPool,
		       std::string svcClass,
		       bool detailed,
                       enum castor::query::DiskPoolQueryType queryType)
	throw (castor::exception::Exception) = 0;

    }; // end of class IQuerySvc

  } // end of namespace query

} // end of namespace castor

#endif // QUERY_IQUERYSVC_HPP
