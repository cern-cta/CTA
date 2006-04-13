/******************************************************************************
 *                      OraQuerySvc.hpp
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
 * @(#)$RCSfile: OraQuerySvc.hpp,v $ $Revision: 1.16 $ $Release$ $Date: 2006/04/13 15:44:49 $ $Author: sponcec3 $
 *
 * Implementation of the IQuerySvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORAQUERYSVC_HPP
#define ORA_ORAQUERYSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#ifdef ORACDBC
#include "castor/db/newora/OraCommonSvc.hpp"
#else
#include "castor/db/ora/OraCommonSvc.hpp"
#endif
#include "castor/query/IQuerySvc.hpp"
#include "occi.h"
#include <list>
#include <map>

namespace castor {

  namespace query {

    // Forward declaration
    class DiskPoolQueryResponse;

  }

  namespace db {

    namespace ora {

      /**
       * Implementation of the IQuerySvc for Oracle
       */
      class OraQuerySvc : public OraCommonSvc,
                          public virtual castor::query::IQuerySvc {

      public:

        /**
         * default constructor
         */
        OraQuerySvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraQuerySvc() throw();

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
         * @return the list of DiskCopies available
         * @exception in case of error
         */
        virtual std::list<castor::stager::DiskCopyInfo*>*
        diskCopies4FileName (std::string fileName,
                             u_signed64 svcClassId)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);


        /**
         * Selects the next request the query service should deal with.
         * Selects a Request in START status and move its status to
         * PROCESSED to avoid double processing.
         * @return the Request to process
         * @exception Exception in case of error
         */
        virtual castor::stager::Request* requestToDo()
          throw (castor::exception::Exception);

        /**
         * Lists diskpools and give details on their machine/filesystems
         * @param svcClass the Service class to consider, or empty string if none
         * @return a vector of diskpool descriptions. Note that the caller
         * is responsible for freeing the allocated memory
         * @exception Exception in case of error
         */
	virtual std::vector<castor::query::DiskPoolQueryResponse*>*
        describeDiskPools (std::string svcClass)
          throw (castor::exception::Exception);
        
        /**
         * Give details on the machines/filesystems of a given diskpool
         * @param diskPool the DiskPool name
         * @return the descriptions of the DiksPool. Note that the caller
         * is responsible for freeing the allocated memory
         * @exception Exception in case of error
         */
        virtual castor::query::DiskPoolQueryResponse*
        describeDiskPool(std::string diskPool)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function diskCopies4FileName
        static const std::string s_diskCopies4FileNameStatementString;

        /// SQL statement object for function diskCopies4File
        oracle::occi::Statement *m_diskCopies4FileNameStatement;

        /// SQL statement for function diskCopies4FileName
        static const std::string s_diskCopies4FileStatementString;

        /// SQL statement object for function diskCopies4File
        oracle::occi::Statement *m_diskCopies4FileStatement;

        /// SQL statement for function diskCopies4ReqId
        static const std::string s_diskCopies4ReqIdStatementString;

        /// SQL statement object for function diskCopies4ReqId
        oracle::occi::Statement *m_diskCopies4ReqIdStatement;

        /// SQL statement for function diskCopies4UserTag
        static const std::string s_diskCopies4UserTagStatementString;

        /// SQL statement object for function diskCopies4UserTag
        oracle::occi::Statement *m_diskCopies4UserTagStatement;

        /// SQL statement for function diskCopies4ReqIdLastRecalls
        static const std::string s_diskCopies4ReqIdLastRecallsStatementString;

        /// SQL statement object for function diskCopies4ReqIdLastRecalls
        oracle::occi::Statement *m_diskCopies4ReqIdLastRecallsStatement;

        /// SQL statement for function diskCopies4UserTagLastRecalls
        static const std::string s_diskCopies4UserTagLastRecallsStatementString;

        /// SQL statement object for function diskCopies4UserTagLastRecalls
        oracle::occi::Statement *m_diskCopies4UserTagLastRecallsStatement;

        /// SQL statement for function requestToDo
        static const std::string s_requestToDoStatementString;

        /// SQL statement object for function requestToDo
        oracle::occi::Statement *m_requestToDoStatement;

        /// SQL statement for function describeDiskPools
        static const std::string s_describeDiskPoolsStatementString;

        /// SQL statement object for function describeDiskPools
        oracle::occi::Statement *m_describeDiskPoolsStatement;

        /// SQL statement for function describeDiskPool
        static const std::string s_describeDiskPoolStatementString;

        /// SQL statement object for function describeDiskPool
        oracle::occi::Statement *m_describeDiskPoolStatement;
        
        /// Private function to parse and return the list of results
        std::list<castor::stager::DiskCopyInfo*>*
        gatherResults(oracle::occi::ResultSet *rset)        
          throw (oracle::occi::SQLException);

      }; // end of class OraQuerySvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORAQUERYSVC_HPP
