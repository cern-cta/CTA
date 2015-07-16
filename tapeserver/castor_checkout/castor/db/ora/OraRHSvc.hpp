/******************************************************************************
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
 * Implementation of the IRHSvc for Oracle
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

// Include Files
#include "castor/db/ora/OraCommonSvc.hpp"
#include "castor/rh/IRHSvc.hpp"
#include <vector>
#include "occi.h"
#include "osdep.h"

namespace castor {

  // Forward Declarations
  namespace bwlist {
    class BWUser;
    class RequestType;
    class Privilege;
    class ListPrivileges;
    class ChangePrivilege;
  }
  namespace rh {
    class Client;
  }
  namespace stager {
    class Request;
    class FileRequest;
    class QueryParameter;
    class Files2Delete;
    class StageAbortRequest;
    class GCFileList;
    class StageFileQueryRequest;
  }

  namespace query {
    class VersionQuery;
    class DiskPoolQuery;
  }
  
  namespace db {

    namespace ora {

      /**
       * Implementation of the IRHSvc for Oracle
       */
      class OraRHSvc : public OraCommonSvc,
                       public virtual castor::rh::IRHSvc {

      public:

        /**
         * default constructor
         */
        OraRHSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraRHSvc() throw();

        /**
         * Get the service id
         */
        virtual inline unsigned int id() const;

        /**
         * Get the service id
         */
        static unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw();

      public:

        /**
         * stores a given request into the stager DB.
         * @param req the user request object
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeRequest(castor::stager::Request* req);

        /**
         * stores a file request into the stager DB.
         * This includes StageGetRequest, StagePrepareToGetRequest, StagePutRequest,
         * StagePrepareToPutRequest, StageRmRequest, StagePutDoneRequest
         * and SetFileGCWeight requests
         * @param request the FileRequest object
         * @param client the client object associated to the request
         * @param freeStrParam an extra string associated to the request.
         * Only used for StagePutDoneRequest requests.
         * @param freeNumParam an extra float associated to the request.
         * Only used for SetFileGCWeight request.
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeFileRequest(castor::stager::FileRequest* req,
                                      const castor::rh::Client *client,
                                      const std::string freeStrParam = "",
                                      const float freeNumParam = 0)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a VersionQuery request into the stager DB.
         * @param request the VersionQuery object
         * @param client the client object associated to the request
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeVersionQueryRequest(castor::query::VersionQuery* req,
                                              const castor::rh::Client *client)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a StageFileQueryRequest request into the stager DB.
         * @param request the StageFileQueryRequest object
         * @param client the client object associated to the request
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeStageFileQueryRequest(castor::stager::StageFileQueryRequest* req,
                                                const castor::rh::Client *client)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a DiskPoolQuery request into the stager DB.
         * @param request the DiskPoolQuery object
         * @param client the client object associated to the request
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeDiskPoolQueryRequest(castor::query::DiskPoolQuery* req,
                                               const castor::rh::Client *client)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a Files2Delete request into the stager DB.
         * @param request the Files2Delete object
         * @param client the client object associated to the request
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeFiles2DeleteRequest(castor::stager::Files2Delete* req,
                                              const castor::rh::Client *client)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a ListPrivileges request into the stager DB.
         * @param request the ListPrivileges object
         * @param client the client object associated to the request
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeListPrivilegesRequest(castor::bwlist::ListPrivileges* req,
                                                const castor::rh::Client *client)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a StageAbortRequest request into the stager DB.
         * @param request the StageAbortRequest object
         * @param client the client object associated to the request
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeStageAbortRequest(castor::stager::StageAbortRequest* req,
                                            const castor::rh::Client *client)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a ChangePrivilege request into the stager DB.
         * @param request the ChangePrivilege object
         * @param client the client object associated to the request
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeChangePrivilegeRequest(castor::bwlist::ChangePrivilege* req,
                                               const castor::rh::Client *client)
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * stores a GC request into the stager DB.
         * This includes NsFilesDeleted, FilesDeleted, FilesDeletionFailed and StgFilesDeleted requests.
         * @param request the GCFileList object
         * @param client the client object associated to the request
         * @param nshost the nshost to be used for this request (only relevant for NsFilesDeleted and
         * StgFilesDeleted requests)
         * @exception throws Exception in case of errors, in particular in case of permission denial
         */
        virtual void storeGCRequest(castor::stager::GCFileList* req,
                                    const castor::rh::Client *client,
                                    const std::string nsHost = "")
          throw (castor::exception::Exception, oracle::occi::SQLException);

        /**
         * change privileges for some users
         * @param svcClassName the service class to be affected.
         * The special value '*' can be used to target all service
         * classes
         * @param users the list of affected users. An empty list
         * can be used to target all users. Similarly, an entry
         * containing a -1 as uid or gid means repectively that
         * all uid or all gid are targeted
         * @param requestTypes the list of affected request types.
         * An empty list can be used to target all types
         * @param isAdd do we add (or delete) these privileges ?
         * @exception in case of error
         */
        virtual void changePrivilege
        (const std::string svcClassName,
         std::vector<castor::bwlist::BWUser*> users,
         std::vector<castor::bwlist::RequestType*> requestTypes,
         bool isAdd)
          ;

        /**
         * list privileges
         * @param svcClassName if not '*', restricts the listing to
         * privileges for this service class
         * @param user if not -1, restricts the listing to privileges
         * concerning this user
         * @param group if not -1, restricts the listing to privileges
         * concerning this group
         * @param requestType if not 0, restricts the listing to
         * privileges concerning this request type
         * @return the list of privileges, as a vectors of individual
         * privileges. Note that it is the responsibility of the caller
         * to deallocate all these privileges
         * @exception in case of error
         */
        virtual std::vector<castor::bwlist::Privilege*>
        listPrivileges
        (const std::string svcClassName, const int user,
         const int group, const int requestType)
          ;

      private:

        /// SQL statement for function storeFileRequest
        static const std::string s_storeFileRequestStatementString;

        /// SQL statement object for function storeFileRequest
        oracle::occi::Statement *m_storeFileRequestStatement;

        /// SQL statement for function storeVersionQueryRequest
        static const std::string s_storeVersionQueryStatementString;

        /// SQL statement object for function storeVersionQueryRequest
        oracle::occi::Statement *m_storeVersionQueryStatement;

        /// SQL statement for function storeStageFileQueryRequest
        static const std::string s_storeStageFileQueryRequestStatementString;

        /// SQL statement object for function storeStageFileQueryRequest
        oracle::occi::Statement *m_storeStageFileQueryRequestStatement;

        /// SQL statement for function storeDiskPoolQueryRequest
        static const std::string s_storeDiskPoolQueryStatementString;

        /// SQL statement object for function storeDiskPoolQueryRequest
        oracle::occi::Statement *m_storeDiskPoolQueryStatement;

        /// SQL statement for function storeFiles2DeleteRequest
        static const std::string s_storeFiles2DeleteStatementString;

        /// SQL statement object for function storeFiles2DeleteRequest
        oracle::occi::Statement *m_storeFiles2DeleteStatement;

        /// SQL statement for function storeListPrivilegesRequest
        static const std::string s_storeListPrivilegesStatementString;

        /// SQL statement object for function storeListPrivilegesRequest
        oracle::occi::Statement *m_storeListPrivilegesStatement;

        /// SQL statement for function storeStageAbortRequest
        static const std::string s_storeStageAbortRequestStatementString;

        /// SQL statement object for function storeStageAbortRequest
        oracle::occi::Statement *m_storeStageAbortRequestStatement;

        /// SQL statement for function storeChangePrivilegeRequest
        static const std::string s_storeChangePrivilegeStatementString;

        /// SQL statement object for function storeChangePrivilegeRequest
        oracle::occi::Statement *m_storeChangePrivilegeStatement;

        /// SQL statement for function storeGCRequest
        static const std::string s_storeGCStatementString;

        /// SQL statement object for function storeGCRequest
        oracle::occi::Statement *m_storeGCStatement;

        /// SQL statement for function addPrivilege
        static const std::string s_addPrivilegeStatementString;

        /// SQL statement object for function addPrivilege
        oracle::occi::Statement *m_addPrivilegeStatement;

        /// SQL statement for function removePrivilege
        static const std::string s_removePrivilegeStatementString;

        /// SQL statement object for function removePrivilege
        oracle::occi::Statement *m_removePrivilegeStatement;

        /// SQL statement for function listPrivilege
        static const std::string s_listPrivilegesStatementString;

        /// SQL statement object for function listPrivilege
        oracle::occi::Statement *m_listPrivilegesStatement;

      }; // end of class OraRHSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

