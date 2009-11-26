/******************************************************************************
 *                      OraRHSvc.hpp
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
 * @(#)$RCSfile: OraRHSvc.hpp,v $ $Revision: 1.9 $ $Release$ $Date: 2009/03/26 14:01:12 $ $Author: itglp $
 *
 * Implementation of the IRHSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORARHSVC_HPP 
#define ORA_ORARHSVC_HPP 1

// Include Files
#include "castor/db/newora/OraCommonSvc.hpp"
#include "castor/rh/IRHSvc.hpp"
#include "castor/stager/Request.hpp"
#include <vector>
#include "occi.h"
#include "osdep.h"

namespace castor {

  // Froward Declarations
  namespace bwlist {
    class BWUser;
    class RequestType;
    class Privilege;
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
         * checks validity of a given service class and returns its id.
         * throws an InvalidArgument exception in case the service class does not exist
         * @param serviceClassName the service class name
         * @exception throws Exception in case the service class does not exist
         */
        virtual u_signed64 checkSvcClass(const std::string& serviceClassName)
          throw (castor::exception::Exception);

        /**
         * checks permission for a given request to be processed.
         * Returns nothing in case permission is granted but
         * throws a permission denied exception otherwise
         * @param req the user request object
         * @exception throws Exception in case of permission denial
         */
        virtual void checkPermission(const castor::stager::Request* req)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function checkSvcClass
        static const std::string s_checkSvcClassStatementString;

        /// SQL statement object for function checkSvcClass
        oracle::occi::Statement *m_checkSvcClassStatement;

        /// SQL statement for function checkPermission
        static const std::string s_checkPermissionStatementString;

        /// SQL statement object for function checkPermission
        oracle::occi::Statement *m_checkPermissionStatement;

        /// SQL statement for function changePrivilege
        static const std::string s_addPrivilegeStatementString;

        /// SQL statement object for function changePrivilege
        oracle::occi::Statement *m_addPrivilegeStatement;

        /// SQL statement for function changePrivilege
        static const std::string s_removePrivilegeStatementString;

        /// SQL statement object for function changePrivilege
        oracle::occi::Statement *m_removePrivilegeStatement;

        /// SQL statement for function listPrivilege
        static const std::string s_listPrivilegesStatementString;

        /// SQL statement object for function listPrivilege
        oracle::occi::Statement *m_listPrivilegesStatement;

      }; // end of class OraRHSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORARHSVC_HPP
