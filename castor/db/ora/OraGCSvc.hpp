/******************************************************************************
 *                castor/db/ora/OraGCSvc.hpp
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
 * @(#)$RCSfile: OraGCSvc.hpp,v $ $Revision: 1.14 $ $Release$ $Date: 2009/01/05 15:53:56 $ $Author: sponcec3 $
 *
 * Implementation of the IGCSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORAGCSVC_HPP
#define ORA_ORAGCSVC_HPP 1

// Include Files
#include "castor/db/newora/OraCommonSvc.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IGCSvc for Oracle
       */
      class OraGCSvc : public OraCommonSvc,
                       public virtual castor::stager::IGCSvc {

      public:

        /**
         * default constructor
         */
        OraGCSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraGCSvc() throw();

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
        void reset() throw ();

      public:

        /**
         * List files to be deleted on a given diskServer.
         * These are the files corresponding to DiskCopies in
         * STAGED status and eligible for garbage collection,
         * plus the INVALID ones. This status is changed
         * to BEINGDELETED atomically.
         * @param diskServer the name of the DiskServer
         * involved
         * @return a list of files. The id of the DiskCopy
         * is given as well as the local path on the server.
         * Note that the returned vector should be deallocated
         * by the caller as well as its content
         */
        virtual std::vector<castor::stager::GCLocalFile*>*
        selectFiles2Delete(std::string diskServer)
          throw (castor::exception::Exception);

        /**
         * Informs the stager of files effectively deleted.
         * The DiskCopy id is given. The corresponding
         * DiskCopies will be deleted from the catalog
         * as well as the CastorFile if there is no other
         * copy.
         * @param diskCopyIds the list of diskcopies deleted
         * given by their id
         */
        virtual void filesDeleted
        (std::vector<u_signed64*>& diskCopyIds)
          throw (castor::exception::Exception);

        /**
         * Informs the stager of files for which deletion failed.
         * The DiskCopy id is given. The corresponding
         * DiskCopies will marked FAILED in the catalog.
         * @param diskCopyIds the list of diskcopies for which
         * deletion failed given by their id
         */
        virtual void filesDeletionFailed
        (std::vector<u_signed64*>& diskCopyIds)
          throw (castor::exception::Exception);

        /**
         * Handles a set of files that were deleted from
         * a nameServer but may still be in the stager.
         * Proper cleaning will be done both of the diskServers
         * where copies of these files exist and of the stager DB
         * @param fileIds the set of files, given by fileids
         * @param nsHost the nameserver in which they reside
         * @return the list of fileIds that were not found in the stager
         */
        virtual std::vector<u_signed64> nsFilesDeleted
        (std::vector<u_signed64> &fileIds,
         std::string nsHost) throw();

        /**
         * Find the files which are not anymore in the Stager
         * @param diskCopyIds a list of diskcopy ids to be checked
         * @param nsHost the nameserver in which they reside
         * @return the list of diskcopy ids that were not found in the stager
         */
        virtual std::vector<u_signed64> stgFilesDeleted
        (std::vector<u_signed64> &diskCopyIds,
         std::string nsHost) throw();
	
        /**
         * Removes requests older than a given timeout.
         * The timeout is retrieved from the configuration table in the db
         */
        virtual void removeTerminatedRequests()
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function selectFiles2Delete
        static const std::string s_selectFiles2DeleteStatementString;

        /// SQL statement object for function selectFiles2Delete
        oracle::occi::Statement *m_selectFiles2DeleteStatement;

        /// SQL statement for function filesDeleted
        static const std::string s_filesDeletedStatementString;

        /// SQL statement object for function filesDeleted
        oracle::occi::Statement *m_filesDeletedStatement;

        /// SQL statement for filesDeletedTruncate
        static const std::string s_filesDeletedTruncateStatementString;

        /// SQL statement object for filesDeletedTruncate
        oracle::occi::Statement *m_filesDeletedTruncateStatement;

        /// SQL statement for function filesDeletionFailed
        static const std::string s_filesDeletionFailedStatementString;

        /// SQL statement object for function filesDeletionFailed
        oracle::occi::Statement *m_filesDeletionFailedStatement;

        /// SQL statement for function nsFilesDeleted
        static const std::string s_nsFilesDeletedStatementString;

        /// SQL statement object for function nsFilesDeleted
        oracle::occi::Statement *m_nsFilesDeletedStatement;

        /// SQL statement object for function stgFilesDeleted
        oracle::occi::Statement *m_stgFilesDeletedStatement;
        
        /// SQL statement for function stgFilesDeleted
        static const std::string s_stgFilesDeletedStatementString;
	
        /// SQL statement for removeTerminatedRequests function
        static const std::string s_removeTerminatedRequestsString;

        /// SQL statement object for removeTerminatedRequests function
        oracle::occi::Statement *m_removeTerminatedRequestsStatement;
        
     }; // end of class OraGCSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORAGCSVC_HPP
