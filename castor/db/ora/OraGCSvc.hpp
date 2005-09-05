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
 * @(#)$RCSfile: OraGCSvc.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2005/09/05 12:53:42 $ $Author: sponcec3 $
 *
 * Implementation of the IGCSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORAGCSVC_HPP
#define ORA_ORAGCSVC_HPP 1

// Include Files
#include "castor/db/ora/OraCommonSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
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
         * List files to be deleted on a given diskServer.
         * These are the files corresponding to DiskCopies
         * in GCCANDIDATE status. This status is changed
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
         * Selects the next request the GC service should deal with.
         * Selects a Request in START status and move its status
         * PROCESSED to avoid double processing.
         * @return the Request to process
         * @exception Exception in case of error
         */
        virtual castor::stager::Request* requestToDo()
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

        /// SQL statement for function filesDeletionFailed
        static const std::string s_filesDeletionFailedStatementString;

        /// SQL statement object for function filesDeletionFailed
        oracle::occi::Statement *m_filesDeletionFailedStatement;

        /// SQL statement for function requestToDo
        static const std::string s_requestToDoStatementString;

        /// SQL statement object for function requestToDo
        oracle::occi::Statement *m_requestToDoStatement;

      }; // end of class OraGCSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORAGCSVC_HPP
