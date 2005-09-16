/******************************************************************************
 *                castor/db/ora/OraCommonSvc.hpp
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
 * @(#)$RCSfile: OraCommonSvc.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/09/16 16:35:19 $ $Author: itglp $
 *
 * Implementation of the ICommonSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORACOMMONSVC_HPP
#define ORA_ORACOMMONSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the ICommonSvc for Oracle
       */
      class OraCommonSvc : public BaseSvc,
                           public DbBaseObj,
                           public virtual castor::stager::ICommonSvc {

      public:

        /**
         * default constructor
         */
        OraCommonSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraCommonSvc() throw();

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
         * Retrieves a tape from the database based on its vid,
         * side and tpmode. If no tape is found, creates one
         * Note that this method creates a lock on the row of the
         * given tape and does not release it. It is the
         * responsability of the caller to commit the transaction.
         * @param vid the vid of the tape
         * @param side the side of the tape
         * @param tpmode the tpmode of the tape
         * @return the tape. the return value can never be 0
         * @exception Exception in case of error (no tape found,
         * several tapes found, DB problem, etc...)
         */
        castor::stager::Tape* selectTape(const std::string vid,
                                         const int side,
                                         const int tpmode)
          throw (castor::exception::Exception);

        /**
         * Retrieves a SvcClass from the database based on its name.
         * Caller is in charge of the deletion of the allocated object
         * @param name the name of the SvcClass
         * @return the SvcClass, or 0 if none found
         * @exception Exception in case of error
         */
        virtual castor::stager::SvcClass* selectSvcClass
        (const std::string name)
          throw (castor::exception::Exception);

        /**
         * Retrieves a FileClass from the database based on its name.
         * Caller is in charge of the deletion of the allocated object
         * @param name the name of the FileClass
         * @return the FileClass, or 0 if none found
         * @exception Exception in case of error
         */
        virtual castor::stager::FileClass* selectFileClass
        (const std::string name)
          throw (castor::exception::Exception);

        /**
         * Retrieves a FileSystem from the database based on its
         * mount point and diskServer name. Keeps a lock on it.
         * Caller is in charge of the deletion of the allocated
         * objects, including the DiskServer Object
         * @param mountPoint the mountPoint of the FileSystem
         * @param diskServer the name of the disk server hosting
         * this file system
         * @return the FileSystem linked to its DiskServer, or 0 if none found
         * @exception Exception in case of error
         */
        virtual castor::stager::FileSystem* selectFileSystem
        (const std::string mountPoint,
         const std::string diskServer)
          throw (castor::exception::Exception);
          
      protected:
          
        /**
         * helper method to commit
         */
        virtual void commit();          

        /**
         * helper method to rollback
         */
        virtual void rollback();
        
        /**
         * helper method to create Oracle statement
         */
        virtual oracle::occi::Statement* createStatement(const std::string &stmtString)
          throw (castor::exception::Exception);
          
        /**
         * helper method to delete Oracle statement
         */
        virtual void deleteStatement(oracle::occi::Statement* stmt)
          throw (castor::exception::Exception);
        
      private:

        /// SQL statement for function selectTape
        static const std::string s_selectTapeStatementString;

        /// SQL statement object for function selectTape
        oracle::occi::Statement *m_selectTapeStatement;

        /// SQL statement for function selectSvcClass
        static const std::string s_selectSvcClassStatementString;

        /// SQL statement object for function selectSvcClass
        oracle::occi::Statement *m_selectSvcClassStatement;

        /// SQL statement for function selectFileClass
        static const std::string s_selectFileClassStatementString;

        /// SQL statement object for function selectFileClass
        oracle::occi::Statement *m_selectFileClassStatement;

        /// SQL statement for function selectFileSystem
        static const std::string s_selectFileSystemStatementString;

        /// SQL statement object for function selectFileSystem
        oracle::occi::Statement *m_selectFileSystemStatement;

      }; // end of class OraCommonSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORACOMMONSVC_HPP
