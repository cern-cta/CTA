/******************************************************************************
 *                castor/db/DbCommonSvc.hpp
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
 * @(#)$RCSfile: DbCommonSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/08/18 10:01:36 $ $Author: itglp $
 *
 * Implementation of the ICommonSvc for CDBC
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef DB_DBCOMMONSVC_HPP
#define DB_DBCOMMONSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/db/IDbStatement.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include <vector>

namespace castor {

  namespace db {

      /**
       * Implementation of the ICommonSvc for CDBC
       */
      class DbCommonSvc : public BaseSvc,
                          public castor::db::DbBaseObj,
                          public virtual castor::stager::ICommonSvc {

      public:

        /**
         * default constructor
         */
        DbCommonSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~DbCommonSvc() throw();

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
         * Selects the next request the stager should deal with.
         * Selects a Request in START status and move its status
         * PROCESSED to avoid double processing.
         * The selection is restricted to Request of a given set
         * of types.
         * @param types the list of accepted types for the request
         * @return the Request to process
         * @exception Exception in case of error
         */
        virtual castor::stager::Request* requestToDo
        (std::vector<ObjectsIds> &types)
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
        
      private:

        /// SQL statement for function selectTape
        static const std::string s_selectTapeStatementString;

        /// SQL statement object for function selectTape
        castor::db::IDbStatement *m_selectTapeStatement;

        /// SQL statement object for function requestToDo
        castor::db::IDbStatement *m_requestToDoStatement;

        /// SQL statement for function selectSvcClass
        static const std::string s_selectSvcClassStatementString;

        /// SQL statement object for function selectSvcClass
        castor::db::IDbStatement *m_selectSvcClassStatement;

        /// SQL statement for function selectFileClass
        static const std::string s_selectFileClassStatementString;

        /// SQL statement object for function selectFileClass
        castor::db::IDbStatement *m_selectFileClassStatement;

        /// SQL statement for function selectFileSystem
        static const std::string s_selectFileSystemStatementString;

        /// SQL statement object for function selectFileSystem
        castor::db::IDbStatement *m_selectFileSystemStatement;

      }; // end of class DbCommonSvc

  } // end of namespace db

} // end of namespace castor

#endif // DB_DBCOMMONSVC_HPP
