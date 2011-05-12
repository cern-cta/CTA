/******************************************************************************
 *                      DbCnvSvc.hpp
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
 * @(#)$RCSfile: DbCnvSvc.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2009/06/17 14:57:27 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_DBCNVSVC_HPP
#define CASTOR_DBCNVSVC_HPP 1

// Include Files
#include "castor/db/IDbStatement.hpp"
#include "castor/BaseCnvSvc.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declarations
  class IAddress;
  class IObject;

  namespace db {

      /**
       * Conversion service for a generic Database via CDBC
       */
      class DbCnvSvc : public BaseCnvSvc {

      public:

        /** default constructor */
        explicit DbCnvSvc(const std::string name);

        /** default destructor */
        virtual ~DbCnvSvc() throw() {};

        /**
         * gets the representation type, that is the type of
         * the representation this conversion service can deal with
         */
        virtual unsigned int repType() const;

        /**
         * gets the representation type, that is the type of
         * the representation this conversion service can deal with
         */
        static unsigned int RepType();

        /**
         * resets the service
         */
        virtual void reset() throw();

        /**
         * create C++ object from foreign representation.
         * Reimplemented from BaseCnvSvc. This version is able to
         * make use of OraAdresses and to deduce the object type in
         * the address from the id by querying the database
         * @param address the place where to find the foreign
         * representation
         * @return the C++ object created from its reprensentation
         * or raises a NoEntry exception if not found.
         * Note that the caller is responsible
         * for the deallocation of the newly created object
         * @exception Exception throws an Exception in case of error
         */
        IObject* createObj (IAddress* address)
          throw (castor::exception::Exception);

        /**
         * Get an object from its id.
         * Essentially a wrapper around createObj
         * @param id the id of the object to retrieve
         * @exception Exception throws an Exception in case of error
         */
        castor::IObject* getObjFromId (u_signed64 id)
          throw (castor::exception::Exception);

        /**
         * Get a set of objects from a set of ids.
         * Essentially a wrapper around bulkCreateObj
         * @param ids the ids of the objects to retrieve
         * @param objType the type of the objects to retrieve
         * Note that they must all have the same one !
         * @exception Exception throws an Exception in case of error
         */
        std::vector<castor::IObject*>
        getObjsFromIds(std::vector<u_signed64> &ids,
                       int objType)
          throw (castor::exception::Exception);

        /**
         * Handles database exceptions and make sure everything is reset
         * so that next time a new connection is established.
         * Default implementation does nothing
         * @param e the database exception
         */
        virtual void handleException(std::exception) {};
          
        /**
         * Get the physical representation type from the
         * concrete class, that is the database type
         * to which this conversion service is connected.
         */
        virtual unsigned int getPhysRepType() const = 0;

      protected:

        /**
         * retrieves the type of an object given by its id
         */
        unsigned int getTypeFromId(const u_signed64 id)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for type retrieval
        static const std::string s_getTypeStatementString;

        /// SQL statement object for type retrieval
        castor::db::IDbStatement* m_getTypeStatement;

      };

  } // end of namespace db

} // end of namespace castor

#endif // CASTOR_DBCNVSVC_HPP
