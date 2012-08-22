/******************************************************************************
 *                      OraResultSet.hpp
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
 * @(#)$RCSfile: OraResultSet.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2008/06/19 15:12:42 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#ifndef CASTOR_ORARESULTSET_HPP
#define CASTOR_ORARESULTSET_HPP

#include <string>
#include "castor/db/IDbResultSet.hpp"
#include "occi.h"


namespace castor {
  namespace db {
    namespace ora {

      /**
       * Oracle implementation of IDbResultSet
       *
       */
      class OraResultSet : public virtual castor::db::IDbResultSet {

      public:

        /**
         * Default constructor
         */
        OraResultSet(oracle::occi::ResultSet* rset, oracle::occi::Statement* statement);

        /**
         * Default destructor
         */
        virtual ~OraResultSet();

        /**
         *
         */
        virtual bool next(int count = 1) throw (castor::exception::SQLError);

        /**
         *
         * @param i
         */
        virtual int getInt(int i) throw (castor::exception::SQLError);
        virtual signed64 getInt64(int i) throw (castor::exception::SQLError);
        virtual u_signed64 getUInt64(int i) throw (castor::exception::SQLError);
        virtual std::string getString(int i) throw (castor::exception::SQLError);
        virtual std::string getClob(int i) throw (castor::exception::Exception);
        virtual float getFloat(int i) throw (castor::exception::SQLError);
        virtual double getDouble(int i) throw (castor::exception::SQLError);

        /**
         *
         */
        virtual void setDataBuffer(int pos, void* buffer, unsigned dbType, unsigned size, void* bufLen)
          throw (castor::exception::SQLError);
        
      private:
        
        oracle::occi::ResultSet *m_rset;
        
        oracle::occi::Statement *m_statement;
      };

    }
  }
}
#endif // CASTOR_ORARESULTSET_HPP

