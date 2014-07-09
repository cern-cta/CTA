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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

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
        virtual bool next(int count = 1) ;

        /**
         *
         * @param i
         */
        virtual int getInt(int i) ;
        virtual signed64 getInt64(int i) ;
        virtual u_signed64 getUInt64(int i) ;
        virtual std::string getString(int i) ;
        virtual std::string getClob(int i) ;
        virtual float getFloat(int i) ;
        virtual double getDouble(int i) ;

        /**
         *
         */
        virtual void setDataBuffer(int pos, void* buffer, unsigned dbType, unsigned size, void* bufLen)
          ;
        
      private:
        
        oracle::occi::ResultSet *m_rset;
        
        oracle::occi::Statement *m_statement;
      };

    }
  }
}

