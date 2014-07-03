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
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#pragma once

#include <string>
#include "castor/db/IDbStatement.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "occi.h"

namespace castor {

  namespace db {

    namespace ora {

      static const oracle::occi::Type oraTypeMap[] = {
        (oracle::occi::Type)0,
        oracle::occi::OCCIINT,
        oracle::occi::OCCIDOUBLE,
        oracle::occi::OCCIDOUBLE,
        oracle::occi::OCCIFLOAT,
        oracle::occi::OCCIDOUBLE,
        oracle::occi::OCCISTRING,
        oracle::occi::OCCICLOB,
        oracle::occi::OCCICURSOR
      };

      static const oracle::occi::Type oraBulkTypeMap[] = {
        (oracle::occi::Type)0,
        oracle::occi::OCCIINT,
        oracle::occi::OCCIBDOUBLE,
        oracle::occi::OCCIBDOUBLE,
        oracle::occi::OCCIBFLOAT,
        oracle::occi::OCCIBDOUBLE,
        oracle::occi::OCCI_SQLT_STR,
        oracle::occi::OCCI_SQLT_CLOB,  // but this one is not yet supported
        oracle::occi::OCCICURSOR       // and this is never used for bulk operations
      };

      /**
       * Oracle implementation for IDbStatement
       */
      class OraStatement : public virtual castor::db::IDbStatement {

      public:

        OraStatement(oracle::occi::Statement* stmt, castor::db::ora::OraCnvSvc* cnvSvc) throw();

        /**
         * Standard destructor. Closes the statement
         */
        virtual ~OraStatement() throw();

        virtual void endTransaction();

        /**
         * Setter methods
         * @param pos
         * @param value
         */
        virtual void setInt(int pos, int value);
        virtual void setInt64(int pos, signed64 value);
        virtual void setUInt64(int pos, u_signed64 value);
        virtual void setString(int pos, std::string value);
        virtual void setFloat(int pos, float value);
        virtual void setDouble(int pos, double value);
        virtual void setClob(int pos, std::string value);
        virtual void setNull(int pos);

        virtual void setDataBuffer(int pos, void* buffer, unsigned dbType, unsigned size, void* bufLens)
          ;

        virtual void setDataBufferArray(int pos, void* buffer, unsigned dbType,
          unsigned size, unsigned elementSize, void* bufLens)
          ;

        virtual void setDataBufferUInt64Array(int pos, std::vector<u_signed64> data)
          ;

        virtual void registerOutParam(int pos, unsigned dbType)
          ;

        /**
         * Getter methods
         * @param pos
         * @return value
         */
        virtual int getInt(int pos) ;
        virtual signed64 getInt64(int pos) ;
        virtual u_signed64 getUInt64(int pos) ;
        virtual std::string getString(int pos) ;
        virtual float getFloat(int pos) ;
        virtual double getDouble(int pos) ;
        virtual std::string getClob(int pos) ;
        virtual castor::db::IDbResultSet* getCursor(int pos) ;

        /**
         *
         */
        virtual IDbResultSet* executeQuery()
          ;

        virtual int execute(int count = 1)
          ;

        inline oracle::occi::Statement* getStatementImpl() {
          return m_statement;
        }

      private:

        oracle::occi::Statement *m_statement;

        castor::db::ora::OraCnvSvc* m_cnvSvc;

        std::string m_clobBuf;

        unsigned m_clobPos;

        void* m_arrayBuf;
        void* m_arrayBufLens;

        unsigned m_arrayPos;

        /**
         * a placeholder for the array size needed by setDataBufferArray
         * setDataBufferArray allocates it if needed and it is then
         * reused and only dropped when the Statement is dropped
         */
        ub4* m_arraySize;
      };

    }
  }
}

