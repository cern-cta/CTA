/******************************************************************************
 *                      OraStatement.hpp
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
 * @(#)$RCSfile: OraStatement.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2007/12/20 10:36:33 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#ifndef CASTOR_ORASTATEMENT_HPP
#define CASTOR_ORASTATEMENT_HPP

#include <string>
#include "castor/db/IDbStatement.hpp"
#include "castor/db/newora/OraCnvSvc.hpp"
#include "occi.h"

namespace castor {
  namespace db {
    namespace ora {

      /**
       * Oracle implementation for IDbStatement
       */
      class OraStatement : public virtual castor::db::IDbStatement {

      public:

        OraStatement(oracle::occi::Statement* stmt, castor::db::ora::OraCnvSvc* cnvSvc);

        /**
         * Standard destructor. Closes the statement
         */
        virtual ~OraStatement();

        virtual void autoCommit();
        
        /**
         * Setter methods
         * @param pos
         * @param value
         */
        virtual void setInt(int pos, int value);
        virtual void setInt64(int pos, signed64 value);
        virtual void setUInt64(int pos, u_signed64 value);
        virtual void setString(int pos, std::string value);
        virtual void setClob(int pos, std::string value);
        virtual void setFloat(int pos, float value);
        virtual void setDouble(int pos, double value);

        virtual void registerOutParam(int pos, int dbType)
          throw (castor::exception::SQLError);

        /**
         * Getter methods
         * @param pos
         * @return value
         */
        virtual int getInt(int pos) throw (castor::exception::SQLError);
        virtual signed64 getInt64(int pos) throw (castor::exception::SQLError);
        virtual u_signed64 getUInt64(int pos) throw (castor::exception::SQLError);
        virtual std::string getString(int pos) throw (castor::exception::SQLError);
        virtual std::string getClob(int pos) throw (castor::exception::SQLError);
        virtual float getFloat(int pos) throw (castor::exception::SQLError);
        virtual double getDouble(int pos) throw (castor::exception::SQLError);

        /**
         *
         */
        virtual IDbResultSet* executeQuery()
          throw (castor::exception::SQLError);

        virtual void execute()
          throw (castor::exception::SQLError);

        inline oracle::occi::Statement* getStatementImpl() {
          return m_statement;
        }

      private:
      
        oracle::occi::Statement *m_statement;
        
        castor::db::ora::OraCnvSvc* m_cnvSvc;
      };

    }
  }
}
#endif // CASTOR_ORASTATEMENT_HPP

