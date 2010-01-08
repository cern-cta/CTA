/******************************************************************************
 *                      IDbStatement.hpp
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
 * @(#)$RCSfile: IDbStatement.hpp,v $ $Revision: 1.16 $ $Release$ $Date: 2009/03/26 14:30:11 $ $Author: itglp $
 *
 * 
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#ifndef CASTOR_DB_IDBSTATEMENT_HPP
#define CASTOR_DB_IDBSTATEMENT_HPP

#include <string>
#include <vector>
#include "osdep.h"
#include "castor/db/IDbResultSet.hpp"
#include "castor/exception/SQLError.hpp"

namespace castor {

  namespace db { 

    /**
     * Interface IDbStatement
     * 
     */
    class IDbStatement {
      
    public:
      
      virtual ~IDbStatement() {};
    
      virtual void endTransaction() = 0;
        
      /**
       * Sets a parameter in the prepared statement 
       * @param pos the index position of the parameter
       * @param value its value
       */
      virtual void setInt(int pos, int value) = 0;
      virtual void setInt64(int pos, signed64 value) = 0;
      virtual void setUInt64(int pos, u_signed64 value) = 0;
      virtual void setString(int pos, std::string value) = 0;
      virtual void setFloat(int pos, float value) = 0;
      virtual void setDouble(int pos, double value) = 0;
      virtual void setClob(int pos, std::string value) = 0;
      virtual void setNull(int pos) = 0;
        
      virtual void setDataBuffer(int pos, void* buffer, unsigned dbType, unsigned size, void* bufLens)
        throw (castor::exception::SQLError) = 0;
        
      virtual void setDataBufferArray(int pos, void* buffer, unsigned dbType, 
				      unsigned size, unsigned elementSize, void* bufLens)
        throw (castor::exception::SQLError) = 0;

      virtual void setDataBufferUInt64Array(int pos, std::vector<u_signed64> data)
        throw (castor::exception::SQLError) = 0;

      virtual void registerOutParam(int pos, unsigned dbType)
        throw (castor::exception::SQLError) = 0;
    
      virtual int getInt(int pos) throw (castor::exception::SQLError) = 0;
      virtual signed64 getInt64(int pos) throw (castor::exception::SQLError) = 0;
      virtual u_signed64 getUInt64(int pos) throw (castor::exception::SQLError) = 0;
      virtual std::string getString(int pos) throw (castor::exception::SQLError) = 0;
      virtual float getFloat(int pos) throw (castor::exception::SQLError) = 0;
      virtual double getDouble(int pos) throw (castor::exception::SQLError) = 0;
      virtual std::string getClob(int pos) throw (castor::exception::SQLError) = 0;
      virtual castor::db::IDbResultSet* getCursor(int pos) throw (castor::exception::SQLError) = 0;
    
      /**
       * 
       */
      virtual castor::db::IDbResultSet* executeQuery()
        throw (castor::exception::SQLError) = 0;
    
      /**
       * 
       */
      virtual int execute(int count = 1)
        throw (castor::exception::SQLError) = 0;
    
    };

    const unsigned DBTYPE_INT = 1;
    const unsigned DBTYPE_INT64 = 2;
    const unsigned DBTYPE_UINT64 = 3;
    const unsigned DBTYPE_FLOAT = 4;
    const unsigned DBTYPE_DOUBLE = 5;
    const unsigned DBTYPE_STRING = 6;
    const unsigned DBTYPE_CLOB = 7;
    const unsigned DBTYPE_CURSOR = 8;
    const unsigned DBTYPE_MAXVALUE = 8;

  }

}

#endif // CASTOR_DB_IDBSTATEMENT_HPP
