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
 * @(#)$RCSfile: IDbStatement.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/08/18 10:09:42 $ $Author: itglp $
 *
 * 
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#ifndef CASTOR_DB_IDBSTATEMENT_HPP
#define CASTOR_DB_IDBSTATEMENT_HPP

#include <string>
#include "osdep.h"
#include "castor/db/IDbResultSet.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {
	namespace db { 

/**
 * Interface IDbStatement
 * 
 */
class IDbStatement {
	
	public:
	
    virtual void autoCommit() = 0;
    /**
     * 
     * @param pos 
     * @param value 
     */
    virtual void setInt(int pos, int value) = 0;
    virtual void setInt64(int pos, u_signed64 value) = 0;
    virtual void setString(int pos, std::string value) = 0;
    virtual void setFloat(int pos, float value) = 0;
    virtual void setDouble(int pos, double value) = 0;
    
    virtual void registerOutParam(int pos, int dbType) = 0;

    virtual int getInt(int pos) = 0;
    virtual u_signed64 getInt64(int pos) = 0;
    virtual std::string getString(int pos) = 0;
    virtual float getFloat(int pos) = 0;

    /**
     * 
     */
    virtual castor::db::IDbResultSet* executeQuery()
	  throw (castor::exception::Exception) = 0;

    /**
     * 
     */
    virtual int execute()
	  throw (castor::exception::Exception) = 0;

};

static int DBTYPE_INT = 1;
static int DBTYPE_INT64 = 2;
static int DBTYPE_DOUBLE = 3;
static int DBTYPE_STRING = 4;

}
} 
#endif // CASTOR_DB_IDBSTATEMENT_HPP

