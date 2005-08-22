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
 * @(#)$RCSfile: OraResultSet.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/08/22 16:32:34 $ $Author: itglp $
 *
 * 
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#ifndef CASTOR_ORARESULTSET_HPP
#define CASTOR_ORARESULTSET_HPP

#include <string>
#include "occi.h"

#include "castor/db/IDbResultSet.hpp"


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
     * 
     */
    OraResultSet(oracle::occi::ResultSet* rset, oracle::occi::Statement* statement);

    /**
     * 
     */
    virtual ~OraResultSet();

    /**
     * 
     */
    virtual bool next();
    
    /**
     * 
     * @param i 
     */
    virtual int getInt(int i);
    virtual u_signed64 getInt64(int i);
    virtual std::string getString(int i);
    virtual float getFloat(int i);
    virtual double getDouble(int i);
	
	private:
    oracle::occi::ResultSet *m_rset;	
    oracle::occi::Statement *m_statement;	
};

}
}
}
#endif // CASTOR_ORARESULTSET_HPP

