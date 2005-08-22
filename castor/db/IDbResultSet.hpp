/******************************************************************************
 *                      IDbResultSet.hpp
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
 * @(#)$RCSfile: IDbResultSet.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/08/22 16:26:40 $ $Author: itglp $
 *
 * 
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#ifndef CASTOR_DB_IDBRESULTSET_HPP
#define CASTOR_DB_IDBRESULTSET_HPP

#include <string>
#include "osdep.h"

namespace castor {
    
	namespace db {

/**
 * Interface IDbResultSet
 * 
 */
class IDbResultSet {

	public:
    /**
     * 
     */
    virtual bool next() = 0;
    
    /**
     * 
     * @param i 
     */
    virtual int getInt(int i) = 0;
    virtual u_signed64 getInt64(int i) = 0;
    virtual std::string getString(int i) = 0;
    virtual float getFloat(int i) = 0;
    virtual double getDouble(int i) = 0;
};

}

} 

#endif // CASTOR_DB_IDBRESULTSET_HPP

