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
#include "osdep.h"
#include "castor/exception/SQLError.hpp"

namespace castor {
    
	namespace db {

    /**
     * Interface IDbResultSet
     * 
     */
    class IDbResultSet {
    
      public:
    
        /**
         * Default destructor 
         */
        virtual ~IDbResultSet() {};
    
        /**
         * 
         */
        virtual bool next(int count = 1)  = 0;
        
        /**
         * 
         * @param i 
         */
        virtual int getInt(int i)  = 0;
        virtual signed64 getInt64(int i)  = 0;
        virtual u_signed64 getUInt64(int i)  = 0;
        virtual std::string getString(int i)  = 0;
        virtual std::string getClob(int i)  = 0;
        virtual float getFloat(int i)  = 0;
        virtual double getDouble(int i)  = 0;
        
        /**
         *
         */
        virtual void setDataBuffer(int pos, void* buffer, unsigned dbType, unsigned size, void* bufLen)
           = 0;
    };

  }

} 


