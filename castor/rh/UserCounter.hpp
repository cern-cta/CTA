/******************************************************************************
 *                      UserCounter.hpp
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
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_RH_USERCOUNTER_HPP
#define CASTOR_RH_USERCOUNTER_HPP 1

// Include Files
#include <string>
#include "castor/metrics/Counter.hpp"

namespace castor {
  
  namespace rh {

    class UserCounter : public castor::metrics::Counter {

    public:    
    
      /// Factory method
      static castor::metrics::Counter* instantiate(castor::IObject* obj);
      
      /// Default constructor
      UserCounter(castor::IObject* obj);
      
      /// Default destructor
      ~UserCounter() {};
    
      /// match
      virtual int match(castor::IObject* obj);
      
    protected:
      
      /// uid,gid identifying a user
      unsigned int m_uid, m_gid;
    
    };

  } // end of namespace rh
  
} // end of namespace castor

#endif // CASTOR_RH_USERCOUNTER_HPP
