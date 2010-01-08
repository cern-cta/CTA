/******************************************************************************
 *                      SvcClassCounter.hpp
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
 * @(#)$RCSfile: SvcClassCounter.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2008/06/03 15:54:42 $ $Author: waldron $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_JOBMANAGER_SVCCLASSCOUNTER_HPP
#define CASTOR_JOBMANAGER_SVCCLASSCOUNTER_HPP 1

// Include Files
#include <string>
#include "castor/metrics/Counter.hpp"

namespace castor {
  
  namespace jobmanager {

    class SvcClassCounter : public castor::metrics::Counter {

    public:    
    
      /// Factory method
      static castor::metrics::Counter* instantiate(castor::IObject* obj);
      
      /// Default constructor
      SvcClassCounter(castor::IObject* obj);
      
      /// Default destructor
      ~SvcClassCounter() {};
    
      /// match
      virtual int match(castor::IObject* obj);
      
    };

  } // end of namespace jobmanager
  
} // end of namespace castor

#endif // CASTOR_JOBMANAGER_SVCCLASSCOUNTER_HPP
