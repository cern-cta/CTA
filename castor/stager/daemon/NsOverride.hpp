/******************************************************************************
 *                castor/stager/daemon/NsOverride.hpp
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
 * Singleton class for the NameServer override feature
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_DAEMON_NSOVERRIDE_HPP
#define STAGER_DAEMON_NSOVERRIDE_HPP 1

#include "castor/IObject.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {
  
  namespace stager {
    
    namespace daemon {
      
      class NsOverride {
        
      public:
      
        static castor::stager::daemon::NsOverride* getInstance()
          throw ();
        
        std::string getCnsHost() {
          return m_cnsHost;
        }
        
        std::string getTargetCnsHost() {
          return m_targetCnsHost;
        }
	
      private:
      
        /// this singleton's instance
        static NsOverride* s_instance;
        
        /**
         * Default costructor
         */
        NsOverride() throw ();
       
        /**
         * Default destructor
         */
        ~NsOverride() throw() {};
        
        std::string m_cnsHost;
        
        std::string m_targetCnsHost;

      };
      
    } // end namespace daemon
    
  } // end namespace stager
  
} //end namespace castor

#endif
