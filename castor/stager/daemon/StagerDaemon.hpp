/******************************************************************************
 *                castor/stager/daemon/StagerDaemon.hpp
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
 * Main stager daemon
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef STAGER_MAIN_DAEMON_HPP
#define STAGER_MAIN_DAEMON_HPP 1


#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"

#include <errno.h>
#include "serrno.h"

#include <iostream>
#include <string>

namespace castor{
  
  namespace stager{
    
    namespace daemon{

      class StagerDaemon : public castor::server::BaseDaemon {

      public:
        /*** constructor ***/
        StagerDaemon() throw (castor::exception::Exception);
        /*** destructor ***/
        virtual ~StagerDaemon() throw() {};
      
        void help(std::string programName);
        
      protected:
        
        virtual void waitAllThreads() throw();

      }; /* end class StagerDaemon */

    }//end namespace daemon

  }//end namespace stager

}//end namespace castor

#endif
