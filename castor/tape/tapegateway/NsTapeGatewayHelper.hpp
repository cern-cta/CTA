/******************************************************************************
 *                      NsTapeGatewayHelper.hpp
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
 * @(#)$RCSfile: NsTapeGatewayHelper.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2009/02/13 08:51:32 $ $Author: gtaur $
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP 

// Include Files



#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace tape {
    
    namespace tapegateway {

      class NsTapeGatewayHelper {
        public:
	
	void updateMigratedFile(castor::tape::tapegateway::FileMigratedNotification* file) throw (castor::exception::Exception);
     
	void updateRepackedFile( tape::tapegateway::FileMigratedNotification* file , std::string repackVid ) throw (castor::exception::Exception);

	void  checkRecalledFile(castor::tape::tapegateway::FileRecalledNotification* file) throw (castor::exception::Exception);
      
	void  checkFileToMigrate(castor::tape::tapegateway::FileToMigrate* file, std::string vid) throw (castor::exception::Exception);
      
	void  getBlockIdToRecall(tape::tapegateway::FileToRecall* file, std::string vid) throw (castor::exception::Exception);

      };
    
    } // end of namespace tapegateway

  } // end of namespace tape

}  // end of namespace castor

#endif // TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP 
