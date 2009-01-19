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
 * @(#)$RCSfile: NsTapeGatewayHelper.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2009/01/19 17:20:33 $ $Author: gtaur $
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP 

// Include Files


#include "castor/tape/tapegateway/NsFileInformation.hpp"
#include "castor/tape/tapegateway/FileMigratedResponse.hpp"
#include "castor/tape/tapegateway/FileRecalledResponse.hpp"
#include "castor/tape/tapegateway/FileToRecallResponse.hpp"
#include "castor/tape/tapegateway/FileToMigrateResponse.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace tape {
    
    namespace tapegateway {

      class NsTapeGatewayHelper {
        public:
	
	int updateMigratedFile(castor::tape::tapegateway::FileMigratedResponse* file) throw (castor::exception::Exception);
     
	int updateRepackedFile( tape::tapegateway::FileMigratedResponse* file , std::string repackVid ) throw (castor::exception::Exception);

	int  checkRecalledFile(castor::tape::tapegateway::FileRecalledResponse* file) throw (castor::exception::Exception);
      
	int  checkFileToMigrate(castor::tape::tapegateway::FileToMigrateResponse* file) throw (castor::exception::Exception);
      
	int  checkFileToRecall(tape::tapegateway::FileToRecallResponse* file) throw (castor::exception::Exception);

	int  getFileOwner(castor::tape::tapegateway::NsFileInformation* file, u_signed64 &uid, u_signed64 &gid ) throw (castor::exception::Exception);

      };
    
    } // end of namespace tapegateway

  } // end of namespace tape

}  // end of namespace castor

#endif // TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP 
