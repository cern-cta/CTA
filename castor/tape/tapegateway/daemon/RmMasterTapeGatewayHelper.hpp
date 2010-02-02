/******************************************************************************
 *                castor/tape/tapegateway/RmMasterTapeGatewayHelper.hpp
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
 * @(#)$RCSfile: RmMasterTapeGatewayHelper.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2009/07/23 12:22:01 $ $Author: waldron $
 *
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef TAPEGATEWAY_RMMASTERTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_RMMASTERTAPEGATEWAYHELPER_HPP 1

// Include Files

#include "castor/exception/Exception.hpp"
#include "castor/monitoring/StreamDirection.hpp"


namespace castor {

  namespace tape {
    
    namespace tapegateway {
      
      class RmMasterTapeGatewayHelper {
	
      public:
	
	/**
	 * default constructor
	 */
	RmMasterTapeGatewayHelper() throw(castor::exception::Exception);
	
	/**
	 * Sends a UDP message to the rmmaster daemon to inform it
	 * of the creation or deletion of a stream on a given
	 * machine/filesystem. This method is always successful
	 * although there is no guaranty that the UDP package is
	 * sent and arrives.
	 * @param diskserver the diskserver where the stream resides
	 * @param filesystem the filesystem where the stream resides
	 * @param direction the stream direction (read, write or readWrite)
	 * @param created whether the stream was created or deleted
	 */

	virtual void sendStreamReport(const std::string diskServer,
				      const std::string fileSystem,
				      const castor::monitoring::StreamDirection direction,
				      const bool created) throw(castor::exception::Exception);

	virtual ~RmMasterTapeGatewayHelper(){};
	
      private:

	/// the rmMaster host
	std::string m_rmMasterHost;

	/// the rmMaster port
	int m_rmMasterPort;
	
      }; // end of class RmMasterTapeGatewayHelper 
    } // end of namespace tapegateway
  } // end of namespace tape
  
} // end of namespace castor

#endif // TAPEGATEWAY_RMMASTERTAPEGATEWAYHELPER_HPP
