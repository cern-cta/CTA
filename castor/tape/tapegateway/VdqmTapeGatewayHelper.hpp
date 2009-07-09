/******************************************************************************
 *                      VdqmTapeGatewayHelper.hpp
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
 * @(#)$RCSfile: VdqmTapeGatewayHelper.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/07/09 15:47:11 $ $Author: gtaur $
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef TAPEGATEWAY_VDQMTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_VDQMTAPEGATEWAYHELPER_HPP 

// Include Files

#include "osdep.h"

#include "castor/exception/Exception.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/tape/tapegateway/TapeGatewayRequest.hpp"

namespace castor {

  namespace tape {
    
    namespace tapegateway {

      class VdqmTapeGatewayHelper {
        public:
	
	int submitTapeToVdqm( castor::stager::Tape* tape, std::string dgn, int port) throw (castor::exception::Exception);
	void checkVdqmForRequest(TapeGatewayRequest* tapeRequest) throw (castor::exception::Exception);

      };
    
    } // end of namespace tapegateway

  } // end of namespace tape

}  // end of namespace castor

#endif // TAPEGATEWAY_VDQMTAPEGATEWAYHELPER_HPP 
