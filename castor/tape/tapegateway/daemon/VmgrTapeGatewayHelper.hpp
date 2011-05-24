/******************************************************************************
 *                      VmgrTapeGatewayHelper.hpp
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
 * @(#)$RCSfile: VmgrTapeGatewayHelper.hpp,v $ $Revision: 1.9 $ $Release$ $Date: 2009/05/18 13:52:38 $ $Author: gtaur $
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef TAPEGATEWAY_VMGRTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_VMGRTAPEGATEWAYHELPER_HPP 

// Include Files
#include "osdep.h"

#include "castor/exception/Exception.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapePool.hpp"

#include "castor/tape/tapegateway/FileMigratedNotification.hpp"


namespace castor {

  namespace tape {
    
    namespace tapegateway {

      class VmgrTapeGatewayHelper {
        public:
	
	void getTapeForStream(const castor::stager::Stream& streamToResolve, const castor::stager::TapePool& tapepool, int& startFseq, castor::stager::Tape& tapeToUse ) throw (castor::exception::Exception);
	
	void getDataFromVmgr(castor::stager::Tape& tape) throw (castor::exception::Exception);
	void getTapeStatusInVmgr(const castor::stager::Tape& tape,int& tapeStatus) throw (castor::exception::Exception);	
	void resetBusyTape(const castor::stager::Tape& tape) throw (castor::exception::Exception);
	void updateTapeInVmgr(const castor::tape::tapegateway::FileMigratedNotification& file,const std::string& vid) throw (castor::exception::Exception);
	void setTapeAsFull(const castor::stager::Tape& tape) throw (castor::exception::Exception);
        void setTapeAsReadonly(const castor::stager::Tape& tape) throw (castor::exception::Exception);
      };
    
    } // end of namespace tapegateway

  } // end of namespace tape

}  // end of namespace castor

#endif // TAPEGATEWAY_VMGRTAPEGATEWAYHELPER_HPP 
