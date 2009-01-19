/******************************************************************************
 *                castor/rtcopy/mighunter/IRecHandlerSvc.hpp
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
 * @(#)$RCSfile: IRecHandlerSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2009/01/19 17:27:46 $ $Author: gtaur $
 *
 * This class provides methods related to tape handling
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef IRECHANDLERSVC_HPP
#define IRECHANDLERSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"
#include <vector>
#include <string>
#include <list>

namespace castor {

  // Forward declaration
  class IObject;
  class IClient;
  class IAddress;

  namespace rtcopy {
    namespace rechandler {


    /**
     * This class provides methods used by rechandler
     */

    class IRecHandlerSvc : public virtual castor::stager::ICommonSvc {

    public:
     

	/**                    
	 * inputForRecallPolicy 
	 */

        virtual std::vector<castor::infoPolicy::PolicyObj*>  inputForRecallPolicy() throw (castor::exception::Exception)=0;

        /**
         * Resurrect Tapes
         */

	virtual void resurrectTapes(std::vector<u_signed64> eligibleTapeIds) throw (castor::exception::Exception)=0;


    }; // end of class IRecHandlerSvc
    
    } // end of namespace rechandler

  } // end of namespace rtcopy

} // end of namespace castor

#endif // IRECHANDLERSVC_HPP
