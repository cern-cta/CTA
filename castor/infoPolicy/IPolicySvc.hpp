/******************************************************************************
 *                castor/infoPolicy/ITapeSvc.hpp
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
 * @(#)$RCSfile: IPolicySvc.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/11/14 16:53:31 $ $Author: gtaur $
 *
 * This class provides methods related to tape handling
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_IPOLICYSVC_HPP
#define STAGER_IPOLICYSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"
#include <vector>
#include <string>
#include <list>

namespace castor {

  // Forward declaration
  class IObject;
  class IClient;
  class IAddress;

  namespace infoPolicy {


    /**
     * This class provides methods related to tape handling
     */

    class IPolicySvc : public virtual castor::stager::ICommonSvc {

    public:
     
	/**
         *  inputForMigrationPolicy
         */

	virtual std::vector<castor::infoPolicy::PolicyObj*> inputForMigrationPolicy(std::string svcClassName, u_signed64* byteThres) throw (castor::exception::Exception)=0;
              
        /**
         * createOrUpdateStream 
         */ 

    virtual int createOrUpdateStream(std::string svcClassId, u_signed64 initialSizeToTransfer, u_signed64 volumeThreashold, u_signed64 initialSizeCeiling,bool doClone, std::vector<castor::infoPolicy::PolicyObj*>  tapeCopyIds) throw (castor::exception::Exception)=0;

         /**
         * inputForStreamPolicy,
         */

        virtual std::vector<castor::infoPolicy::PolicyObj*> inputForStreamPolicy(std::string svcClassName)throw (castor::exception::Exception)=0;

          /**
         * startChosenStreams 
         */

	virtual void startChosenStreams(std::vector<PolicyObj*> outputFromStreamPolicy,u_signed64 initialSize) throw (castor::exception::Exception)=0;


	/**                    
	 * inputForRecallPolicy 
	 */

        virtual std::vector<castor::infoPolicy::PolicyObj*>  inputForRecallPolicy() throw (castor::exception::Exception)=0;

        /**
         * Resurrect Tapes
         */

	virtual void resurrectTapes(std::vector<u_signed64> eligibleTapeIds) throw (castor::exception::Exception)=0;

	/** 
	 * Attach TapeCopies To Streams
	 */
	
	virtual void  attachTapeCopiesToStreams(std::vector<PolicyObj*> outputFromMigrationPolicy) throw (castor::exception::Exception)=0;

      	/**
	 * resurrect tape copies 
	 */

	virtual void resurrectTapeCopies(std::vector<PolicyObj*> tapeCopiesInfo) throw (castor::exception::Exception)=0;


    }; // end of class IPolicySvc

  } // end of namespace infoPolicy

} // end of namespace castor

#endif // STAGER_IPOLICYSVC_HPP
