/******************************************************************************
 *                castor/rtcopy/mighunter/IMigHunterSvc.hpp
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
 * @(#)$RCSfile: IMigHunterSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2009/01/19 17:27:40 $ $Author: gtaur $
 *
 * This class provides methods related to tape handling
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef IMIGHUNTERSVC_HPP
#define IMIGHUNTERSVC_HPP 1

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
    namespace  mighunter {


    /**
     * This class provides methods used by the mighunter
     */

    class IMigHunterSvc : public virtual castor::stager::ICommonSvc {

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

	virtual void startChosenStreams(std::vector<castor::infoPolicy::PolicyObj*> outputFromStreamPolicy,u_signed64 initialSize) throw (castor::exception::Exception)=0;

          /**
         * stopChosenStreams 
         */

	virtual void stopChosenStreams(std::vector<castor::infoPolicy::PolicyObj*> outputFromStreamPolicy) throw (castor::exception::Exception)=0;

	/** 
	 * Attach TapeCopies To Streams
	 */
	
	virtual void  attachTapeCopiesToStreams(std::vector<castor::infoPolicy::PolicyObj*> outputFromMigrationPolicy) throw (castor::exception::Exception)=0;

      	/**
	 * resurrect tape copies 
	 */

	virtual void resurrectTapeCopies(std::vector<castor::infoPolicy::PolicyObj*> tapeCopiesInfo) throw (castor::exception::Exception)=0;

      /** 
       * invalidate tapecopies
       */

        virtual void invalidateTapeCopies(std::vector<castor::infoPolicy::PolicyObj*> tapeCopiesInfo) throw (castor::exception::Exception)=0;

      /** 
       * migHunterCleanUp
       */

        virtual void migHunterCleanUp(std::string svcClassName) throw (castor::exception::Exception)=0;

    }; // end of class IPolicySvc

    } // end of namespace mighunter

  } // end of namespace rtcopy

} // end of namespace castor

#endif // IMIGHUNTERSVC_HPP
