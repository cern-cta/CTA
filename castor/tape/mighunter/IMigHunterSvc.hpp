/******************************************************************************
 *                castor/tape/mighunter/IMigHunterSvc.hpp
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
#include "castor/tape/mighunter/IdToTapePoolForStreamPolicyMap.hpp"
#include "castor/tape/mighunter/MigrationPolicyElement.hpp"
#include "castor/tape/mighunter/StreamPolicyElement.hpp"
#include "castor/tape/mighunter/StreamForStreamPolicyList.hpp"

#include <string>
#include <list>

namespace castor {

  // Forward declaration
  class IObject;
  class IClient;
  class IAddress;

  namespace tape {
    namespace  mighunter {


    /**
     * This class provides methods used by the mighunter
     */

    class IMigHunterSvc : public virtual stager::ICommonSvc {

    public:
     
	/**
         *  inputForMigrationPolicy
         */

      virtual void inputForMigrationPolicy(std::string svcClassName, u_signed64* byteThres,std::list<castor::tape::mighunter::MigrationPolicyElement>& candidates  ) throw (castor::exception::Exception)=0;
              
        /**
         * createOrUpdateStream 
         */ 

      virtual int createOrUpdateStream(std::string svcClassId, u_signed64 initialSizeToTransfer, u_signed64 volumeThreashold, u_signed64 initialSizeCeiling,bool doClone, int tapeCopyNb) throw (castor::exception::Exception)=0;

      /**
       * Retrieves the candidate streams to be passed to the stream-policy
       * Python-function of the specified service-class.
       *
       * @param svcClassName     Input: The name of the service-class for which
       *                         the candidate streams are to be retrieved.
       * @param svcClassId       Output: The database ID of the specified
       *                         service-class.
       * @param streamPolicyName Output: The name of the stream-policy
       *                         Python-function for the specified
       *                         service-class.
       * @param nbDrives         Output: The maximum number of drives the
       *                         stream-policy can have running at any single
       *                         moment in time.  This value is for debugging
       *                         purposes only as the stream-policy is driven
       *                         by existing streams and cannot create new ones.
       * @param streamsForPolicy Output: The list of candidate streams to
       *                         be passed to the stream-policy Python-function
       *                         of the specified service-class.
       */
      virtual void inputForStreamPolicy(
        const std::string         &svcClassName,
        u_signed64                &svcClassId,
        std::string               &streamPolicyName,
        u_signed64                &nbDrives,
        StreamForStreamPolicyList &streamsForPolicy)
        throw (castor::exception::Exception)=0;

      /**
       * Retrieves the map of database IDs to tape-pool names and numbers of
       * running streams required by the stream-policy of a service-class.
       *
       * @param svcClassName       Input: The database ID of the service-class
       *                           for which tape-pools are to be retrieved.
       * @param tapePoolsForPolicy Output: The map of retrieved tape-pools.
       */
      virtual void tapePoolsForStreamPolicy(
        const u_signed64               svcClassId,
        IdToTapePoolForStreamPolicyMap &tapePoolsForPolicy)
        throw (castor::exception::Exception)=0;

      /**
       * Starts the specified set of streams.
       */
      virtual void startChosenStreams(
        const std::list<StreamPolicyElement>& outputFromStreamPolicy)
        throw (castor::exception::Exception)=0;

      /**
       * Starts the specified set of streams.
       *
       * @param streamIds The database IDs of the streams to be started.
       */
      virtual void startChosenStreams(const std::list<u_signed64> &streamIds)
        throw (castor::exception::Exception)=0;

      /**
       * stopChosenStreams 
       */
      virtual void stopChosenStreams(
        const std::list<StreamPolicyElement>& outputFromStreamPolicy)
        throw (castor::exception::Exception)=0;

      /**
       * Stops the specified set of streams.
       *
       * @param streamIds The database IDs of the streams to be stopped.
       */
      virtual void stopChosenStreams(const std::list<u_signed64> &streamIds)
        throw (castor::exception::Exception)=0;

	/** 
	 * Attach TapeCopies To Streams
	 */
	
	virtual void  attachTapeCopiesToStreams(const std::list<MigrationPolicyElement>& outputFromMigrationPolicy) throw (castor::exception::Exception)=0;

      	/**
	 * resurrect tape copies 
	 */

	virtual void resurrectTapeCopies(const std::list<MigrationPolicyElement>& tapeCopiesInfo) throw (castor::exception::Exception)=0;

      /** 
       * invalidate tapecopies
       */

        virtual void invalidateTapeCopies(const std::list<MigrationPolicyElement>& tapeCopiesInfo) throw (castor::exception::Exception)=0;

      /** 
       * migHunterCleanUp
       */

        virtual void migHunterCleanUp(std::string svcClassName) throw (castor::exception::Exception)=0;

    }; // end of class IPolicySvc

    } // end of namespace mighunter

  } // end of namespace tape

} // end of namespace castor

#endif // IMIGHUNTERSVC_HPP
