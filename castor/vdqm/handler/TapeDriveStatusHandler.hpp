/******************************************************************************
 *                      TapeDriveStatusHandler.hpp
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
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _TAPEDRIVESTATUSHANDLER_HPP_
#define _TAPEDRIVESTATUSHANDLER_HPP_

#include "BaseRequestHandler.hpp"


namespace castor {

  namespace vdqm {

    //Forward declaration
    class TapeDrive;

    namespace handler {
      /**
       * The TapeDriveStatusHandler is only used by the TapeDriveHandler
       * class. It handles the status, which has been send from the tdaemon
       * to keep the db up to date.
       * Note that this class is not used outside of this namespace!
       */
      class TapeDriveStatusHandler : public BaseRequestHandler {
        
        /**
         * Like a real friend, TapeDriveHandler will respect the privacy 
         * wishes of this class ;-)
         */
        friend class TapeDriveHandler;
  
        public:
        
          /**
           * Entry point for handling the status, which has been send from
           * the tape daemon to vdqm.
           * 
           * @exception In case of error
           */
          void handleOldStatus() throw (castor::exception::Exception);
  
  
        protected:
  
          /**
           * Constructor
           * 
           * @param tapeDrive The tape Drive, which needs a consistency check
           * @param driveRequest The TapeDriveRequest from the old protocol
           * @param cuuid The unique id of the request. Needed for dlf
           * @param newRequestId If a tape drive to tape allocation could be
           * reused, then this parameter will be set to the ID of the request
           * that reused the allocation, else 0.  Note that if an allocation
           * was reused then the database has already been updated and must
           * not be updated again as this would cause a race condition with
           * the RTCPJobSubmitter threads.
           * @exception In case of error
           */
          TapeDriveStatusHandler(castor::vdqm::TapeDrive *const tapeDrive, 
            vdqmDrvReq_t *const driveRequest, const Cuuid_t &cuuid,
            u_signed64 *const newRequestId) 
            throw(castor::exception::Exception);
          
          /**
           * Destructor
           */
          virtual ~TapeDriveStatusHandler() throw();
          
            
        private:
          castor::vdqm::TapeDrive *const ptr_tapeDrive;
          vdqmDrvReq_t *const ptr_driveRequest;
          const Cuuid_t &m_cuuid;
          u_signed64 *const ptr_newRequestId;
          
          
          /**
           * This function is only used internally from handleOldStatus() to 
           * handle the case that a VDQM_VOL_MOUNT request is beeing sent from
           * the client.
           * 
           * @exception In case of error
           */
          void handleVolMountStatus() throw (castor::exception::Exception);
          
          /**
           * This function is only used internally from handleOldStatus() to 
           * handle the case that a VDQM_VOL_UNMOUNT request is beeing sent from
           * the client.
           * 
           * @exception In case of error
           */
          void handleVolUnmountStatus() throw (castor::exception::Exception);
          
          /**
           * This function is only used internally from handleOldStatus() to 
           * handle the case that a VDQM_UNIT_RELEASE request is beeing sent 
           * from the client.
           * 
           * @exception In case of error
           */
          void handleUnitReleaseStatus() throw (castor::exception::Exception);                    
          
          /**
           * This function is only used internally from handleOldStatus() to 
           * handle the case that a VDQM_UNIT_FREE request is beeing sent 
           * from the client. It switches the TapeDrive to UNIT_UP status.
           * 
           * @exception In case of error
           */
          void handleUnitFreeStatus() throw (castor::exception::Exception);          
                        
      }; // class TapeDriveStatusHandler
    
    } // end of namespace handler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEDRIVESTATUSHANDLER_HPP_
