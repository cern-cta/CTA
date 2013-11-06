/******************************************************************************
 *                      TapeDriveHandler.hpp
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

#ifndef _TAPEDRIVEHANDLER_HPP_
#define _TAPEDRIVEHANDLER_HPP_

#include "castor/vdqm/DeviceGroupName.hpp"
#include "castor/vdqm/TapeAccessSpecification.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeDriveDedication.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/VdqmTape.hpp"
#include "castor/vdqm/handler/BaseRequestHandler.hpp"
#include "h/vdqm_messages.h"


namespace castor {

  namespace vdqm {

    //Forward declaration
    class TapeServer;
    class TapeDrive;
    class OldProtocolInterpreter;

    namespace handler {

      /**
       * The TapeDriveHandler provides functions to handle all vdqm related
       * tape drive issues. It handles for example the VDQM_DRV_REQ
       */
      class TapeDriveHandler : public BaseRequestHandler {
  
      public:
  
        /**
         * Constructor
         * 
         * @param header The header of the old Protocol
         * @param driveRequest The TapeDriveRequest from the old protocol
         * @param cuuid The unique id of the request. Needed for dlf
         */
        TapeDriveHandler(vdqmHdr_t *const header,
          vdqmDrvReq_t *const driveRequest, const Cuuid_t cuuid)
          throw(castor::exception::Exception);
        
        /**
         * Destructor.
         */
        virtual ~TapeDriveHandler() throw();
        
        /**
         * This function replaces the old vdqm_NewDrvReq() C-function and is
         * called, when a VDQM_DRV_REQ message comes from a client. It stores
         * the request into the data Base or updates the status of existing
         * TapeDrives in the db.
         */
        void newTapeDriveRequest() throw (castor::exception::Exception);
        
        /**
         * This function replaces the old vdqm_DelDrvReq() C-function and is
         * called, when a VDQM_DEL_DRVREQ message comes from a client. It 
         * deletes the TapeDrive with the specified ID in the db.
         */
        void deleteTapeDrive() throw (castor::exception::Exception);
        
        /**
         * This function replaces the old vdqm_GetDrvQueue() C-function. 
         * It looks into the data base for all stored tapeDrives and sends
         * them back to the client via the OldProtocolInterpreter interface.
         *
         * @param volumeRequest The TapeRequest in the old protocol
         * @param oldProtInterpreter The interface to send the queue to the
         * client
         */
        void sendTapeDriveQueue(const vdqmVolReq_t *const volumeRequest,
          OldProtocolInterpreter *const oldProtInterpreter) 
          throw (castor::exception::Exception);  

        /**
         * This method replaces the old vdqm_DedicateDrv() C-function.
         * This method is used to handle request for dedicating drives to VIDs
         * and to hosts.
         */
        void dedicateTapeDrive()
          throw (castor::exception::Exception);
          
          
      private:

        /**
         * Inner auto pointer class used to delete the owned tape drive object
         * and all its child objects from the heap except for the associated
         * tape server.
         */
        class TapeDriveAutoPtr {

        public:

           /**
            * Constructor.
            */
           TapeDriveAutoPtr(castor::vdqm::TapeDrive *const tapeDrive) throw();

           /**
            * Returns a pointer to the owned tape drive object.
            */
           castor::vdqm::TapeDrive *get() throw();

           /**
            * Destructor.
            *
            * Deletes the owned tape drive object and all of its child objects
            * from the heap except for the associated tape server object.  The
            * tape server object is not deleted because it has a bi-directional
            * link with the tape drive object.
            */
           ~TapeDriveAutoPtr() throw();

        private:

          castor::vdqm::TapeDrive *const m_tapeDrive;
        };

        // Private variables
        vdqmHdr_t    *const ptr_header;
        vdqmDrvReq_t *const ptr_driveRequest;
        const Cuuid_t       m_cuuid;
        
        /**
         * Handles the communication with the data base to get the TapeDrive.
         * If there is no entry in the db, a new TapeDrive Object will be
         * created.  Please notice, that this object is not stored in the db.
         * This happens at the very end of newTapeDriveRequest()
         * 
         * @param tapeServer The tape server, to which the drive belong to.
         * @exception In case of error
         */
        TapeDrive* getTapeDrive(TapeServer* tapeServer) 
          throw (castor::exception::Exception);
          
        /**
         * Copies the informations of the tape drive, which it has received 
         * from the db back to the old vdqmDrvReq_t struct, to inform the
         * old RTCPD client.
         * 
         * @param tapeDrive The received TapeDrive frome the db
         * @exception In case of error
         */
        void copyTapeDriveInformations(TapeDrive* tapeDrive)
          throw (castor::exception::Exception);  

        /**
         * Returns the equivalent VDQM1 bitset of the VDQM2 tape drive status.
         *
         * @param status the VDQM2 tape drive status
         * @return the equivalent VDQM1 bitset
         * @exception if the specified VDQM2 tape drive status is unknown.
         */
        int tapeDriveStatus2Bitset(const TapeDriveStatusCodes status)
          throw (castor::exception::Exception);
        
        /**
         * Creates a log messages for the old and new status code. If the 
         * value of the old status is 0, the function just prints the new 
         * status.
         * 
         * @param oldProtocolStatus The status value of the old Protocol
         * @param newActStatus The current status of the tape drive in the db
         * @exception In case of error
         */  
        void printStatus(const int oldProtocolStatus, const int newActStatus)
          throw (castor::exception::Exception);
          
        /**
         * Connects the new tape drive with the TapeDriveCompatibility objects.
         * If there are now rows for this model inside the table, it starts
         * to create the default entries for tapes with the same dgName. 
         * Therefore it has to retrieve information from the VMGR daemon.
         * 
         * @param newTapeDrive the new Tape Drive
         * @param tapeModel The model of the tape drive
         * @exception In case of errors
         */  
        void handleTapeDriveCompatibilities(
          castor::vdqm::TapeDrive *newTapeDrive, std::string driveModel) 
          throw (castor::exception::Exception);

      }; // class TapeDriveHandler
    
    } // end of namespace handler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEDRIVEHANDLER_HPP_
