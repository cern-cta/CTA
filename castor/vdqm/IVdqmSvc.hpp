/******************************************************************************
 *                      IVdqmSvc.hpp
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
 * @(#)RCSfile: IVdqmSvc.hpp  Revision: 1.0  Release Date: Apr 20, 2005  Author: mbraeger 
 *
 * This class provides methods to deal with the VDQM service
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef VDQM_IVDQMSVC_HPP
#define VDQM_IVDQMSVC_HPP 1

// Include Files
#include <string>
#include <vector>

#include "castor/IService.hpp"

// Forward declaration
typedef struct newVdqmDrvReq newVdqmDrvReq_t;
typedef struct newVdqmVolReq newVdqmVolReq_t;

namespace castor {

  namespace vdqm {

    // Forward declaration
    class DeviceGroupName;
    class TapeAccessSpecification;
    class TapeRequest;
    class TapeDrive;
    class TapeServer;
    class TapeDriveCompatibility;
    class VdqmTape;

    /**
     * This class provides methods to deal with the VDQM service
     */
    class IVdqmSvc : public virtual castor::IService {

      public:
                
        /**
         * Retrieves a TapeServer from the database based on its serverName. 
         * If no tapeServer is found, creates one.
         * Note that this method creates a lock on the row of the
         * given tapeServer and does not release it. It is the
         * responsability of the caller to commit the transaction.
         * The caller is also responsible for the deletion of the
         * allocated object
         * @param serverName The name of the server
         * @return the tapeServer. the return value can never be 0
         * @exception Exception in case of error (no tape server found,
         * several tape servers found, DB problem, etc...)
         */
        virtual TapeServer* selectTapeServer(const std::string serverName,
          bool withTapeDrive)
          throw (castor::exception::Exception) = 0;
          
        /**
         * Checks, if there is already an entry for that tapeRequest. The entry
         * must have exactly the same associations!
         * 
         * @return true, if the request does not exist.
         */
        virtual bool checkTapeRequest(const TapeRequest *newTapeRequest)
          throw (castor::exception::Exception) = 0;
        
        /**
         * Returns the queue position of the tape request.
         * 
         * @return The row number or -1 if there is no entry for it.
         */  
        virtual int getQueuePosition(const u_signed64 tapeRequestId)
          throw (castor::exception::Exception) = 0;
        
        /**
         * Tries to allocate in the database a free tape drive to a pending
         * request.
         *
         * The scheduling algorithm looks for the best fitting tape drive. For
         * example if the tape is relatively old, then the algorithm will
         * favour an older drive over a newer one in order to help keep the
         * newer drives free for newer tapes.
         * 
         * @param tapeDriveId if a free drive was successfully allocated then
         * the value of this parameter will be the ID of the allocated tape
         * drive, else the value of this parameter will be undefined.
         * @param tapeDriveId if a free drive was successfully allocated then
         * the value of this parameter will be the name of the allocated tape
         * drive, else the value of this parameter will be undefined.
         * @param tapeRequestId if a free drive was successfully allocated then
         * the value of this parameter will be the ID of the pending request,
         * else the value of this parameter will be undefined.
         * @param tapeRequestVid if a free drive was successfully allocated
         * then the value of this parameter will be the VID of the pending
         * request, else the value of this parameter will be undefined.
         * @return 1 if a free drive was successfully allocated to a pending
         * request, or 0 if no possible allocation could be found or -1 if an
         * allocation was found but was invalidated by other threads before the
         * appropriate locks could be taken.
         */  
       virtual int allocateDrive(u_signed64 *tapeDriveId,
         std::string *tapeDriveName, u_signed64 *tapeRequestId,
         std::string *tapeRequestVid)
         throw (castor::exception::Exception) = 0;

       /**
        * Tries to re-use a drive allocation.
        *
        * This method is to be called when a tape is released.  The method will
        * try to match a pending tape request with the drive in which the tape
        * is still mounted.  This method does not conflict with allocateDrive()
        * because the allocateDrive() does not match pending tape requests
        * whose tapes are busy, and this method can only match tape requests
        * whose tapes are busy.  Please note that this method does not commit
        * the changes to the DB."
        *
        * @param tape the tape which has been released.
        * @param drive the tape drive in which the tape is still mounted.
        * @param tapeRequestId if the drive allocation was successfully reused
        * then the value of this parameter will be the ID of the newly assigned
        * request, else the value of this parameter will be undefined.
        * @return 1 if the specified drive allocation was successfully reused,
        * 0 if no possible reuse was found or -1 if a possible reuse was found
        * but was invalidated by other threads before the appropriate locks
        * could be taken.
        */
       virtual int reuseDriveAllocation(const castor::vdqm::VdqmTape *tape,
         const castor::vdqm::TapeDrive *drive, u_signed64 *tapeRequestId)
         throw (castor::exception::Exception) = 0;

       /**
        * Returns a matched "tape drive / tape request" pair if one exists,
        * else NULL.
        */
       virtual castor::vdqm::TapeRequest *requestToSubmit()
         throw (castor::exception::Exception) = 0;

        /**
         * Looks, wether the specific tape access exist in the db. If not the
         * return value is NULL.
         * Please notice that caller is responsible for deleting the object.
         * @parameter accessMode the access mode for the tape
         * @parameter density its tape density
         * @parameter tapeModel the model of the requested tape
         * @return the reference in the db or NULL if it is not a right
         * specification
         */  
        virtual TapeAccessSpecification* selectTapeAccessSpecification(
          const int accessMode,
          const std::string density,
          const std::string tapeModel) 
          throw (castor::exception::Exception) = 0;
          
        /**
         * Looks, if the specified dgName exists in the database. 
         * If it is the case, it will return the object. If not, a new entry
         * will be created!
         * Please notice that caller is responsible for deleting the object.
         * @parameter dgName The dgn which the client has sent to vdqm
         * @return the requested DeviceGroupName, or NULL if it does not exists
         */  
        virtual DeviceGroupName* selectDeviceGroupName(
          const std::string dgName) 
          throw (castor::exception::Exception) = 0;
          
        /**
         * Returns the tape requests queue with the specified dgn an server.
         * If you don't want to specify one of the arguments, just give an
         * empty string instead.
         * Please note: The caller is responsible for the deletion of the
         * allocated vector!
         * @param dgn The device group name to be used to restrict the queue
         * of requests returned.  If the list should not be restricted by device
         * group name then set this parameter to be an empty string.
         * @param requestedSrv The server name to be used to restrict the queue
         * of tape requests returned.  If the list should not be restricted by
         * server name then set this parameter to be an empty string.
         * @return vector of messages to be used to send the queue of tape
         * requests to the showqueues comman-line application.
         * Note that the returned vector should be deallocated by the caller.
         */
        virtual std::vector<newVdqmVolReq_t>* selectTapeRequestQueue(
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception) = 0;   
          
        /**
         * Returns the tape drives queue with the specified dgn and server.
         * If you don't want to specify one of the arguments, just give an
         * empty string instead.
         * Please note: The caller is responsible for the deletion of the
         * allocated vector!
         * @param dgn The device group name to be used to restrict the queue
         * of drives returned.  If the list should not be restricted by device
         * group name then set this parameter to be an empty string.
         * @param requestedSrv The server name to be used to restrict the queue
         * of tape drives returned.  If the list should not be restricted by
         * server name then set this parameter to be an empty string.
         * @return vector of messages to be used to send the queue of tape
         * drives to the showqueues comman-line application.
         * Note that the returned vector should be deallocated by the caller.
         */
        virtual std::vector<newVdqmDrvReq_t>* selectTapeDriveQueue(
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception) = 0;    
                                       
        /**
         * Manages the casting of VolReqID, to find also tape requests, which
         * have an id bigger than 32 bit.
         * 
         * @param VolReqID The id, which has been sent by RTCPCopyD
         * @return The tape request and its ClientIdentification, or NULL
         */
        virtual TapeRequest* selectTapeRequest(const int VolReqID) 
          throw (castor::exception::Exception) = 0;                                       
   
          
//------------------ functions for TapeDriveHandler -------------------------

        /**
         * Retrieves a tapeDrive from the database based on its struct 
         * representation. If no tapedrive is found, a Null pointer will 
         * be returned.
         * Note that this method creates a lock on the row of the
         * given tapedrive and does not release it. It is the
         * responsability of the caller to commit the transaction.
         * The caller is also responsible for the deletion of the
         * allocated object
         * @param driveRequest The old struct, which represents the tapeDrive
         * @exception Exception in case of error (several tapes drive found, 
         * DB problem, etc...)
         */
        virtual TapeDrive* selectTapeDrive(
          const newVdqmDrvReq_t* driveRequest,
          TapeServer* tapeServer)
          throw (castor::exception::Exception) = 0;  

        /**
         * Inserts the specified dedications for the specified drive into the
         * database.
         *
         * @driveName  drive name
         * @serverName server name
         * @dgName     device group name
         * @accessMode access mode
         * @clientHost client host
         */
        virtual void dedicateDrive(std::string driveName,
          std::string serverName, std::string dgName,
          const unsigned int accessMode, std::string clientHost,
          std::string vid) throw (castor::exception::Exception) = 0;

        /**
         * Deletes the specified drive from the database;
         *
         * @driveName  drive name
         * @serverName server name
         * @dgName     device group name
         */
        virtual void deleteDrive(std::string driveName, std::string serverName,
          std::string dgName) throw (castor::exception::Exception) = 0;
                  
//---------------- functions for TapeDriveStatusHandler ------------------------

        /**
         * Retrieves a tape from the database based on its vid,
         * side and tpmode. If no tape is found, creates one.
         * Note that this method creates a lock on the row of the
         * given tape and does not release it. It is the
         * responsability of the caller to commit the transaction.
         * The caller is also responsible for the deletion of the
         * allocated object
         * @param vid the vid of the tape
         * @param side the side of the tape
         * @param tpmode the tpmode of the tape
         * @return the tape. the return value can never be 0
         * @exception Exception in case of error (no tape found,
         * several tapes found, DB problem, etc...)
         */
        virtual castor::vdqm::VdqmTape* selectTape(const std::string vid,
                                                   const int side,
                                                   const int tpmode)
          throw (castor::exception::Exception) = 0;

        /**
         * Check whether another request is currently
         * using the specified tape vid. Note that the tape can still
         * be mounted on a drive if not in use by another request.
         * The volume is also considered to be in use if it is mounted
         * on a tape drive which has UNKNOWN or ERROR status.
         * 
         * @param volid the vid of the Tape
         * @exception Exception in case of error (several tapes drive found, 
         * DB problem, etc...)
         * @return true if there is one       
         */
        virtual bool existTapeDriveWithTapeInUse(
          const std::string volid)
          throw (castor::exception::Exception) = 0;
          
        /**
         * Check whether the tape, specified by the vid is mounted
         * on a tape drive or not.
         * 
         * @param volid the vid of the Tape
         * @exception Exception in case of error (several tapes drive found, 
         * DB problem, etc...)
         * @return true if there is one        
         */          
        virtual bool existTapeDriveWithTapeMounted(
          const std::string volid)
          throw (castor::exception::Exception) = 0;
        
        /**
         * Returns the tape with this vid
         * 
         * @param vid the vid of the Tape
         * @exception Exception in case of error (several tapes or no tape found, 
         * DB problem, etc...)  
         * @return The found TapeDrive             
         */  
        virtual castor::vdqm::VdqmTape* selectTapeByVid(
          const std::string vid)
          throw (castor::exception::Exception) = 0;
          
          
        /**
         * Looks through the tape requests, whether there is one for the
         * mounted tape on the tapeDrive.
         * 
         * @param tapeDrive the tape drive, with the mounted tape
         * @exception Exception in case of error (DB problem, no mounted Tape, 
         * etc...)  
         * @return The found tape request           
         */  
        virtual TapeRequest* selectTapeReqForMountedTape(
          const TapeDrive* tapeDrive)
          throw (castor::exception::Exception) = 0;  
          
        
        /**
         * Selects from the TapeDriveCompatibility table all entries for the
         * specified drive model.
         * 
         * @param tapeDriveModel The model of the tape drive
         * @exception Exception in case of error (DB problem, no mounted Tape, 
         * etc...)  
         * @return All entries in the table for the selected drive model
         */
        virtual std::vector<TapeDriveCompatibility*>* 
          selectCompatibilitiesForDriveModel(const std::string tapeDriveModel)
          throw (castor::exception::Exception) = 0;
        
        /**
         * Selects from the TapeAccessSpecification table all entries for the
         * specified tape model.
         * 
         * @param tapeModel The model of the tape
         * @exception Exception in case of error (DB problem, no mounted Tape, 
         * etc...)  
         * @return All entries in the table for the selected tape model. The 
         * list is sorted by accessMode (first write, then read)
         */  
        virtual std::vector<TapeAccessSpecification*>*
          selectTapeAccessSpecifications(const std::string tapeModel)
          throw (castor::exception::Exception) = 0;                  
          
    }; // end of class IVdqmSvc

  } // end of namespace vdqm

} // end of namespace castor

#endif // VDQM_IVDQMSVC_HPP
