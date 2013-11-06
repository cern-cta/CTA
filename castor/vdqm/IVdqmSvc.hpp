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
 *
 * This class provides methods to deal with the VDQM service
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef VDQM_IVDQMSVC_HPP
#define VDQM_IVDQMSVC_HPP 1

#include "castor/IService.hpp"
#include "castor/vdqm/TapeDriveCompatibility.hpp"
#include "h/vdqm_messages.h"

#include <list>
#include <string>
#include <vector>


namespace castor {

  namespace vdqm {

    // Forward declaration
    class DeviceGroupName;
    class TapeAccessSpecification;
    class TapeRequest;
    class TapeDrive;
    class TapeServer;
    class VdqmTape;

    /**
     * This class provides methods to deal with the VDQM service
     */
    class IVdqmSvc : public virtual castor::IService {

      public:

        /**
         * Inner class used to specifiy the database exception constants.
         */
        class DbExceptions {

          public:
            enum Constants {
              NOT_IMPLEMENTED        = 20001,
              NULL_IS_INVALID        = 20002,
              INVALID_DRIVE_DEDICATE = 20003,
              DRIVE_NOT_FOUND        = 20004,
              DRIVE_SERVER_NOT_FOUND = 20005,
              DRIVE_DGN_NOT_FOUND    = 20006,
              INVALID_REGEXP_HOST    = 20007,
              INVALID_REGEXP_VID     = 20008
            };
        };

        /**
         * Helper method to commit
         */
        virtual void commit() = 0;

        /**
         * Helper method to rollback
         */
        virtual void rollback() = 0;

        /**
         * Retrieves a TapeServer from the database based on its serverName. 
         * If no tapeServer is found, then one is created.
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
        virtual TapeServer* selectOrCreateTapeServer(
          const std::string serverName, bool withTapeDrive)
          throw (castor::exception::Exception) = 0;
          
        /**
         * Checks, if there is already an entry for that tapeRequest. The entry
         * must have exactly the same associations!
         * 
         * @return true, if the request does not exist.
         */
        virtual bool checkTapeRequest(const TapeRequest *const newTapeRequest)
          throw (castor::exception::Exception) = 0;
        
        /**
         * Returns the queue position of the tape request.
         * 
         * @return The row number or -1 if there is no entry for it.
         */  
        virtual int getQueuePosition(const u_signed64 tapeRequestId)
          throw (castor::exception::Exception) = 0;

        /**
         * Sets the priority of a volume.
         *
         * @param priority the priority where 0 is the lowest priority and
         * INT_MAX is the highest.
         * @param clientUID the user id of the client.
         * @param clientGID the group id fo the client.
         * @param clientHost the host of the client.
         * @param vid the visual identifier of the volume.
         * @param tpMode the tape access mode.  Valid values are either 0
         * meaning write-disabled or 1 meaning write-enabled.
         * @param lifespanType the type of lifespan to be assigned to the
         * priority setting.  Valid values are either 0 meaning single-mount or
         * 1 meaning unlimited.
         */
        virtual void setVolPriority(const int priority, const int clientUID,
          const int clientGID, const std::string clientHost,
          const std::string vid, const int tpMode, const int lifespanType)
          throw (castor::exception::Exception) = 0;

        /**
         * Deletes the specified volume priority if it exists, else does
         * nothing.
         *
         * @param vid the visual identifier of the volume.
         * @param tpMode the tape access mode.  Valid values are either 0
         * meaning write-disabled or 1 meaning write-enabled.
         * @param lifespanType the type of lifespan to be assigned to the
         * priority setting.  Valid values are either 0 meaning single-mount or
         * 1 meaning unlimited.
         *
         * @return the ID of the deleted priority if one was deleted, else 0
         */
        virtual u_signed64 deleteVolPriority(const std::string vid,
          const int tpMode, const int lifespanType, int *const priority,
          int *const clientUID, int *const clientGID,
          std::string *const clientHost)
          throw (castor::exception::Exception) = 0;

        /**
         * Deletes volume priorities older than the specfied age.
         *
         * @param maxAge the maximum age of volume priority in seconds.
         *
         * @return the number of volume priorities deleted.
         */
        virtual unsigned int deleteOldVolPriorities(const unsigned int maxAge)
          throw (castor::exception::Exception) = 0;

        /**
         * Inner class used to return the result of listVolPriorities.
         */
        struct VolPriority {
          int        priority;
          int        clientUID;
          int        clientGID;
          char       clientHost[CA_MAXHOSTNAMELEN+1];
          char       vid[CA_MAXVIDLEN+1];
          int        tpMode;
          int        lifespanType;
          u_signed64 id;
          u_signed64 creationTime;
          u_signed64 modificationTime;
        };

        /**
         * Gets the list of all volume priorities.
         *
         * @param priorities The list of priorities to be filled.
         * @return the list of all volume priorities.
         * Note that the returned list should be deallocated by the caller.
         */
        virtual void getAllVolPriorities(std::list<VolPriority> &priorities)
          throw (castor::exception::Exception) = 0;

        /**
         * Gets the list of effective volume priorities.
         *
         * @param priorities The list of priorities to be filled.
         * @return the list of effective volume priorities.
         * Note that the returned list should be deallocated by the caller.
         */
        virtual void getEffectiveVolPriorities(
          std::list<VolPriority> &priorities)
          throw (castor::exception::Exception) = 0;

        /**
         * Gets the list of volume priorities with the specified lifespan type.
         *
         * @param priorities The list of priorities to be filled.
         * @param lifespanType the lifespan type of the volume priorities to be
         * retrieved.  Valid values are 0 for single-mount and 1 for unlimited.
         * @return the list of volume priorities.
         * Note that the returned list should be deallocated by the caller.
         */
        virtual void getVolPriorities(std::list<VolPriority> &priorities,
          const int lifespanType) throw (castor::exception::Exception) = 0;

        /**
         * Inner class used together with VolRequestList to return the result
         * of getVolRequestsPriorityOrder.
         */
        struct VolRequest {
          u_signed64  id;
          std::string driveName;
          u_signed64  tapeDriveId;
          int         priority; // Not the priority used for ordering requests
          int         clientPort;
          int         clientEuid;
          int         clientEgid;
          int         accessMode;
          int         creationTime;
          std::string clientMachine;
          std::string vid;
          std::string tapeServer;
          std::string dgName;
          std::string clientUsername;
          int         volumePriority;  // Priority used for ordering requests
          std::string remoteCopyType;
        };

        /**
         * Inner class used to help manage the allocated memory associated with
         * a list of volume requests returned from getVolRequestsPriorityOrder.
         */
        class VolRequestList : public std::list<VolRequest*> {
        public:

          /**
           * Destructor which deletes each of the VolRequest objects
           * pointed to by the pointers within this container.
           */
          ~VolRequestList() {
            for(std::list<VolRequest*>::iterator itor=begin();
              itor != end(); itor++) {
              delete *itor;
            }
          }
        };

        /**
         * Gets the list of volume requests ordered by priority.
         * If you don't want to specify a DGN and or a request server, then
         * just give empty strings.
         *
         * @param requests The list of requests to be filled.
         * @param dgn The device group name to be used to restrict the queue
         * of requests returned.  If the list should not be restricted by device
         * group name then set this parameter to be an empty string.
         * @param requestedSrv The server name to be used to restrict the queue
         * of tape requests returned.  If the list should not be restricted by
         * server name then set this parameter to be an empty string.
         */
        virtual void getVolRequestsPriorityOrder(VolRequestList &requests,
          const std::string dgn, const std::string requestedSrv)
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
        * because allocateDrive() does not match pending tape requests whose
        * tapes are busy, and this method can only match tape requests whose
        * tapes are busy.  Please note that this method does not commit the
        * changes to the DB.
        *
        * @param tape the tape which has been released.
        * @param drive the tape drive in which the tape is still mounted.
        * @param accessMode the access mode (R/W) of the drive allocation to
        * be reused.
        * @param tapeRequestId if the drive allocation was successfully reused
        * then the value of this parameter will be the ID of the newly assigned
        * request, else the value of this parameter will be undefined.
        * @return 1 if the specified drive allocation was successfully reused,
        * 0 if no possible reuse was found or -1 if a possible reuse was found
        * but was invalidated by other threads before the appropriate locks
        * could be taken.
        */
       virtual int reuseDriveAllocation(castor::vdqm::VdqmTape *const tape,
         castor::vdqm::TapeDrive *const drive, const int accessMode,
         u_signed64 *const tapeRequestId)
         throw (castor::exception::Exception) = 0;

       /**
        * Returns a matched "tape drive / tape request" pair if one exists,
        * else NULL.
        */
       virtual castor::vdqm::TapeRequest *requestToSubmit()
         throw (castor::exception::Exception) = 0;

        /**
         * Looks, whether the specific tape access exist in the db. If not the
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
         * Inner class used to help manage the allocated memory associated with
         * a list of volume request messages for showqueues
         */
        class VolReqMsgList : public std::list<vdqmVolReq_t*> {
        public:

          /**
           * Destructor which deletes each of the vdqmVolReq_t messages
           * pointed to by the pointers within this container.
           */
          ~VolReqMsgList() {
            for(std::list<vdqmVolReq_t*>::iterator itor=begin();
              itor != end(); itor++) {
              delete *itor;
            }
          }
        };

        /**
         * Gets the tape requests queue with the specified dgn an server.
         * If you don't want to specify a DGN and or a request server, then
         * just give empty strings.
         * @param requests The list of request messages to be filled.
         * @param dgn The device group name to be used to restrict the queue
         * of requests returned.  If the list should not be restricted by device
         * group name then set this parameter to be an empty string.
         * @param requestedSrv The server name to be used to restrict the queue
         * of tape requests returned.  If the list should not be restricted by
         * server name then set this parameter to be an empty string.
         * @return VolReqMsgList of messages to be used to send the queue of
         * tape requests to the showqueues comman-line application.
         * Note that the returned VolReqMsgList should be deallocated by the
         * caller.
         */
        virtual void getTapeRequestQueue(VolReqMsgList &requests,
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception) = 0;   
          
        /**
         * Gets the tape drives queue with the specified dgn and server.
         * If you don't want to specify a DGN and or a requested server, then
         * just give empty strings.
         * @param drvReqs The list of requests to be filled.
         * @param dgn The device group name to be used to restrict the queue
         * of drives returned.  If the list should not be restricted by device
         * group name then set this parameter to be an empty string.
         * @param requestedSrv The server name to be used to restrict the queue
         * of tape drives returned.  If the list should not be restricted by
         * server name then set this parameter to be an empty string.
         * @return list of messages to be used to send the queue of tape
         * drives to the showqueues comman-line application.
         * Note that the returned list should be deallocated by the caller.
         */
        virtual void getTapeDriveQueue(std::list<vdqmDrvReq_t> &drvReqs,
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception) = 0;    
                                       
        /**
         * Manages the casting of VolReqID, to find also tape requests, which
         * have an id bigger than 32 bit.
         *
         * @param VolReqID The id, which has been sent by RTCPCopyD
         * @return The tape request and its ClientIdentification, or NULL
         */
        virtual TapeRequest* selectTapeRequest(const int volReqID)
          throw (castor::exception::Exception) = 0;

        /**
         * Selects the tape request with the specified id for update.
         * 
         * @param VolReqID The id.
         * @return true if tape request exists, else false.
         */
        virtual bool selectTapeRequestForUpdate(const int volReqID) 
          throw (castor::exception::Exception) = 0;

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
          const vdqmDrvReq_t* driveRequest,
          TapeServer* tapeServer)
          throw (castor::exception::Exception) = 0;  

        /**
         * Inserts the specified dedications for the specified drive into the
         * database.
         *
         * @driveName  drive name
         * @serverName server name
         * @dgName     device group name
         * @dedicate   dedication string
         */
        virtual void dedicateDrive(const std::string driveName,
          const std::string serverName, const std::string dgName,
          const std::string dedicate) throw (castor::exception::Exception) = 0;

        /**
         * Deletes the specified drive from the database;
         *
         * @driveName  drive name
         * @serverName server name
         * @dgName     device group name
         */
        virtual void deleteDrive(std::string driveName, std::string serverName,
          std::string dgName) throw (castor::exception::Exception) = 0;
                  
        /**
         * Retrieves a tape from the database based on its vid.  If no tape is
         * found, then one is created.
         *
         * Note that this method creates a lock on the row of the given tape
         * and does not release it. It is the responsability of the caller to
         * commit the transaction.  The caller is also responsible for the
         * deletion of the allocated object.
         *
         * @param vid the VID of the tape
         * @return the tape. the return value can never be 0
         * @exception Exception in case of error (no tape found,
         * several tapes found, DB problem, etc...)
         */
        virtual castor::vdqm::VdqmTape* selectOrCreateTape(
          const std::string vid) throw (castor::exception::Exception) = 0;

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
         * @exception Exception in case of error (several tapes or no tape
         * found, DB problem, etc...)  
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
         * specified drive model and adds them to the specified Tapedrive.
         *
         * @param tapeDrive The tape drive.
         * @param tapeDriveModel The model of the tape drive.
         */
        virtual void selectCompatibilitiesForDriveModel(
          TapeDrive *const tapeDrive, const std::string tapeDriveModel)
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

        /**
         * Tries to move a the state of the specified request from
         * REQUEST_BEINGSUBMITTED to REQUEST_SUBMITTED.
         *
         * This state transition may not be possible if the corresponding drive
         * and tape request states have been modified by other threads.  For
         * example a thread handling a tape drive request message may have put
         * the drive into the down state.
         *
         * If the state of the request cannot be moved to REQUEST_SUBMITTED,
         * then this method will take the appropriate actions.
         *
         * All the details of the actions taken by the procedure, both in
         * success and failure, are reflected in the xxxxBefore and xxxxAfter
         * parameters of this method.
         *
         * Note that except for database ids, a xxxxBefore or a xxxxAfter
         * parameter will contain the value -1 if the actual value is unknown.
         * In the case of database ids, a value of NULL may mean unknown or
         * NULL.
         *
         * @param tapeDriveId the ID of the drive
         * @param tapeRequestId the ID of the tape request
         * @param driveExists -1 = unknown, 0 = no drive, 1 = drive exists
         * @param driveStatusBefore -1 = unknown, else drive status before this
         * method
         * @param driveStatusAfter -1 = unknown, else drive status after this
         * method
         * @param runningRequestIdBefore the id of the drive's running request
         * before this method
         * @param runningRequestIdAfter the id of the drive's running request
         * after this method
         * @param requestExists -1 = unknown, 0 = no request, 1 request exists
         * @param requestStatusBefore -1 = unknown, else the request status
         * before this method
         * @param requestStatusAfter -1 = unknown, else the request status
         * after this method
         * @param requestDriveIdBefore -1 = the id of the request's drive
         * before this method
         * @param requestDriveIdAfter -1 = the id of the request's drive after
         * this method
         * @return 1 if the state of the request was successfully moved to
         * REQUEST_SUBMITTED, else 0
         */
        virtual bool requestSubmitted(
          const u_signed64  driveId,
          const u_signed64  requestId,
          bool             &driveExists,
          int              &driveStatusBefore,
          int              &driveStatusAfter,
          u_signed64       &runningRequestIdBefore,
          u_signed64       &runningRequestIdAfter,
          bool             &requestExists,
          int              &requestStatusBefore,
          int              &requestStatusAfter,
          u_signed64       &requestDriveIdBefore,
          u_signed64       &requestDriveIdAfter)
          throw (castor::exception::Exception) = 0;

        /**
         * Resets the specified drive and request.
         *
         * The status of the drive is set to REQUEST_PENDING and its running
         * request is set to NULL.
         *
         * The status of the request is set to REQUEST_PENDING and its drive is
         * set to NULL.
         *
         * All the details of the actions taken by this method are reflected
         * in the xxxxBeforeVar and xxxxAfterVar OUT parameters of this
         * method.
         *
         * Note that except for database ids, a xxxxBefore or a xxxxAfter
         * parameter will contain the value -1 if the actual value is unknown.
         * In the case of database ids, a value of NULL may mean unknown or
         * NULL.
         *
         * @param tapeDriveIdVar the ID of the drive
         * @param tapeRequestIdVar the ID of the tape request
         * @param driveExistsVar -1 = unknown, 0 = no drive, 1 = drive exists
         * @param driveStatusBeforeVar -1 = unknown, else drive status before
         * this method
         * @param driveStatusAfterVar -1 = unknown, else drive status after this
         * method
         * @param runningRequestIdBeforeVar the id of the drive's running
         * request before this method
         * @param runningRequestIdAfterVar the id of the drive's running
         * request after this method
         * @param requestExistsVar -1 = unknown, 0 = no request, 1 request
         * exists
         * @param requestStatusBeforeVar -1 = unknown, else the request status
         * before this method
         * @param requestStatusAfterVar -1 = unknown, else the request status
         * after this method
         * @param requestDriveIdBeforeVar the id of the request's drive before
         * this method
         * @param requestDriveIdAfterVar the id of the* request's drive after
         * this method
         */
        virtual void resetDriveAndRequest(
          const u_signed64  driveId,
          const u_signed64  requestId,
          bool             &driveExists,
          int              &driveStatusBefore,
          int              &driveStatusAfter,
          u_signed64       &runningRequestIdBefore,
          u_signed64       &runningRequestIdAfter,
          bool             &requestExists,
          int              &requestStatusBefore,
          int              &requestStatusAfter,
          u_signed64       &requestDriveIdBefore,
          u_signed64       &requestDriveIdAfter)
          throw (castor::exception::Exception) = 0;

    }; // end of class IVdqmSvc

  } // end of namespace vdqm

} // end of namespace castor

#endif // VDQM_IVDQMSVC_HPP
