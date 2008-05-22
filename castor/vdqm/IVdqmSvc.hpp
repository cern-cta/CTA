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

#include "castor/IService.hpp"
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
    class TapeDriveCompatibility;
    class VdqmTape;

    /**
     * This class provides methods to deal with the VDQM service
     */
    class IVdqmSvc : public virtual castor::IService {

      public:

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
         * Inner class used to help manage the allocated memory associated with
         * a list of volume requests for showqueues
         */
        class VolReqList : public std::list<vdqmVolReq_t*> {
        public:

          /**
           * Destructor which deletes each of the vdqmVolReq_t messages
           * pointed to by the pointers within this container.
           */
          ~VolReqList() {
            for(std::list<vdqmVolReq_t*>::iterator itor=begin();
              itor != end(); itor++) {
              delete *itor;
            }
          }
        };

        /**
         * Inner class used to return the result of listVolPriorities.
         */
        class VolPriority {
        public:
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
         * priority setting.  Valid values are either 0 meaning single-shot or
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
         * priority setting.  Valid values are either 0 meaning single-shot or
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
         * Returns the list of effective volume priorities.
         * Please note: The caller is responsible for the deletion of the
         * allocated list!
         *
         * @return the list of all volume priorities.
         * Note that the returned list should be deallocated by the caller.
         */
        virtual std::list<VolPriority> *getVolPriorities()
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
         * allocated VolReqList!
         * @param dgn The device group name to be used to restrict the queue
         * of requests returned.  If the list should not be restricted by device
         * group name then set this parameter to be an empty string.
         * @param requestedSrv The server name to be used to restrict the queue
         * of tape requests returned.  If the list should not be restricted by
         * server name then set this parameter to be an empty string.
         * @return VolReqList of messages to be used to send the queue of tape
         * requests to the showqueues comman-line application.
         * Note that the returned VolReqList should be deallocated by the
         * caller.
         */
        virtual VolReqList* selectTapeRequestQueue(
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception) = 0;   
          
        /**
         * Returns the tape drives queue with the specified dgn and server.
         * If you don't want to specify one of the arguments, just give an
         * empty string instead.
         * Please note: The caller is responsible for the deletion of the
         * allocated list!
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
        virtual std::list<vdqmDrvReq_t>* selectTapeDriveQueue(
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
                  
//---------------- functions for TapeDriveStatusHandler ------------------------

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

        /**
         * Tries to write to the database the fact that a successful RTCPD job
         * submission has occured.  This update of the database may not be
         * possible if the corresponding drive and tape request states have
         * been modified by other threads.  For example a thread handling a
         * tape drive request message may have put the drive into the down
         * state.  The RTCPD job submission should be ignored in this case.
         *
         * @param tapeDriveId the ID of the drive
         * @param tapeRequestId the ID of the tape request
         * @return true if the occurance of the RTCPD job submission was
         * successfully written to the database, else false.
         */
        virtual bool writeRTPCDJobSubmission(const u_signed64 tapeDriveId,
          const u_signed64 tapeRequestId)
          throw (castor::exception::Exception) = 0;

        /**
         * Tries to write to the database the fact that a failed RTCPD job
         * submission has occured.  This update of the database may not be
         * possible if the corresponding drive and tape request states have
         * been modified by other threads.  For example a thread handling a
         * tape drive request message may have put the drive into the down
         * state.  The failed RTCPD job submission should be ignored in this
         * case.
         *
         * @param tapeDriveId the ID of the drive
         * @param tapeRequestId the ID of the tape request
         * @return true if the occurance of the failed RTCPD job submission was
         * successfully written to the database, else false.
         */
        virtual bool writeFailedRTPCDJobSubmission(const u_signed64 tapeDriveId,
          const u_signed64 tapeRequestId)
          throw (castor::exception::Exception) = 0;

    }; // end of class IVdqmSvc

  } // end of namespace vdqm

} // end of namespace castor

#endif // VDQM_IVDQMSVC_HPP
