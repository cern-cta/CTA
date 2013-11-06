/******************************************************************************
 *                      OraVdqmSvc.hpp
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
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _ORAVDQMSVC_HPP_
#define _ORAVDQMSVC_HPP_

#include "castor/BaseSvc.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"
#include "h/vdqm_messages.h"

#include "occi.h"
#include <map>
#include <string>
#include <utility>


namespace castor {
  
  namespace db {
    
    namespace ora {

      
      /**
       * Implementation of the IVdqmSvc for Oracle
       */
      class OraVdqmSvc : public BaseSvc,
                         public DbBaseObj,
                         public virtual castor::vdqm::IVdqmSvc {
  
      public:

        /**
         * Enumeration of constants used to identify the SQL statements
         */
        enum StatementId {
          SELECT_TAPE_SQL_STMT,
          SELECT_TAPE_SERVER_SQL_STMT,
          CHECK_TAPE_REQUEST1_SQL_STMT,
          CHECK_TAPE_REQUEST2_SQL_STMT,
          GET_QUEUE_POSITION_SQL_STMT,
          SET_VOL_PRIORITY_SQL_STMT,
          DELETE_VOL_PRIORITY_SQL_STMT,
          DELETE_OLD_VOL_PRIORITIES_SQL_STMT,
          GET_ALL_VOL_PRIORITIES_SQL_STMT,
          GET_EFFECTIVE_VOL_PRIORITIES_SQL_STMT,
          GET_VOL_PRIORITIES_SQL_STMT,
          SELECT_TAPE_DRIVE_SQL_STMT,
          DEDICATE_DRIVE_SQL_STMT,
          DELETE_DRIVE_SQL_STMT,
          WRITE_RTCPD_JOB_SUBMISSION_SQL_STMT,
          WRITE_FAILED_RTCPD_JOB_SUBMISSION_SQL_STMT,
          EXIST_TAPE_DRIVE_WITH_TAPE_IN_USE_SQL_STMT,
          EXIST_TAPE_DRIVE_WITH_TAPE_MOUNTED_SQL_STMT,
          SELECT_TAPE_BY_VID_SQL_STMT,
          SELECT_TAPE_REQ_FOR_MOUNTED_TAPE_SQL_STMT,
          SELECT_TAPE_ACCESS_SPECIFICATION_SQL_STMT,
          SELECT_DEVICE_GROUP_NAME_SQL_STMT,
          SELECT_VOL_REQS_DGN_CREATION_TIME_ORDER_SQL_STMT,
          SELECT_VOL_REQS_PRIORITY_ORDER_SQL_STMT,
          SELECT_TAPE_DRIVE_QUEUE_SQL_STMT,
          SELECT_TAPE_REQUEST_SQL_STMT,
          SELECT_TAPE_REQUEST_FOR_UPDATE_SQL_STMT,
          SELECT_COMPATIBILITIES_FOR_DRIVE_MODEL_SQL_STMT,
          SELECT_TAPE_ACCESS_SPECIFICATIONS_SQL_STMT,
          ALLOCATE_DRIVE_SQL_STMT,
          REUSE_DRIVE_ALLOCATION_SQL_STMT,
          REQUEST_TO_SUBMIT_SQL_STMT,
          REQUEST_SUBMITTED_SQL_STMT,
          RESET_DRIVE_AND_REQUEST_SQL_STMT
        };

        /**
         * Constructor
         */
        OraVdqmSvc(const std::string name);
  
        /**
         * default destructor
         */
        virtual ~OraVdqmSvc() throw();
  
        /**
         * Get the service id
         */
        virtual inline unsigned int id() const;
  
        /**
         * Get the service id
         */
        static unsigned int ID();
  
        /**
         * Reset the converter statements.
         */
        void reset() throw ();

        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual void commit();

        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual void rollback();
  
        /**
         * Retrieves a TapeServer from the database based on its serverName. 
         * If no tapeServer is found, then one is created.
         * Note that this method creates a lock on the row of the
         * given tapeServer and does not release it. It is the
         * responsability of the caller to commit the transaction.
         * The caller is also responsible for the deletion of the
         * allocated object
         * @param serverName The name of the server
         * @param withTapeDrive True, if the selected server should include
         * its tape drives.
         * @return the tapeServer. the return value can never be 0
         * @exception Exception in case of error (no tape server found,
         * several tape servers found, DB problem, etc...)
         */
        virtual castor::vdqm::TapeServer* selectOrCreateTapeServer
        (const std::string serverName, bool withTapeDrives)
          throw (castor::exception::Exception);
  
        /**
         * Checks, if there is already an entry for that tapeRequest. The entry
         * must have exactly the same associations!
         * 
         * @return true, if the request does not exist.
         * @exception in case of error
         */
        virtual bool checkTapeRequest
        (const castor::vdqm::TapeRequest *const newTapeRequest)
          throw (castor::exception::Exception);
  
        /**
         * Returns the queue position of the tape request.
         * 
         * @return The row number, 
         *         0 : The request is handled at the moment from a TapeDrive, 
         *         -1: if there is no entry for it.
         * @exception in case of error
         */  
        virtual int getQueuePosition(const u_signed64 tapeRequestId)
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual void setVolPriority(const int priority, const int clientUID,
          const int clientGID, const std::string clientHost,
          const std::string vid, const int tpMode, const int lifespanType)
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual u_signed64 deleteVolPriority(const std::string vid,
          const int tpMode, const int lifespanType, int *const priority,
          int *const clientUID, int *const clientGID,
          std::string *const clientHost) throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual unsigned int deleteOldVolPriorities(const unsigned int maxAge)
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual void getAllVolPriorities(
          std::list<castor::vdqm::IVdqmSvc::VolPriority> &priorities)
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual void getEffectiveVolPriorities(
          std::list<castor::vdqm::IVdqmSvc::VolPriority> &priorities)
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual void getVolPriorities(
          std::list<castor::vdqm::IVdqmSvc::VolPriority> &priorities,
          const int lifespanType) throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual void getVolRequestsPriorityOrder(VolRequestList &requests,
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception);
  
        /**
         * Looks, wether the specific tape access exist in the db. If not the
         * return value is NULL.
         * Please notice that caller is responsible for deleting the object.
         * @parameter accessMode the access mode for the tape
         * @parameter density its tape density
         * @parameter tapeModel the model of the requested tape
         * @return the reference in the db or NULL if it is not a right
         * specification
         * @exception in case of error
         */  
        virtual castor::vdqm::TapeAccessSpecification*
          selectTapeAccessSpecification(const int accessMode,
          const std::string density, const std::string tapeModel) 
          throw (castor::exception::Exception);
  
        /**
         * Looks, if the specified dgName exists in the database. 
         * If it is the case, it will return the object. If not, a new entry
         * will be created!
         * Please notice that caller is responsible for deleting the object.
         * @parameter dgName The dgn which the client has sent to vdqm
         * @return the requested DeviceGroupName
         * @exception in case of error
         */
        virtual castor::vdqm::DeviceGroupName* selectDeviceGroupName
        (const std::string dgName) 
          throw (castor::exception::Exception);
  
        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual void getTapeRequestQueue(
          castor::vdqm::IVdqmSvc::VolReqMsgList &requests,
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception);        

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual void getTapeDriveQueue(std::list<vdqmDrvReq_t> &drvReqs,
          const std::string dgn, const std::string requestedSrv)
          throw (castor::exception::Exception);     

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        void dedicateDrive(const std::string driveName,
          const std::string serverName, const std::string dgName,
          const std::string dedicate) throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        void deleteDrive(std::string driveName, std::string serverName,
          std::string dgName) throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        bool requestSubmitted(
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
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        void resetDriveAndRequest(
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
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        void selectCompatibilitiesForDriveModel(
          castor::vdqm::TapeDrive *const tapeDrive,
          const std::string tapeDriveModel)
          throw (castor::exception::Exception);
  
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
        virtual std::vector<castor::vdqm::TapeAccessSpecification*>*
        selectTapeAccessSpecifications(const std::string tapeModel)
          throw (castor::exception::Exception);  
  
        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual castor::vdqm::TapeRequest* selectTapeRequest(
          const int volReqID) throw (castor::exception::Exception);
  
        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual bool selectTapeRequestForUpdate(const int volReqID)
          throw (castor::exception::Exception);                         

        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual int allocateDrive(u_signed64 *tapeDriveId,
          std::string *tapeDriveName, u_signed64 *tapeRequestId,
          std::string *tapeRequestVid)
          throw (castor::exception::Exception);

        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual int reuseDriveAllocation(
          castor::vdqm::VdqmTape *const tape,
          castor::vdqm::TapeDrive *const drive, const int accessMode,
          u_signed64 *const tapeRequestId)
          throw (castor::exception::Exception);

        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual castor::vdqm::TapeRequest *requestToSubmit()
          throw (castor::exception::Exception);            

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
        virtual castor::vdqm::TapeDrive* selectTapeDrive
        (const vdqmDrvReq_t* driveRequest,
         castor::vdqm::TapeServer* tapeServer)
          throw (castor::exception::Exception);      
  
        //------------ functions for TapeDriveStatusHandler ------------------
  
        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual castor::vdqm::VdqmTape* selectOrCreateTape(
          const std::string vid) throw (castor::exception::Exception);

        /**
         * Check whether another request is currently
         * using the specified tape vid. Note that the tape can still
         * be mounted on a drive if not in use by another request.
         * The volume is also considered to be in use if it is mounted
         * on a tape drive which has UNKNOWN or ERROR status.
         * 
         * @param vid the volid of the Tape
         * @exception Exception in case of error (several tapes drive found, 
         * DB problem, etc...)
         * @return true if there is one       
         */
        virtual bool existTapeDriveWithTapeInUse(const std::string volid)
          throw (castor::exception::Exception);
  
        /**
         * Check whether the tape, specified by the vid is mounted
         * on a tape drive or not.
         * 
         * @param volid the vid of the Tape
         * @exception Exception in case of error (several tapes drive found, 
         * DB problem, etc...)
         * @return true if there is one        
         */
        virtual bool existTapeDriveWithTapeMounted(const std::string volid)
          throw (castor::exception::Exception);
  
        /**
         * Returns the tape with this vid
         * 
         * @param vid the vid of the Tape
         * @exception Exception in case of error (several tapes or no tape
         * found, DB problem, etc...)  
         * @return The found TapeDrive             
         */  
        virtual castor::vdqm::VdqmTape* selectTapeByVid(const std::string vid)
          throw (castor::exception::Exception);
  
        /**
         * Looks through the tape requests, whether there is one for the
         * mounted tape on the tapeDrive.
         * 
         * @param tapeDrive the tape drive, with the mounted tape
         * @exception Exception in case of error (DB problem, no mounted Tape, 
         * etc...)  
         * @return The found tape request             
         */  
        virtual castor::vdqm::TapeRequest* selectTapeReqForMountedTape
        (const castor::vdqm::TapeDrive* tapeDrive)
          throw (castor::exception::Exception);            

        /**
         * Inner class used to store a map of the SQL statment strings.
         */
        class StatementStringMap : public std::map<int, std::string> {
        public:

          /**
           * Constructor which fills the map
           */
          StatementStringMap();

          /**
           * Helper method to add SQL statement strings.
           */
          void addStmtStr(const StatementId stmtId, const char *stmt);

        private:

          /**
           * Map of statment IDs to statement strings.
           */
          std::map<int, std::string> m_stmtStrs;
        };


      private:

        /**
         * The static map of SQL statement strings
         */
        static StatementStringMap s_statementStrings;

        /**
         * The map of statement objects
         */
        std::map<int, oracle::occi::Statement* > m_statements;

        /**
         * XXX to be removed when all statements are converted using
         * the generic db API.
         * Helper method to handle exceptions - see OraCnvSvc
         * @param e an Oracle exception
         */
        void handleException(oracle::occi::SQLException& e);
        
        /**
         * XXX to be removed when all statements are converted using
         * the generic db API.
         * helper method to create Oracle statement
         */
        virtual oracle::occi::Statement*
          createStatement(const std::string& stmtString)
          throw (castor::exception::Exception);
          
        /**
         * XXX to be removed when all statements are converted using
         * the generic db API.
         * helper method to delete Oracle statement
         */
        virtual void deleteStatement(oracle::occi::Statement* stmt)
          throw (castor::exception::Exception);

        /**
         * Translates the new status of a Tape drive into the old status
         * representation.
         *
         * @param newStatusCode The status value of the new Protocol
         * @return The translation into the old status
         * @exception In case of error
         */
        int translateNewStatus(castor::vdqm::TapeDriveStatusCodes newStatusCode)
          throw (castor::exception::Exception);

        /**
         * Returns the Statement object corresponding to the specified
         * statement ID if one exists else NULL.
         *
         * @param stmtId the statement ID
         * @return the Statement object if one exists else NULL
         */
        oracle::occi::Statement *getStatement(const StatementId stmtId);

        /**
         * Stores the specified Statement object, ready for retreival by the
         * getStatement() method.
         */
        void storeStatement(const StatementId stmtId,
          oracle::occi::Statement *const stmt);

      }; // class OraVdqmSvc

    } // namespace ora

  } // namespace db

} // namespace castor

#endif //_ORAVDQMSVC_HPP_
