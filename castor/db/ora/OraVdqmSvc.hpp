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
 * @(#)RCSfile: OraVdqmSvc.hpp  Revision: 1.0  Release Date: May 31, 2005  Author: mbraeger 
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _ORAVDQMSVC_HPP_
#define _ORAVDQMSVC_HPP_

#include "castor/BaseSvc.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"

#include <string>
#include <vector>
#include "occi.h"


typedef struct newVdqmDrvReq newVdqmDrvReq_t;

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
         * default constructor
         */
        OraVdqmSvc(const std::string name);
  
        /**
         * default destructor
         */
        virtual ~OraVdqmSvc() throw();
  
        /**
         * Get the service id
         */
        virtual inline const unsigned int id() const;
  
        /**
         * Get the service id
         */
        static const unsigned int ID();
  
        /**
         * Reset the converter statements.
         */
        void reset() throw ();
  
        /**
         * Retrieves a TapeServer from the database based on its serverName. 
         * If no tapeServer is found, creates one.
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
        virtual castor::vdqm::TapeServer* selectTapeServer
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
        (const castor::vdqm::TapeRequest *newTapeRequest)
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
        virtual std::vector<newVdqmVolReq_t>*
          selectTapeRequestQueue(const std::string dgn, 
          const std::string requestedSrv) throw (castor::exception::Exception);        
        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        virtual std::vector<newVdqmDrvReq_t>*
          selectTapeDriveQueue(const std::string dgn,
          const std::string requestedSrv)
          throw (castor::exception::Exception);     

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        void dedicateDrive(std::string driveName, std::string serverName,
          std::string dgName, const unsigned int accessMode,
          std::string clientHost, std::string vid)
          throw (castor::exception::Exception);

        /**
         * See the documentation for castor::vdqm::IVdqmSvc.
         */
        void deleteDrive(std::string driveName, std::string serverName,
          std::string dgName) throw (castor::exception::Exception);
  
        /**
         * Selects from the TapeDriveCompatibility table all entries for the
         * specified drive model.
         * 
         * @param tapeDriveModel The model of the tape drive
         * @exception Exception in case of error (DB problem, no mounted Tape, 
         * etc...)  
         * @return All entries in the table for the selected drive model
         */
        virtual std::vector<castor::vdqm::TapeDriveCompatibility*>* 
        selectCompatibilitiesForDriveModel(const std::string tapeDriveModel)
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
         * Manages the casting of VolReqID, to find also tape requests, which
         * have an id bigger than 32 bit.
         * 
         * @param VolReqID The id, which has been sent by RTCPCopyD
         * @return The tape request and its ClientIdentification, or NULL
         * @exception in case of error
         */
        virtual castor::vdqm::TapeRequest* selectTapeRequest(const int VolReqID)
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
          const castor::vdqm::VdqmTape *tape,
          const castor::vdqm::TapeDrive *drive,
          u_signed64 *tapeRequestId)
          throw (castor::exception::Exception);

        /**
         * See castor::vdqm::IVdqmSvc documentation.
         */
        virtual u_signed64 reuseTapeAllocation(
          const castor::vdqm::VdqmTape *tape,
          const castor::vdqm::TapeDrive *drive)
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
        (const newVdqmDrvReq_t* driveRequest,
         castor::vdqm::TapeServer* tapeServer)
          throw (castor::exception::Exception);      
  
        //------------ functions for TapeDriveStatusHandler ------------------
  
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
          throw (castor::exception::Exception);

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
  
      private:
  
        /// SQL statement for function selectTape
        static const std::string s_selectTapeStatementString;

        /// SQL statement object for function selectTape
        oracle::occi::Statement *m_selectTapeStatement;
        
        /// SQL statement for function getTapeServer
        static const std::string s_selectTapeServerStatementString;
  
        /// SQL statement object for function getTapeServer
        oracle::occi::Statement *m_selectTapeServerStatement; 
  
        /// SQL statement for function checkTapeRequest
        static const std::string s_checkTapeRequestStatement1String;
  
        /// SQL statement object for function checkTapeRequest
        oracle::occi::Statement *m_checkTapeRequestStatement1;
  
        /// SQL statement for function checkTapeRequest
        static const std::string s_checkTapeRequestStatement2String;
  
        /// SQL statement object for function checkTapeRequest
        oracle::occi::Statement *m_checkTapeRequestStatement2; 
  
        /// SQL statement for function getQueuePosition
        static const std::string s_getQueuePositionStatementString;
  
        /// SQL statement object for function getQueuePosition
        oracle::occi::Statement *m_getQueuePositionStatement;  
          
        /// SQL statement for function selectTapeDrive
        static const std::string s_selectTapeDriveStatementString;
  
        /// SQL statement object for function selectTapeDrive
        oracle::occi::Statement *m_selectTapeDriveStatement;

        /// SQL statement for function dedicateDrive
        static const std::string s_dedicateDriveStatementString;

        /// SQL statement object for function dedicateDrive
        oracle::occi::Statement *m_dedicateDriveStatement;

        /// SQL statement for function deleteDrive
        static const std::string s_deleteDriveStatementString;

        /// SQL statement object for function deleteDrive
        oracle::occi::Statement *m_deleteDriveStatement;
  
        /// SQL statement for function existTapeDriveWithTapeInUse
        static const std::string s_existTapeDriveWithTapeInUseStatementString;
  
        /// SQL statement object for function existTapeDriveWithTapeInUse
        oracle::occi::Statement *m_existTapeDriveWithTapeInUseStatement;
  
        /// SQL statement for function existTapeDriveWithTapeMounted
        static const std::string s_existTapeDriveWithTapeMountedStatementString;
  
        /// SQL statement object for function existTapeDriveWithTapeMounted
        oracle::occi::Statement *m_existTapeDriveWithTapeMountedStatement;
  
        /// SQL statement for function selectTapeByVid
        static const std::string s_selectTapeByVidStatementString;
  
        /// SQL statement object for function selectTapeByVid
        oracle::occi::Statement *m_selectTapeByVidStatement;
  
        /// SQL statement for function selectTapeReqForMountedTape
        static const std::string s_selectTapeReqForMountedTapeStatementString;
  
        /// SQL statement object for function selectTapeReqForMountedTape
        oracle::occi::Statement *m_selectTapeReqForMountedTapeStatement;
  
        /// SQL statement for function selectTapeAccessSpecification
        static const std::string s_selectTapeAccessSpecificationStatementString;
  
        /// SQL statement object for function selectTapeAccessSpecification
        oracle::occi::Statement *m_selectTapeAccessSpecificationStatement;
  
        /// SQL statement for function selectDeviceGroupName
        static const std::string s_selectDeviceGroupNameStatementString;
  
        /// SQL statement object for function selectDeviceGroupName
        oracle::occi::Statement *m_selectDeviceGroupNameStatement;
  
        /// SQL statement for function selectTapeRequestQueue
        static const std::string s_selectTapeRequestQueueStatementString;
  
        /// SQL statement object for function selectTapeRequestQueue
        oracle::occi::Statement *m_selectTapeRequestQueueStatement;
  
        /// SQL statement for function selectTapeDriveQueue
        static const std::string s_selectTapeDriveQueueStatementString;
  
        /// SQL statement object for function selectTapeDriveQueue
        oracle::occi::Statement *m_selectTapeDriveQueueStatement;
  
        /// SQL statement for function allocateDrive
        static const std::string s_allocateDriveStatementString;
  
        /// SQL statement object for function allocateDrive
        oracle::occi::Statement *m_allocateDriveStatement;
  
        /// SQL statement for function reuseDriveAllocation
        static const std::string s_reuseDriveAllocationStatementString;
  
        /// SQL statement object for function reuseDiveAllocation
        oracle::occi::Statement *m_reuseDriveAllocationStatement;
  
        /// SQL statement for function reuseTapeAllocation
        static const std::string s_reuseTapeAllocationStatementString;
  
        /// SQL statement object for function reuseTapeAllocation
        oracle::occi::Statement *m_reuseTapeAllocationStatement;
  
        /// SQL statement for function requestToSubmit
        static const std::string s_requestToSubmitStatementString;
  
        /// SQL statement object for function requestToSubmit
        oracle::occi::Statement *m_requestToSubmitStatement;
  
        /// SQL statement for function selectCompatibilitiesForDriveModel
        static const std::string
          s_selectCompatibilitiesForDriveModelStatementString;
  
        /// SQL statement object for function selectCompatibilitiesForDriveModel
        oracle::occi::Statement *m_selectCompatibilitiesForDriveModelStatement;
  
        /// SQL statement for function selectTapeAccessSpecifications
        static const std::string
          s_selectTapeAccessSpecificationsStatementString;
  
        /// SQL statement object for function selectTapeAccessSpecifications
        oracle::occi::Statement *m_selectTapeAccessSpecificationsStatement;
  
        /// SQL statement for function selectTapeRequest
        static const std::string s_selectTapeRequestStatementString;
  
        /// SQL statement object for function selectTapeRequest
        oracle::occi::Statement *m_selectTapeRequestStatement;

      private:

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

      }; // class OraVdqmSvc

    } // namespace ora

  } // namespace db

} // namespace castor

#endif //_ORAVDQMSVC_HPP_
