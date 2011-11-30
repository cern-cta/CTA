/******************************************************************************
 *                castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp
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
 * @(#)$RCSfile: ITapeGatewaySvc.hpp,v $ $Revision: 1.17 $ $Release$ $Date: 2009/07/15 08:39:33 $ $Author: gtaur $
 *
 * This class provides methods related to tape handling
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef STAGER_ITAPEGATEWAYSVC_HPP
#define STAGER_ITAPEGATEWAYSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/ICommonSvc.hpp"

#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileErrorReport.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/RetryPolicyElement.hpp"
#include "castor/tape/tapegateway/TapeGatewayRequest.hpp" 
#include "castor/tape/tapegateway/VdqmTapeGatewayRequest.hpp" 
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"

#include <list>
#include <string>

namespace castor      {
namespace tape        {
namespace tapegateway {
    /**
     * This class provides methods related to tape gateway handling
     */
      class ITapeGatewaySvc : public virtual castor::stager::ICommonSvc {
      public:

        /**
         * Little nested struct to simplify the interface of getMigrationMountsWithoutTapes
         */
        struct migrationMountParameters {
          u_signed64 migrationMountId;
          u_signed64 initialSizeToTransfer;
          std::string tapePoolName;
        };
	/**
	 * Get all the pending migration mounts
	 */
	virtual void  getMigrationMountsWithoutTapes(std::list<migrationMountParameters>& migrationMounts)
	  throw (castor::exception::Exception)=0;

        /**
         * Associate to each migrationMountParameters a Tape
         */
        virtual void attachTapesToMigrationMounts(const std::list<u_signed64>& strIds,
					  const std::list<std::string>& vids,
					  const std::list<int>& fseqs)
          throw (castor::exception::Exception)=0;

        /*
         * Get a tape for which we have to send a request to VDQM.     
         */
	virtual void  getTapeWithoutDriveReq (castor::tape::tapegateway::VdqmTapeGatewayRequest& request)
	  throw (castor::exception::Exception)=0;


	/*
         * Update the request that we sent  to VDQM.     
         */
	virtual void attachDriveReqToTape(const castor::tape::tapegateway::VdqmTapeGatewayRequest& tapeRequest,
					  const castor::stager::Tape& tape)
	  throw (castor::exception::Exception)=0; 

        /**
         * Get all the tapes for which we have to check that VDQM didn't lost 
	 * our request
         */
        virtual void  getTapesWithDriveReqs(std::list<castor::tape::tapegateway::TapeGatewayRequest>& requests,
					    std::list<std::string>& vids,
					    const u_signed64& timeOut) 
          throw (castor::exception::Exception)=0;

        /**
         * Restart the request lost by VDQM or left around
         */

	virtual void restartLostReqs(const std::list<castor::tape::tapegateway::TapeGatewayRequest>& tapes) 
	  throw (castor::exception::Exception)=0;
        /**
         * Get the best file to migrate
         */

        virtual void  getFileToMigrate(const castor::tape::tapegateway::FileToMigrateRequest& req,
				       castor::tape::tapegateway::FileToMigrate& file)
          throw (castor::exception::Exception)=0;

        /*
         * Update the db for a file which is migrated successfully 
         */
        virtual  void  setFileMigrated(const castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception)=0;

        /*
         * Update the db for a successful migration that turned out useless (file changed elsewhere)
         */
        virtual  void  setFileStaleInMigration(const castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception)=0;

        /*
	 * Get the best file to recall 
         */
        virtual void getFileToRecall(const castor::tape::tapegateway::FileToRecallRequest&  req,
				     castor::tape::tapegateway::FileToRecall& file )
          throw (castor::exception::Exception)=0;

	
        /**
         * Update the db for a file which has been recalled successfully 
	 */
        virtual void  setFileRecalled(const castor::tape::tapegateway::FileRecalledNotification& resp) 
	  throw (castor::exception::Exception)=0;

	/**
	 * Get the tapecopies which faced a migration failure
	 */
	virtual void  getFailedMigrations(std::list<castor::tape::tapegateway::RetryPolicyElement>& candidates)
	  throw (castor::exception::Exception)=0;

	/**
	 * Update the db using the retry migration policy returned values
	 */
	virtual void  setMigRetryResult(const std::list<u_signed64>& mjToRetry,
					const std::list<u_signed64>& mjToFail ) 
	  throw (castor::exception::Exception)=0;

	/**
	 * Get the tapecopies which faced a recall failure 
	 */
	virtual void  getFailedRecalls(std::list<castor::tape::tapegateway::RetryPolicyElement>& candidates)
	  throw (castor::exception::Exception)=0;
	

	/**
	 * Update the db using the retry recall policy returned values
	 */
        virtual void  setRecRetryResult(const std::list<u_signed64>& rjToRetry,
					const std::list<u_signed64>& rjToFail) 
	  throw (castor::exception::Exception)=0;


	/**
	 * Access the db to retrieve the information about a completed migration
	 */
	virtual void getMigratedFileInfo(const castor::tape::tapegateway::FileMigratedNotification& file,
                                         std::string& vid,
                                         int& copyNumber,
                                         u_signed64& lastModificationTime,
                                         std::string& originalVid,
                                         int& originalCopyNumber,
                                         std::string& fileClass)
	  throw (castor::exception::Exception)=0;

	/*
	 * Update the database when the Tapegateway allows us to serve a request
	 */
	virtual void  startTapeSession( const castor::tape::tapegateway::VolumeRequest& startReq,
					castor::tape::tapegateway::Volume& volume) 
	  throw (castor::exception::Exception)=0; 

	/*
	 * Update the database when the tape request has been served 
	 */
	virtual void endTapeSession(const castor::tape::tapegateway::EndNotification& endRequest)
	  throw (castor::exception::Exception)=0; 

	/*
	 * Access the db to retrieve the information about a completed recall 
	 */
	virtual void getSegmentInfo(const FileRecalledNotification &fileRecalled,
				    std::string& vid,
				    int& copyNb)
	  throw (castor::exception::Exception)=0; 

	/*
	 * update the db after a major failure
	 */
	virtual void failTapeSession(const castor::tape::tapegateway::EndNotificationErrorReport& failure)
	  throw (castor::exception::Exception)=0;

	/*
	 * update the db after a file failure
	 */
	virtual void failFileTransfer(const FileErrorReport& failure)
	  throw (castor::exception::Exception)=0;

	/* Structure carrying the tape information needed for a release. */
	enum TapeMode {
	  read  = 0,
	  write = 1
	};
	struct TapeToReleaseInfo {
	  std::string vid;
	  enum TapeMode mode;
	  bool full;
	};
	/* get tapes to release in vmgr */
	virtual void  getTapeToRelease(const u_signed64& mountTransactionId,
	  castor::tape::tapegateway::ITapeGatewaySvc::TapeToReleaseInfo& tape)
	  throw (castor::exception::Exception)=0;

	/* commit transaction */
	virtual void endTransaction() 
	  throw (castor::exception::Exception)=0;

	/* delete migration mounts with wrong tapepool */
	virtual void deleteMigrationMountWithBadTapePool(const u_signed64 migrationMountId)
	  throw (castor::exception::Exception)=0;
	
	/* delete tape request associated to a bad tape */
	virtual void deleteTapeRequest(const u_signed64& tapeRequestId)throw (castor::exception::Exception)=0;

	// Mark tape full for the tape session.
	// This is typically called when a file migration gets a tape full
	// error so that we remember to make the tape as full at the end of
	// the session. Session is passed by VDQM request id (like for end/failSession).
	virtual void flagTapeFullForMigrationSession(const u_signed64& tapeRequestId)
	throw (castor::exception::Exception) = 0;

	// Find the VID (and just it) for a migration mount.
	// This allows a safer update for the VMGR's fseq on this tape.
	// Past that update, fiddling with a file will only affect the file itself
	virtual void getMigrationMountVid(FileMigratedNotification & fileMigrated,
	    std::string& vid, std::string& tapePool) = 0;

    /* Bypass access the the underlying DB accessor allowing safe handling from the caller */
    virtual void commit() = 0;

    /* Bypass access the the underlying DB accessor allowing safe handling from the caller */
    virtual void rollback() = 0;



      }; // end of class ITapeGatewaySvc

} // end of namespace tapegateway
} // end of namespace tape
} // end of namespace castor

#endif // STAGER_ITAPEGATEWAYSVC_HPP
