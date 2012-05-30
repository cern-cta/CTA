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
#include "castor/stager/ICommonSvc.hpp"

#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/RetryPolicyElement.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"

#include <list>
#include <string>
#include <queue>
#include <inttypes.h>
#include <iomanip>

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
        virtual void attachTapesToMigMounts(const std::list<u_signed64>& strIds,
                                            const std::list<std::string>& vids,
                                            const std::list<int>& fseqs)
          throw (castor::exception::Exception)=0;

        /** get the tapes for which a VDQM request si needed
         * @param vidsForMigr a vector to be filled with tapes to handle for migration
         * @param tapesForRecall this vector is filled with tapes that need a VDQM request for recall
         * Each tape is given by the pair VID, vdqmPriority
         * @exception throws castor exceptions in case of failure
         */
        virtual void getTapeWithoutDriveReq(std::vector<std::string> &vidsForMigr,
                                            std::vector<std::pair<std::string, int> > &tapesForRecall)
          throw (castor::exception::Exception) = 0;

        /** updates the db with the VDQM mountTransaction id
         * after we have sent a recall or migration request to VDQM
         * @param vid the tape's vid identifying the request
         * @param mountTransactionId the mountTransactionId
         * @param mode the request mode (WRITE_DISABLE for Recalls, WRITE_ENABLE for Migrations)
         * @param label the label of the tape
         * @param density the density of the tape
         * @exception throws castor exceptions in case of failure
         */
        virtual void attachDriveReq(const std::string &vid,
                                    const u_signed64 mountTransactionId,
                                    const int mode,
                                    const char *label,
                                    const char *density)
          throw (castor::exception::Exception) = 0;

        /**
         * Little nested struct to simplify the interface of getTapesWithDriveReqs
         */
        struct TapeRequest {
          int mode;
          u_signed64 mountTransactionId;
          std::string vid;
        };

        /** To get the transactionids and vids of all ongoing recall or migrations
         * for which there should be a VDQM request
         * @param request a list of TapeRequests to be filled
         * @param timeout only lists requests that have not been checked for more than this
         * @exception throws castor exceptions in case of failure
         */
        virtual void getTapesWithDriveReqs(std::list<TapeRequest>& requests,
                                           const u_signed64& timeOut)
          throw (castor::exception::Exception) = 0;

        /** restarts requests lost by VDQM or a request which was processed
         * while the tapegateway was down
         * @param mountTransactionIds the transaction ids of the requests to be restarted
         */
        virtual void restartLostReqs(const std::list<int>& mountTransactionIds)
          throw (castor::exception::Exception) = 0;

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
         * Update the database when the Tapegateway allows us to serve a request
         */
        virtual void  startTapeSession( const castor::tape::tapegateway::VolumeRequest& startReq,
                                        castor::tape::tapegateway::Volume& volume) 
          throw (castor::exception::Exception)=0; 

        /**
         *  Structure carrying the tape information needed for a release.
         */
        enum TapeMode {
          read  = 0,
          write = 1
        };
        struct TapeToReleaseInfo {
          std::string vid;
          enum TapeMode mode;
          bool full;
        };
        /**
         *  get tapes to release in vmgr
         */
        virtual void  getTapeToRelease(const u_signed64& mountTransactionId,
          castor::tape::tapegateway::ITapeGatewaySvc::TapeToReleaseInfo& tape)
          throw (castor::exception::Exception)=0;

          /** Ends a tape session by dropping it from the DB. If the tapebridge
           * comes afterwards asking for more data on the dropped session, it will
           * get an error that shall be gracefully handled on its side.
           * @param mountTransactionId the mountTansactionId of the session to end
           * @param errorCode an error code is the session is ended due to an error
           * if not given, defaults to 0
           * @exception throws castor exceptions in case of failure
           */
        virtual void endTapeSession(const u_signed64 mountTransactionId,
                                    const int errorCode = 0)
          throw (castor::exception::Exception) = 0;

        /**
         *  delete migration mounts with wrong tapepool
         */
        virtual void deleteMigrationMountWithBadTapePool(const u_signed64 migrationMountId)
          throw (castor::exception::Exception)=0;
      
        /**
         * Mark tape full for the tape session.
         * This is typically called when a file migration gets a tape full
         * error so that we remember to make the tape as full at the end of
         * the session. Session is passed by VDQM request id (like for end/failSession).
         */
        virtual void flagTapeFullForMigrationSession(const u_signed64& tapeRequestId)
          throw (castor::exception::Exception) = 0;
      
        /**
         * Find the VID (and just it) for a migration mount.
         * This allows a safer update for the VMGR's fseq on this tape.
         * Past that update, fiddling with a file will only affect the file itself.
         */
        virtual void getMigrationMountVid(const u_signed64&  mountTransactionId,
            std::string& vid, std::string& tapePool) = 0;

        /** cancels a migration or recall for the given tape
         * @param mode the request mode (WRITE_DISABLE for Recalls, WRITE_ENABLE for Migrations)
         * @param vid the name of the tape concerned
         * @param errorCode the code of the error that triggered the cancelation
         * @param errorMsg the message of the error that triggered the cancelation
         * @exception throws castor exceptions in case of failure
         */
        virtual void cancelMigrationOrRecall(const int mode,
                                             const std::string &vid,
                                             const int errorCode,
                                             const std::string &errorMsg)
          throw (castor::exception::Exception) = 0;

        /**
         * Get the next best files to migrate
         */
        virtual void getBulkFilesToMigrate (
            const std::string & context,
            u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
            std::queue<castor::tape::tapegateway::FileToMigrateStruct>& filesToMigrate)
          throw (castor::exception::Exception)=0;

        /**
         * Get the next best files to recall
         */
        virtual void getBulkFilesToRecall (
            const std::string & context,
            u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
            std::queue<castor::tape::tapegateway::FileToRecallStruct>& filesToRecall)
          throw (castor::exception::Exception)=0;

        /**
         * Check and update the NS and then the stager DB accordingly from migration result
         * transmitted by the tape server.
         */

        virtual  void  setBulkFileMigrationResult (
            const std::string & context, u_signed64 mountTransactionId,
            std::vector<FileMigratedNotificationStruct *>& successes,
            std::vector<FileErrorReportStruct *>& failures)
          throw (castor::exception::Exception)=0;

        /**
         * Check the NS and update the stager DB accordingly for files from recall result
         * transmitted by the tape server.
         */
        virtual  void  setBulkFileRecallResult (
            const std::string & context, u_signed64 mountTransactionId,
            std::vector<FileRecalledNotificationStruct *>& successes,
            std::vector<FileErrorReportStruct *>& failures)
        throw (castor::exception::Exception)=0;

        /**
         *  Bypass access the the underlying DB accessor allowing safe handling from the caller
         */
        virtual void commit() = 0;
    
        /**
         *  Bypass access the the underlying DB accessor allowing safe handling from the caller
         */
        virtual void rollback() = 0;

      }; // end of class ITapeGatewaySvc

    } // end of namespace tapegateway
  } // end of namespace tape
} // end of namespace castor

#endif // STAGER_ITAPEGATEWAYSVC_HPP
