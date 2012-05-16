/******************************************************************************
 *                castor/tape/tapegateway/ora/OraTapeGateway.hpp
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
 * Implementation of the ITapeGatewaySvc for Oracle
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef ORA_ORATAPEGATEWAYSVC_HPP
#define ORA_ORATAPEGATEWAYSVC_HPP 1

// Include Files

#include <u64subr.h>
#include <list>

#include "occi.h"

#include "castor/BaseSvc.hpp"

#include "castor/db/newora/OraCommonSvc.hpp"

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"


namespace castor      {
  namespace tape        {
    namespace tapegateway {
      namespace ora         {

        /**
         * Implementation of the ITapeGatewaySvc for Oracle
         */
        class OraTapeGatewaySvc :
          public castor::db::ora::OraCommonSvc,
          public virtual castor::tape::tapegateway::ITapeGatewaySvc {

        public:

          OraTapeGatewaySvc(const std::string name);
          virtual ~OraTapeGatewaySvc() throw();
          virtual inline unsigned int id() const;
          static unsigned int ID();
          void reset() throw ();

        public:

          // To get all the migration mount without a Tape associated to it
          virtual void  getMigrationMountsWithoutTapes(std::list<castor::tape::tapegateway::ITapeGatewaySvc::migrationMountParameters>& migrationMounts)
          throw (castor::exception::Exception);

          // To create the db link between a Tape and a migrationMountParameters
          virtual void attachTapesToMigMounts(const std::list<u_signed64>& strIds,
              const std::list<std::string>& vids,
              const std::list<int>& fseqs)
          throw (castor::exception::Exception);

          /** get the tapes for which a VDQM request si needed
           * @param vidsForMigr a vector to be filled with tapes to handle for migration
           * @param tapesForRecall this vector is filled with tapes that need a VDQM request for recall
           * Each tape is given by the pair VID, vdqmPriority
           * @exception throws castor exceptions in case of failure
           */
          virtual void getTapeWithoutDriveReq(std::vector<std::string> &vidsForMigr,
              std::vector<std::pair<std::string, int> > &tapesForRecall)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

          /** To get the transactionids and vids of all ongoing recall or migrations
           * for which there should be a VDQM request
           * @param request a list of TapeRequests to be filled
           * @param timeout only lists requests that have not been checked for more than this
           * @exception throws castor exceptions in case of failure
           */
          virtual void getTapesWithDriveReqs(std::list<struct TapeRequest>& requests,
              const u_signed64& timeOut)
          throw (castor::exception::Exception);

          /** restarts requests lost by VDQM or a request which was processed
           * while the tapegateway was down
           * @param mountTransactionIds the transaction ids of the requests to be restarted
           */
          virtual void restartLostReqs(const std::list<int>& mountTransactionIds)
          throw (castor::exception::Exception);

          //  To get the best file to migrate when the function is called
          virtual void  getFileToMigrate(
              const castor::tape::tapegateway::FileToMigrateRequest& req,
              castor::tape::tapegateway::FileToMigrate& file)
          throw (castor::exception::Exception);

          // To update the db for a file which is migrated successfully
          virtual  void  setFileMigrated(
              const castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception);

          // Update the db for a semgent whose migration was rejected as non-necessary by the name server
          virtual  void  dropSuperfluousSegment(
              const castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception);

          // To update the db for a file which can't be referenced in the
          // name server anymore after a successful migration (file changed in the mean time)
          virtual  void  setFileStaleInMigration(
              const castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception);

          //  To get the best file to recall when the function is called
          virtual void getFileToRecall(
              const castor::tape::tapegateway::FileToRecallRequest&  req,
              castor::tape::tapegateway::FileToRecall& file )
          throw (castor::exception::Exception);

          /** updates the db for a file which has been recalled successfully.
           * Simple wrapper around tg_setFileRecalled PL/SQL procedure
           * @param fileRecalled the FileRecalledNotification object received
           * @exception throws castor exceptions when failing
           */
          virtual void setFileRecalled(const castor::tape::tapegateway::FileRecalledNotification& fileRecalled)
          throw (castor::exception::Exception);

          // To get the tapecopies which faced a migration failure
          virtual void  getFailedMigrations(std::list<castor::tape::tapegateway::RetryPolicyElement>& candidates)
          throw (castor::exception::Exception);

          // To update the db using the retry migration policy returned values
          virtual void  setMigRetryResult(const std::list<u_signed64>& mjToRetry,
              const std::list<u_signed64>& mjToFail )
          throw (castor::exception::Exception);

          // To access the db to retrieve the information about a completed migration
          virtual void getMigratedFileInfo(const castor::tape::tapegateway::FileMigratedNotification& file,
              std::string& vid,
              int& copyNumber,
              u_signed64& lastModificationTime,
              std::string& originalVid,
              int& originalCopyNumber,
              std::string& fileClass)
          throw (castor::exception::Exception);

          // To update the database when the tapebridge allows
          // us to serve a request
          virtual void  startTapeSession(const castor::tape::tapegateway::VolumeRequest& startReq,
              castor::tape::tapegateway::Volume& volume)
          throw (castor::exception::Exception);

          /** Ends a tape session
           * @param mountTransactionId the mountTansactionId of the session to end
           * @param errorCode an error code is the session is ended due to an error
           * if not given, defaults to 0
           * @exception throws castor exceptions in case of failure
           */
          virtual void endTapeSession(const u_signed64 mountTransactionId,
              const int errorCode = 0)
          throw (castor::exception::Exception);

          /** updates the stager db after a file failure
           * @param mountTransactionId the transaction id of the mount where the failure took place
           * @param fileId the file concerned by the failure
           * @param nsHost the namespace where the file concerned by the failure lives
           * @param errorCode the code of the error that triggered the failure
           * @exception throws castor exceptions in case of failure
           */
          virtual void failFileTransfer(const u_signed64 mountTransactionId,
              const u_signed64 fileId,
              const std::string &nsHost,
              const int errorCode)
          throw (castor::exception::Exception);

          // To get tapes to release in vmgr */
          virtual void  getTapeToRelease(const u_signed64& mountTransactionId,
              castor::tape::tapegateway::ITapeGatewaySvc::TapeToReleaseInfo& tape)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

          // To delete migartion mounts with wrong tapepool
          virtual void deleteMigrationMountWithBadTapePool(
              const u_signed64 migrationMountId)
          throw (castor::exception::Exception);

          // Find the VID (and just it) for a migration mount.
          // This allows a safer update for the VMGR's fseq on this tape.
          // Past that update, fiddling with a file will only affect the file itself
          virtual void getMigrationMountVid(const u_signed64&  mountTransactionId,
              std::string& vid, std::string& tapePool);

          // Mark tape full for the tape session.
          // This is typically called when a file migration gets a tape full
          // error so that we remember to make the tape as full at the end of
          // the session. Session is passed by VDQM request id (like for end/failSession).
          virtual void flagTapeFullForMigrationSession(const u_signed64& tapeRequestId)
          throw (castor::exception::Exception);

          /**
           * Set tape session to closing: moves the tape session to a state
           * where no more work will be retrieved, in order to get the session
           * to fold down gracefully. This mechanism moves the error handling
           * back to the tape gateway from the tape server.
           * Thanks to this, most of the replies to the tape server can be a neutral
           * "got it".
           */
          virtual void setTapeSessionClosing (u_signed64 mountTransactionId)
          throw (castor::exception::Exception);

          /**
           * Get the next best files to migrate
           */
          virtual void getBulkFilesToMigrate (
              u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
              std::queue<castor::tape::tapegateway::FileToMigrateStruct>& filesToMigrate)
          throw (castor::exception::Exception);

          /**
           * Get the next best files to recall
           */
          virtual void getBulkFilesToRecall (
              u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
              std::queue<castor::tape::tapegateway::FileToRecallStruct>& filesToRecall)
          throw (castor::exception::Exception);

          /**
           * Check and update the NS and then the stager DB accordingly from migration result
           * transmitted by the tape server.
           */
          virtual  void  setBulkFileMigrationResult (u_signed64 mountTransactionId,
              std::vector<FileMigratedNotificationStruct *>& successes,
              std::vector<FileErrorReportStruct *>& failures,
              ptr2ref_vector<BulkDbRecordingResult>& dbResults)
          throw (castor::exception::Exception);

          /**
           * Check the NS and update the stager DB accordingly for files from recall result
           * transmitted by the tape server.
           */
          virtual  void  setBulkFileRecallResult (u_signed64 mountTransactionId,
              std::vector<FileRecalledNotificationStruct *>& successes,
              std::vector<FileErrorReportStruct *>& failures,
              ptr2ref_vector<BulkDbRecordingResult>& dbResults)
          throw (castor::exception::Exception);

          // To directly commit
          virtual void commit()
          throw (castor::exception::Exception);

          // To direcly rollback
          virtual void rollback()
          throw (castor::exception::Exception);

        private:

          oracle::occi::Statement *m_getMigrationMountsWithoutTapesStatement;
          oracle::occi::Statement *m_attachTapesToMigMountsStatement;
          oracle::occi::Statement *m_getTapeWithoutDriveReqStatement;
          oracle::occi::Statement *m_attachDriveReqStatement;
          oracle::occi::Statement *m_getTapesWithDriveReqsStatement;
          oracle::occi::Statement *m_restartLostReqsStatement;
          oracle::occi::Statement *m_getFileToMigrateStatement;
          oracle::occi::Statement *m_setFileMigratedStatement;
          oracle::occi::Statement *m_setFileStaleInMigrationStatement;
          oracle::occi::Statement *m_getFileToRecallStatement;
          oracle::occi::Statement *m_setFileRecalledStatement;
          oracle::occi::Statement *m_getFailedMigrationsStatement;
          oracle::occi::Statement *m_setMigRetryResultStatement;
          oracle::occi::Statement *m_getRepackVidAndFileInfoStatement;
          oracle::occi::Statement *m_startTapeSessionStatement;
          oracle::occi::Statement *m_endTapeSessionStatement;
          oracle::occi::Statement *m_failFileTransferStatement;
          oracle::occi::Statement *m_getTapeToReleaseStatement;
          oracle::occi::Statement *m_cancelMigrationOrRecallStatement;
          oracle::occi::Statement *m_deleteMigrationMountWithBadTapePoolStatement;
          oracle::occi::Statement *m_flagTapeFullForMigrationSession;
          oracle::occi::Statement *m_getMigrationMountVid;
          oracle::occi::Statement *m_dropSuperfluousSegmentStatement;
          oracle::occi::Statement *m_setTapeSessionClosing;
          oracle::occi::Statement *m_getBulkFilesToMigrate;
          oracle::occi::Statement *m_getBulkFilesToRecall;
          oracle::occi::Statement *m_setBulkFileMigrationResult;
          oracle::occi::Statement *m_setBulkFileRecallResult;

          // Private helper class used to introspect cursors, making the OCCI code independent of the order of elements
          // in the cursor (especially with %ROWTYPE contexts).
          class resultSetIntrospector {
          public:
            // At construction time, extract the metadata from result set.
            // both STL and OCCI throw (sub classes of) std::exception.
            resultSetIntrospector(oracle::occi::ResultSet *rs) throw (std::exception): m_rsStruct(rs->getColumnListMetaData()){}
            // Trivial destructor.
            virtual ~resultSetIntrospector() {};
            // Look for a given column in the metadata array.
            int findColumnIndex (const std::string& colName, int colType)
            // We could throw std::exception for the STL, or a castor exception.
            const throw (std::exception, castor::exception::Internal) {
              for (unsigned int i=0; i<m_rsStruct.size(); i++) {
                if (colName == m_rsStruct[i].getString(oracle::occi::MetaData::ATTR_NAME) &&
                    colType == m_rsStruct[i].getInt(oracle::occi::MetaData::ATTR_DATA_TYPE))
                  return i+1; // The indexes in OCCI are counted from 1, not 0.
              }
              // getting here means we did not find the column.
              // We will dump all names and type (in numeric form) in the exception to ease diagnostic.
              castor::exception::Internal ex;
              ex.getMessage() << "resultSetIntrospector could not find column " << colName << " of type " << colType
                  << " columns are: ";
              for (unsigned int i=0; i<m_rsStruct.size(); i++) {
                if (i) ex.getMessage() << ", ";
                ex.getMessage() << i << ":(" << m_rsStruct[i].getString(oracle::occi::MetaData::ATTR_NAME) << ","
                    << m_rsStruct[i].getInt(oracle::occi::MetaData::ATTR_DATA_TYPE) << ")";
              }
              throw ex;
            }
          private:
            std::vector<oracle::occi::MetaData> m_rsStruct;
          };

          // Private helper class allowing auto rollback in case of problem inside the
          // OCCI caller function
          class scopedRollback {
          public:
            scopedRollback (OraTapeGatewaySvc * client): m_client(client), m_active(true) {};
            ~scopedRollback ()
            {
              if (m_active) {
                try {
                  m_client->rollback();
                } catch (...) {};
              }
            }
            void engage() { m_active = true; };
            void disengage() { m_active = false; };
          private:
            OraTapeGatewaySvc * m_client;
            bool m_active;
          };

          /* Small wrapper class for occi number, protecting from null values.
           * (Casting from null occi numbers trigger run time exceptions)
           */
          class occiNumber {
          public:
            occiNumber (): m_n(0) {};
            occiNumber (const oracle::occi::Number & n): m_n(n) {};
            /* Hand crafted cast-constructor for 64 bits */
            occiNumber (u_signed64 n) {
              /* From example grabbed from the web:
               * https://forums.oracle.com/forums/thread.jspa?threadID=663724 */
              static const oracle::occi::Number two(2);
              static const oracle::occi::Number _32(two.intPower(32));
              m_n = uint32_t (n & 0xFFFFFFFF);
              m_n += oracle::occi::Number(uint32_t(n >> 32)) * _32;
            };
            ~occiNumber () {};
            occiNumber (const occiNumber & oN): m_n(oN.m_n) {};
            /* Hand crafted cast operator for 64 bits */
            operator u_signed64 () {
              static const oracle::occi::Number two(2);
              static const oracle::occi::Number _32(two.intPower(32));
              uint32_t n_high = m_n / _32;
              uint32_t n_low  = m_n - oracle::occi::Number(n_high) * _32;
              return u_signed64 (n_high) << 32  | u_signed64 (n_low);
            }
            template <typename T>
            operator T () {
              if (m_n.isNull()) {
                return ~0;
              } else {
                return (T) m_n;
              }
            }
          private:
            oracle::occi::Number m_n;
          }; // end of class occiNumber
          }; // end of class OraTapeGateway

      } // end of namespace ora
    } // end of namespace tapegateway
  }  // end of namespace tape
} // end of namespace castor

#endif // ORA_ORATAPEGATEWAYSVC_HPP
