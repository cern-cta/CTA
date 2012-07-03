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

          // Find vdqm request Ids for migration mounts still referencing those tapes.
          virtual void getMigrationMountReqsForVids(const std::list<std::string>& vids,
                                                            std::list<blockingSessionInfo>& blockingSessions)
            throw (castor::exception::Exception);

          // To create the db link between a Tape and a migrationMountParameters
          virtual void attachTapesToMigMounts(const std::list<u_signed64>& strIds,
              const std::list<std::string>& vids,
              const std::list<int>& fseqs)
          throw (castor::exception::Exception);

          /** get a tape for which a VDQM request si needed
           * @param vid a string filled with the VID of the selected tape
           * @param vdqmPriority an int filled with the priority to use in the call to VDQM
           * @param mode the mode of access to the tape (WRITE_DISABLE or WRITE_ENABLE)
           * @exception throws castor exceptions in case of failure
           */
          virtual void getTapeWithoutDriveReq(std::string &vid,
                                              int &vdqmPriority,
                                              int &mode)
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

          // To get the tapecopies which faced a migration failure
          virtual void  getFailedMigrations(std::list<castor::tape::tapegateway::RetryPolicyElement>& candidates)
          throw (castor::exception::Exception);

          // To update the db using the retry migration policy returned values
          virtual void  setMigRetryResult(const std::list<u_signed64>& mjToRetry,
              const std::list<u_signed64>& mjToFail )
          throw (castor::exception::Exception);

          // To update the database when the tapebridge allows
          // us to serve a request
          virtual void  startTapeSession(const castor::tape::tapegateway::VolumeRequest& startReq,
              castor::tape::tapegateway::Volume& volume)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);


          /** Ends a tape session by dropping it from the DB. If the tapebridge
           * comes afterwards asking for more data on the dropped session, it will
           * get an error that shall be gracefully handled on its side.
           * This version is an autonomous transaction version, allowing the cleanup
           * of old leftover sessions while creating new ones
           * @param mountTransactionId the mountTansactionId of the session to end
           * @param errorCode an error code is the session is ended due to an error
           * if not given, defaults to 0
           * @exception throws castor exceptions in case of failure
           */
        virtual void endTapeSessionAutonomous(const u_signed64 mountTransactionId,
                                    const int errorCode = 0)
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
           * Get the next best files to migrate
           */
          virtual void getBulkFilesToMigrate (const std::string & context,
              u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
              std::queue<castor::tape::tapegateway::FileToMigrateStruct>& filesToMigrate)
          throw (castor::exception::Exception);

          /**
           * Get the next best files to recall
           */
          virtual void getBulkFilesToRecall (const std::string & context,
              u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
              std::queue<castor::tape::tapegateway::FileToRecallStruct>& filesToRecall)
          throw (castor::exception::Exception);

          /**
           * Check and update the NS and then the stager DB accordingly from migration result
           * transmitted by the tape server.
           */
          virtual  void  setBulkFileMigrationResult (
              const std::string & context, u_signed64 mountTransactionId,
              std::vector<FileMigratedNotificationStruct *>& successes,
              std::vector<FileErrorReportStruct *>& failures)
          throw (castor::exception::Exception);

          /**
           * Check the NS and update the stager DB accordingly for files from recall result
           * transmitted by the tape server.
           */
          virtual  void  setBulkFileRecallResult (
              const std::string & context, u_signed64 mountTransactionId,
              std::vector<FileRecalledNotificationStruct *>& successes,
              std::vector<FileErrorReportStruct *>& failures)
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
          oracle::occi::Statement *m_getFailedMigrationsStatement;
          oracle::occi::Statement *m_setMigRetryResultStatement;
          oracle::occi::Statement *m_startTapeSessionStatement;
          oracle::occi::Statement *m_endTapeSessionStatement;
          oracle::occi::Statement *m_endTapeSessionAutonomousStatement;
          oracle::occi::Statement *m_failFileTransferStatement;
          oracle::occi::Statement *m_getTapeToReleaseStatement;
          oracle::occi::Statement *m_cancelMigrationOrRecallStatement;
          oracle::occi::Statement *m_deleteMigrationMountWithBadTapePoolStatement;
          oracle::occi::Statement *m_flagTapeFullForMigrationSession;
          oracle::occi::Statement *m_getMigrationMountVid;
          oracle::occi::Statement *m_getBulkFilesToMigrate;
          oracle::occi::Statement *m_getBulkFilesToRecall;
          oracle::occi::Statement *m_setBulkFileMigrationResult;
          oracle::occi::Statement *m_setBulkFileRecallResult;
          oracle::occi::Statement *m_getMigrationMountReqsForVids;

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
              try {
                uint32_t n_high = m_n / _32;  // throws an exception when m_n < _32
                uint32_t n_low  = m_n - oracle::occi::Number(n_high) * _32;
                u_signed64 ret = u_signed64 (n_high) << 32  | u_signed64 (n_low);
                return ret;
              } catch (oracle::occi::SQLException& oe) {
                if(oe.getErrorCode() == 22054) {  // ORA-22054 = Underflow error
                  u_signed64 ret = (uint32_t) m_n;
                  return ret;
                }
                else throw oe;
              }
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
