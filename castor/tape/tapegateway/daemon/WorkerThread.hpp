
/******************************************************************************
 *                      WorkerThread.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef WORKER_THREAD_HPP
#define WORKER_THREAD_HPP 1

#include "castor/exception/Internal.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/IThread.hpp"
#include "castor/tape/net/Constants.hpp"
#include "castor/tape/utils/ShutdownBoolFunctor.hpp"
#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include <list>


namespace castor     {
namespace tape       {
namespace tapegateway{

  /**
   * Worker  tread.
   */
  
  class WorkerThread : public virtual castor::server::IThread,
                       public castor::BaseObject {
	
  public:

    WorkerThread();
    virtual ~WorkerThread() throw() {};

    /**
     * Initialization of the thread.
     */
    virtual void init() {}

    /**
     * Main work for this thread
     */
    virtual void run(void* param);

    /**
     * Stop of the thread
     */
    virtual void stop() {m_shuttingDown.set();}

  private:
    // Package of a requester's information
    class requesterInfo {
    public:
      requesterInfo():port(0),ip(0){
        hostName[0]='\0';
      };
      virtual ~requesterInfo() throw() {};
      char hostName[castor::tape::net::HOSTNAMEBUFLEN];
      unsigned short port;
      unsigned long ip;
      // Helper converting requester structure into a context string
      std::string str();
    };
    // Helper converting requester structure into a context string
    std::string requester2str (const requesterInfo & reqer);
    // handlers used with the different message types
    castor::IObject* handleStartWorker      (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw(castor::exception::Exception);
    castor::IObject* handleEndWorker        (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw(castor::exception::Exception);
    castor::IObject* handleFailWorker       (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw(castor::exception::Exception);
    castor::IObject* handleFileMigrationReportList   (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw(castor::exception::Exception);
    castor::IObject* handleFileRecallReportList  (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw(castor::exception::Exception);
    castor::IObject* handleFilesToMigrateListRequest (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw(castor::exception::Exception);
    castor::IObject* handleFilesToRecallListRequest  (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw(castor::exception::Exception);

    // Helper function for setting a sesssion to closing in the DB.
    // This is usually in reaction to a problem, which is logged in the caller.
    // Comes with logging of its own problems as internal error (context in a
    // string).
    // As it's a last ditch action, there is no need to send further exceptions.
    void setSessionToClosing (castor::tape::tapegateway::ITapeGatewaySvc&  oraSvc,
        castor::tape::tapegateway::WorkerThread::requesterInfo& requester,
        u_signed64 mountTransactionId)
        throw ();

    // Error classifiers. They return a class with booleans indicating the decision.
    // They are used for dispatch criteria in handle report list functions.
    // The default values reflect the usual behavior: just retry the file.
    struct fileErrorClassification {
      fileErrorClassification():
        fileInvolved(true),
        fileRetryable(true),
        tapeIsFull(false) {};
      virtual ~fileErrorClassification() {};
      bool fileInvolved;
      bool fileRetryable;
      bool tapeIsFull;
    };
    fileErrorClassification classifyBridgeMigrationFileError(int errorCode) throw ();
    fileErrorClassification classifyBridgeRecallFileError(int errorCode) throw ();

    // Helper functions for logging database errors (overloaded variants with different
    // parameters.
    void logInternalError (castor::exception::Exception e, requesterInfo& requester,
        FileMigrationReportList & fileMigrationReportList) throw ();
    void logInternalError (castor::exception::Exception e, requesterInfo& requester,
        FileRecallReportList &fileRecallReportList) throw ();
    void logMigrationNotified (Cuuid_t uuid,
        u_signed64 mountTransactionId, u_signed64 aggregatorTransactionId,
        struct Cns_fileid* castorFileId,
        const requesterInfo& requester, const FileMigratedNotificationStruct & fileMigrated);
    void logMigrationBulkVmgrUpdate (Cuuid_t uuid,
        const requesterInfo& requester,  const FileMigrationReportList & fileMigrationReportList,
        int filesCount, u_signed64 highestFseq, u_signed64 totalBytes,
        u_signed64 totalCompressedBytes, const std::string & tapePool,
        const std::string & vid, signed64 procTime);
    void logMigrationVmgrFailure (Cuuid_t uuid, const requesterInfo& requester,
        const FileMigrationReportList &fileMigrationReportList,
        const std::string & vid, const std::string & tapePool,
        castor::exception::Exception & e);
    void logMigrationCannotFindVid (Cuuid_t uuid,
        const requesterInfo& requester, const FileMigrationReportList & migrationReport,
        castor::exception::Exception & e);
    void logTapeReadOnly (Cuuid_t uuid, const requesterInfo& requester,
        const FileMigrationReportList & fileMigrationReportList,
        const std::string & tapePool, const std::string & vid);
    void logCannotReadOnly (Cuuid_t uuid, const requesterInfo& requester,
        const FileMigrationReportList & fileMigrationReportList,
        const std::string & tapePool, const std::string & vid,
        castor::exception::Exception & e);

    utils::ShutdownBoolFunctor m_shuttingDown;

    // Utility class for sorting migration reports in fseq order.
    class FileMigratedFseqComparator { public:
      bool operator()(const FileMigratedNotificationStruct *a, const FileMigratedNotificationStruct *b) const
        { return a->fseq() < b->fseq(); }
    } m_fileMigratedFseqComparator;
  };
  
} // end of tapegateway  
} // end of namespace tape
} // end of namespace castor

#endif // WORKER_THREAD_HPP
