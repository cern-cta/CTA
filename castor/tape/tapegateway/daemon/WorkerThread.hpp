
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
 * @(#)$RCSfile: WorkerThread.hpp,v $ $Author: murrayc3 $
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
    };
    // handlers used with the different message types
    castor::IObject* handleStartWorker      (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleRecallUpdate     (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleMigrationUpdate  (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleRecallMoreWork   (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleMigrationMoreWork(castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleEndWorker        (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleFailWorker       (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleFileFailWorker   (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleFileMigrationReportList   (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleFileRecallReportList  (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleFilesToMigrateListRequest (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();
    castor::IObject* handleFilesToRecallListRequest  (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc, requesterInfo& requester ) throw();

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
