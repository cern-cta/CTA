
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
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef WORKER_THREAD_HPP
#define WORKER_THREAD_HPP 1

#include "castor/exception/Internal.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/IThread.hpp"


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
    virtual void stop() {}

  private:
	
    // handlers used with the different message types

    castor::IObject* handleStartWorker(castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw();
    
    castor::IObject* handleRecallUpdate( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc  ) throw();

    castor::IObject* handleMigrationUpdate( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw();

    castor::IObject* handleRecallMoreWork( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw();

    castor::IObject* handleMigrationMoreWork( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw();

    castor::IObject* handleEndWorker( castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw();

    castor::IObject* handleFailWorker( castor::IObject&  obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc ) throw();


    // map to deal with the different request types

    typedef castor::IObject* (WorkerThread::*Handler) (castor::IObject& obj, castor::tape::tapegateway::ITapeGatewaySvc& oraSvc );

    typedef std::map<u_signed64, Handler> HandlerMap;
    HandlerMap m_handlerMap;

  };
  
} // end of tapegateway  
} // end of namespace tape
} // end of namespace castor

#endif // WORKER_THREAD_HPP
