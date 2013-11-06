/******************************************************************************
 *                castor/stager/daemon/LoggingThread.cpp
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
 * Thread for dumping database logs to DLF
 *
 * @author castor dev team
 *****************************************************************************/

#include <vector>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/stager/daemon/LoggingThread.hpp"

//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::stager::daemon::LoggingThread::run(void*) throw() {
  try {
    // get the StagerSvc
    castor::Services* svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
    castor::stager::IStagerSvc *gcSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);

    // as dlopen is not reentrant (i.e., symbols might be still loading now due to the dlopen
    // of another thread), it may happen that the service is not yet valid or dynamic_cast fails.
    // In such a case we simply give up for this round.
    if(gcSvc == 0) return;

    // actual work: dump the DB logs to DLF
    gcSvc->dumpDBLogs();
    
    // log we're done
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, STAGER_LOGGING_DONE);

  } catch (castor::exception::Exception& e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "LoggingThread::run"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_LOGGING_EXCEPT, 3, params);
  }
}
