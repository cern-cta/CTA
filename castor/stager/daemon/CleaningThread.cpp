/******************************************************************************
 *                castor/stager/daemon/CleaningThread.cpp
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
 * @(#)$RCSfile: CleaningThread.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2009/03/05 11:45:42 $ $Author: itglp $
 *
 * Thread for logging database cleaning operations
 *
 * @author castor dev team
 *****************************************************************************/

#include "Cthread_api.h"
#include "Cmutex.h"

#include <vector>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/stager/daemon/CleaningThread.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::daemon::CleaningThread::CleaningThread() throw () {}

//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::stager::daemon::CleaningThread::run(void*) throw() {
  try {
    // get the GCSvc
    castor::Services* svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbGCSvc", castor::SVC_DBGCSVC);
    castor::stager::IGCSvc *gcSvc = dynamic_cast<castor::stager::IGCSvc*>(svc);

    // actual work: dump the cleanup logs to DLF (cf. cleaningDaemon)
    gcSvc->dumpCleanupLogs();
    
    // log we're done
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STAGER_GCSVC_CLEANUPDONE);

    // now let's free up the database connection, which otherwise remains idle
    svc = svcs->service("DbCnvSvc", castor::SVC_DBCNV);
    castor::db::DbCnvSvc* dbSvc = dynamic_cast<castor::db::DbCnvSvc*>(svc);
    dbSvc->dropConnection();    
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "CleaningThread::run"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
  }
}
