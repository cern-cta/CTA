/******************************************************************************
 *                      SelectProcessThread.cpp
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
 * @(#)$RCSfile: SelectProcessThread.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/07/25 15:32:29 $ $Author: itglp $
 *
 * Base thread for the select/process model: it loops until select() returns
 * something to do. If stop() is called, the underlying database connection is dropped.
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include <iostream>
#include <string>
#include "castor/server/SelectProcessThread.hpp"
#include "castor/Services.hpp"
#include "castor/db/DbCnvSvc.hpp"

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::SelectProcessThread::run(void* param) {
  while(!m_stopped) {
    castor::IObject* selectOutput = select();
    if(selectOutput == 0)
      break;
    process(selectOutput);
  }
  if(m_stopped) {
    // this is true on a SIGTERM only
    // drop the db connection only if already instantiated
    castor::IService* s = svcs()->service("DbCnvSvc", 0);
    castor::db::DbCnvSvc* dbs = dynamic_cast<castor::db::DbCnvSvc*>(s);
    if(dbs) {
      // if it is already dropped, this is a no-op
      dbs->dropConnection();
    }
  }
}

//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void castor::server::SelectProcessThread::stop() {
  m_stopped = true;
}

