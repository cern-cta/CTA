/******************************************************************************
 *                      BaseDbThread.cpp
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
 * @(#)$RCSfile: BaseDbThread.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2008/04/15 07:42:35 $ $Author: murrayc3 $
 *
 * Base class for a database oriented thread. It correctly implements the stop
 * method by dropping the db connection for each thread in the pool.
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include <iostream>
#include <string>
#include "castor/server/BaseDbThread.hpp"
#include "castor/Services.hpp"
#include "castor/db/DbCnvSvc.hpp"

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseDbThread::init() {
  svcs()->service("DbCnvSvc", castor::SVC_DBCNV);
}

//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void castor::server::BaseDbThread::stop() {
  // this method is called on SIGTERM
  castor::IService* s = svcs()->service("DbCnvSvc", 0);
  castor::db::DbCnvSvc* dbs = dynamic_cast<castor::db::DbCnvSvc*>(s);
  if(dbs) {
    dbs->dropConnection();
    dbs->release();
  }
}
