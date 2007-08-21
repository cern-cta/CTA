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
 * @(#)$RCSfile: BaseDbThread.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/08/21 16:58:18 $ $Author: itglp $
 *
 * Base class for a database oriented thread. It correctly implements the stop
 * method, but it can be used only for a pool with a single thread.
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include <iostream>
#include <string>
#include "castor/server/BaseDbThread.hpp"
#include "castor/Services.hpp"

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseDbThread::init() {
  castor::IService* s = svcs()->service("DbCnvSvc", castor::SVC_DBCNV);
  m_cnvSvc = dynamic_cast<castor::db::DbCnvSvc*>(s);
}

//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void castor::server::BaseDbThread::stop() {
  if(m_cnvSvc) {
    m_cnvSvc->dropConnection();
  }
}
