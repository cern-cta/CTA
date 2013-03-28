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
 *
 * Base thread for the select/process model: it loops until select() returns
 * something to do, or until a stop signal is received.
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/Services.hpp"

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::SelectProcessThread::run(void* param) {
  //castor::IService* svc = getService();
  SignalThreadPool* p = (SignalThreadPool*)param;
  while(!p->stopped()) {
    castor::IObject* selectOutput = select(); //svc);
    if(selectOutput == 0)
      break;
    process(selectOutput);  // (svc, );
  }
}
