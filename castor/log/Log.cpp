/******************************************************************************
 *                      Log.cpp
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
 * @(#)Dlf.cpp,v 1.1 $Release$ 2005/04/05 11:51:33 sponcec3
 *
 * Interface to the CASTOR logging system
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/log/Log.hpp"

//-----------------------------------------------------------------------------
// writeMsg
//-----------------------------------------------------------------------------
void castor::log::writeMsg(
  const int severity,
  const std::string &msg,
  const int numParams,
  const castor::log::Param params[],
  const struct timeval &timeStamp) throw() {
  //---------------------------------------------------------------------------
  // Note that we do here part of the work of the real syslog call, by building
  // the message ourselves. We then only call a reduced version of syslog
  // (namely dlf_syslog). The reason behind it is to be able to set the
  // message timestamp ourselves, in case we log messages asynchronously, as
  // we do when retrieving logs from the DB
  //----------------------------------------------------------------------------

  // TO BE DONE
}

//-----------------------------------------------------------------------------
// writeMsg
//-----------------------------------------------------------------------------
void castor::log::writeMsg(
  const int severity,
  const std::string &msg,
  const int numParams,
  const castor::log::Param params[]) throw() {

  struct timeval timeStamp;
  gettimeofday(&timeStamp, NULL);

  writeMsg(severity, msg, numParams, params, timeStamp);
}
