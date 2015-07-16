/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/NsProxyTcpIp.hpp"
#include "Cns.h"
#include "getconfent.h"
#include "Cns_api.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::NsProxyTcpIp::NsProxyTcpIp(log::Logger &log, const int netTimeout) throw():
    m_log(log),
    m_netTimeout(netTimeout) {
} 

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::NsProxyTcpIp::~NsProxyTcpIp() throw() {
}

//------------------------------------------------------------------------------
// doesTapeHaveNsFiles
//------------------------------------------------------------------------------
castor::legacymsg::NsProxy::TapeNsStatus castor::legacymsg::NsProxyTcpIp::doesTapeHaveNsFiles(const std::string &vid) throw(castor::exception::Exception) {
  int flags = CNS_LIST_BEGIN;
  struct Cns_direntape *dtp = NULL;
  char *const host = getconfent (CNS_SCE, "HOST", 0);
  Cns_list list;
  
  while((dtp = Cns_listtape(host, vid.c_str(), flags, &list, 0)) != NULL) {
    flags = CNS_LIST_CONTINUE;
    if (dtp->s_status == 'D') {
      return NSPROXY_TAPE_HAS_AT_LEAST_ONE_DISABLED_SEGMENT;
    } else {
      return NSPROXY_TAPE_HAS_AT_LEAST_ONE_ACTIVE_SEGMENT;
    }
  }
  if(serrno > 0 && serrno != ENOENT) {
    castor::exception::Internal ex;
    ex.getMessage() << "Received error code from Cns_listtape while listing files on tape: " <<
      vid << " serrno=" << serrno;
    throw ex;
  }
  if(serrno == 0) {
    (void)Cns_listtape(host, vid.c_str(), CNS_LIST_END, &list, 0);
  }
  return NSPROXY_TAPE_EMPTY;
}
