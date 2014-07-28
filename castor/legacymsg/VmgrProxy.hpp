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

#pragma once


#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/VmgrTapeInfoMsgBody.hpp"

#include <string>

namespace castor {
namespace legacymsg {

/**
 * Proxy class representing the vmgr daemon.
 */
class VmgrProxy {
public:

  /**
   * Destructor.
   */
  virtual ~VmgrProxy() throw() = 0;

  /**
   * Notifies the vmgrd daemon that the specified tape has been mounted for read.
   *
   * @param vid The volume identifier of the mounted tape.
   * @param jid The VDQM job id. In pactice, the local unix PID of the
   * forked session process. This process will end at the end of the session.
   * The same pid/jid can be queried using tpstat.
   */
  virtual void tapeMountedForRead(const std::string &vid, uint32_t jid)
     = 0;

  /**
   * Notifies the vmgrd daemon that the specified tape has been mounted for read.
   *
   * @param vid The volume identifier of the mounted tape.
   * @param jid The VDQM job id. In pactice, the local unix PID of the
   * forked session process. This process will end at the end of the session.
   * The same pid/jid can be queried using tpstat.
   */
  virtual void tapeMountedForWrite(const std::string &vid, uint32_t jid)
     = 0;
  
  /**
   * Gets information from vmgrd about the specified tape
   * 
   * @param vid   The volume identifier of the tape.
   * @param reply The structure containing the reply from vmgrd
   */
  virtual void queryTape(const std::string &vid, legacymsg::VmgrTapeInfoMsgBody &reply) = 0;

}; // class VmgrProxy

} // namespace legacymsg
} // namespace castor

