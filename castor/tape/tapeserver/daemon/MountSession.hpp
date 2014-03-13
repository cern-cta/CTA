/******************************************************************************
 *                      MountSession.hpp
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/log/Logger.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/utils/utils.hpp"
#include "ClientInterface.hpp"

using namespace castor::tape;
using namespace castor::log;

namespace castor {
namespace tape {
namespace server {
  /**
   * The main class handling a tape session. This is the main container started
   * by the master process. It will drive a separate process. Only the sub
   * process interface is not included here to allow testability.
   */
  class MountSession {
  public:
    /** Constructor */
    MountSession(const legacymsg::RtcpJobRqstMsgBody & clientRequest, 
            castor::log::Logger & logger, System::virtualWrapper & sysWrapper,
            const utils::TpconfigLines & tpConfig);
    /** The only method. It will execute (like a task, that it is) */
    void execute() throw (Exception);
    /** Temporary method used for debugging while building the session class */
    std::string getVid() { return m_volInfo.vid; }
  private:
    legacymsg::RtcpJobRqstMsgBody m_request;
    castor::log::Logger & m_logger;
    ClientInterface m_clientIf;
    ClientInterface::VolumeInfo m_volInfo;
    System::virtualWrapper & m_sysWrapper;
    utils::TpconfigLines m_tpConfig;
    /** sub-part of execute for the read sessions */
    void executeRead(LogContext & lc);
    /** sub-part of execute for a write session */
    void executeWrite(LogContext & lc);
    /** sub-part of execute for a dump session */
    void executeDump(LogContext & lc);
  };
}
}
}
