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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/messages/TapeserverProxy.hpp"
#include "castor/log/Logger.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"

namespace castor {
namespace legacymsg {
  class VdqmProxy;
  class VmgrProxy;
  class RmcProxy;
}
namespace tape {
namespace tapeserver {
namespace daemon {
  /**
   * The main class handling a tape session. This is the main container started
   * by the master process. It will drive a separate process. Only the sub
   * process interface is not included here to allow testability.
   */
  class DataTransferSession {
  public:
    /** Subclass holding all the contents from castor.conf file. The pre-
     * extraction of the contents by the caller instead of direct getconfent
     * calls allows unit testing */
    class CastorConf {
    public:
      CastorConf(): 
        rtcopydBufsz(0), rtcopydNbBufs(0),
        tapebridgeBulkRequestMigrationMaxBytes(0),
        tapebridgeBulkRequestMigrationMaxFiles(0),
        tapebridgeBulkRequestRecallMaxBytes(0),
        tapebridgeBulkRequestRecallMaxFiles(0),
        tapebridgeMaxBytesBeforeFlush(0),
        tapebridgeMaxFilesBeforeFlush(0),
        tapeserverdDiskThreads(0) {}
      uint32_t rtcopydBufsz;
      uint32_t rtcopydNbBufs;
      std::string tapeBadMIRHandlingRepair;
      uint64_t tapebridgeBulkRequestMigrationMaxBytes;
      uint64_t tapebridgeBulkRequestMigrationMaxFiles;
      uint64_t tapebridgeBulkRequestRecallMaxBytes;
      uint64_t tapebridgeBulkRequestRecallMaxFiles;
      uint64_t tapebridgeMaxBytesBeforeFlush;
      uint64_t tapebridgeMaxFilesBeforeFlush;
// Other values found on production tape servers
//      TAPE CRASHED_RLS_HANDLING RETRY
//      TAPE CRASHED_RLS_HANDLING_RETRIES 3
//      TAPE CRASHED_RLS_HANDLING_RETRY_DELAY 180
//      TAPE DOWN_ON_TPALERT NO
//      TAPE DOWN_ON_UNLOAD_FAILURE NO
//      TAPEBRIDGE BULKREQUESTMIGRATIONMAXBYTES 5000000000
//      TAPEBRIDGE BULKREQUESTMIGRATIONMAXFILES 500
//      TAPEBRIDGE BULKREQUESTRECALLMAXBYTES 5000000000
//      TAPEBRIDGE BULKREQUESTRECALLMAXFILES 500
//      TAPEBRIDGE MAXBYTESBEFOREFLUSH 32000000000
//      TAPEBRIDGE MAXFILESBEFOREFLUSH 500
//      TAPEBRIDGE TAPEFLUSHMODE ONE_FLUSH_PER_N_FILES
//      UPV HOST castorcupv
//      VDQM HOST castorvdqm1
//      VMGR HOST castorvmgr

      // Additions for tapeserverd
      uint32_t tapeserverdDiskThreads;
    };
    /** Constructor */
    DataTransferSession(
      const std::string & hostname,
      const legacymsg::RtcpJobRqstMsgBody & clientRequest, 
      castor::log::Logger & logger,
      System::virtualWrapper & sysWrapper,
      const utils::DriveConfig & driveConfig,
      castor::legacymsg::RmcProxy & rmc,
      castor::messages::TapeserverProxy & initialProcess,
      castor::server::ProcessCap &capUtils,
      const CastorConf & castorConf);
    /** The only method. It will execute (like a task, that it is) */
    int execute() ;
    /** Temporary method used for debugging while building the session class */
    std::string getVid() { return m_volInfo.vid; }
    
    /**
     * Return the global shared zmq context for the data-transfer session
     * THIS FUNCTION SHALL ONLY BE CALLED IN THE FORKED PROCESS
     *
     * @return The global shared zmq context for the data-transfer session.
     */
    static void *getZmqContext();
    
    ~DataTransferSession();

  private:

    /**
     * The global shared zmq context for the date-transfer session.
     */
    static void *m_zmqContext;

    legacymsg::RtcpJobRqstMsgBody m_request;
    castor::log::Logger & m_logger;
    client::ClientProxy m_clientProxy;
    client::ClientProxy::VolumeInfo m_volInfo;
    System::virtualWrapper & m_sysWrapper;
    /**
     * The configuration of the tape drive to be used by this session.
     */
    const utils::DriveConfig m_driveConfig;
    const CastorConf & m_castorConf;
    /** utility to find the drive on the system. This function logs 
     * all errors and hence does not throw exceptions. It returns NULL
     * in case of failure. */
    castor::tape::drives::DriveInterface * findDrive(
     const utils::DriveConfig &driveConfig,log::LogContext & lc);
        
    /** sub-part of execute for the read sessions */
    int executeRead(log::LogContext & lc);
    /** sub-part of execute for a write session */
    int executeWrite(log::LogContext & lc);
    /** sub-part of execute for a dump session */
    void executeDump(log::LogContext & lc);
    /** Reference to the RmcProxy, allowing the mounting of the tape by the
     * library. It will be used exclusively by the tape thread. */
    castor::legacymsg::RmcProxy & m_rmc;
    /** Reference to the tape server's parent process to report detailed status */
    castor::messages::TapeserverProxy & m_intialProcess;
    /** Object providing utilities for working UNIX capabilities. */
    castor::server::ProcessCap &m_capUtils;
    /** hostname, used to report status of the drive */
    const std::string m_hostname;
  };
}
}
}
}
