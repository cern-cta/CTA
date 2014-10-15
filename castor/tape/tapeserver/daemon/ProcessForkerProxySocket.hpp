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

#include "castor/log/Logger.hpp"
#include "castor/messages/ForkDataTransfer.pb.h"
#include "castor/messages/ForkLabel.pb.h"
#include "castor/messages/StopProcessForker.pb.h"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"
#include "castor/tape/utils/DriveConfig.hpp"

#include <google/protobuf/message.h>
#include <stdint.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Concrete proxy class representing the process forker.
 *
 * This class uses a socket to communicate with the process forker.
 */
class ProcessForkerProxySocket: public ProcessForkerProxy {
public:

  /**
   * Constructor.
   *
   * This class takes ownership of the specified socket file-descriptor.  The
   * destructor of this class will close it.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param socketFd The file-descriptor of the socket to be used to communicate
   * with the process forker.
   */
  ProcessForkerProxySocket(log::Logger &log, const int socketFd) throw();

  /**
   * Destructor.
   *
   * Closes the file-descriptor of the socket used to communicate with the
   * process forker.
   */
  ~ProcessForkerProxySocket() throw();

  /**
   * Tells the ProcessForker to stop executing.
   *
   * @param reason Human readable string for logging purposes that describes
   * the reason for stopping.
   */
  void stopProcessForker(const std::string &reason);

  /**
   * Forks a data-transfer session for the specified tape drive.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vdqmJob The job received from the vdqmd daemon.
   * @return The process identifier of the newly forked session.
   */ 
  pid_t forkDataTransfer(const utils::DriveConfig &driveConfig,
    const legacymsg::RtcpJobRqstMsgBody vdqmJob);

  /** 
   * Forks a label-session process for the specified tape drive.
   *  
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The job received from the tape-labeling command-line tool.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @return The process identifier of the newly forked session.
   */
  pid_t forkLabel(const utils::DriveConfig &driveConfig,
    const legacymsg::TapeLabelRqstMsgBody &labelJob,
    const unsigned short rmcPort);

  /**
   * Forks a cleaner session for the specified tape drive.
   * 
   * @param driveConfig The configuration of the tape drive.
   * @param vid If known then this string specifies the volume identifier of the
   * tape in the drive if there is in fact a tape in the drive and its volume
   * identifier is known.  If the volume identifier is not known then this
   * parameter should be set to an empty string.
   * @return The process identifier of the newly forked session.
   */
  pid_t forkCleaner(const utils::DriveConfig &driveConfig,
    const std::string &vid);

private:

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The file-descriptor of the socket to be used to communicate
   * with the process forker.
   */
  const int m_socketFd;

  /**
   * Creates a StopProcessForker message.
   *
   * @param reason Human readable string for logging purposes that describes
   * the reason for stopping.
   * @return The message.
   */
  messages::StopProcessForker createStopProcessForkerMsg(
    const std::string &reason);

  /**
   * Creates a ForkDataTransfer message from the specified tape-drive
   * configuration, VDQM job and data-transfer session configuration.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vdqmJob The job received from the vdqmd daemon.
   * @return The message.
   */
  messages::ForkDataTransfer createForkDataTransferMsg(
    const utils::DriveConfig &driveConfig,
    const legacymsg::RtcpJobRqstMsgBody vdqmJob);

  /**
   * Fills the appropriate members of the specified message with the information
   * stored in the specified tape-drive configuration.
   *
   * @param msg In/out parameter: The message whose appropriate members will
   * be filled.
   * @param driveConfig The tape-drive configuration.
   */
  template <typename T> void fillMsgWithDriveConfig(T &msg,
    const utils::DriveConfig &driveConfig) {
    msg.set_unitname(driveConfig.unitName);
    msg.set_dgn(driveConfig.dgn);
    msg.set_devfilename(driveConfig.devFilename);
    const std::list<std::string> &densities = driveConfig.densities;
    for(std::list<std::string>::const_iterator itor = densities.begin();
      itor != densities.end(); itor++) {
      msg.add_density(*itor);
    }
    msg.set_libraryslot(driveConfig.librarySlot.str());
    msg.set_devtype(driveConfig.devType);
  }

  /**
   * Creates a ForkDataLabel message from the specified tape-drive
   * configuration and label job.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The job received from the tape-labeling command-line tool.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @return The message.
   */
  messages::ForkLabel createForkLabelMsg(const utils::DriveConfig &driveConfig,
    const legacymsg::TapeLabelRqstMsgBody &labelJob,
    const unsigned short rmcPort);

  /**
   * Fills the appropriate members of the specified message with the information
   * stored in the specified job received from the tape-labeling command-line
   * tool.
   *
   * @param msg In/out parameter: The message whose appropriate members will
   * be filled.
   * @param labelJob The job received from the tape-labeling command-line tool.
   */
  template <typename T> void fillMsgWithLabelJob(T &msg,
    const legacymsg::TapeLabelRqstMsgBody &labelJob) {
    msg.set_force(labelJob.force);
    msg.set_uid(labelJob.uid);
    msg.set_gid(labelJob.gid);
    msg.set_vid(labelJob.vid);
  }

  /**
   * Creates a ForkDataLabel message from the specified tape-drive
   * configuration and label job.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vid The volume identifier of the tape associated with the tape
   * drive.
   * @return The message.
   */
  messages::ForkCleaner createForkCleanerMsg(
    const utils::DriveConfig &driveConfig, const std::string &vid);

}; // class ProcessForkerProxySocket

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
