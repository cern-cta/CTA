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
#include "castor/tape/tapeserver/daemon/DriveConfig.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"

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
  pid_t forkDataTransfer(const DriveConfig &driveConfig,
    const legacymsg::RtcpJobRqstMsgBody vdqmJob);

  /** 
   * Forks a label-session process for the specified tape drive.
   *  
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The job received from the tape-labeling command-line tool.
   * @return The process identifier of the newly forked session.
   */
  pid_t forkLabel(const DriveConfig &driveConfig,
    const legacymsg::TapeLabelRqstMsgBody &labelJob);

  /**
   * Forks a cleaner session for the specified tape drive.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vid If known then this string specifies the volume identifier of the
   * tape in the drive if there is in fact a tape in the drive and its volume
   * identifier is known.  If the volume identifier is not known then this
   * parameter should be set to an empty string.
   * @param driveReadyDelayInSeconds The maximum number of seconds to wait for
   * the drive to be ready with a tape inside of it.
   * @return The process identifier of the newly forked session.
   */
  pid_t forkCleaner(const DriveConfig &driveConfig,
    const std::string &vid, const uint32_t driveReadyDelayInSeconds);
  
  /**
   * Forks a probe session for the specified tape drive.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param driveReadyDelayInSeconds The maximum number of seconds to wait for
   * the drive to be ready with a tape inside of it.
   * @return The process identifier of the newly forked session.
   */
  virtual pid_t forkProbe(const DriveConfig &driveConfig, 
    const uint32_t driveReadyDelayInSeconds);

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
    const DriveConfig &driveConfig,
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
    const DriveConfig &driveConfig) {
    msg.set_unitname(driveConfig.getUnitName());
    msg.set_dgn(driveConfig.getDgn());
    msg.set_devfilename(driveConfig.getDevFilename());
    msg.set_libraryslot(driveConfig.getLibrarySlot().str());
  }

  /**
   * Creates a ForkDataLabel message from the specified tape-drive
   * configuration and label job.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The job received from the tape-labeling command-line tool.
   * @return The message.
   */
  messages::ForkLabel createForkLabelMsg(const DriveConfig &driveConfig,
    const legacymsg::TapeLabelRqstMsgBody &labelJob);

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
   * Creates a ForkCleaner message from the specified tape-drive
   * configuration and cleaner job.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vid The volume identifier of the tape associated with the tape
   * drive.
   * @param driveReadyDelayInSeconds The maximum number of seconds to wait for
   * the drive to be raedy with a tape inside of it.
   * @return The message.
   */
  messages::ForkCleaner createForkCleanerMsg(
    const DriveConfig &driveConfig, const std::string &vid,
    const uint32_t driveReadyDelayInSeconds);

  /**
   * Creates a ForkProbe message from the specified tape-drive
   * configuration and probe job.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param driveReadyDelayInSeconds The maximum number of seconds to wait for
   * the drive to be raedy with a tape inside of it.
   * @return The message.
   */
  messages::ForkProbe createForkProbeMsg(
    const DriveConfig &driveConfig,
    const uint32_t driveReadyDelayInSeconds);
  
}; // class ProcessForkerProxySocket

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
