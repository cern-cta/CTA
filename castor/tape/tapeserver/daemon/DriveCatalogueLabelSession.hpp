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

#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueSession.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"
#include "castor/tape/utils/DriveConfig.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Represention of a label session within the tape-drive catalogue.
 */
class DriveCatalogueLabelSession : public DriveCatalogueSession {
public:

  /**
   * Creates a DriveCatalogueLabelSession object.
   *
   * Except in the case of unit testing, a DriveCatalogueLabelSession object
   * should only be created using the static create() method.
   *
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param log Object representing the API of the CASTOR logging system.
   * @param processForker Proxy object representing the ProcessForker.
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The label job received from the castor-tape-label
   * command-line tool.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection with
   * the tape labeling command-line tool castor-tape-label.
   * @return A newly created DriveCatalogueSession object.
   */
  static DriveCatalogueLabelSession *create(
    const int netTimeout,
    log::Logger &log,
    ProcessForkerProxy &processForker,
    const tape::utils::DriveConfig &driveConfig,
    const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
    const unsigned short rmcPort,
    const int labelCmdConnection);

  /**
   * Destructor.
   *
   * If still open, closes the file descriptor of the TCP/IP connection with
   * the tape labeling command-line tool.
   */
  ~DriveCatalogueLabelSession() throw();

  /**
   * To be called when the session has ended with success.
   */
  void sessionSucceeded();
  
  /**
   * To be called when the session has ended with failure.
   */
  void sessionFailed();

  /**
   * labelJob getter method
   *
   * @return label job received from the castor-tape-label command-line tool
   */
  castor::legacymsg::TapeLabelRqstMsgBody getLabelJob() const throw();

  /**
   * Returns the volume identifier of the tape associated with the tape drive.
   *
   * @return The volume identifier of the tape associated with the tape drive.
   */
  std::string getVid() const throw();

  /**
   * Gets the access mode of the label sesison which is always migration
   * (WRITE_ENABLE).
   *
   * @return Always WRITE_ENABLE.
   */
  int getMode() const throw();

  /**
   * Gets the process identifier of the session.
   *
   * @return The process identifier of the session.
   */
  pid_t getPid() const throw();

  /**
   * Gets the time at which the tape drive was assigned a data transfer job.
   */
  time_t getAssignmentTime() const throw();

  /**
   * Always returns false.  A label session does not indicate when it is
   * mounting a tape.
   *
   * @return Always false.
   */
  bool tapeIsBeingMounted() const throw();

protected:

  /**
   * Protected constructor.
   *
   * Except in the case of unit testing a DriveCatalogueLabelSession object
   * should only be created using the static create() method.  This constructor
   * is protected so that unit tests can go around this restriction for sole
   * purpose of unit testing.
   *
   * @param pid The process ID of the session.
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param log Object representing the API of the CASTOR logging system.
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The label job received from the castor-tape-label
   * command-line tool.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection with
   * the tape labeling command-line tool castor-tape-label.
   */
  DriveCatalogueLabelSession(
    const pid_t pid,
    const int netTimeout,
    log::Logger &log,
    const tape::utils::DriveConfig &driveConfig,
    const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
    const int labelCmdConnection) throw();

private:

  /**
   * The process ID of the session.
   */
  const pid_t m_pid;

  /**
   * The time at which the tape drive was assigned a data transfer job.
   */
  const time_t m_assignmentTime;

  /**
   * Timeout in seconds to be used when performing network I/O.
   */
  const int m_netTimeout;

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The configuration of the tape drive.
   */
  tape::utils::DriveConfig m_driveConfig;

  /**
   * The label job received from the castor-tape-label command-line tool.
   */
  castor::legacymsg::TapeLabelRqstMsgBody m_labelJob;

  /**
   * File descriptor of the TCP/IP connection with the tape-labeling
   * command-line tool.
   */
  const int m_labelCmdConnection;

  /**
   * Uses the ProcessForker to fork a label-session.
   *
   * @param processForker Proxy object representing the ProcessForker.
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The label job received from the castor-tape-label
   * command-line tool.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @return The process identifier of the session.
   */
  static pid_t forkLabelSession(ProcessForkerProxy &processForker,
    const tape::utils::DriveConfig &driveConfig,
    const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
    const unsigned short rmcPort);

}; // class DriveCatalogueLabelSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
