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

#include "castor/legacymsg/CupvProxy.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueSession.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"
#include "castor/tape/utils/DriveConfig.hpp"

#include <list>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Represention of a label session within the tape-drive catalogue.
 */
class CatalogueLabelSession : public CatalogueSession {
public:

  /**
   * Creates a CatalogueLabelSession object.
   *
   * Except in the case of unit testing, a CatalogueLabelSession object
   * should only be created using the static create() method.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The label job received from the castor-tape-label
   * command-line tool.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection with
   * the tape labeling command-line tool castor-tape-label.
   * @param cupv Proxy object representing the cupvd daemon.
   * @param processForker Proxy object representing the ProcessForker.
   * @return A newly created CatalogueSession object.
   */
  static CatalogueLabelSession *create(
    log::Logger &log,
    const int netTimeout,
    const tape::utils::DriveConfig &driveConfig,
    const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
    const int labelCmdConnection,
    legacymsg::CupvProxy &cupv,
    ProcessForkerProxy &processForker);

  /**
   * Destructor.
   *
   * If still open, closes the file descriptor of the TCP/IP connection with
   * the tape labeling command-line tool.
   */
  ~CatalogueLabelSession() throw();

  /**
   * Handles a tick in time.  Time driven actions such as alarms should be
   * implemented here.
   *
   * This method does not have to be called at any time precise interval,
   * though it should be called at least twice as fast as the quickest reaction
   * time imposed on the catalogue.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleTick();

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

  /**
   * To be called if the label session has sent tapserverd a label error.
   *
   * @param labelEx The label error.
   */
  void receivedLabelError(const castor::exception::Exception &labelEx);

protected:

  /**
   * Protected constructor.
   *
   * Except in the case of unit testing a CatalogueLabelSession object
   * should only be created using the static create() method.  This constructor
   * is protected so that unit tests can go around this restriction for sole
   * purpose of unit testing.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param pid The process ID of the session.
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The label job received from the castor-tape-label
   * command-line tool.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection with
   * the tape labeling command-line tool castor-tape-label.
   */
  CatalogueLabelSession(
    log::Logger &log,
    const int netTimeout,
    const pid_t pid,
    const tape::utils::DriveConfig &driveConfig,
    const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
    const int labelCmdConnection) throw();

private:

  /**
   * The time at which the tape drive was assigned a data transfer job.
   */
  const time_t m_assignmentTime;

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
   * List of label errors receved from the label session.
   */
  std::list<castor::exception::Exception> m_labelErrors;

  /**
   * Determines whether or not the user of the label session has the access
   * rights to label the tape.
   *
   * This method throws a castor::exception::Exception if the user does not
   * have the necessary access rights or there is an error which prevents this
   * method for determining if they have such rights.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param cupv Proxy object representing the cupvd daemon.
   * @param labelJob The label job received from the castor-tape-label
   * command-line tool.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection with
   * the tape labeling command-line tool castor-tape-label.
   */
  static void checkUserCanLabelTape(
    log::Logger &log,
    legacymsg::CupvProxy &cupv,
    const legacymsg::TapeLabelRqstMsgBody &labelJob,
    const int labelCmdConnection);

}; // class CatalogueLabelSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
