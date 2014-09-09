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

#include "castor/tape/tapeserver/daemon/DriveCatalogueSession.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Concrete class representing a cleaner session within the tape drive
 * catalogue.
 */
class DriveCatalogueCleanerSession : public DriveCatalogueSession {
public:

  /**
   * Creates a DriveCatalogueCleanerSession object.
   *
   * Except in the case of unit testing, a DriveCatalogueCleanerSession object
   * should only be created using the static create() method.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param processForker Proxy object representing the ProcessForker.
   * @param driveConfig The configuration of the tape drive.
   * @param vid The volume identifier ofthe tape associated with the tape
   * drive.  If the volume identifier is not known then this parameter should
   * be set to the empty string.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @param assignmentTime The time at which a job was assigned to the tape
   * drive.
   * @return A newly created DriveCatalogueCleanerSession object.
   */
  static DriveCatalogueCleanerSession *create(
    log::Logger &log,
    ProcessForkerProxy &processForker,
    const tape::utils::DriveConfig &driveConfig,
    const std::string &vid,
    const unsigned short rmcPort,
    const time_t assignmentTime);
    
  /**
   * To be called when the session has ended with success.
   */
  void sessionSucceeded();

  /**
   * To be called when the session has ended with failure.
   */
  void sessionFailed();

  /**
   * Gets the volume identifier of the tape associated with the tape drive.
   *
   * This method will throw castor::exception::Exception if the volume
   * identifier is not known.
   *
   * @return The volume identifier of the tape associated with the tape drive.
   */
  std::string getVid() const;

  /**
   * Gets the access mode of the cleaner sesison which is always recall
   * (WRITE_DISABLE).
   *
   * @return Always WRITE_DISABLE.
   */
  int getMode() const throw();

  /**
   * Gets the process identifier of the session.
   *
   * @return The process identifier of the session.
   */
  pid_t getPid() const throw();

  /**
   * Gets the point in time when the drive was assigned a tape.
   *
   * @return The point in time when the drive was assigned a tape. 
   */
  time_t getAssignmentTime() const throw();

  /** 
   * Always returns false.  A cleaner session does not mount a tape.
   *  
   * @return Always false.
   */
  bool tapeIsBeingMounted() const throw();

protected:

  /**
   * Protected constructor.
   *
   * Except in the case of unit testing a DriveCatalogueCleanerSession object
   * should only be created using the static create() method.  This constructor
   * is protected so that unit tests can go around this restriction for sole
   * purpose of unit testing.
   *
   * @param pid Process identifier of the session.
   * @param log Object representing the API of the CASTOR logging system.
   * @param driveConfig The configuration of the tape drive.
   * @param vid The volume identifier ofthe tape associated with the tape
   * drive.  If the volume identifier is not known then this parameter should
   * be set to the empty string.
   * @param assignmentTime The time at which a job was assigned to the tape
   * drive.
   */
  DriveCatalogueCleanerSession(
    const pid_t pid,
    log::Logger &log,
    const tape::utils::DriveConfig &driveConfig,
    const std::string &vid,
    const time_t assignmentTime) throw();

private:

  /**
   * The process identifier of the session.
   */
  const pid_t m_pid;

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The volume identifier of the tape associated with the tape drive.  If the
   * volume identifier was not known when the cleaner session was created then
   * the value of this member value will be the empty string.
   */
  const std::string m_vid;

  /**
   * The time at which a job was assigned to the tape drive.
   */
  const time_t m_assignmentTime;

  /**
   * Forks a cleaner session.
   *
   * @param processForker Proxy object representing the ProcessForker.
   * @param driveConfig The configuration of the tape drive.
   * @param vid The volume identifier ofthe tape associated with the tape
   * drive.  If the volume identifier is not known then this parameter should
   * be set to the empty string.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @return The process identifier of the session.
   */
  static pid_t forkCleanerSession(
    ProcessForkerProxy &processForker,
    const tape::utils::DriveConfig &driveConfig,
    const std::string &vid,
    const unsigned short rmcPort);

}; // class DriveCatalogueCleanerSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
