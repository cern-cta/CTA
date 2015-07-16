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

#include "castor/tape/tapeserver/daemon/CatalogueSession.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Concrete class representing a cleaner session within the tape drive
 * catalogue.
 */
class CatalogueCleanerSession : public CatalogueSession {
public:

  /**
   * Creates a CatalogueCleanerSession object.
   *
   * Except in the case of unit testing, a CatalogueCleanerSession object
   * should only be created using the static create() method.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param driveConfig The configuration of the tape drive.
   * @param processForker Proxy object representing the ProcessForker.
   * @param vid The volume identifier ofthe tape associated with the tape
   * drive.  If the volume identifier is not known then this parameter should
   * be set to the empty string.
   * @param assignmentTime The time at which a job was assigned to the tape
   * drive.
   * @param waitMediaInDrive true if we want to check the presence of the media in the drive before cleaning,
   * false otherwise.
   * @param waitMediaInDriveTimeout The maximum number of seconds to wait for
   * the media to be ready for operations inside the drive.
   * @return A newly created CatalogueCleanerSession object.
   */
  static CatalogueCleanerSession *create(
    log::Logger &log,
    const int netTimeout,
    const DriveConfig &driveConfig,
    ProcessForkerProxy &processForker,
    const std::string &vid,
    const time_t assignmentTime,
    const bool waitMediaInDrive,
    const uint32_t waitMediaInDriveTimeout);

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
   * Gets the volume identifier of the tape associated with the tape drive.
   *
   * @return The volume identifier of the tape associated with the tape drive
   * or an empty string if there is no tape.
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
   * Except in the case of unit testing a CatalogueCleanerSession object
   * should only be created using the static create() method.  This constructor
   * is protected so that unit tests can go around this restriction for sole
   * purpose of unit testing.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param pid The process identifier of the session.
   * @param driveConfig The configuration of the tape drive.
   * @param vid The volume identifier ofthe tape associated with the tape
   * drive.  If the volume identifier is not known then this parameter should
   * be set to the empty string.
   * @param assignmentTime The time at which a job was assigned to the tape
   * drive.
   */
  CatalogueCleanerSession(
    log::Logger &log,
    const int netTimeout,
    const pid_t pid,
    const DriveConfig &driveConfig,
    const std::string &vid,
    const time_t assignmentTime) throw();

private:

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

}; // class CatalogueCleanerSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
