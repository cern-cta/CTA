/******************************************************************************
 *         castor/tape/tapeserver/daemon/DriveCatalogue.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/tape/utils/DriveConfigMap.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

#include <map>
#include <string>
#include <string.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class responsible for keeping track of the tape drive being controlled by
 * the tapeserverd daemon.
 */
class DriveCatalogue {
public:

  /**
   * Destructor.
   *
   * Closes the connection with the label command if the drive catalogue owns
   * the connection at the time of destruction.
   */
  ~DriveCatalogue() throw();

  /**
   * Poplates the catalogue using the specified tape-drive configurations.
   *
   * @param driveConfigs Tape-drive configurations.
   */
  void populateCatalogue(const utils::DriveConfigMap &driveConfigs);

  /**
   * Returns a const reference to the tape-drive entry corresponding to the
   * tape drive with the specified unit name.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param unitName The unit name of the tape drive.
   */
  const DriveCatalogueEntry &findConstDriveEntry(const std::string &unitName)
    const;

  /**
   * Returns the unit name of the tape drive on which the specified mount
   * session process is running.
   *
   * @param sessionPid The process ID of the mount session.
   * @return the unit name of the tape drive.
   */
  std::string getUnitName(const pid_t sessionPid) const ;

  /**
   * Returns an unordered list of the unit names of all of the tape drives
   * stored within the tape drive catalogue.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNames() const;

  /**
   * Returns an unordered list of the unit names of the tape drives in the
   * specified state.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNames(
   const DriveCatalogueEntry::DriveState state) const;

  /**
   * Returns the device group name (DGN) of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDgn(const std::string &unitName) const;
  
  /**
   * Returns the VID of the tape mounted on the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getVid(const std::string &unitName) const;
  
  /**
   * Returns the time when the tape was mounted on the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  time_t getAssignmentTime(const std::string &unitName) const;

  /**
   * Returns the filename of the device file of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDevFilename(const std::string &unitName) const;

  /**
   * Returns the tape densities supported by the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::list<std::string> &getDensities(const std::string &unitName) const;

  /**
   * Returns the type of the specified session.
   *
   * @param sessionPid The process ID of the session.
   */
  DriveCatalogueEntry::SessionType getSessionType(const pid_t sessionPid) const;

  /**
   * Returns the current state of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  DriveCatalogueEntry::DriveState getState(const std::string &unitName) const;

  /**
   * Returns the library slot of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getLibrarySlot(const std::string &unitName) const;

  /**
   * Returns the device type of the specified tape drive in its libary.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDevType(const std::string &unitName) const;
  
  /**
   * Returns the last tape mode of the tape mounted on this drive
   *
   * @param unitName The unit name of the tape drive.
   */
  castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeMode getTapeMode(const std::string &unitName) const;
  
  /**
   * Returns the last tape event related to this drive
   *
   * @param unitName The unit name of the tape drive.
   */
  castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent getTapeEvent(const std::string &unitName) const;

  /**
   * Releases and returns the file descriptor of the connection with the
   * command-line tool castor-tape-label.
   *
   * @param unitName The unit name of the tape drive.
   */

  int releaseLabelCmdConnection(const std::string &unitName);

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_UP.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP, DRIVE_STATE_DOWN or DRIVE_STATE_WAITDOWN.
   *
   * configureUp() is idempotent.
   *
   * @param unitName The unit name of the tape drive.
   */
  void configureUp(const std::string &unitName);

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP, DRIVE_STATE_DOWN or DRIVE_STATE_RUNNING.
   *
   * configureDown() is idempotent.
   *
   * @param unitName The unit name of the tape drive.
   */
  void configureDown(const std::string &unitName);

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_WAITFORK.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP.
   *
   * The unit name of a tape drive is unique for a given host.  No two drives
   * on the same host can have the same unit name.
   *
   * A tape drive cannot be a member of more than one device group name (DGN).
   *
   * This method throws an exception if the DGN field of the specified vdqm job
   * does not match the value that was entered into the catalogue with the
   * populateCatalogue() method.
   *
   * @param job The job received from the vdqmd daemon.
   */
  void receivedVdqmJob(const legacymsg::RtcpJobRqstMsgBody &job);

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_WAITLABEL.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP.
   *
   * The unit name of a tape drive is unique for a given host.  No two drives
   * on the same host can have the same unit name.
   *
   * A tape drive cannot be a member of more than one device group name (DGN).
   *
   * This method throws an exception if the DGN field of the specified vdqm job
   * does not match the value that was entered into the catalogue with the
   * populateCatalogue() method.
   *
   * PLEASE NOTE: If this method throws an exception then it does NOT close
   * the file descriptor of the TCP/IP connection with the tape labeling
   * command-line tool castor-tape-label.  The caller of this method is left
   * to close the connection because this gives them the opportunity to send
   * an approprioate error message to the client.
   *
   * @param job The label job.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection
   * with the tape labeling command-line tool castor-tape-label.
   */
  void receivedLabelJob(const legacymsg::TapeLabelRqstMsgBody &job,
    const int labelCmdConnection) ;

  /**
   * Returns the job received from the vdqmd daemon for the specified tape
   * drive.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITFORK, DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   * @return The job received from the vdqmd daemon.
   */
  const legacymsg::RtcpJobRqstMsgBody &getVdqmJob(const std::string &unitName)
    const ;
  
  /**
   * Returns the job received from the vdqmd daemon for the specified tape
   * drive.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITFORK, DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   * @return The job received from the vdqmd daemon.
   */
  const legacymsg::TapeLabelRqstMsgBody &getLabelJob(const std::string &unitName)
    const ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_RUNNING.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITFORK.
   *
   * @param unitName The unit name of the tape drive.
   * @param sessionPid The process ID of the child process responsible for
   * running the mount session.
   */
  void forkedMountSession(const std::string &unitName, const pid_t sessionPid); 

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_RUNNING.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITLABEL.
   *
   * @param unitName The unit name of the tape drive.
   * @param sessionPid The process ID of the child process responsible for
   * running the label session.
   */
  void forkedLabelSession(const std::string &unitName, const pid_t sessionPid);

  /**
   * Returns the process ID of the child process responsible the mount session
   * running on the specified tape drive.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   * @return The process ID of the child process responsible for mount session
   * running on the specified tape drive.
   */
  pid_t getSessionPid(const std::string &unitName) const ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_UP if the
   * current state is DRIVE_STATE_RUNNING or to DRIVE_STATE_DOWN if the
   * current state is DRIVE_STATE_WAIT_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param sessionPid Process ID of the child process handling the session.
   */
  void sessionSucceeded(const pid_t sessionPid) ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_UP if the
   * current state is DRIVE_STATE_RUNNING or to DRIVE_STATE_DOWN if the
   * current state is DRIVE_STATE_WAIT_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   */
  void sessionSucceeded(const std::string &unitName) ;
  
  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   *
   * @param sessionPid Process ID of the child process handling the session.
   */
  void sessionFailed(const pid_t sessionPid) ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   *
   * @param unitName The unit name of the tape drive.
   */
  void sessionFailed(const std::string &unitName) ;
  
  /**
   * Updates the vid and assignment time of the specified drive
   * 
   * @param vid Volume ID of the tape mounted
   * @param unitName Name of the drive
   */
  void updateDriveVolumeInfo(const legacymsg::TapeUpdateDriveRqstMsgBody &body) ;

private:

  /**
   * Type that maps the unit name of a tape drive to the catalogue entry of
   * that drive.
   */
  typedef std::map<std::string, DriveCatalogueEntry> DriveMap;

  /**
   * Map from the unit name of a tape drive to the catalogue entry of that
   * drive.
   */
  DriveMap m_drives;

  /** 
   * Enters the specified tape-drive configuration into the catalogue.
   *
   * @param driveConfig The tape-drive configuration.
   */
  void enterDriveConfig(const utils::DriveConfig &driveConfig);

  /**
   * Returns a const reference to the tape-drive entry corresponding to the
   * tape drive with the specified unit name.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param unitName The unit name of the tape drive.
   * @param taskOfCaller Human readable string explaining the task of the
   * caller method.  This string is used to create an explanatory exception
   * string in the case where the tape-drive entry cannot be found.
   */
  const DriveCatalogueEntry &findConstDriveEntry(const std::string &unitName,
    const std::string &taskOfCaller) const;

}; // class DriveCatalogue

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

