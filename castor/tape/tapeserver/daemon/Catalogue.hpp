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

#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/CupvProxy.hpp"
#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/utils/DriveConfigMap.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueDrive.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"

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
class Catalogue {
public:

  /**
   * Constructor.
   *
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param log Object representing the API of the CASTOR logging system.
   * @param dataTransferConfig The configuration of a data-transfer session.
   * @param processForker Proxy object representing the ProcessForker.
   * @param cupv Proxy object representing the cupvd daemon.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param vmgr Proxy object representing the vmgrd daemon.
   * @param hostName The name of the host on which the daemon is running.  This
   * name is needed to fill in messages to be sent to the vdqmd daemon.
   */
  Catalogue(
    const int netTimeout,
    log::Logger &log,
    const DataTransferSession::CastorConf &dataTransferConfig,
    ProcessForkerProxy &processForker,
    legacymsg::CupvProxy &cupv,
    legacymsg::VdqmProxy &vdqm,
    legacymsg::VmgrProxy &vmgr,
    const std::string &hostName);

  /**
   * Destructor.
   *
   * Closes the connection with the label command if the drive catalogue owns
   * the connection at the time of destruction.
   */
  ~Catalogue() throw();

  /**
   * Poplates the catalogue using the specified tape-drive configurations.
   *
   * @param driveConfigs Tape-drive configurations.
   */
  void populate(const utils::DriveConfigMap &driveConfigs);

  /**
   * Returns a const reference to the tape-drive entry corresponding to the
   * tape drive with the specified unit name.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param unitName The unit name of the tape drive.
   */
  const CatalogueDrive &findDrive(const std::string &unitName)
    const;

  /**
   * Returns a const reference to the tape-drive entry associated with the
   * session with the specified process ID.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param sessionPid The process ID of the session.
   */
  const CatalogueDrive &findDrive(const pid_t sessionPid) const;

  /**
   * Returns a reference to the tape-drive entry corresponding to the tape
   * drive with the specified unit name.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param unitName The unit name of the tape drive.
   */
  CatalogueDrive &findDrive(const std::string &unitName);

  /**
   * Returns a reference to the tape-drive entry associated with the
   * session with the specified process ID.
   *
   * This method throws an exception if the tape-drive entry cannot be found.
   *
   * @param sessionPid The process ID of the session.
   */
  CatalogueDrive &findDrive(const pid_t sessionPid);

  /**
   * Returns an unordered list of the unit names of all of the tape drives
   * stored within the tape drive catalogue.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNames() const;

private:

  /**
   * Timeout in seconds to be used when performing network I/O.
   */
  const int m_netTimeout;

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The configuration of a data-transfer session.
   */
  const DataTransferSession::CastorConf m_dataTransferConfig;

  /**
   * Proxy object representing the ProcessForker.
   */
  ProcessForkerProxy &m_processForker;

  /**
   * Proxy object representing the cupvd daemon.
   */
  legacymsg::CupvProxy &m_cupv;

  /**
   * Proxy object representing the vdqmd daemon.
   */
  legacymsg::VdqmProxy &m_vdqm;

  /**
   * Proxy object representing the vmgrd daemon.
   */
  legacymsg::VmgrProxy &m_vmgr;

  /**
   * The name of the host on which the daemon is running.  This name is
   * needed to fill in messages to be sent to the vdqmd daemon.
   */
  const std::string m_hostName;

  /**
   * Type that maps the unit name of a tape drive to the catalogue entry of
   * that drive.
   */
  typedef std::map<std::string, CatalogueDrive *> DriveMap;

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

}; // class Catalogue

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
