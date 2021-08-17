/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/Logger.hpp"
#include "TapeDrivesCatalogueState.hpp"

namespace cta {

TapeDrivesCatalogueState::TapeDrivesCatalogueState(catalogue::Catalogue &catalogue) : m_catalogue(catalogue) {}

void TapeDrivesCatalogueState::checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo) {
  const auto driveNames = m_catalogue.getTapeDriveNames();
  try {
    const auto tapeDrive = m_catalogue.getTapeDrive(driveInfo.driveName);
    if (!tapeDrive) return;  // tape drive does not exist
    if (tapeDrive.value().logicalLibrary != driveInfo.logicalLibrary || tapeDrive.value().host != driveInfo.host) {
      throw DriveAlreadyExistsException(std::string("The drive name=") + driveInfo.driveName +
        " logicalLibrary=" + driveInfo.logicalLibrary +
        " host=" + driveInfo.host +
        " cannot be created because a drive with a same name with logicalLibrary=" + tapeDrive.value().logicalLibrary +
        " host=" + tapeDrive.value().host +
        " already exists.");
    }
  } catch (cta::exception::Exception & ex) {
    // Drive does not exist
    // We can create it, do nothing then
  }
}

std::list<cta::common::dataStructures::TapeDrive> TapeDrivesCatalogueState::getDriveStates(
  log::LogContext & lc) const {
  std::list<cta::common::dataStructures::TapeDrive> tapeDrivesList;
  const auto driveNames = m_catalogue.getTapeDriveNames();
  for (const auto& driveName : driveNames) {
    const auto tapeDrive = m_catalogue.getTapeDrive(driveName);
    tapeDrivesList.push_back(tapeDrive.value());
  }
  return tapeDrivesList;
}

void TapeDrivesCatalogueState::removeDrive(const std::string& drive, log::LogContext &lc) {
  try {
    m_catalogue.deleteTapeDrive(drive);
    log::ScopedParamContainer params(lc);
    params.add("driveName", drive);
    lc.log(log::INFO, "In TapeDrivesCatalogueState::removeDrive(): removed tape drive from database.");
  } catch (cta::exception::Exception & ex) {
    lc.log(log::WARNING, "In TapeDrivesCatalogueState::removeDrive(): Problem to remove tape drive from database.");
  }
}

void TapeDrivesCatalogueState::setDesiredDriveState(const std::string& drive,
  const common::dataStructures::DesiredDriveState & desiredState, log::LogContext &lc) {
  common::dataStructures::DesiredDriveState newDesiredState = desiredState;
  auto driveState = m_catalogue.getTapeDrive(drive);
  if (!driveState) return;
  if(desiredState.comment){
    //In case we modify the comment, we want to keep the same status and forceDown of the drive
    newDesiredState.up = driveState.value().desiredUp;
    newDesiredState.forceDown = driveState.value().desiredForceDown;
  }
  driveState.value().desiredUp = newDesiredState.up;
  driveState.value().desiredForceDown = newDesiredState.forceDown;
  driveState.value().reasonUpDown = newDesiredState.reason;
  driveState.value().userComment = newDesiredState.comment;
  m_catalogue.modifyTapeDrive(driveState.value());
}

}  // namespace cta
