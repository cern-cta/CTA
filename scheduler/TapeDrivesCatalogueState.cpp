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

#include "TapeDrivesCatalogueState.hpp"

namespace cta {

TapeDrivesCatalogueState::TapeDrivesCatalogueState(catalogue::Catalogue &catalogue) : m_catalogue(catalogue) {}

void TapeDrivesCatalogueState::removeDrive(const std::string& drive, log::LogContext &lc) {
  try {
    m_catalogue.deleteTapeDrive(drive);
    log::ScopedParamContainer params(lc);
    params.add("driveName", drive);
    lc.log(log::INFO, "In OStoreDB::removeDrive(): removed tape drive from database.");
  } catch (cta::exception::Exception & ex) {
    lc.log(log::WARNING, "In OStoreDB::removeDrive(): Problem to remove tape drive from database.");
  }
}
}  // namespace cta
