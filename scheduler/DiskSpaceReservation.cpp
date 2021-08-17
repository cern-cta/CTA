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

#include <map>
#include <string>
#include <tuple>

#include "DiskSpaceReservation.hpp"

namespace cta {

void DiskSpaceReservationRequest::addRequest(const std::string& diskSystemName, uint64_t size) {
  try {
    at(diskSystemName) += size;
  } catch (std::out_of_range &) {
    operator[](diskSystemName) = size;
  }
}

std::map<std::string, uint64_t> DiskSpaceReservation::getExistingDrivesReservations(
  catalogue::Catalogue* catalogue) {
  std::map<std::string, uint64_t> ret;
  const auto tdNames = catalogue->getTapeDriveNames();
  for (const auto& driveName : tdNames) {
    const auto tdStatus = catalogue->getTapeDrive(driveName);
    try {
      ret.at(tdStatus.value().diskSystemName) += tdStatus.value().reservedBytes;
    } catch (std::out_of_range &) {
      ret[tdStatus.value().diskSystemName] = tdStatus.value().reservedBytes;
    }
  }
  return ret;
}

void DiskSpaceReservation::reserveDiskSpace(catalogue::Catalogue* catalogue, const std::string& driveName,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;
  // Try add our reservation to the drive status.
  auto setLogParam = [&lc](const std::string& diskSystemName, uint64_t bytes) {
    log::ScopedParamContainer params(lc);
    params.add("diskSystem", diskSystemName)
          .add("reservation", bytes);
  };
  std::string diskSystemName = diskSpaceReservation.begin()->first;
  uint64_t bytes = diskSpaceReservation.begin()->second;
  setLogParam(diskSystemName, bytes);
  lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): reservation request content.");

  std::tie(diskSystemName, bytes) = getDiskSpaceReservation(catalogue, driveName);
  setLogParam(diskSystemName, bytes);
  lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): state before reservation.");

  addDiskSpaceReservation(catalogue, driveName, diskSpaceReservation.begin()->first,
    diskSpaceReservation.begin()->second);
  std::tie(diskSystemName, bytes) = getDiskSpaceReservation(catalogue, driveName);
  setLogParam(diskSystemName, bytes);
  lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): state after reservation.");
}

void DiskSpaceReservation::addDiskSpaceReservation(catalogue::Catalogue* catalogue, const std::string& driveName,
  const std::string& diskSystemName, uint64_t bytes) {
  auto tdStatus = catalogue->getTapeDrive(driveName);
  if (!tdStatus) return;
  tdStatus.value().diskSystemName = diskSystemName;
  tdStatus.value().reservedBytes += bytes;
  catalogue->modifyTapeDrive(tdStatus.value());
}

std::tuple<std::string, uint64_t> DiskSpaceReservation::getDiskSpaceReservation(catalogue::Catalogue* catalogue,
  const std::string& driveName) {
  const auto tdStatus = catalogue->getTapeDrive(driveName);
  return std::make_tuple(tdStatus.value().diskSystemName, tdStatus.value().reservedBytes);
}

void DiskSpaceReservation::releaseDiskSpace(catalogue::Catalogue* catalogue, const std::string& driveName,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;
  // Try add our reservation to the drive status.
  auto setLogParam = [&lc](const std::string& diskSystemName, uint64_t bytes) {
    log::ScopedParamContainer params(lc);
    params.add("diskSystem", diskSystemName)
          .add("reservation", bytes);
  };
  std::string diskSystemName = diskSpaceReservation.begin()->first;
  uint64_t bytes = diskSpaceReservation.begin()->second;
  setLogParam(diskSystemName, bytes);
  lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): release request content.");

  std::tie(diskSystemName, bytes) = getDiskSpaceReservation(catalogue, driveName);
  setLogParam(diskSystemName, bytes);
  lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): state before release.");

  subtractDiskSpaceReservation(catalogue, driveName, diskSpaceReservation.begin()->first,
    diskSpaceReservation.begin()->second);

  std::tie(diskSystemName, bytes) = getDiskSpaceReservation(catalogue, driveName);
  setLogParam(diskSystemName, bytes);
  lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): state after release.");
}

void DiskSpaceReservation::subtractDiskSpaceReservation(catalogue::Catalogue* catalogue,
  const std::string& driveName, const std::string& diskSystemName, uint64_t bytes) {
    auto tdStatus = catalogue->getTapeDrive(driveName);
    if (bytes > tdStatus.value().reservedBytes) throw NegativeDiskSpaceReservationReached(
      "In DriveState::subtractDiskSpaceReservation(): we would reach a negative reservation size.");
    tdStatus.value().diskSystemName = diskSystemName;
    tdStatus.value().reservedBytes -= bytes;
    catalogue->modifyTapeDrive(tdStatus.value());
}

}
