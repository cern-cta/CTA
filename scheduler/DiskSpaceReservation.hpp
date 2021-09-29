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

#pragma once

#include <map>
#include <string>
#include <tuple>

#include "catalogue/Catalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta {

struct DiskSpaceReservationRequest: public std::map<std::string, uint64_t> {
  void addRequest(const std::string &diskSystemName, uint64_t size);
};

class DiskSpaceReservation {
 public:
  static std::map<std::string, uint64_t> getExistingDrivesReservations(catalogue::Catalogue* catalogue);
  static void reserveDiskSpace(catalogue::Catalogue* catalogue, const std::string& driveName,
    const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc);
  static void addDiskSpaceReservation(catalogue::Catalogue* catalogue, const std::string& driveName,
    const std::string& diskSystemName, uint64_t bytes);
  CTA_GENERATE_EXCEPTION_CLASS(NegativeDiskSpaceReservationReached);
  static void subtractDiskSpaceReservation(catalogue::Catalogue* catalogue, const std::string& driveName,
    const std::string& diskSystemName, uint64_t bytes);
  static std::tuple<std::string, uint64_t> getDiskSpaceReservation(catalogue::Catalogue* catalogue,
    const std::string& driveName);
  static void releaseDiskSpace(catalogue::Catalogue* catalogue, const std::string& driveName,
    const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc);
};

}
