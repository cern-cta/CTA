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

#include "DiskSpaceReservationRequest.hpp"
#include "common/exception/Exception.hpp"

namespace cta {

void DiskSpaceReservationRequest::addRequest(const std::string& diskSystemName, uint64_t size) {
  try {
    at(diskSystemName) += size;
  } catch (std::out_of_range &) {
    operator[](diskSystemName) = size;
  }
}

void DiskSpaceReservationRequest::removeRequest(const std::string& diskSystemName, uint64_t size) {
  try {
    if (at(diskSystemName) < size) {
      throw cta::exception::Exception("At DiskSpaceReservationRequest::removeRequest(): Removing request of " + std::to_string(size) +
        " bytes from disk system" + diskSystemName + " which only has " + std::to_string(at(diskSystemName)) + " bytes");
    }
    at(diskSystemName) -= size;
  } catch (std::out_of_range &) {
      throw cta::exception::Exception("At DiskSpaceReservationRequest::removeRequest(): Removing request of " + std::to_string(size) +
        " bytes from disk system" + diskSystemName + " which does not exist");
  }
}
}