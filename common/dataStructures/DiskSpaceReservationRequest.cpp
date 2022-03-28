/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
      at(diskSystemName) = 0;
    } else {
      at(diskSystemName) -= size;
    }
  } catch (std::out_of_range &) {
      throw cta::exception::Exception("At DiskSpaceReservationRequest::removeRequest(): Removing request of " + std::to_string(size) +
        " bytes from disk system" + diskSystemName + " which does not exist");
  }
}
}