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

#include "LabelMount.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta {

LabelMount::LabelMount(catalogue::Catalogue& catalogue): m_catalogue(catalogue) {
  throw exception::NotImplementedException();
}

void LabelMount::complete() {
  throw exception::NotImplementedException();
}

std::string LabelMount::getMountTransactionId() const {
  throw exception::NotImplementedException();
}

common::dataStructures::MountType LabelMount::getMountType() const {
  throw exception::NotImplementedException();
}

uint32_t LabelMount::getNbFiles() const {
  throw exception::NotImplementedException();
}

std::string LabelMount::getPoolName() const {
  throw exception::NotImplementedException();
}

std::string LabelMount::getVid() const {
  throw exception::NotImplementedException();
}

void LabelMount::setDriveStatus(common::dataStructures::DriveStatus status, const std::optional<std::string> & reason) {
  throw exception::NotImplementedException();
}

void LabelMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  throw exception::NotImplementedException();
}

void LabelMount::setTapeMounted(log::LogContext &logContext) const {
  throw exception::NotImplementedException();
}

LabelMount::LabelMount(catalogue::Catalogue& catalogue,
  [[maybe_unused]] std::unique_ptr<SchedulerDatabase::LabelMount> dbMount) : m_catalogue(catalogue) {
  throw exception::NotImplementedException();
}

} // namespace cta
