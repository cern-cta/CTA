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

namespace cta {

LabelMount::LabelMount(catalogue::Catalogue& catalogue) : m_catalogue(catalogue) {
  throw 0;
  // TODO
}

void LabelMount::complete() {
  throw 0;
  // TODO
}

std::string LabelMount::getMountTransactionId() const {
  throw 0;
  // TODO
}

cta::common::dataStructures::MountType LabelMount::getMountType() const {
  throw 0;
  // TODO
}

uint32_t LabelMount::getNbFiles() const {
  throw 0;
  // TODO
}

std::string LabelMount::getPoolName() const {
  throw 0;
  // TODO
}

std::string LabelMount::getVid() const {
  throw 0;
  // TODO
}

void LabelMount::setDriveStatus(cta::common::dataStructures::DriveStatus status,
                                const std::optional<std::string>& reason) {
  throw 0;
  // TODO
}

void LabelMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) {
  throw 0;
  // TODO
}

void LabelMount::setTapeMounted(log::LogContext& logContext) const {
  throw 0;
  // TODO
}

LabelMount::LabelMount(catalogue::Catalogue& catalogue, std::unique_ptr<cta::SchedulerDatabase::LabelMount> dbMount) :
m_catalogue(catalogue) {
  throw 0;
  // TODO;
}

LabelMount::~LabelMount() {
  //TODO
}

}  // namespace cta.
