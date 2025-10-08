/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "LabelMount.hpp"

namespace cta {

LabelMount::LabelMount(catalogue::Catalogue& catalogue): m_catalogue(catalogue) {
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

void LabelMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, const std::optional<std::string> & reason) {
  throw 0;
  // TODO
}

void LabelMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  throw 0;
  // TODO
}

void LabelMount::setTapeMounted(log::LogContext &logContext) const {
  throw 0;
  // TODO
}

LabelMount::LabelMount(catalogue::Catalogue& catalogue, std::unique_ptr<cta::SchedulerDatabase::LabelMount> dbMount):
  m_catalogue(catalogue) {
  throw 0;
  // TODO;
}

} // namespace cta
