/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "LabelMount.hpp"

#include "common/exception/NotImplementedException.hpp"

namespace cta {

LabelMount::LabelMount(catalogue::Catalogue& catalogue) : m_catalogue(catalogue) {
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

void LabelMount::setDriveStatus(common::dataStructures::DriveStatus status, const std::optional<std::string>& reason) {
  throw exception::NotImplementedException();
}

void LabelMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) {
  throw exception::NotImplementedException();
}

void LabelMount::setTapeMounted(log::LogContext& logContext) const {
  throw exception::NotImplementedException();
}

LabelMount::LabelMount(catalogue::Catalogue& catalogue,
                       [[maybe_unused]] std::unique_ptr<SchedulerDatabase::LabelMount> dbMount)
    : m_catalogue(catalogue) {
  throw exception::NotImplementedException();
}

}  // namespace cta
