/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeMount.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta {

/**
 * A dummy tape mount used on unit tests.
 * Information reporting functions should not be needed and will throw exceptions.
 * Null returning functions do nothing.
 */
class TapeMountDummy : public TapeMount {
  void complete() override {};

  std::string getMountTransactionId() const override { throw exception::NotImplementedException(); }

  cta::common::dataStructures::MountType getMountType() const override { throw exception::NotImplementedException(); }

  std::optional<std::string> getActivity() const override { throw exception::NotImplementedException(); }

  uint32_t getNbFiles() const override { throw exception::NotImplementedException(); }

  std::string getVid() const override { throw exception::NotImplementedException(); }

  std::string getVo() const override { throw exception::NotImplementedException(); }

  std::string getMediaType() const override { throw exception::NotImplementedException(); }

  std::string getVendor() const override { throw exception::NotImplementedException(); }

  std::string getPoolName() const override { throw exception::NotImplementedException(); }

  uint64_t getCapacityInBytes() const override { throw exception::NotImplementedException(); }

  std::optional<std::string> getEncryptionKeyName() const override { throw exception::NotImplementedException(); }

  void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                      const std::optional<std::string>& reason) override {}

  cta::common::dataStructures::Label::Format getLabelFormat() const override {
    throw exception::NotImplementedException();
  }

  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) override {};
  void setTapeMounted(log::LogContext& logContext) const override {};
};

}  // namespace cta
