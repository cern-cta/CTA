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

#pragma once

#include "TapeMount.hpp"

namespace cta {

/**
 * A dummy tape mount used on unit tests.
 * Information reporting functions should not be needed and will throw exceptions.
 * Null returning functions do nothing.
 */
class TapeMountDummy: public TapeMount {
  void complete() override {};
  std::string getMountTransactionId() const override {
    throw exception::Exception("In DummyTapeMount::getMountTransactionId() : not implemented");
  }
  cta::common::dataStructures::MountType getMountType() const override {
    throw exception::Exception("In DummyTapeMount::getMountType() : not implemented");
  }
  std::optional<std::string> getActivity() const override {
    throw exception::Exception("In DummyTapeMount::getActivity() : not implemented");
  }
  uint32_t getNbFiles() const override {
    throw exception::Exception("In DummyTapeMount::getNbFiles() : not implemented");
  }
  std::string getVid() const override {
    throw exception::Exception("In DummyTapeMount::getNbFiles() : not implemented");
  }
  std::string getVo() const override{
      throw exception::Exception("In DummyTapeMount::getVo() : not implemented");
  }

  std::string getMediaType() const override {
      throw exception::Exception("In DummyTapeMount::getMediaType() : not implemented");
  }

  std::string getVendor() const override{
      throw exception::Exception("In DummyTapeMount::getVendor() : not implemented");
  }

  std::string getPoolName() const override{
    throw exception::Exception("In DummyTapeMount::getPoolName() : not implemented");
  }

  uint64_t getCapacityInBytes() const override{
    throw exception::Exception("In DummyTapeMount::getCapacityInBytes() : not implemented");
  }

  std::optional<std::string> getEncryptionKeyName() const override{
    throw exception::Exception("In DummyTapeMount::getEncryptionKeyName() : not implemented");
  }

  void setDriveStatus(cta::common::dataStructures::DriveStatus status, const std::optional<std::string> & reason) override {}

  cta::common::dataStructures::Label::Format getLabelFormat() const override {
      throw exception::Exception("In DummyTapeMount::getLabelFormat() : not implemented");
  }

  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override {};
  void setTapeMounted(log::LogContext &logContext) const override {};
};

}
