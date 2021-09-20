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

#include "TapeMount.hpp"

namespace cta {

/**
 * A dummy tape mount used on unit tests.
 * Information reporting functions should not be needed and will throw exceptions.
 * Null returning functions do nothing.
 */
class TapeMountDummy: public TapeMount {
  void abort(const std::string&) override {};
  std::string getMountTransactionId() const override {
    throw exception::Exception("In DummyTapeMount::getMountTransactionId() : not implemented");
  }
  cta::common::dataStructures::MountType getMountType() const override {
    throw exception::Exception("In DummyTapeMount::getMountType() : not implemented");
  }
  optional<std::string> getActivity() const override {
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

  void setDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason) override {}
  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override {};
  void setTapeMounted(log::LogContext &logContext) const override {};
};

}
