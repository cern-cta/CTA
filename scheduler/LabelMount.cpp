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

#include "LabelMount.hpp"

namespace cta {

LabelMount::LabelMount(catalogue::Catalogue& catalogue): m_catalogue(catalogue) {
  throw 0;
  // TODO
}

void LabelMount::abort(const std::string& reason) {
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

void LabelMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason) {
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



LabelMount::~LabelMount() {
  //TODO
}



} // namespace cta.
