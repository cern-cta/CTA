/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "ArchiveQueueAlgorithms.hpp"
#include "common/Timer.hpp"

namespace cta { namespace objectstore {

// ArchiveQueue full specialisations for ContainerTraits.

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueue>::c_containerTypeName = "ArchiveQueue";

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueue>::c_identifierType = "tapepool";



#if 0
#if 0
<<<<<<< HEAD
=======

void ContainerTraits<ArchiveQueue_t,ArchiveQueue>::PoppedElementsList::insertBack(PoppedElementsList&& insertedList) {
  for (auto &e: insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

void ContainerTraits<ArchiveQueue_t,ArchiveQueue>::PoppedElementsList::insertBack(PoppedElement&& e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

auto ContainerTraits<ArchiveQueue_t,ArchiveQueue>::PopCriteria::operator-=(const PoppedElementsSummary& pes) -> PopCriteria & {
  bytes -= pes.bytes;
  files -= pes.files;
  return *this;
}

>>>>>>> reportQueues
#endif
#endif

}} // namespace cta::objectstore

