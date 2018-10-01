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
#include "common/make_unique.hpp"

namespace cta { namespace objectstore {

// ArchiveQueueFailed full specialisations for ContainerTraits.

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueueFailed>::c_containerTypeName = "ArchiveQueueFailed";

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueueFailed>::c_identifierType = "tapepool";

template<>
void ContainerTraits<ArchiveQueue,ArchiveQueueFailed>::
trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock, const ContainerIdentifier &cId,
  log::LogContext &lc)
{
  trimContainerIfNeeded(cont, JobQueueType::FailedJobs, contLock, cId, lc);
}

}} // namespace cta::objectstore
