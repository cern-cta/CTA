/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ArchiveQueueAlgorithms.hpp"
#include "common/Timer.hpp"

namespace cta::objectstore {

// ArchiveQueueFailed full specialisations for ContainerTraits.

template<>
const std::string ContainerTraits<ArchiveQueue, ArchiveQueueFailed>::c_containerTypeName = "ArchiveQueueFailed";

template<>
const std::string ContainerTraits<ArchiveQueue, ArchiveQueueFailed>::c_identifierType = "tapepool";

template<>
auto ContainerTraits<ArchiveQueue, ArchiveQueueFailed>::getContainerSummary(Container& cont) -> ContainerSummary {
  ContainerSummary ret;
  ret.JobsSummary::operator=(cont.getJobsSummary());
  return ret;
}

}  // namespace cta::objectstore
