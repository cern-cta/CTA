/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackQueueAlgorithms.hpp"

namespace cta::objectstore {

template<>
const std::string ContainerTraits<RepackQueue, RepackQueuePending>::c_containerTypeName = "RepackQueuePending";

template<>
const std::string ContainerTraits<RepackQueue, RepackQueuePending>::c_identifierType = "uniqueQueue";

template<>
auto ContainerTraits<RepackQueue, RepackQueuePending>::getContainerSummary(Container& cont) -> ContainerSummary {
  ContainerSummary ret;
  ret.requests = cont.getRequestsSummary().requests;
  return ret;
}

}  // namespace cta::objectstore
