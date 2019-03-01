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

namespace cta { namespace objectstore {
  
  template<>
  const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForFailure>::c_containerTypeName = "ArchiveQueueToReportToRepackForFailure";
  
  template<>
  const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForFailure>::c_identifierType = "tapepool";
  
  template<>
  auto ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForFailure>::
  getContainerSummary(Container& cont) -> ContainerSummary {
    ContainerSummary ret;
    ret.JobsSummary::operator=(cont.getJobsSummary());
    return ret;
}
  
}}