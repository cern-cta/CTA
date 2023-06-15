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

#include <list>
#include <memory>

#include "AgentReferenceInterface.hpp"
#include "ArchiveRequest.hpp"
#include "common/dataStructures/JobQueueType.hpp"

namespace cta {
namespace objectstore {
/**
 * This structure holds the necessary data to queue a job taken from the ArchiveRequest that needs to be queued.
 */
struct SorterArchiveJob {
  std::shared_ptr<ArchiveRequest> archiveRequest;
  ArchiveRequest::JobDump jobDump;
  common::dataStructures::ArchiveFile archiveFile;
  AgentReferenceInterface* previousOwner;
  common::dataStructures::MountPolicy mountPolicy;
  common::dataStructures::JobQueueType jobQueueType;
};

/**
 * This structure holds the datas the user have to
 * give to insert an ArchiveRequest without any fetch needed on the Request
 */
struct SorterArchiveRequest {
  std::list<SorterArchiveJob> archiveJobs;
};

}  // namespace objectstore
}  // namespace cta
