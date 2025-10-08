/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <memory>

#include "AgentReferenceInterface.hpp"
#include "ArchiveRequest.hpp"
#include "common/dataStructures/JobQueueType.hpp"

namespace cta::objectstore {

/**
 * This structure holds the necessary data to queue a job taken from the ArchiveRequest that needs to be queued.
 */
struct SorterArchiveJob{
  std::shared_ptr<ArchiveRequest> archiveRequest;
  ArchiveRequest::JobDump jobDump;
  common::dataStructures::ArchiveFile archiveFile;
  AgentReferenceInterface * previousOwner;
  common::dataStructures::MountPolicy mountPolicy;
  common::dataStructures::JobQueueType jobQueueType;
};

/**
 * This structure holds the datas the user have to
 * give to insert an ArchiveRequest without any fetch needed on the Request
 */
struct SorterArchiveRequest{
  std::list<SorterArchiveJob> archiveJobs;
};

} // namespace cta::objectstore
